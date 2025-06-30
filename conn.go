package rewind

import (
	"context"
	"database/sql/driver"
	"regexp"
	"sync"
)

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
)

var (
	reCommit   = regexp.MustCompile(`(?i)^\s*COMMIT\s*;?\s*$`)
	reRollback = regexp.MustCompile(`(?i)^\s*ROLLBACK\s*;?\s*$`)
)

type conn struct {
	mu            sync.Mutex
	impl          driver.Conn
	savepointName string
}

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	return cn.PrepareContext(context.Background(), query)
}

func (cn *conn) Close() error {
	// nothing to do
	return nil
}

func (cn *conn) Begin() (driver.Tx, error) {
	return cn.BeginTx(context.Background(), driver.TxOptions{})
}

func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	spn, err := savepoint(ctx, cn)

	if err != nil {
		return nil, err
	}

	cn.savepointName = spn
	return &tx{conn: cn}, nil
}

func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := cn.impl.(driver.QueryerContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	return queryer.QueryContext(ctx, query, args)
}

func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if reCommit.MatchString(query) {
		cn.mu.Lock()
		defer cn.mu.Unlock()
		spn, err := savepoint(ctx, cn)

		if err != nil {
			return nil, err
		}

		cn.savepointName = spn
		return &nullResult{}, nil
	} else if reRollback.MatchString(query) {
		return &nullResult{}, rollbackToSavepoint(ctx, cn, cn.savepointName)
	} else {
		execer, ok := cn.impl.(driver.ExecerContext)

		if !ok {
			return nil, driver.ErrSkip
		}

		return execer.ExecContext(ctx, query, args)
	}
}

func (cn *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	preparer, ok := cn.impl.(driver.ConnPrepareContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	return preparer.PrepareContext(ctx, query)
}

func (cn *conn) Ping(ctx context.Context) error {
	pinger, ok := cn.impl.(driver.Pinger)

	if !ok {
		return driver.ErrSkip
	}

	return pinger.Ping(ctx)
}
