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
	reBegin    = regexp.MustCompile(`(?i)^\s*BEGIN\s*;?\s*$`)
	reCommit   = regexp.MustCompile(`(?i)^\s*COMMIT\s*;?\s*$`)
	reRollback = regexp.MustCompile(`(?i)^\s*ROLLBACK\s*;?\s*$`)
)

type conn struct {
	mu            sync.Mutex
	rawConn       driver.Conn
	savepointName string
}

func (cn *conn) Prepare(query string) (driver.Stmt, error) {
	return cn.PrepareContext(context.Background(), query)
}

func (cn *conn) Close() error {
	cn.mu.TryLock()
	cn.mu.Unlock()
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
	queryer, ok := cn.rawConn.(driver.QueryerContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	cn.mu.Lock()
	rs, err := queryer.QueryContext(ctx, query, args)

	if err != nil {
		cn.mu.Unlock()
		return nil, err
	}

	return &rows{mu: &cn.mu, rawRows: rs}, err
}

func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if reBegin.MatchString(query) || reCommit.MatchString(query) {
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
		return cn.execContext0(ctx, query, args)
	}
}

func (cn *conn) execContext0(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := cn.rawConn.(driver.ExecerContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	return execer.ExecContext(ctx, query, args)
}

func (cn *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	preparer, ok := cn.rawConn.(driver.ConnPrepareContext)

	if !ok {
		return nil, driver.ErrSkip
	}

	return preparer.PrepareContext(ctx, query)
}

func (cn *conn) Ping(ctx context.Context) error {
	pinger, ok := cn.rawConn.(driver.Pinger)

	if !ok {
		return driver.ErrSkip
	}

	return pinger.Ping(ctx)
}
