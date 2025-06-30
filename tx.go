package rewind

import (
	"context"
	"database/sql/driver"
)

var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	conn *conn
}

func (t *tx) Commit() error {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	spn, err := savepoint(context.Background(), t.conn)

	if err != nil {
		return err
	}

	t.conn.savepointName = spn
	return nil
}

func (t *tx) Rollback() error {
	return rollbackToSavepoint(context.Background(), t.conn, t.conn.savepointName)
}
