package rewind

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
)

var (
	_ driver.Connector = (*connector)(nil)
	_ io.Closer        = (*connector)(nil)
)

type connector struct {
	mu      sync.Mutex
	driver  driver.Driver
	connect func(context.Context) (driver.Conn, error)
	conn    *conn
}

func (cnct *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cnct.mu.Lock()
	defer cnct.mu.Unlock()

	if cnct.conn == nil {
		impl, err := cnct.connect(ctx)

		if err != nil {
			return nil, err
		}

		cn := &conn{impl: impl}
		_, err = cn.execContext0(ctx, "BEGIN", nil)

		if err != nil {
			return nil, err
		}

		cnct.conn = cn
	}

	return cnct.conn, nil
}

func (cnct *connector) Driver() driver.Driver {
	return cnct.driver
}

func (cnct *connector) Close() error {
	if cnct.conn == nil {
		return nil
	}

	_, err := cnct.conn.execContext0(context.Background(), "ROLLBACK", nil)

	if err != nil {
		return err
	}

	return cnct.conn.impl.Close()
}
