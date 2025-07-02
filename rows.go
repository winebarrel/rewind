package rewind

import (
	"database/sql/driver"
	"sync"
)

var (
	_ driver.Rows = (*rows)(nil)
)

type rows struct {
	mu      *sync.Mutex
	rawRows driver.Rows
}

func (rs *rows) Columns() []string {
	return rs.rawRows.Columns()
}

func (rs *rows) Close() error {
	defer rs.mu.Unlock()
	return rs.rawRows.Close()
}

func (rs *rows) Next(dest []driver.Value) error {
	return rs.rawRows.Next(dest)
}
