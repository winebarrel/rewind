package rewind

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func Open(driverName string, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, "")

	if err != nil {
		return nil, err
	}

	drv := db.Driver()

	cnct := &connector{
		driver: drv,
		connect: func(ctx context.Context) (driver.Conn, error) {
			return drv.Open(dsn)
		},
	}

	return sql.OpenDB(cnct), nil
}

func OpenDB(src driver.Connector) *sql.DB {
	cnct := &connector{
		driver: src.Driver(),
		connect: func(ctx context.Context) (driver.Conn, error) {
			return src.Connect(ctx)
		},
	}

	return sql.OpenDB(cnct)
}
