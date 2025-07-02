package rewind

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

var (
	_ sql.Result = (*nullResult)(nil)
)

type nullResult struct{}

func (*nullResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (*nullResult) RowsAffected() (int64, error) {
	return 0, nil
}

func savepoint(ctx context.Context, execer driver.ExecerContext) (string, error) {
	spn := newSavepointName()
	_, err := execer.ExecContext(ctx, "SAVEPOINT "+spn, nil)
	return spn, err
}

func rollbackToSavepoint(ctx context.Context, execer driver.ExecerContext, spn string) error {
	if spn == "" {
		return nil
	}

	_, err := execer.ExecContext(ctx, "ROLLBACK TO "+spn, nil)
	return err
}
