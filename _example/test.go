package main

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/samber/lo"
	"github.com/winebarrel/rewind"
)

func main() {
	db := lo.Must(rewind.Open("pgx", "postgres://postgres@localhost:15432/postgres"))
	defer func() { lo.Must0(db.Close()) }()

	// setup DB
	exec(db, "CREATE TABLE books (id INTEGER PRIMARY KEY, title VARCHAR(255))")

	// with Tx
	tx := dbBegin(db)

	exec(tx, "INSERT INTO books VALUES (1, 'Fourth Wing'), (2, 'Onyx Storm'), (3, 'Iron Flame')")
	printBooks(tx)
	txCommit(tx) // 3 records

	tx = dbBegin(db)
	exec(tx, "INSERT INTO books VALUES (4, 'Quicksilver'), (5, 'Shield of Sparrows'), (6, 'A Court of Thorns and Roses')")
	printBooks(tx) // 6 records

	txRollback(tx)
	printBooks(db) // 6 -> 3 records

	// raw COMMIT/ROLLBACK
	exec(db, "INSERT INTO books VALUES (4, 'Quicksilver'), (5, 'Shield of Sparrows'), (6, 'A Court of Thorns and Roses')")
	exec(db, "COMMIT") // 6 records

	exec(db, "INSERT INTO books VALUES (7, 'Dungeon Crawler Carl'), (8, 'A Court of Mist and Fury'), (9, 'A Court of Wings and Ruin')")
	printBooks(db) // 9 records

	exec(db, "ROLLBACK")
	printBooks(db) // 9 -> 6 records

	dbClose(db) // rewind DB
}

func printBooks(db interface {
	Query(string, ...any) (*sql.Rows, error)
}) {
	rows := lo.Must(db.Query("SELECT id, title FROM books"))
	fmt.Println("---")
	for rows.Next() {
		var (
			id    int
			title string
		)
		lo.Must0(rows.Scan(&id, &title))
		fmt.Println(id, title)
	}
	fmt.Println("---")
}

func exec(db interface {
	Exec(string, ...any) (sql.Result, error)
}, sql string) {
	fmt.Printf("exec: %s\n", sql)
	lo.Must(db.Exec(sql))
}

func dbBegin(db *sql.DB) *sql.Tx {
	fmt.Println("db.Begin()")
	return lo.Must(db.Begin())
}

func dbClose(db *sql.DB) {
	fmt.Println("db.Close()")
	lo.Must0(db.Close())
}

func txCommit(tx *sql.Tx) {
	fmt.Println("tx.Commit()")
	lo.Must0(tx.Commit())
}

func txRollback(tx *sql.Tx) {
	fmt.Println("tx.Rollback()")
	lo.Must0(tx.Rollback())
}
