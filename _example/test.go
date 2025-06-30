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
	lo.Must(db.Exec("CREATE TABLE books (id INTEGER PRIMARY KEY, title VARCHAR(255))"))
	fmt.Println("tx.Begin()")
	tx := lo.Must(db.Begin())
	lo.Must(tx.Exec("INSERT INTO books VALUES (1, 'Fourth Wing'), (2, 'Onyx Storm'), (3, 'Iron Flame')"))
	printBooks(tx)
	fmt.Println("tx.Commit()")
	lo.Must0(tx.Commit())
	fmt.Println("tx.Begin()")
	tx = lo.Must(db.Begin())
	lo.Must(tx.Exec("INSERT INTO books VALUES (4, 'Quicksilver'), (5, 'Shield of Sparrows'), (6, 'A Court of Thorns and Roses')"))
	printBooks(tx)
	fmt.Println("tx.Rollback()")
	lo.Must0(tx.Rollback())
	printBooks(db)
	lo.Must(db.Exec("INSERT INTO books VALUES (4, 'Quicksilver'), (5, 'Shield of Sparrows'), (6, 'A Court of Thorns and Roses')"))
	lo.Must(db.Exec("COMMIT"))
	lo.Must(db.Exec("INSERT INTO books VALUES (7, 'Dungeon Crawler Carl'), (8, 'A Court of Mist and Fury'), (9, 'A Court of Wings and Ruin')"))
	printBooks(db)
	lo.Must(db.Exec("ROLLBACK"))
	printBooks(db)
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
