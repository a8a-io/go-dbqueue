package godbqueue

import (
	"fmt"

	"github.com/go-pg/pg/v10"
)

func initDBConnection(connUrl string) *pg.DB {
	opt, err := pg.ParseURL(connUrl)
	if err != nil {
		panic(err)
	}
	db := pg.Connect(opt)
	if db == nil {
		panic("Not connected to db")
	}
	fmt.Println("Connected to db: ", db.Options().Database)
	return db
}

func createDB(db *pg.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS messages (
			id BIGSERIAL PRIMARY KEY,
			topic VARCHAR(100),
			key BYTEA,
			body BYTEA,
			time_stamp BIGINT,
			status INT not null default 0
		)
	`
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}
