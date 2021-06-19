package godbqueue

import (
	"fmt"

	"github.com/go-pg/pg"
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
		CREATE TABLE IF NOT EXISTS queue {
			id BIGSERIAL,
			topic varchar(100)
			key BINARY,
			body BINARY,
			ts BIGINT,
			status INT,
			PRIMARY KEY(id)
		}
	`
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}
