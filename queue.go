package godbqueue

import (
	"time"

	"github.com/go-pg/pg"
)

type Message struct {
	Id        int64    `pg:"id"`
	Topic     string   `pg:"topic"`
	Key       []byte   `pg:"key"`
	Body      []byte   `pg:"body"`
	TimeStamp int64    `pg:"ts"`
	Status    int32    `pg:"status"`
	tableName struct{} `pg:"queue"`
}

type DBQueue interface {
	Enque(topic string, key []byte, message []byte) error
	Deque(topic string) (Message, error)
	Commit(topic string, id int64) error
	Release(topic string, id int64) error
}

func NewDBQueue(dbConnUrl string, appName string) DBQueue {
	db := initDBConnection(dbConnUrl)
	_ = createDB(db)
	return &_DBQueue{db, appName}
}

type _DBQueue struct {
	db      *pg.DB
	appName string
}

func (this *_DBQueue) Enque(topic string, key []byte, body []byte) error {
	msg := Message{Topic: topic, Key: key, Body: body, TimeStamp: time.Now().Unix()}
	_, err := this.db.Model(&msg).Insert()

	if err != nil {
		return err
	}

	return nil
}

func (this *_DBQueue) Deque(topic string) (Message, error) {
	tx, err := this.db.Begin()
	if err != nil {
		return Message{}, err
	}
	defer tx.Rollback()

	query := `SELECT q.*
		FROM "queue" q
		WHERE
			q.status = 0
		ORDER BY ts
		LIMIT 1 FOR UPDATE SKIP LOCKED
   `
	var message Message
	_, err = tx.QueryOne(&message, query)
	if err != nil {
		return Message{}, err
	}

	message.Status = 1
	err = tx.Update(message)
	if err != nil {
		return Message{}, err
	}

	err = tx.Commit()
	if err != nil {
		return Message{}, err
	}
	return message, nil
}

func (this *_DBQueue) Commit(topic string, id int64) error {
	query := `update queue set status=2 where topic = ? and id = ?`
	_, err := this.db.Exec(query, topic, id)
	return err
}

func (this *_DBQueue) Release(topic string, id int64) error {
	query := `update queue set status=0 where topic = ? and id = ?`
	_, err := this.db.Exec(query, topic, id)
	return err
}
