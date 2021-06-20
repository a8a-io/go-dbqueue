package godbqueue

import (
	"crypto/md5"
	"fmt"
	"hash"
	"time"

	"github.com/go-pg/pg/v10"
)

type Message struct {
	Id        int64  `pg:"id,pk"`
	Topic     string `pg:"topic,notnull"`
	Key       []byte `pg:"key"`
	Body      []byte `pg:"body"`
	TimeStamp int64  `pg:"time_stamp,notnull"`
	Status    int32  `pg:"status,default:0,notnull,use_zero"`
}

type DBQueue interface {
	Enque(topic string, key []byte, message []byte) error
	Deque(topic string) (*Message, error)
	Commit(topic string, id int64) error
	Release(topic string, id int64) error
}

func NewDBQueue(dbConnUrl string, appName string) (DBQueue, error) {
	db := initDBConnection(dbConnUrl)
	err := createDB(db)
	if err != nil {
		return nil, err
	}

	return &_DBQueue{md5.New(), db, appName}, nil
}

// func NewDBQueueConn(db *pg.DB, appName string) DBQueue {
// 	_ = createDB(db)
// 	return &_DBQueue{db, appName}
// }

type _DBQueue struct {
	h       hash.Hash
	db      *pg.DB
	appName string
}

func (this *_DBQueue) Enque(topic string, key []byte, body []byte) error {
	fmt.Println("enque")
	if key == nil {
		key = this.h.Sum(body)
	}
	msg := Message{Topic: topic, Key: key, Body: body, TimeStamp: time.Now().Unix(), Status: 0}
	fmt.Println("Calling Save")
	res, err := this.db.Model(&msg).Insert()
	fmt.Println(res.RowsAffected())
	if err != nil {
		return err
	}

	return nil
}

func (this *_DBQueue) Deque(topic string) (*Message, error) {
	tx, err := this.db.Begin()
	if err != nil {
		return &Message{}, err
	}
	defer tx.Rollback()

	query := `SELECT m.*
		FROM "messages" m
		WHERE
			m.status = 0
		ORDER BY time_stamp, id
		LIMIT 1 FOR UPDATE SKIP LOCKED
   `
	var message Message
	_, err = tx.QueryOne(&message, query)
	if err != nil {
		return &Message{}, err
	}

	message.Status = 1
	_, err = tx.Model(&message).Column("status").WherePK().Update()
	if err != nil {
		return &Message{}, err
	}

	err = tx.Commit()
	if err != nil {
		return &Message{}, err
	}
	return &message, nil
}

func (this *_DBQueue) Commit(topic string, id int64) error {
	query := `update messages set status=2 where topic = ? and id = ?`
	_, err := this.db.Exec(query, topic, id)
	return err
}

func (this *_DBQueue) Release(topic string, id int64) error {
	query := `update messages set status=0 where topic = ? and id = ?`
	_, err := this.db.Exec(query, topic, id)
	return err
}
