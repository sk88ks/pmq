package pmq

import (
	"bytes"
	"encoding/gob"
	"time"

	"gopkg.in/redis.v5"
)

// MessageQueue is message queue client
type MessageQueue struct {
	rtxc        chan struct{}
	queueID     string
	redisClient *redis.Client
}

// Message is data of message
type Message struct {
	Body      []byte
	Timestamp int64
}

type Config struct {
	Name      string
	RedisAddr string
	RedisDB   int
}

func (mq *MessageQueue) startTx() {
	<-mq.rtxc
}

func (mq *MessageQueue) endTx() {
	mq.rtxc <- struct{}{}
}

func (mq *MessageQueue) Put(msg Message) error {
	if msg.Timestamp == 0 {
		// Use unix timestamp micro seconds
		msg.Timestamp = time.Now().UnixNano() / 1000
	}

	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(msg)
	if err != nil {
		return err
	}

	res := mq.redisClient.ZAdd(mq.queueID, redis.Z{Score: float64(msg.Timestamp), Member: buf.Bytes()})
	if err := res.Err(); err != nil {
		return err
	}

	return nil
}

func (mq *MessageQueue) Get() (msg Message, err error) {
	mq.startTx()
	defer mq.endTx()

	res := mq.redisClient.ZRevRange(mq.queueID, 0, 0)
	if _err := res.Err(); _err != nil {
		err = _err
		return
	}

	if len(res.Val()) == 0 {
		return
	}

	member := res.Val()[0]
	buf := bytes.NewBuffer([]byte(member))
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&msg)
	if err != nil {
		return
	}

	ic := mq.redisClient.ZRem(mq.queueID, member)
	if _err := ic.Err(); _err != nil {
		err = _err
		return
	}

	return
}

func NewMQ(cfg Config) (*MessageQueue, error) {
	rc := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})

	// Make redis connect sure
	res := rc.Ping()
	if err := res.Err(); err != nil {
		return nil, err
	}

	rtxc := make(chan struct{}, 1)
	rtxc <- struct{}{}

	return &MessageQueue{
		queueID:     cfg.Name,
		rtxc:        rtxc,
		redisClient: rc,
	}, nil
}

// NewMessage creates a new message
func NewMessage(body []byte) Message {
	return Message{
		Body: body,
	}
}
