package mq

import (
	"errors"
	"strconv"
	"time"

	"gopkg.in/redis.v5"
)

const (
	// Prefix is unixtime micro
	prefixLength = 16
)

type broker struct {
	id           string
	redisClient  *redis.Client
	consumerAckC chan *consumerAck
	done         chan struct{}
}

// MessageQueue is message queue client
type MessageQueue struct {
	broker *broker
}

type Config struct {
	Name      string
	RedisAddr string
	RedisDB   int
}

type Consumer struct {
	broker           *broker
	notAckedMessages PrioritizedMessages
}

type consumerAck struct {
	members []string
	errC    chan error
}

// PrioritizedMessage is message data with priority
type PrioritizedMessage struct {
	member   string
	priority float64
}

type PrioritizedMessages []PrioritizedMessage

func getMember(body []byte) string {
	// Added prefix to let redis sort them lexicographically
	prefix := strconv.FormatInt(time.Now().UnixNano()/1000, 10)
	return prefix + string(body)
}

func getBody(member string) []byte {
	return []byte(member[prefixLength:])
}

func (pm PrioritizedMessages) getMembers() []string {
	members := make([]string, 0, len(pm))
	for i := range pm {
		members = append(members, pm[i].member)
	}

	return members
}

func (pm PrioritizedMessages) refreshMembers() {
	for i := range pm {
		pm[i].member = getMember(getBody(pm[i].member))
	}
}

func (pm *PrioritizedMessage) convertToZ() redis.Z {
	return redis.Z{
		Member: pm.member,
		Score:  -pm.priority,
	}
}

// GetBody gets message body
func (pm *PrioritizedMessage) GetBody() []byte {
	return getBody(pm.member)
}

// GetPriority gets messages priority
func (pm *PrioritizedMessage) GetPriority() float64 {
	return pm.priority
}

// AddPriority adds additional priority
func (pm *PrioritizedMessage) AddPriority(p float64) {
	pm.priority += p
}

func (b *broker) startAckListner() {
	go func() {
		defer close(b.done)

		for ca := range b.consumerAckC {
			if ca.errC == nil {
				ca.errC = make(chan error, 1)
			}

			var err error
			for i := range ca.members {
				ic := b.redisClient.ZRem(b.id, ca.members[i])
				if _err := ic.Err(); _err != nil {
					err = _err
					break
				}
			}

			ca.errC <- err
			close(ca.errC)
		}
	}()
}

func (b *broker) put(messages ...PrioritizedMessage) error {

	var data []redis.Z
	for i := range messages {
		data = append(data, messages[i].convertToZ())
	}

	res := b.redisClient.ZAdd(b.id, data...)
	if err := res.Err(); err != nil {
		return err
	}

	return nil
}

func (b *broker) get(num int64) (messages PrioritizedMessages, err error) {
	res := b.redisClient.ZRangeWithScores(b.id, 0, num-1)
	if _err := res.Err(); _err != nil {
		err = _err
		return
	}

	if len(res.Val()) == 0 {
		return
	}

	for i := range res.Val() {
		member, ok := res.Val()[i].Member.(string)
		if !ok {
			err = errors.New("Member has invalid type data")
			return
		}
		messages = append(messages, PrioritizedMessage{
			member:   member,
			priority: -res.Val()[i].Score,
		})
	}

	return
}

// NewPriorityMQ creates a new message queue
func NewPriorityMQ(cfg Config) (*MessageQueue, error) {
	rc := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		DB:   cfg.RedisDB,
	})

	// Make redis connect sure
	res := rc.Ping()
	if err := res.Err(); err != nil {
		return nil, err
	}

	broker := &broker{
		id:           cfg.Name,
		redisClient:  rc,
		consumerAckC: make(chan *consumerAck),
		done:         make(chan struct{}),
	}
	broker.startAckListner()

	return &MessageQueue{
		broker: broker,
	}, nil
}

// Put puts message and priority
func (mq *MessageQueue) Put(body []byte, priority float64) error {
	return mq.broker.put(PrioritizedMessage{member: getMember(body), priority: priority})
}

// Close close message queue
func (mq *MessageQueue) Close() {
	close(mq.broker.consumerAckC)
	<-mq.broker.done
}

func (mq *MessageQueue) GetConsumer() *Consumer {
	c := &Consumer{
		broker: mq.broker,
	}

	return c
}

// Get gets bodies and priorities
func (c *Consumer) Get(num int64) (messages PrioritizedMessages, err error) {
	if len(c.notAckedMessages) != 0 {
		messages = c.notAckedMessages
		return
	}

	messages, err = c.broker.get(num)
	if err != nil {
		return
	}

	c.notAckedMessages = messages

	return
}

func (c *Consumer) Ack() error {
	if len(c.notAckedMessages) == 0 {
		return nil
	}

	errC := make(chan error)
	c.broker.consumerAckC <- &consumerAck{
		members: c.notAckedMessages.getMembers(),
		errC:    errC,
	}

	for err := range errC {
		if err != nil {
			return err
		}
	}

	c.notAckedMessages = nil

	return nil
}

// ReQueue queue members again
func (c *Consumer) ReQueue() error {
	if len(c.notAckedMessages) == 0 {
		return nil
	}

	notAckedMessages := c.notAckedMessages

	// Ack at first
	err := c.Ack()
	if err != nil {
		return err
	}

	notAckedMessages.refreshMembers()

	err = c.broker.put(notAckedMessages...)
	if err != nil {
		return err
	}

	return nil
}
