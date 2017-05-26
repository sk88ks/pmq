package mq

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewPriorityMQ(t *testing.T) {
	Convey("Given config", t, func() {
		queueID := "test_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		Convey("When creating new mq with invalid redis addr", func() {
			_, err := NewPriorityMQ(Config{
				Name:      queueID,
				RedisAddr: "invalid_host:5000",
				RedisDB:   redisDB,
			})

			Convey("Then error should be occurred", func() {
				So(err, ShouldNotBeNil)

			})
		})

		Convey("When creating new mq", func() {
			mq, err := NewPriorityMQ(cfg)
			defer mq.Close()

			Convey("Then a new one should be created", func() {
				So(err, ShouldBeNil)
				So(mq, ShouldNotBeNil)

			})
		})
	})
}

func TestMessageQueue_Put(t *testing.T) {
	Convey("Given config", t, func() {
		queueID := "test_put_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		mq, _ := NewPriorityMQ(cfg)
		defer mq.Close()
		defer mq.broker.redisClient.Del(queueID)

		Convey("When putting a new message", func() {
			body := "This is put data for tests"
			err := mq.Put([]byte(body), 0)

			Convey("Then the data should be put into target db", func() {
				So(err, ShouldBeNil)

				res := mq.broker.redisClient.ZRange(queueID, 0, -1)
				vals := res.Val()
				So(len(vals), ShouldEqual, 1)
				So(string(getBody(vals[0])), ShouldEqual, body)

			})
		})
	})
}

func TestMessageQueue_GetConsumer(t *testing.T) {
	Convey("Given MessageQueue instance", t, func() {
		queueID := "test_get_consumer_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		mq, _ := NewPriorityMQ(cfg)
		defer mq.Close()
		defer mq.broker.redisClient.Del(queueID)

		Convey("When get a new consumer", func() {
			c := mq.GetConsumer()

			Convey("Then valid consumer should be created", func() {
				So(c, ShouldNotBeNil)
				So(c.broker, ShouldNotBeNil)

			})
		})
	})
}

func TestConsumer_Get(t *testing.T) {
	Convey("Given created consumer and saved data", t, func() {
		queueID := "test_consumer_get_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		mq, _ := NewPriorityMQ(cfg)
		defer mq.Close()
		defer mq.broker.redisClient.Del(queueID)

		c := mq.GetConsumer()

		for i := 0; i < 100; i++ {
			num := fmt.Sprintf("%03d", i)
			mq.Put([]byte("consumer_get_data_"+num), 0)
		}

		Convey("When get data with consumer", func() {
			messages, err := c.Get(10)

			Convey("Then expectd data should be returnedj", func() {
				So(err, ShouldBeNil)
				So(len(messages), ShouldEqual, 10)

				for i := range messages {
					num := fmt.Sprintf("%03d", i)
					So(string(messages[i].GetBody()), ShouldEqual, "consumer_get_data_"+num)
					So(messages[i].GetPriority(), ShouldEqual, 0)
				}
			})
		})

		Convey("When duplicate get data with consumer", func() {
			messages, err := c.Get(10)

			Convey("Then expectd data should be returnedj", func() {
				So(err, ShouldBeNil)
				So(len(messages), ShouldEqual, 10)

				for i := range messages {
					num := fmt.Sprintf("%03d", i)
					So(string(messages[i].GetBody()), ShouldEqual, "consumer_get_data_"+num)
					So(messages[i].GetPriority(), ShouldEqual, 0)
				}
			})
		})

	})
}

func TestConsumer_Ack(t *testing.T) {
	Convey("Given created consumer and saved data", t, func() {
		queueID := "test_consumer_ack_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		mq, _ := NewPriorityMQ(cfg)
		defer mq.Close()
		defer mq.broker.redisClient.Del(queueID)

		c := mq.GetConsumer()

		for i := 0; i < 100; i++ {
			num := fmt.Sprintf("%03d", i)
			mq.Put([]byte("consumer_ack_data_"+num), 0)
		}

		Convey("When get and ack", func() {
			c.Get(10)
			err := c.Ack()

			Convey("Then acked members should be deleted", func() {
				So(err, ShouldBeNil)
				res := mq.broker.redisClient.ZRange(queueID, 0, 99)
				So(len(res.Val()), ShouldEqual, 90)
				for i := 0; i < 90; i++ {
					num := fmt.Sprintf("%03d", i+10)
					So(string(getBody(res.Val()[i])), ShouldEqual, "consumer_ack_data_"+num)
				}

				So(len(c.notAckedMessages), ShouldEqual, 0)

			})
		})
	})
}

func TestConsumer_ReQueue(t *testing.T) {
	Convey("Given created consumer and saved data", t, func() {
		queueID := "test_consumer_requeue_mq"
		redisAddr := "localhost:6379"
		redisDB := 1
		cfg := Config{
			Name:      queueID,
			RedisAddr: redisAddr,
			RedisDB:   redisDB,
		}

		mq, _ := NewPriorityMQ(cfg)
		defer mq.Close()
		defer mq.broker.redisClient.Del(queueID)

		c := mq.GetConsumer()

		for i := 0; i < 100; i++ {
			num := fmt.Sprintf("%03d", i)
			mq.Put([]byte("consumer_ack_data_"+num), 0)
		}

		Convey("When get and ack", func() {
			c.Get(10)
			err := c.ReQueue()

			Convey("Then acked members should be deleted", func() {
				So(err, ShouldBeNil)
				res := mq.broker.redisClient.ZRange(queueID, 0, 99)
				So(len(res.Val()), ShouldEqual, 100)
				for i := 90; i < 100; i++ {
					num := fmt.Sprintf("%03d", i-90)
					So(string(getBody(res.Val()[i])), ShouldEqual, "consumer_ack_data_"+num)
				}

				So(len(c.notAckedMessages), ShouldEqual, 0)

			})
		})
	})
}
