package que

import (
	"strconv"
	"strings"

	"github.com/jcoene/env"
	"github.com/jcoene/que/vendor/go-nsq"
)

// A Consumer receives messages on a given topic and channel and
type Consumer struct {
	Topic    string
	Channel  string
	Count    int
	Config   *nsq.Config
	wrapper  *wrapper
	consumer *nsq.Consumer
}

// Creates a new Consumer with a given topic, channel, concurrency and handler generator. The
// nsq max_in_flight setting defaults to the given concurrency value (you can change it later).
func NewConsumer(topic string, channel string, defaultCount int, generator HandlerGenerator) (c *Consumer) {
	config := nsq.NewConfig()
	count := getConcurrency(topic, defaultCount)

	config.Set("max_in_flight", count)

	return &Consumer{
		Topic:   topic,
		Channel: channel,
		Count:   count,
		Config:  config,
		wrapper: &wrapper{topic, channel, generator},
	}
}

// Identifies the consumer using the topic name
func (c *Consumer) Id() string {
	return c.Topic
}

// Creates the nsq consumer, adds handler wrapper and connects to nsq to start processing messages.
func (c *Consumer) ConnectToNSQLookupd(lookupdAddr string) (err error) {
	if c.consumer, err = nsq.NewConsumer(c.Topic, c.Channel, c.Config); err != nil {
		return
	}

	c.consumer.AddConcurrentHandlers(c.wrapper, c.Count)

	return c.consumer.ConnectToNSQLookupd(lookupdAddr)
}

func getConcurrency(key string, fallback int) int {
	if s := env.Get("TOPIC_CONCURRENCY_" + strings.ToUpper(key)); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}

	return fallback
}
