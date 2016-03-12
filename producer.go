package que

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"sync"

	"github.com/jcoene/env"
	"github.com/nsqio/go-nsq"
)

var ppool []*nsq.Producer
var pcount int
var ponce sync.Once

func Publish(topic string, v interface{}) (err error) {
	var p *nsq.Producer
	var buf []byte

	if buf, err = json.Marshal(v); err != nil {
		return
	}

	if p, err = getProducer(); err != nil {
		return err
	}

	return p.Publish(topic, buf)
}

func PublishBytes(topic string, buf []byte) error {
	p, err := getProducer()
	if err != nil {
		return err
	}

	return p.Publish(topic, buf)
}

func MultiPublish(topic string, vs ...interface{}) (err error) {
	var p *nsq.Producer
	body := make([][]byte, 0)

	for _, v := range vs {
		var buf []byte
		if buf, err = json.Marshal(v); err != nil {
			return
		}
		body = append(body, buf)
	}

	if p, err = getProducer(); err != nil {
		return err
	}

	return p.MultiPublish(topic, body)
}

func getProducer() (*nsq.Producer, error) {
	// Performed once to set up the slice of nsq.Producer
	ponce.Do(setupProducers)

	// Choose a random connection from the pool
	return ppool[rand.Intn(pcount)], nil
}

// Performed only once the first time a producer is needed. Sets up a slice of
// nsq.Producer which will be used round-robin.
func setupProducers() {
	// Create an empty pool of producers
	ppool = make([]*nsq.Producer, 0)

	// Determine how many producers with the NSQD_POOL environment variable.
	// Defaults to 1
	pcount = 1
	if env.Get("NSQD_POOL") != "" {
		num, err := strconv.Atoi(env.Get("NSQD_POOL"))
		if err == nil && num > 0 {
			pcount = num
		}
	}

	// Get the nsqd host (you can use haproxy if > 1 pool connection)
	host := env.MustGet("NSQD_HOST")

	// Fill the pool with connections
	for i := 0; i < pcount; i++ {
		p, err := nsq.NewProducer(host, nsq.NewConfig())
		if err != nil {
			panic(err)
		}
		ppool = append(ppool, p)
	}
}
