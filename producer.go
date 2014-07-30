package que

import (
	"encoding/json"
	"sync"

	"github.com/jcoene/env"
	"github.com/jcoene/que/vendor/go-nsq"
)

var producer *nsq.Producer
var pmu sync.Mutex

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
	var err error

	pmu.Lock()

	if producer == nil {
		producer, err = nsq.NewProducer(env.MustGet("NSQD_HOST"), nsq.NewConfig())
	}

	pmu.Unlock()

	return producer, err
}
