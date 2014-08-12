package que

import (
	"fmt"
	"time"

	"github.com/jcoene/que/vendor/go-nsq"
	"github.com/jcoene/statsd-client"
)

type wrapper struct {
	topic     string
	channel   string
	generator HandlerGenerator
}

func (w *wrapper) HandleMessage(message *nsq.Message) (err error) {
	payload := &Payload{w.topic, w.channel, message}

	t := time.Now()

	handler, err := w.generator(payload)
	if err != nil {
		Logger.Error("%s: unable to generate handler for message %v: %s", w.topic, message.ID, err)
		return
	}

	Logger.Info("%s %s: starting...", w.topic, handler.Id())
	if err = handler.Perform(); err != nil {
		Logger.Error("%s %s: %s in %v", w.topic, handler.Id(), err, time.Since(t))
		statsd.Count(fmt.Sprintf("worker.%s.error.count", w.topic), 1)
		statsd.MeasureDur(fmt.Sprintf("worker.%s.error.runtime", w.topic), time.Since(t))
	} else {
		Logger.Info("%s %s: completed in %v", w.topic, handler.Id(), time.Since(t))
		statsd.Count(fmt.Sprintf("worker.%s.success.count", w.topic), 1)
		statsd.MeasureDur(fmt.Sprintf("worker.%s.success.runtime", w.topic), time.Since(t))
	}

	return
}
