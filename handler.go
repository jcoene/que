package que

import (
	"github.com/nsqio/go-nsq"
)

// A HandlerGenerator is a function that takes a Payload and returns a Handler and error (or nil if success)
type HandlerGenerator func(*Payload) (Handler, error)

// A Handler is a type that responds to the Id and Perform methods.
type Handler interface {
	Id() string
	Perform() error
}

// A Payload contains the Topic, Channel and nsq Message.
type Payload struct {
	Topic   string
	Channel string
	Message *nsq.Message
}
