package que

import (
	"testing"
)

type han struct{}

func (h *han) Id() string     { return "1" }
func (h *han) Perform() error { return nil }

func genHan(p *Payload) (Handler, error) { return &han{}, nil }

func TestNewConsumer(t *testing.T) {
	c := NewConsumer("top", "cha", 10, genHan)

	if c.Topic != "top" {
		t.Errorf("expected topic %s, got %s", "top", c.Topic)
	}

	if c.Channel != "cha" {
		t.Errorf("expected channel %s, got %s", "cha", c.Channel)
	}
}
