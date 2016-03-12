package que

import (
	"os"
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

func TestGetConcurrency(t *testing.T) {
	// Not set returns default
	if v := getConcurrency("unknown_thing", 5); v != 5 {
		t.Errorf("expected default to be 5, got %d", v)
	}

	// Set and valid returns value
	os.Setenv("TOPIC_CONCURRENCY_USER_PROFILE", "50")
	if v := getConcurrency("user_profile", 5); v != 50 {
		t.Errorf("expected valid to be 50, got %d", v)
	}

	// Set and invalid returns default
	os.Setenv("TOPIC_CONCURRENCY_USER_PROFILE", "doodads")
	if v := getConcurrency("user_profile", 5); v != 5 {
		t.Errorf("expected invalid to default to 5, got %d", v)
	}
}
