package que

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jcoene/env"
	"github.com/jcoene/gologger"
)

var log = logger.NewDefaultLogger("worker")

// A Manager coordinates the startup and shutdown of multiple Consumers
type Manager struct {
	ConsumerTimeout time.Duration
	consumers       []*Consumer
	wg              sync.WaitGroup
}

// Create a new Manager
func NewManager() *Manager {
	return &Manager{
		ConsumerTimeout: 1 * time.Minute,
	}
}

// Add a Consumer to this manager
func (m *Manager) AddConsumer(c *Consumer) {
	m.consumers = append(m.consumers, c)
}

// Connects Consumers to NSQ and blocks waiting for a signal to shut down,
// coordinating the shutdown of consumers for a clean exit.
func (m *Manager) Run() (err error) {
	lookupdAddr := env.MustGet("NSQLOOKUPD_HOST")

	for _, c := range m.consumers {
		if err = c.ConnectToNSQLookupd(lookupdAddr); err != nil {
			return
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for {
			sig := <-sig
			log.Info("received signal: %s", sig)
			for _, c := range m.consumers {
				log.Info("stopping consumer %s...", c.Id())
				c.consumer.Stop()
				select {
				case <-c.consumer.StopChan:
					log.Info("stopped consumer %s...", c.Id())
					m.wg.Done()
				case <-time.After(m.ConsumerTimeout):
					log.Warn("timeout while stopping consumer %s (waited %v)", c.Id(), m.ConsumerTimeout)
				}
			}
		}
	}()

	log.Info("waiting on all consumers")
	m.wg.Wait()
	log.Info("stopped.")

	return
}
