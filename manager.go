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

var Logger = logger.NewDefaultLogger("worker")

// Coworker defines an interface that will be started and stopped alongside consumers.
type Coworker interface {
	Start()
	Stop()
	Wait()
}

// A Manager coordinates the startup and shutdown of multiple Consumers
type Manager struct {
	ConsumerTimeout time.Duration
	consumers       []*Consumer
	coworker        Coworker
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

// Adds a Coworker to this manager
func (m *Manager) AddCoworker(c Coworker) {
	m.coworker = c
}

// Connects Consumers to NSQ and blocks waiting for a signal to shut down,
// coordinating the shutdown of consumers for a clean exit.
func (m *Manager) Run() (err error) {
	lookupdAddr := env.MustGet("NSQLOOKUPD_HOST")

	// Create consumers.
	for _, c := range m.consumers {
		if err = c.ConnectToNSQLookupd(lookupdAddr); err != nil {
			return
		}
		m.wg.Add(1)
	}

	// Start the Coworker if it exists.
	if m.coworker != nil {
		m.coworker.Start()
		m.wg.Add(1)
	}

	// Setup signal handling
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal in a goroutine
	go func() {
		for {
			// Wait for signal
			sig := <-sig
			Logger.Info("received signal: %s", sig)

			// Stop the coworker
			if m.coworker != nil {
				Logger.Info("stopping coworker...")
				m.coworker.Stop()

				// Wait for it to stop in a new goroutine
				go func() {
					m.coworker.Wait()
					Logger.Info("stopped coworker")
					m.wg.Done()
				}()
			}

			// Iterate through consumers
			for _, c := range m.consumers {
				// Stop the consumer
				Logger.Info("stopping consumer %s...", c.Id())
				c.consumer.Stop()

				// Wait for it to stop in a new goroutine
				go func(c *Consumer) {
					select {
					case <-c.consumer.StopChan:
						Logger.Info("stopped consumer %s.", c.Id())
						m.wg.Done()
					case <-time.After(m.ConsumerTimeout):
						Logger.Warn("timeout while stopping consumer %s (waited %v)", c.Id(), m.ConsumerTimeout)
					}
				}(c)
			}
		}
	}()

	// Wait for everything to clean up.
	Logger.Info("waiting on all consumers")
	m.wg.Wait()
	Logger.Info("stopped.")

	return
}
