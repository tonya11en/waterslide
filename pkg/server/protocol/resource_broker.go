package protocol

import (
	"fmt"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
)

// Ripped everything off from:
// https://stackoverflow.com/questions/36417199/how-to-broadcast-message-using-channel

type resourceSet map[chan *discovery.Resource]struct{}

type resourceBroker struct {
	publishCh chan *discovery.Resource
	subCh     chan chan *discovery.Resource
	unsubCh   chan chan *discovery.Resource
	done      chan struct{}
	subs      resourceSet
	running   bool

	log *zap.SugaredLogger
}

func newResourceBroker(log *zap.SugaredLogger) (*resourceBroker, error) {
	if log == nil {
		l, err := zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
		log = l.Sugar()
	}

	return &resourceBroker{
		done:      make(chan struct{}),
		publishCh: make(chan *discovery.Resource, 4),
		subCh:     make(chan chan *discovery.Resource),
		unsubCh:   make(chan chan *discovery.Resource),

		subs:    make(resourceSet),
		log:     log,
		running: false,
	}, nil
}

func (b *resourceBroker) Start() error {
	if b.running {
		return fmt.Errorf("calling Start() on running broker")
	}
	b.running = true

	go func() {
		for {
			if b.doWorkStep() {
				return
			}
		}
	}()

	return nil
}

// Returns true if done.
func (b *resourceBroker) doWorkStep() bool {
	select {
	// Termination condition.
	case <-b.done:
		b.log.Info("terminating broker")
		return true
	// Subscribe.
	case msgCh := <-b.subCh:
		b.subs[msgCh] = struct{}{}
	// Unsubscribe.
	case msgCh := <-b.unsubCh:
		delete(b.subs, msgCh)
	// Publish the resource out to subscribers.
	case msg := <-b.publishCh:
		for msgCh := range b.subs {
			// TODO: Could potentially hang here.
			msgCh <- msg
		}
	}
	return false
}

func (b *resourceBroker) Subscribe(msgCh chan *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.subCh <- msgCh
}

func (b *resourceBroker) Unsubscribe(msgCh chan *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.unsubCh <- msgCh
}

func (b *resourceBroker) Publish(msg *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.publishCh <- msg
}

func (b *resourceBroker) Stop() {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.log.Info("stopping resource broker")
	b.done <- struct{}{}
	b.running = false
}
