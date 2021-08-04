package util

import (
	"fmt"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
)

// Ripped everything off from:
// https://stackoverflow.com/questions/36417199/how-to-broadcast-message-using-channel

type resourceSet map[chan *discovery.Resource]struct{}

type ResourceBroker struct {
	publishCh chan *discovery.Resource
	subCh     chan chan *discovery.Resource
	unsubCh   chan chan *discovery.Resource
	done      chan struct{}
	subs      resourceSet
	running   bool

	log *zap.SugaredLogger
}

func NewResourceBroker(log *zap.SugaredLogger) *ResourceBroker {
	if log == nil {
		panic("invalid logger")
	}

	return &ResourceBroker{
		done:      make(chan struct{}),
		publishCh: make(chan *discovery.Resource, 4),
		subCh:     make(chan chan *discovery.Resource),
		unsubCh:   make(chan chan *discovery.Resource),

		subs:    make(resourceSet),
		log:     log,
		running: false,
	}
}

func (b *ResourceBroker) Start() error {
	if b.running {
		return fmt.Errorf("calling Start() on running broker")
	}
	b.running = true

	go func() {
		for {
			select {
			case <-b.done:
				// Termination condition.
				b.running = false
				b.log.Info("terminating broker")
				return
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
		}
	}()

	return nil
}

func (b *ResourceBroker) Subscribe(msgCh chan *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.subCh <- msgCh
}

func (b *ResourceBroker) Unsubscribe(msgCh chan *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.unsubCh <- msgCh
}

func (b *ResourceBroker) Publish(msg *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.publishCh <- msg
}

func (b *ResourceBroker) PublisherChannel() chan *discovery.Resource {
	return b.publishCh
}

func (b *ResourceBroker) Stop() {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.log.Info("stopping resource broker")
	b.done <- struct{}{}
}
