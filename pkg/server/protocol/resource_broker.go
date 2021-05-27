package protocol

import (
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Ripped everything off from:
// https://stackoverflow.com/questions/36417199/how-to-broadcast-message-using-channel

const bufSize = 8

type resourceSet map[chan *discovery.Resource]struct{}

type resourceBroker struct {
	publishCh chan *discovery.Resource
	subCh     chan chan *discovery.Resource
	unsubCh   chan chan *discovery.Resource

	stop chan struct{}
}

func newResourceBroker() *resourceBroker {
	return &resourceBroker{
		stop:      make(chan struct{}),
		publishCh: make(chan *discovery.Resource, 1),
		subCh:     make(chan chan *discovery.Resource),
		unsubCh:   make(chan chan *discovery.Resource),
	}
}

func (b *resourceBroker) Start() {
	subs := make(resourceSet)
	for {
		b.doWorkStep(subs)
	}
}

func (b *resourceBroker) doWorkStep(subs resourceSet) {
	select {
	// Termination condition.
	case <-b.stop:
		// Close out all the message channels.
		for msgCh := range subs {
			close(msgCh)
		}
		return
	// Subscribe.
	case msgCh := <-b.subCh:
		subs[msgCh] = struct{}{}
	// Unsubscribe.
	case msgCh := <-b.unsubCh:
		delete(subs, msgCh)
	// Publish the resource out to subscribers.
	case msg := <-b.publishCh:
		for msgCh := range subs {
			// msgCh is buffered, use non-blocking send to protect the broker:
			select {
			case msgCh <- msg:
			default:
			}
		}
	}
}

func (b *resourceBroker) Stop() {
	close(b.stop)
}

func (b *resourceBroker) Subscribe() chan *discovery.Resource {
	msgCh := make(chan *discovery.Resource, bufSize)
	b.subCh <- msgCh
	return msgCh
}

func (b *resourceBroker) Unsubscribe(msgCh chan *discovery.Resource) {
	b.unsubCh <- msgCh
	close(msgCh)
}

func (b *resourceBroker) Publish(msg *discovery.Resource) {
	b.publishCh <- msg
}
