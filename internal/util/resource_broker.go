package util

import (
	"context"
	"fmt"
	"sync"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
)

// Ripped everything off from:
// https://stackoverflow.com/questions/36417199/how-to-broadcast-message-using-channel

type ResourceBroker struct {
	publishCh chan *discovery.Resource
	subCh     chan chan *discovery.Resource
	unsubCh   chan chan *discovery.Resource
	ctx       context.Context
	stop      chan struct{}
	subs      sync.Map
	running   bool

	log *zap.SugaredLogger
}

func NewResourceBroker(ctx context.Context, log *zap.SugaredLogger) *ResourceBroker {
	// TODO: If the resource broker has no subscriptions for some amount of time, have it
	// automatically destroy itself. This will prevent floods of bogus resource names that can cause
	// some resource broker leak.
	if log == nil {
		panic("invalid logger")
	}

	return &ResourceBroker{
		publishCh: make(chan *discovery.Resource),
		subCh:     make(chan chan *discovery.Resource),
		unsubCh:   make(chan chan *discovery.Resource),
		ctx:       ctx,
		stop:      make(chan struct{}),
		log:       log,
		running:   false,
	}
}

func (b *ResourceBroker) Start() error {
	if b.running {
		return fmt.Errorf("calling Start() on running broker")
	}
	b.running = true

	go b.work()

	return nil
}

func (b *ResourceBroker) work() {
	for {
		select {
		case <-b.stop:
			b.running = false
			b.log.Info("stopping resource broker")
			return

		case <-b.ctx.Done():
			// Termination condition.
			b.running = false
			b.log.Info("terminating resource broker")
			return

		// Subscribe.
		case msgCh := <-b.subCh:
			b.subs.Store(msgCh, struct{}{})

		// Unsubscribe.
		case msgCh := <-b.unsubCh:
			b.subs.Delete(msgCh)

		// Publish the resource out to subscribers.
		case msg := <-b.publishCh:
			go func() {
				b.subs.Range(func(msgCh interface{}, _ interface{}) bool {
					select {
					case msgCh.(chan *discovery.Resource) <- msg:
					case <-b.ctx.Done():
						// Halts iteration.
						return false
					}
					return true
				})
			}()
		}
	}
}

func (b *ResourceBroker) Subscribe(ctx context.Context, msgCh chan *discovery.Resource) {
	if !b.running {
		b.log.Fatal("broker is not started")
	}

	b.subCh <- msgCh

	go func() {
		// Cleanup the subscription when done.
		<-ctx.Done()
		b.Unsubscribe(msgCh)
	}()
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

	b.stop <- struct{}{}
}

type BrokerMap struct {
	brokers sync.Map
	log     *zap.SugaredLogger
}

// Looks up a resource broker by name or creates it if it doesn't exist and returns it. Will return
// true if a resource broker was loaded and not created. False is returned if a resource broker is newly created.
//
// Note that a newly created resource broker is only created and not started.
func NewBrokerMap(log *zap.SugaredLogger) *BrokerMap {
	return &BrokerMap{
		log: log,
	}
}

func (bm *BrokerMap) LoadOrStore(ctx context.Context, resourceName string) (*ResourceBroker, bool) {
	b, loaded := bm.brokers.LoadOrStore(resourceName, NewResourceBroker(ctx, bm.log))
	return b.(*ResourceBroker), loaded
}

func (bm *BrokerMap) Range(f func(resourceName string, broker *ResourceBroker) bool) {
	bm.brokers.Range(func(key, val interface{}) bool {
		return f(key.(string), val.(*ResourceBroker))
	})
}
