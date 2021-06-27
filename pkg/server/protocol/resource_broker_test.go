package protocol

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

func TestSubscribe(t *testing.T) {
	broker, err := newResourceBroker(nil)
	assert.Nil(t, err)
	assert.Nil(t, broker.Start())
	defer broker.Stop()

	s1 := make(chan *discovery.Resource, 4)
	s2 := make(chan *discovery.Resource, 4)
	s3 := make(chan *discovery.Resource, 4)
	broker.Subscribe(s1)
	broker.Subscribe(s2)
	broker.Subscribe(s3)

	rfoo := &discovery.Resource{
		Name: "foo",
	}
	rbar := &discovery.Resource{
		Name: "bar",
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < 1000; i++ {
			broker.Publish(rfoo)
			broker.Publish(rbar)
		}
		wg.Done()
	}()

	fooCount := []int{0, 0, 0}
	barCount := []int{0, 0, 0}

	go func() {
		for i := 0; i < 2000; i++ {
			s1r := <-s1
			s2r := <-s2
			s3r := <-s3

			if s1r.GetName() == "foo" {
				fooCount[0]++
			} else {
				barCount[0]++
			}
			if s2r.GetName() == "foo" {
				fooCount[1]++
			} else {
				barCount[1]++
			}
			if s3r.GetName() == "foo" {
				fooCount[2]++
			} else {
				barCount[2]++
			}
		}
		wg.Done()
	}()
	wg.Wait()

	for i := 0; i < 3; i++ {
		assert.Equal(t, 1000, fooCount[i])
		assert.Equal(t, 1000, barCount[i])
	}
}

func TestUnsubscribe(t *testing.T) {
	broker, err := newResourceBroker(nil)
	assert.Nil(t, err)
	assert.Nil(t, broker.Start())
	defer broker.Stop()

	s1 := make(chan *discovery.Resource, 4)
	s2 := make(chan *discovery.Resource, 4)
	s3 := make(chan *discovery.Resource, 4)
	broker.Subscribe(s1)
	broker.Subscribe(s2)
	broker.Subscribe(s3)

	rfoo := &discovery.Resource{
		Name: "foo",
	}
	broker.Publish(rfoo)

	_, ok := <-s1
	assert.Equal(t, len(s1), 0)
	assert.True(t, ok)

	_, ok = <-s2
	assert.Equal(t, len(s2), 0)
	assert.True(t, ok)

	_, ok = <-s3
	assert.Equal(t, len(s3), 0)
	assert.True(t, ok)

	broker.Unsubscribe(s2)
	broker.Publish(rfoo)

	_, ok = <-s1
	assert.Equal(t, len(s1), 0)
	assert.True(t, ok)

	// s2 was unsubscribed, so there's no need to pull from channel.
	assert.Equal(t, len(s2), 0)

	_, ok = <-s3
	assert.Equal(t, len(s3), 0)
	assert.True(t, ok)
}

func TestDoubleStart(t *testing.T) {
	broker, err := newResourceBroker(nil)
	assert.Nil(t, err)
	assert.Nil(t, broker.Start())

	assert.NotNil(t, broker.Start())
}

func broadcastRunner(i int, b *testing.B) {
	broker, _ := newResourceBroker(nil)
	broker.Start()

	// Setup.
	for n := 0; n < i; n++ {
		ch := make(chan *discovery.Resource)
		go func() {
			for {
				_, ok := <-ch
				if !ok {
					return
				}
			}
		}()

		broker.Subscribe(ch)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		broker.Publish(nil)
	}
}

func BenchmarkBroadcast100(b *testing.B) {
	broadcastRunner(100, b)
}

func BenchmarkBroadcast1000(b *testing.B) {
	broadcastRunner(1000, b)
}

func BenchmarkBroadcast10000(b *testing.B) {
	broadcastRunner(10000, b)
}

func BenchmarkBroadcast100000(b *testing.B) {
	broadcastRunner(100000, b)
}

func BenchmarkBroadcast1000000(b *testing.B) {
	broadcastRunner(1000000, b)
}
