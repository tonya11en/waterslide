package protocol

import (
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

	broker.Publish(rfoo)
	assert.Equal(t, rfoo, <-s1)
	assert.Equal(t, rfoo, <-s2)
	assert.Equal(t, rfoo, <-s3)

	broker.Publish(rbar)
	assert.Equal(t, rbar, <-s1)
	assert.Equal(t, rbar, <-s2)
	assert.Equal(t, rbar, <-s3)
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
