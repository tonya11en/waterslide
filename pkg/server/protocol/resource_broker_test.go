package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

func TestSubscribe(t *testing.T) {
	broker := newResourceBroker()
	broker.Start()
	defer broker.Stop()

	s1 := broker.Subscribe()
	s2 := broker.Subscribe()
	s3 := broker.Subscribe()

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
	broker := newResourceBroker()
	broker.Start()
	defer broker.Stop()

	s1 := broker.Subscribe()
	s2 := broker.Subscribe()
	s3 := broker.Subscribe()

	rfoo := &discovery.Resource{
		Name: "foo",
	}
	broker.Publish(rfoo)

	_, ok := <-s1
	assert.True(t, ok)
	_, ok = <-s2
	assert.True(t, ok)
	_, ok = <-s3
	assert.True(t, ok)

	broker.Publish(rfoo)
	broker.Unsubscribe(s2)
	_, ok = <-s2
	assert.False(t, ok)
}
