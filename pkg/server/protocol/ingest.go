package protocol

import (
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Responsible for delivering new resources of the same type into the ingestStream.
type Ingester interface {
	// Begins the task of resource delivery. This should be async.
	Start()

	// The stream of resources that have been ingested.
	ResourceStream() chan *discovery.Resource
}

type TestIngest struct {
	istream chan *discovery.Resource
}

func (t *TestIngest) Start() {
	t.istream = make(chan *discovery.Resource)
}

func (t *TestIngest) ResourceStream() chan *discovery.Resource {
	return t.istream
}

func (t *TestIngest) addResource(res *discovery.Resource) {
	go func() {
		t.istream <- res
	}()
}
