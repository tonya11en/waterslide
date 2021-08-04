package protocol

import (
	"context"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Responsible for delivering new resources of the same type into the ingestStream.
type Ingester interface {
	// Begins the task of resource delivery. This should be async.
	Start(ctx context.Context, ingestStream chan *discovery.Resource)
}

type NoopIngest struct {
}

func (n *NoopIngest) Start(ctx context.Context, ingestStream chan *discovery.Resource) {
}
