package protocol

import (
	"context"
	"log"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// The processor stores the state of the delta xDS protocol for a single resource type.
type Processor struct {
	// Maps resource names to the proto and all subscribers to that resource.
	rmaps resourceMap

	// A set of subscriber channels for clients who've initiated a wildcard subscription.
	wildcardSubs wildcardSubscriptions

	// Sinks for the delta discovery requests.
	initialRequests    chan *discovery.DeltaDiscoveryRequest
	subsequentRequests chan *discovery.DeltaDiscoveryRequest

	ctx context.Context
}

func NewDeltaDiscoveryProcessor(ctx context.Context) *Processor {
	p := &Processor{
		rmaps:              make(resourceMap),
		wildcardSubs:       make(wildcardSubscriptions),
		initialRequests:    make(chan *discovery.DeltaDiscoveryRequest),
		subsequentRequests: make(chan *discovery.DeltaDiscoveryRequest),
		ctx:                ctx,
	}

	// Spin off the single goroutine that performs mutations on the resource maps.
	go p.resourceMapMutator()

	return p
}

func (p *Processor) ProcessInitialDeltaDiscoveryRequest(ddr *discovery.DeltaDiscoveryRequest, stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {
	log.Println("initial request")
	log.Println(ddr.String())
	p.processInitialRequestInternal(ddr, stream)

}

func (p *Processor) ProcessSubsequentDeltaDiscoveryRequest(ddr *discovery.DeltaDiscoveryRequest) {
	go func() { p.subsequentRequests <- ddr }()
}

func (p *Processor) resourceMapMutator() {
	for ddr := range p.subsequentRequests {
		log.Println("subsequent request")
		log.Println(ddr.String())
	}
}

func (p *Processor) processInitialRequestInternal(ddr *discovery.DeltaDiscoveryRequest, stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {
	resChan := make(chan *discovery.Resource)
	go p.processResponses(stream, resChan, stream.Context())

	if len(ddr.ResourceNamesSubscribe) == 0 {
		// Initial requests with empty resource subscription lists signal a wildcard subscription.
		log.Printf("node %s initiated wildcard subscription for %s", ddr.GetNode(), ddr.GetTypeUrl())
		p.wildcardSubs[resChan] = struct{}{}
	} else {
		// Run through the resource names the client wants to subscribe to.
		for _, name := range ddr.GetResourceNamesSubscribe() {
			val, ok := p.rmaps[name]
			if !ok {
				val = resourceBundle{
					subscribers: make(map[subscriberChannel]struct{}),
				}
			}
			// Add the client resource channel to the subscriber set.
			val.subscribers[resChan] = struct{}{}
		}
	}
}

func (p *Processor) processResponses(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, ch chan *discovery.Resource, ctx context.Context) {
	<-ctx.Done()
	log.Println("@tallen it's graceful yo")
}
