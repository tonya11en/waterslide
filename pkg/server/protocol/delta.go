package protocol

import (
	"context"
	"log"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

const (
	typeUrlPrefix   = "type.googleapis.com/"
	ClusterTypeUrl  = typeUrlPrefix + "envoy.config.cluster.v3.Cluster"
	ListenerTypeUrl = typeUrlPrefix + "envoy.config.listener.v3.Listener"
	RuntimeTypeUrl  = typeUrlPrefix + "envoy.service.runtime.v3.Runtime"
	EndpointTypeUrl = typeUrlPrefix + "envoy.service.endpoint.v3.ClusterLoadAssignment"
	RouteTypeUrl    = typeUrlPrefix + "envoy.config.route.v3.RouteConfiguration"
)

// The processor stores the state of the delta xDS protocol for a single resource type.
type Processor struct {
	brokerMap      map[string]*resourceBundle
	wildcardBroker *resourceBroker
	ctx            context.Context
}

type resourceBundle struct {
	resource *discovery.Resource
	broker   *resourceBroker
}

func NewDeltaDiscoveryProcessor(ctx context.Context) *Processor {
	p := &Processor{
		brokerMap:      make(map[string]*resourceBundle),
		ctx:            context.Background(),
		wildcardBroker: newResourceBroker(),
	}

	return p
}

func (p *Processor) ProcessInitialDeltaDiscoveryRequest(ddr *discovery.DeltaDiscoveryRequest, stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {
}

func (p *Processor) ProcessSubsequentDeltaDiscoveryRequest(ddr *discovery.DeltaDiscoveryRequest) {
}

func (p *Processor) processInitialRequestInternal(ddr *discovery.DeltaDiscoveryRequest, stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {
	if len(ddr.ResourceNamesSubscribe) == 0 {
		// Initial requests with empty resource subscription lists signal a wildcard subscription.
		log.Printf("node %s initiated wildcard subscription for %s", ddr.GetNode(), ddr.GetTypeUrl())
	} else {
		// Run through the resource names the client wants to subscribe to.
		for _, name := range ddr.GetResourceNamesSubscribe() {
			// stuff
			log.Println(name)
		}
	}
}

func (p *Processor) processResponses(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, ch chan discovery.Resource, ctx context.Context) {
	<-ctx.Done()
	log.Println("@tallen it's graceful yo")
}
