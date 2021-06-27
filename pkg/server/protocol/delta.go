package protocol

import (
	"context"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
)

const (
	typeUrlPrefix   = "type.googleapis.com/"
	ClusterTypeUrl  = typeUrlPrefix + "envoy.config.cluster.v3.Cluster"
	ListenerTypeUrl = typeUrlPrefix + "envoy.config.listener.v3.Listener"
	RuntimeTypeUrl  = typeUrlPrefix + "envoy.service.runtime.v3.Runtime"
	EndpointTypeUrl = typeUrlPrefix + "envoy.service.endpoint.v3.ClusterLoadAssignment"
	RouteTypeUrl    = typeUrlPrefix + "envoy.config.route.v3.RouteConfiguration"

	updateInterval = time.Second * 1
)

// The processor stores the state of the delta xDS protocol for a single resource type.
type Processor struct {
	brokerMap map[string]*resourceBundle
	rwLock    sync.RWMutex

	wildcardBroker *resourceBroker
	ctx            context.Context
	log            *zap.SugaredLogger
}

type resourceBundle struct {
	resource *discovery.Resource
	broker   *resourceBroker
}

func NewDeltaDiscoveryProcessor(ctx context.Context, log *zap.SugaredLogger) (*Processor, error) {
	broker, err := newResourceBroker(log)
	if err != nil {
		return nil, err
	}

	p := &Processor{
		brokerMap:      make(map[string]*resourceBundle),
		ctx:            context.Background(),
		wildcardBroker: broker,
	}
	p.wildcardBroker.Start()

	return p, nil
}

func (p *Processor) ProcessSubsequentDeltaDiscoveryRequest(
	ctx context.Context, ddr *discovery.DeltaDiscoveryRequest,
	stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, subCh chan *discovery.Resource) {
}

func (p *Processor) ProcessInitialDeltaDiscoveryRequest(
	ctx context.Context, ddr *discovery.DeltaDiscoveryRequest,
	stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, subCh chan *discovery.Resource) {

	if len(ddr.ResourceNamesSubscribe) == 0 {
		// Initial requests with empty resource subscription lists signal a wildcard subscription.
		p.log.Infow("node initiated wildcard subscription", "node", ddr.GetNode(), "resource_type", ddr.GetTypeUrl())
		p.doWildcardSubscription(ctx, subCh)
	} else {
		// Subscription to specific resources.
		p.log.Infow("node initiating subscriptions", "node", ddr.GetNode(), "resources", ddr.GetResourceNamesSubscribe(), "resource_type", ddr.GetTypeUrl())
		p.doIndividualSubscriptions(ctx, subCh, ddr.GetResourceNamesSubscribe())
	}

	if len(ddr.GetResourceNamesUnsubscribe()) > 0 {
		p.log.Errorw("initial delta discovery request contains resource names in unsubscribe field",
			"node", ddr.GetNode(),
			"unsubscribe_resources", ddr.GetResourceNamesUnsubscribe(),
			"resource_type", ddr.GetTypeUrl())
	}

	go p.processResponses(ctx, stream, subCh)
}

// Fans in resource subscriptions into subCh and
func (p *Processor) doIndividualSubscriptions(ctx context.Context, subCh chan *discovery.Resource, resource_names []string) {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()

	// Run through the resource names the client wants to subscribe to.
	for _, res := range resource_names {
		bundle, ok := p.brokerMap[res]
		if !ok {
			p.log.Warnw("node attempted subscribe to resource that is not in the broker map", "resource_name", res)
			continue
		}

		subCh <- bundle.resource

		bundle.broker.Subscribe(subCh)
	}
}

// Plugs a wildcard subscription into the provided channel meant to feed resources to a subscriber.
func (p *Processor) doWildcardSubscription(ctx context.Context, subCh chan *discovery.Resource) {
	p.rwLock.RLock()
	defer p.rwLock.RUnlock()

	p.wildcardBroker.Subscribe(subCh)

	// Give the subscriber all of the current resources.
	for _, bundle := range p.brokerMap {
		subCh <- bundle.resource
	}
}

func (p *Processor) processResponses(
	ctx context.Context, stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer,
	ch chan *discovery.Resource) {

	ticker := time.NewTicker(updateInterval)
	var resources []*discovery.Resource

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(resources) > 0 {
				// TODO: fire off the resources to the node.
				p.log.Infow("should fire off resources now")
				resources = []*discovery.Resource{}
			}
		case res := <-ch:
			// TODO: dedup by version num and add to res.
			resources = append(resources, res)
			p.log.Debugw("appended resources", "resources", resources)
		}
	}
}
