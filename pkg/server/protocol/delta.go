package protocol

import (
	"context"
	"sync"
	"time"

	"allen.gg/waterslide/pkg/server/watcher"

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

	resourceStream chan *discovery.Resource

	// TODO: Use a resource ingest interface to abstract away fs watcher from some other resource stream.
	fswatcher *watcher.ResourceWatcher

	wildcardBroker *resourceBroker
	ctx            context.Context
	log            *zap.SugaredLogger
}

type resourceBundle struct {
	resource *discovery.Resource
	broker   *resourceBroker
}

func NewDeltaDiscoveryProcessor(ctx context.Context, log *zap.SugaredLogger, fsWatchFile string) (*Processor, error) {
	broker, err := newResourceBroker(log)
	if err != nil {
		return nil, err
	}

	resourceStream := make(chan *discovery.Resource)
	fswatcher, err := watcher.NewFilesystemResourceWatcher(log, resourceStream)
	if err != nil {
		return nil, err
	}

	p := &Processor{
		brokerMap:      make(map[string]*resourceBundle),
		ctx:            context.Background(),
		wildcardBroker: broker,
		log:            log,
		fswatcher:      fswatcher,
		resourceStream: resourceStream,
	}

	err = p.wildcardBroker.Start()
	if err != nil {
		return nil, err
	}

	go p.ingestResources()

	fswatcher.Start(fsWatchFile)

	return p, nil
}

func (p *Processor) ingestResources() {
	for {
		res := <-p.resourceStream
		p.log.Infow("resource ingested", "resource", res.String())

		go func() {
			p.rwLock.RLock()
			defer p.rwLock.RUnlock()
			bundle := p.brokerMap[res.GetName()]
			bundle.broker.Publish(res)
		}()
	}
}

func (p *Processor) ProcessSubsequentDeltaDiscoveryRequest(
	ctx context.Context, ddr *discovery.DeltaDiscoveryRequest,
	stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, subCh chan *discovery.Resource) {
	// TODO
	panic("not implemented")
}

func (p *Processor) ProcessInitialDeltaDiscoveryRequest(
	ctx context.Context, ddr *discovery.DeltaDiscoveryRequest,
	stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, subCh chan *discovery.Resource) {
	p.log.Info("processing initial delta discovery request")

	// TODO: Support aliases, not just the resource name.

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

// Fans in resource subscriptions into subCh.
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

		bundle.broker.Subscribe(subCh)
		subCh <- bundle.resource
	}
}

// Plugs a wildcard subscription into the provided channel meant to feed resources to a subscriber.
func (p *Processor) doWildcardSubscription(ctx context.Context, subCh chan *discovery.Resource) {
	p.wildcardBroker.Subscribe(subCh)

	p.rwLock.RLock()
	defer p.rwLock.RUnlock()

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
