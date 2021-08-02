package protocol

import (
	"context"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	"allen.gg/waterslide/pkg/server/util"
)

func NewDeltaDiscoveryProcessor(config ProcessorConfig) (*Processor, error) {
	broker, err := util.NewResourceBroker(config.Log)
	if err != nil {
		return nil, err
	}

	p := &Processor{
		brokerMap:      make(map[string]*resourceBundle),
		ctx:            config.Ctx,
		wildcardBroker: broker,
		log:            config.Log,
		ingest:         config.Ingest,
		resourceStream: config.ResourceStream,
		clientStateMap: make(map[ClientStream]clientState),
		typeURL:        config.TypeURL,
	}

	err = p.wildcardBroker.Start()
	if err != nil {
		return nil, err
	}

	go p.ingestResources()

	return p, nil
}

func makeClientState() clientState {
	return clientState{
		nonceMap:   make(map[string]*discovery.DeltaDiscoveryResponse),
		rqReceived: make(map[string]struct{}),
		subCh:      make(chan *discovery.Resource),
	}
}

func (p *Processor) newResourceBundle(res *discovery.Resource) (*resourceBundle, error) {
	var bundle resourceBundle
	var err error

	bundle.resource = res
	bundle.broker, err = util.NewResourceBroker(p.log)
	if err != nil {
		p.log.Errorw("error creating resource bundle", "resource", res.String(), "error", err.Error())
		return nil, err
	}

	bundle.broker.Start()

	// For each new broker, we want the wildcard broker to get all resources it publishes.
	bundle.broker.Subscribe(p.wildcardBroker.PublisherChannel())

	return &bundle, nil
}

func (p *Processor) doResourceIngest(res *discovery.Resource) {
	var bundle *resourceBundle
	var err error

	p.rwLock.Lock()
	defer p.rwLock.Unlock()

	bundle, ok := p.brokerMap[res.GetName()]
	if !ok {
		p.log.Infow("encountered new resource", "resource", res.String())
		bundle, err = p.newResourceBundle(res)
		if err != nil {
			p.log.Errorw("dropping resource due to error creating resource bundle", "error", err.Error(), "resource", res.String())
			return
		}
		p.brokerMap[res.GetName()] = bundle
	}

	bundle.broker.Publish(res)
}

func (p *Processor) ingestResources() {
	for res := range p.resourceStream {
		p.log.Infow("resource ingested", "resource", res.String())
		p.doResourceIngest(res)
	}
}

// Plugs a wildcard subscription into the provided channel meant to feed resources to a subscriber.
func (p *Processor) doWildcardSubscription(ctx context.Context, subCh chan *discovery.Resource) {
	p.wildcardBroker.Subscribe(subCh)
	p.log.Info("subscribed to wildcard broker")

	p.rwLock.RLock()
	defer p.rwLock.RUnlock()

	p.log.Info("grabbed lock", "broker_map", p.brokerMap)

	// Give the subscriber all of the current resources.
	for _, bundle := range p.brokerMap {
		p.log.Infow("adding resource to sub channel", "name", bundle.resource.GetName())
		subCh <- bundle.resource
	}
}

func (p *Processor) ProcessDeltaDiscoveryRequest(
	ctx context.Context, ddr *discovery.DeltaDiscoveryRequest, stream ClientStream, send chan *discovery.DeltaDiscoveryResponse) {

	p.log.Info("processing initial delta discovery request")

	// TODO: Support aliases, not just the resource name.

	state, ok := p.clientStateMap[stream]
	if !ok {
		p.clientStateMap[stream] = makeClientState()
		p.processResponses(ctx, stream, state.subCh, send)
	}

	_, first := state.rqReceived[ddr.GetTypeUrl()]
	if first {
		p.log.Infow("encountered first request of type", "type", ddr.GetTypeUrl(), "discovery_request", ddr.String())
	}

	// Handle responses to previous requests.
	if isAck(ddr) || isNack(ddr) {
		if isNack(ddr) {
			p.log.Errorw("client ACK/NACK received", "nonce", ddr.GetResponseNonce(), "error_detail", ddr.GetErrorDetail())
		}
		if _, ok := state.nonceMap[ddr.GetResponseNonce()]; !ok {
			p.log.Errorw("bogus nonce received", "nonce", ddr.GetResponseNonce())
		}
		delete(state.nonceMap, ddr.GetResponseNonce())
		return
	}

	if len(ddr.ResourceNamesSubscribe) == 0 {
		// Initial requests with empty resource subscription lists signal a wildcard subscription.
		p.log.Infow("node initiated wildcard subscription", "id", ddr.GetNode().GetId(), "resource_type", ddr.GetTypeUrl())
		p.doWildcardSubscription(ctx, state.subCh)
	} else {
		// Subscription to specific resources.
		p.log.Infow("node initiating subscriptions", "id", ddr.GetNode().GetId(), "resources", ddr.GetResourceNamesSubscribe(), "resource_type", ddr.GetTypeUrl())
		p.doIndividualSubscriptions(ctx, state.subCh, ddr.GetResourceNamesSubscribe())
	}

	if len(ddr.GetResourceNamesUnsubscribe()) > 0 {
		p.log.Errorw("initial delta discovery request contains resource names in unsubscribe field",
			"node", ddr.GetNode(),
			"unsubscribe_resources", ddr.GetResourceNamesUnsubscribe(),
			"resource_type", ddr.GetTypeUrl())
	}
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

func (p *Processor) ProcessDeltaDiscoveryResponse(
	ctx context.Context, ddr *discovery.DeltaDiscoveryResponse, stream ClientStream) error {
	state, ok := p.clientStateMap[stream]
	if !ok {
		p.log.Fatalw("client state not found", "delta_discovery_response", ddr.String())
	}

	nonce := util.MakeRandomNonce()
	state.nonceMap[nonce] = ddr

	return (*stream).Send(ddr)
}

func isAck(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() == nil
}

func isNack(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() != nil
}

// Fires off discovery responses to the "send" channel.
func (p *Processor) processResponses(
	ctx context.Context, stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer,
	ch chan *discovery.Resource, send chan *discovery.DeltaDiscoveryResponse) {

	// TODO: handle deletes

	p.log.Infow("processing responses for client")

	ticker := time.NewTicker(util.UpdateInterval)
	resources := []*discovery.Resource{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(resources) == 0 {
				continue
			}
			p.log.Infow("sending response")
			send <- &discovery.DeltaDiscoveryResponse{
				TypeUrl:   p.typeURL,
				Resources: resources,
			}
		case res := <-ch:
			p.log.Infow("processing a resource", "name", res.GetName())
			// TODO: dedup by version num and add to res.
			resources = append(resources, res)
		}
	}
}
