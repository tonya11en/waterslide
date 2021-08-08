package protocol

import (
	"context"
	"fmt"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
)

// The processor handles discovery requests and tracks the state associated with each client stream.
type Processor struct {
	brokers *util.BrokerMap

	// Incoming resource updates.
	resourceStream chan *discovery.Resource

	dbhandle db.DatabaseHandle

	// TODO: Use a resource ingest interface to abstract away fs watcher from some other resource
	// stream.
	ingest Ingester

	wildcardBroker *util.ResourceBroker
	ctx            context.Context
	log            *zap.SugaredLogger
	typeURL        string
	clientStateMap clientStateMapping
}

type ProcessorConfig struct {
	Ctx            context.Context
	Log            *zap.SugaredLogger
	TypeURL        string
	ResourceStream chan *discovery.Resource
	Ingest         Ingester
	DBHandle       db.DatabaseHandle
}

func NewDeltaDiscoveryProcessor(config ProcessorConfig) (*Processor, error) {
	p := &Processor{
		ctx:            config.Ctx,
		log:            config.Log,
		ingest:         config.Ingest,
		resourceStream: config.ResourceStream,
		typeURL:        config.TypeURL,
		dbhandle:       config.DBHandle,
		brokers:        util.NewBrokerMap(config.Log),
	}

	p.log.Info("initializing wildcard broker")
	err := p.initializeWildcardBroker()
	if err != nil {
		return nil, err
	}

	// Deal with the incoming resource stream.
	go p.ingestResources()

	return p, nil
}

func (p *Processor) initializeWildcardBroker() error {
	var loaded bool
	p.wildcardBroker, loaded = p.brokers.LoadOrStore(p.ctx, "*")
	if loaded {
		p.log.Fatal("wildcard broker already initialized")
	}

	return p.wildcardBroker.Start()
}

func (p *Processor) getBroker(name string) *util.ResourceBroker {
	if name == "*" {
		p.log.Fatal("incorrect handling of the wildcard broker")
	}

	b, loaded := p.brokers.LoadOrStore(p.ctx, name)
	if !loaded {
		err := b.Start()
		if err != nil {
			p.log.Fatal(err.Error())
		}
		// For each new broker, we want the wildcard broker to get all resources it publishes. This plugs
		// the output of the broker into the publisher of the wildcard broker, resulting in the wildcard
		// broker publishing any time an individual resource publishes.
		b.Subscribe(p.wildcardBroker.PublisherChannel())
	}
	return b
}

func (p *Processor) doResourceIngest(res *discovery.Resource) {
	if p.typeURL != res.Resource.GetTypeUrl() {
		p.log.Fatalf("received %s resource on processor for %s\n", res.Resource.GetTypeUrl(), p.typeURL)
	}
	b := p.getBroker(res.GetName())
	p.dbhandle.Put(p.ctx, res, p.typeURL)
	b.Publish(res)
}

func (p *Processor) ingestResources() {
	for res := range p.resourceStream {
		p.log.Infow("resource ingested", "resource", res.String())
		p.doResourceIngest(res)
	}
}

// Plugs a wildcard subscription into the provided channel meant to feed resources to a subscriber.
func (p *Processor) doWildcardSubscription(subCh chan *discovery.Resource) {
	p.wildcardBroker.Subscribe(subCh)
	p.log.Info("subscribed to wildcard broker")

	// Give the subscriber all of the current resources.
	p.dbhandle.ForEach(p.ctx, func(res *discovery.Resource) {
		p.log.Infow("adding resource to sub channel", "name", res.GetName())
		subCh <- res
	}, p.typeURL)
}

func isWildcardSubscriptionRequest(ddr *discovery.DeltaDiscoveryRequest) bool {
	return (len(ddr.GetResourceNamesSubscribe()) > 0 && ddr.GetResourceNamesSubscribe()[0] == "*")
}

func (p *Processor) isValidRequest(ddr *discovery.DeltaDiscoveryRequest) bool {
	if isWildcardSubscriptionRequest(ddr) && len(ddr.GetResourceNamesUnsubscribe()) > 0 {
		p.log.Warnw("received invalid request", "reason", "wildcard subscription included with individual resource sub")
		return false
	}

	return true
}

func (p *Processor) ProcessDeltaDiscoveryRequest(
	ctx context.Context,
	ddr *discovery.DeltaDiscoveryRequest,
	stream ClientStream) error {

	// Error out if the request does not conform to the protocol.
	if !p.isValidRequest(ddr) {
		return fmt.Errorf("received nonsensical request")
	}

	state := p.clientStateMap.getState(stream)
	p.log.Debugw("received delta discovery request",
		"nonce", ddr.GetResponseNonce(), "is_ack", isAck(ddr), "is_nack", isNack(ddr))

	// Unconditionally handle resource subscriptions.
	for _, subName := range ddr.GetResourceNamesSubscribe() {
		p.log.Debugw("client subscribing to resource", "resource", subName)

		if subName == "*" {
			p.doWildcardSubscription(state.subscriberStream())
			continue
		}

		// Lookup the specific resource.
		res, err := p.dbhandle.Get(p.ctx, subName, p.typeURL)
		if err != nil {
			p.log.Warnw("lookup failed for resource", "resource_name", subName, "error", err.Error())
		}

		if res == nil {
			p.log.Infow("client subscribing to non-existent resource", "resource", subName)
			p.sendEmptyResponse(stream, p.typeURL)
		} else {
			state.subscriberStream() <- res
			b := p.getBroker(subName)
			b.Subscribe(state.subscriberStream())
		}
	}

	// Unconditionally handle resource subscriptions.
	for _, unsubName := range ddr.GetResourceNamesUnsubscribe() {
		p.log.Debugw("client unsubscribing from resource", "resource", unsubName)
		b := p.getBroker(unsubName)
		b.Unsubscribe(state.subscriberStream())
	}

	nonce := ddr.GetResponseNonce()

	state.setNonceInactive(nonce)
	return nil
}

func isAck(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() == nil
}

func isNack(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() != nil
}

func (p *Processor) sendEmptyResponse(stream ClientStream, typeURL string) {
	go func() {
		nonce := util.MakeRandomNonce()
		state := p.clientStateMap.getState(stream)
		state.setNonceActive(nonce)
		stream.send <- &discovery.DeltaDiscoveryResponse{
			Nonce:   nonce,
			TypeUrl: typeURL,
		}
	}()
}
