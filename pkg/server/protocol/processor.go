package protocol

import (
	"context"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
)

// The processor handles discovery requests and tracks the state associated with each client stream.
type Processor struct {
	brokers        *util.BrokerMap
	wildcardBroker *util.ResourceBroker

	// Incoming resource updates.
	resourceStream chan *discovery.Resource

	dbhandle db.DatabaseHandle

	// TODO: Use a resource ingest interface to abstract away fs watcher from some other resource
	// stream.
	ingest Ingester

	ctx            context.Context
	log            *zap.SugaredLogger
	typeURL        string
	clientStateMap clientStateMapping
}

type ProcessorConfig struct {
	Ctx      context.Context
	Log      *zap.SugaredLogger
	TypeURL  string
	Ingest   Ingester
	DBHandle db.DatabaseHandle
}

func NewDeltaDiscoveryProcessor(config ProcessorConfig) (*Processor, error) {
	p := &Processor{
		ctx:      config.Ctx,
		log:      config.Log,
		ingest:   config.Ingest,
		typeURL:  config.TypeURL,
		dbhandle: config.DBHandle,
		brokers:  util.NewBrokerMap(config.Log),
		clientStateMap: clientStateMapping{
			log: config.Log,
		},
	}

	p.log.Info("initializing wildcard broker")
	err := p.initializeWildcardBroker()
	if err != nil {
		return nil, err
	}

	// Deal with the incoming resource stream.
	p.ingest.Start()
	p.resourceStream = p.ingest.ResourceStream()
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
		b.Subscribe(p.ctx, p.wildcardBroker.PublisherChannel())
	}
	return b
}

// Consumes from the inbound resource stream.
func (p *Processor) ingestResources() {
	p.log.Debugw("starting ingest resource stream")
	for res := range p.resourceStream {
		p.log.Infow("resource ingested", "resource", res.String())
		if p.typeURL != res.Resource.GetTypeUrl() {
			p.log.Fatalf("received %s resource on processor for %s\n", res.Resource.GetTypeUrl(), p.typeURL)
		}
		b := p.getBroker(res.GetName())
		p.dbhandle.Put(p.ctx, res, p.typeURL)
		b.Publish(res)
	}
}

// Plugs a wildcard subscription into the provided channel meant to feed resources to a subscriber.
func (p *Processor) doWildcardSubscription(ctx context.Context, subCh chan *discovery.Resource) {
	p.wildcardBroker.Subscribe(ctx, subCh)
	p.log.Info("subscribed to wildcard broker")

	// Give the subscriber all of the current resources.
	p.dbhandle.ForEach(p.ctx, func(res *discovery.Resource) {
		p.log.Infow("adding resource to sub channel", "name", res.GetName())
		subCh <- res
	}, p.typeURL)
}

func (p *Processor) ProcessDeltaDiscoveryRequest(
	ctx context.Context,
	ddr *discovery.DeltaDiscoveryRequest,
	stream ClientStream) {

	state := p.clientStateMap.getState(ctx, stream, p.typeURL)
	p.log.Debugw("received delta discovery request",
		"nonce", ddr.GetResponseNonce(), "is_ack", isAck(ddr), "is_nack", isNack(ddr))

	// Unconditionally handle resource subscriptions.
	for _, subName := range ddr.GetResourceNamesSubscribe() {
		p.log.Debugw("client subscribing to resource", "resource", subName)

		if subName == "*" {
			p.doWildcardSubscription(ctx, state.subscriberStream())
			continue
		}

		// Lookup the specific resource.
		res, err := p.dbhandle.Get(p.ctx, subName, p.typeURL)
		if err != nil {
			p.log.Warnw("lookup failed for resource", "resource_name", subName, "error", err.Error())
		}

		if res == nil {
			p.log.Debugw("client subscribing to non-existent resource, sending empty resource", "resource", subName)
			state.subscriberStream() <- &discovery.Resource{
				Name: subName,
			}
		} else {
			state.subscriberStream() <- res
			b := p.getBroker(subName)
			b.Subscribe(ctx, state.subscriberStream())
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
}

func (p *Processor) CleanupSubscriptions(msgCh chan *discovery.Resource) {
	p.wildcardBroker.Unsubscribe(msgCh)
	p.brokers.Range(func(_ string, broker *util.ResourceBroker) bool {
		broker.Unsubscribe(msgCh)
		return true
	})
}

func isAck(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() == nil
}

func isNack(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() != nil
}
