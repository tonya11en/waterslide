package protocol

import (
	"context"
	"sync"

	"allen.gg/waterslide/internal/util"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
)

type clientState struct {
	// Tracks the active nonces and the responses sent for them.
	nonceMap map[string]*discovery.DeltaDiscoveryResponse

	// For a particular type URL, we track whether a rq has been encountered. Let's us know if a
	// request is the first for a particular type.
	rqReceived map[string]struct{}

	// Where we receive resources that the client is subscribed to.
	subCh chan *discovery.Resource
}

// The processor stores the state of the delta xDS protocol for a single resource type.
type ClientStream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer
type Processor struct {
	brokerMap map[string]*resourceBundle
	rwLock    sync.RWMutex

	resourceStream chan *discovery.Resource

	// TODO: Use a resource ingest interface to abstract away fs watcher from some other resource stream.
	ingest Ingester

	wildcardBroker *util.ResourceBroker
	ctx            context.Context
	log            *zap.SugaredLogger
	typeURL        string

	clientStateMap map[ClientStream]clientState
}

type resourceBundle struct {
	resource *discovery.Resource
	broker   *util.ResourceBroker
}

type ProcessorConfig struct {
	Ctx            context.Context
	Log            *zap.SugaredLogger
	TypeURL        string
	ResourceStream chan *discovery.Resource
	Ingest         Ingester
}
