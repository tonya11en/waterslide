package protocol

import (
	"context"
	"sync"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
)

type clientState struct {
	// Tracks the active nonces for the stream.
	nonces map[string]struct{}

	// Locks the nonce map.
	nmLock sync.RWMutex

	// Where we receive resources that the client is subscribed to.
	subCh chan *discovery.Resource
}

func newClientState() *clientState {
	return &clientState{
		nonces: make(map[string]struct{}),
		subCh:  make(chan *discovery.Resource),
	}
}

func (cs *clientState) NonceIsActive(nonce string) bool {
	cs.nmLock.RLock()
	defer cs.nmLock.RUnlock()
	_, ok := cs.nonces[nonce]
	return ok
}

func (cs *clientState) SetNonceActive(nonce string) {
	cs.nmLock.Lock()
	defer cs.nmLock.Unlock()
	cs.nonces[nonce] = struct{}{}
}

// The channel that streams the xDS resources a client is subscribed to.
func (cs *clientState) SubscriberStream() chan *discovery.Resource {
	return cs.subCh
}

// Maps the stream object to
type ClientStateMapping struct {
	cmap sync.Map
}

// Returns the state associated with a stream. Creates it if non-existent.
func (csm *ClientStateMapping) GetState(stream ClientStream) *clientState {
	val, _ := csm.cmap.LoadOrStore(stream, newClientState())
	return val.(*clientState)
}

// The processor stores the state of the delta xDS protocol for a single resource type.
type ClientStream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer
type Processor struct {
	brokerMap      sync.Map
	resourceStream chan *discovery.Resource
	dbhandle       db.DatabaseHandle

	// TODO: Use a resource ingest interface to abstract away fs watcher from some other resource stream.
	ingest Ingester

	wildcardBroker *util.ResourceBroker
	ctx            context.Context
	log            *zap.SugaredLogger
	typeURL        string
	clientStateMap ClientStateMapping
}

type ProcessorConfig struct {
	Ctx            context.Context
	Log            *zap.SugaredLogger
	TypeURL        string
	ResourceStream chan *discovery.Resource
	Ingest         Ingester
	DBHandle       db.DatabaseHandle
}
