package protocol

import (
	"context"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/util"
)

type clientState struct {
	// Tracks the active nonces for the stream.
	nonces map[string]struct{}

	// Locks the nonce map.
	nmLock sync.RWMutex

	// Where we receive resources that the client is subscribed to.
	subCh chan *discovery.Resource

	// The discovery responses destined for this particular client.
	send chan *discovery.DeltaDiscoveryResponse
}

func newClientState(send chan *discovery.DeltaDiscoveryResponse) *clientState {
	return &clientState{
		nonces: make(map[string]struct{}),
		subCh:  make(chan *discovery.Resource),
		send:   send,
	}
}

// Creates a new nonce and begins the staleness timer for it.
func (cs *clientState) newNonceLifetime(lifetime time.Duration) string {
	n := util.MakeRandomNonce()

	cs.nmLock.Lock()
	cs.nonces[n] = struct{}{}
	cs.nmLock.Unlock()

	// Deal with stale nonces.
	go func() {
		time.Sleep(lifetime)

		cs.nmLock.Lock()
		defer cs.nmLock.Unlock()
		delete(cs.nonces, n)
	}()

	return n
}

// Renders the nonce stale.
func (cs *clientState) setNonceInactive(nonce string) {
	cs.nmLock.Lock()
	defer cs.nmLock.Unlock()
	delete(cs.nonces, nonce)
}

// The channel that streams the xDS resources a client is subscribed to.
func (cs *clientState) subscriberStream() chan *discovery.Resource {
	return cs.subCh
}

// Maps the stream object to a client state.
type clientStateMapping struct {
	cmap sync.Map
	log  *zap.SugaredLogger
}

// Returns the state associated with a stream. Creates it if non-existent.
func (csm *clientStateMapping) getState(ctx context.Context, stream ClientStream, typeURL string) *clientState {
	val, loaded := csm.cmap.LoadOrStore(stream, newClientState(stream.send))
	state := val.(*clientState)
	if !loaded {
		// Created a new client state, so let's get those responses processing.
		go csm.processResponses(ctx, csm.log, state, typeURL)
	}

	return val.(*clientState)
}

// Fires off discovery responses to the "send" channel.
// TODO: handle deletes somehow
func (csm *clientStateMapping) processResponses(
	ctx context.Context, log *zap.SugaredLogger, state *clientState, typeURL string) {

	log.Infow("processing responses for client")
	defer close(state.send)

	ticker := time.NewTicker(util.UpdateInterval)
	resources := []*discovery.Resource{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Debugw("ticker fired", "resources", resources)
			if len(resources) == 0 {
				continue
			}

			log.Infow("sending response")
			state.send <- &discovery.DeltaDiscoveryResponse{
				TypeUrl:   typeURL,
				Resources: resources,
				Nonce:     state.newNonceLifetime(30 * time.Second),
			}
			resources = []*discovery.Resource{}

		case res, more := <-state.subscriberStream():
			if !more {
				return
			}
			log.Infow("processing a resource", "name", res.GetName())
			// TODO: dedup by version num and add to res.
			resources = append(resources, res)
		}
	}
}

// The processor stores the state of the delta xDS protocol for a single resource type.
type ClientStream struct {
	stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer
	send   chan *discovery.DeltaDiscoveryResponse
}

func NewClientStream(stream *discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, send chan *discovery.DeltaDiscoveryResponse) ClientStream {
	return ClientStream{
		stream: stream,
		send:   send,
	}
}
