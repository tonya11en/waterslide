package client_state

import (
	"context"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	flatbuffers "github.com/google/flatbuffers/go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/db/flatbuffers/waterslide_bufs"
	"allen.gg/waterslide/internal/util"
)

type clientState struct {
	ctx context.Context
	log *zap.SugaredLogger

	// Tracks the active nonces for the stream.
	activeNonces    map[string]context.CancelFunc
	deactivateNonce chan string

	// Active subscriptions. Calling the cancel function will unsubscribe the client from the resource.
	activeSubs map[string]context.CancelFunc

	// Resource names to (un)subscribe from.
	unsubscribe chan string
	subscribe   chan subscriptionInfo

	// Resources staged for update. Maps the type URL to a map of names to resource flatbuffer.
	//
	// typeURL -> name -> flatbuf.
	stagedUpdates map[string]map[string]*waterslide_bufs.Resource
	updateStream  chan *waterslide_bufs.Resource

	// Handle for the xDS database.
	dbhandle db.DatabaseHandle

	// The client stream.
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer
}

type subscriptionInfo struct {
	namespace    string
	typeURL      string
	resourceName string
}

func NewClientState(ctx context.Context, log *zap.SugaredLogger, stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, handle db.DatabaseHandle) *clientState {
	return &clientState{
		ctx:             ctx,
		log:             log,
		stream:          stream,
		activeNonces:    make(map[string]context.CancelFunc),
		deactivateNonce: make(chan string),
		subscribe:       make(chan subscriptionInfo),
		unsubscribe:     make(chan string),
		activeSubs:      make(map[string]context.CancelFunc),
		stagedUpdates:   make(map[string]map[string]*waterslide_bufs.Resource),
		updateStream:    make(chan *waterslide_bufs.Resource),
		dbhandle:        handle,
	}
}

// Takes a nonce and begins the staleness timer for it. Call cancelfunc to render a nonce stale.
func (cs *clientState) startNonceLifetime(dur time.Duration, n string) context.CancelFunc {
	lifeCtx, cancel := context.WithDeadline(cs.ctx, time.Now().Add(dur))
	cs.activeNonces[n] = cancel

	// Deal with stale nonces.
	go func() {
		<-lifeCtx.Done()
		cs.deactivateNonce <- n
	}()

	return cancel
}

func (cs *clientState) MarkNonceStale(n string) {
	cs.deactivateNonce <- n
}

// Renders the nonce stale if it exists.
func (cs *clientState) setNonceInactive(nonce string) {
	if cancelFunc, ok := cs.activeNonces[nonce]; ok {
		cancelFunc()
		delete(cs.activeNonces, nonce)
	}
}
func (cs *clientState) DoUnsubscribe(resourceName string) {
	cs.unsubscribe <- resourceName
}

func (cs *clientState) doUnsubscribeInternal(resourceName string) {
	cancel, ok := cs.activeSubs[resourceName]
	if !ok {
		cs.log.Debugw("tried unsubscribing from resource without a subscription", "resource_name", resourceName)
		return
	}
	cs.log.Debugw("performing unsubscribe", "resource_name", resourceName)
	cancel()
	delete(cs.activeSubs, resourceName)
}

func (cs *clientState) DoSubscribe(namespace string, typeURL string, resourceName string) {
	cs.log.Debugw("@tallen doing sub")
	cs.subscribe <- subscriptionInfo{
		namespace:    namespace,
		typeURL:      typeURL,
		resourceName: resourceName,
	}
	cs.log.Debugw("@tallen pushed in chan")
}

func (cs *clientState) doSubscribeInternal(namespace string, typeURL string, resourceName string) <-chan *waterslide_bufs.Resource {
	cs.log.Debugw("@tallen do sub internal")
	out := make(chan *waterslide_bufs.Resource)

	if _, ok := cs.activeSubs[resourceName]; ok {
		// We're already subscribed.
		cs.log.Debugw("tried subscribing to resource with existing subscription", "namespace", namespace, "typeURL", typeURL, "resource_name", resourceName)
		return out
	}
	cs.log.Debugw("performing subscribe", "namespace", namespace, "typeURL", typeURL, "resource_name", resourceName)

	ctx, cancel := context.WithCancel(cs.ctx)
	cs.activeSubs[resourceName] = cancel
	fn := func(key string, fbuf *waterslide_bufs.Resource) error {
		cs.updateStream <- fbuf
		return nil
	}

	// It's fine to do this async.
	go func() {
		defer close(out)
		cs.log.Debugw("@tallen async thing")
		if resourceName == "*" {
			cs.log.Debugw("@tallen start wildcard")
			err := cs.dbhandle.WildcardSubscribe(ctx, namespace, typeURL, fn)
			if err != nil {
				cs.log.Fatalw(err.Error())
			}

			all, err := cs.dbhandle.GetAll(ctx, namespace, typeURL)
			if err != nil {
				cs.log.Fatalw(err.Error())
			}

			cs.log.Debugw("@tallen wildcard", "all", all)
			for _, fb := range all {
				out <- fb
			}
			cs.log.Debugw("@tallen leaving wildcard", "all", all)
		} else {
			cs.log.Debugw("@tallen start single")
			err := cs.dbhandle.ResourceSubscribe(ctx, namespace, typeURL, resourceName, fn)
			if err != nil {
				cs.log.Fatalw(err.Error())
			}

			cs.log.Debugw("@tallen singleton")
			fb, err := cs.dbhandle.Get(ctx, namespace, typeURL, resourceName)
			if err != nil {
				cs.log.Fatalw(err.Error())
			}
			out <- fb
			cs.log.Debugw("@tallen leaving singleton")
		}
	}()
	return out
}

func (cs *clientState) sendResponse(typeURL string) {
	cs.log.Debugw("ticker fired -- sending response", "typeURL", typeURL)
	staged := cs.stagedUpdates[typeURL]
	resources := []*discovery.Resource{}
	toRemove := []string{}
	for name, b := range staged {
		if b == nil {
			toRemove = append(toRemove, name)
			continue
		}

		res, err := util.CreateResourceFromBytes(b.ResourceProto())
		if err != nil {
			cs.log.Errorw("unable to unmarshal resource from flatbuf", "error", err.Error())
			continue
		}
		resources = append(resources, res)
		cs.log.Debugw("ticker", "resource", res.String())
	}

	n := util.MakeRandomNonce()
	cs.startNonceLifetime(15*time.Second, n)
	cs.stream.Send(&discovery.DeltaDiscoveryResponse{
		Resources:        resources,
		TypeUrl:          typeURL,
		RemovedResources: toRemove,
		Nonce:            n,
	})
}

func (cs *clientState) processUpdate(resBuf *waterslide_bufs.Resource) error {
	cs.log.Infow("@tallen process update", "resbuf", resBuf)
	res, err := util.ResourceProtoFromFlat(resBuf)
	if err != nil {
		return err
	}

	typeURL := res.GetResource().GetTypeUrl()
	m, ok := cs.stagedUpdates[typeURL]
	if !ok {
		m = make(map[string]*waterslide_bufs.Resource)
		cs.stagedUpdates[typeURL] = m
	}

	old, ok := m[res.GetName()]
	if ok && old.Gsn() >= resBuf.Gsn() {
		// This is not a more recent version of the resource, so we'll just disregard it.
		return nil
	}
	m[res.GetName()] = resBuf
	return nil
}

func makeEmptyFlatResource(resourceName string) *waterslide_bufs.Resource {
	res := discovery.Resource{
		Name: resourceName,
	}

	b, err := proto.Marshal(&res)
	if err != nil {
		panic(err.Error())
	}

	builder := flatbuffers.NewBuilder(1000)
	bv := builder.CreateByteString(b)
	v := builder.CreateString("0")
	waterslide_bufs.ResourceStart(builder)
	waterslide_bufs.ResourceAddResourceProto(builder, bv)
	waterslide_bufs.ResourceAddVersion(builder, v)
	waterslide_bufs.ResourceAddGsn(builder, 0)
	re := waterslide_bufs.ResourceEnd(builder)
	builder.Finish(re)

	// TODO: There is probably a more efficient way to do this.
	return waterslide_bufs.GetRootAsResource(builder.FinishedBytes(), 0)
}

func (cs *clientState) ProcessResponses() {
	cs.log.Infow("processing responses for client")
	ticker := time.NewTicker(util.UpdateInterval)

	for {
		select {
		case <-cs.ctx.Done():
			return

		// Handle subscribes.
		case subInfo := <-cs.subscribe:
			cs.log.Info("@tallen subscribing")
			for fb := range cs.doSubscribeInternal(subInfo.namespace, subInfo.typeURL, subInfo.resourceName) {
				// If the flatbuffer is nil, the object does not exist.
				if fb == nil {
					cs.log.Info("@tallen FB IS NIL")
					fb = makeEmptyFlatResource(subInfo.resourceName)
				}

				err := cs.processUpdate(fb)
				if err != nil {
					cs.log.Fatalw("error unmarshaling resource proto", "error", err.Error())
				}
			}
			cs.log.Info("@tallen DONE subscribing")

		// Handle unsubscribes.
		case resName := <-cs.unsubscribe:
			cs.doUnsubscribeInternal(resName)

		// Set a nonce as stale.
		case n := <-cs.deactivateNonce:
			cs.log.Info("setting nonce inactive", "nonce", n)
			cs.setNonceInactive(n)

		// Stage the updates.
		case resBuf := <-cs.updateStream:
			err := cs.processUpdate(resBuf)
			if err != nil {
				cs.log.Fatalw("error unmarshaling resource proto", "error", err.Error())
			}

		// Periodically send a response with the relevant updates.
		case <-ticker.C:
			cs.log.Info("@tallen tick tickticktick")
			if len(cs.stagedUpdates) == 0 {
				cs.log.Info("@tallen no staged updates")
				continue
			}
			for turl, m := range cs.stagedUpdates {
				if len(m) == 0 {
					cs.log.Debugw("ticker fired -- nothing to process", "typeURL", turl)
					continue
				}
				cs.sendResponse(turl)
			}
		}
	}
}

// Maps the stream object to a client state.
type ClientStateMapping struct {
	cmap sync.Map
	Log  *zap.SugaredLogger
}

// Returns the state associated with a stream. Creates it if non-existent.
func (csm *ClientStateMapping) GetState(ctx context.Context, stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer, handle db.DatabaseHandle) *clientState {
	val, loaded := csm.cmap.LoadOrStore(stream, NewClientState(stream.Context(), csm.Log, stream, handle))
	state := val.(*clientState)
	if !loaded {
		// Created a new client state, so let's get those responses processing. When it's done
		// processing, we'll just delete it form the map.
		go func() {
			state.ProcessResponses()
			csm.cmap.Delete(stream)
		}()
	}

	return val.(*clientState)
}