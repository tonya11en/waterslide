package server

import (
	"context"
	"fmt"
	"io"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
	"allen.gg/waterslide/internal/watcher"
	"allen.gg/waterslide/pkg/server/protocol"
)

// An xDS delta protocol server.
type server struct {
	ctx                  context.Context
	listenerProcessor    *protocol.Processor
	clusterProcessor     *protocol.Processor
	routeProcessor       *protocol.Processor
	endpointProcessor    *protocol.Processor
	scopedRouteProcessor *protocol.Processor
	log                  *zap.SugaredLogger
	dbhandle             *db.DatabaseHandle
}

// Creates a new server. If the DB filepath is empty, it will make an in-memory DB.
func NewServer(ctx context.Context, log *zap.SugaredLogger, handle db.DatabaseHandle) *server {
	if log == nil {
		panic("passed in nil logger")
	}

	config := protocol.ProcessorConfig{
		Ctx:      ctx,
		Log:      log,
		DBHandle: handle,
	}

	config.TypeURL = util.ListenerTypeUrl
	lp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatalw("unable to create delta discovery processor", "error", err.Error(), "typeURL", config.TypeURL)
	}

	config.TypeURL = util.ClusterTypeUrl
	cp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatalw("unable to create delta discovery processor", "error", err.Error(), "typeURL", config.TypeURL)
	}

	config.TypeURL = util.RouteTypeUrl
	rp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatalw("unable to create delta discovery processor", "error", err.Error(), "typeURL", config.TypeURL)
	}

	config.TypeURL = util.ScopedRouteTypeUrl
	srp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatalw("unable to create delta discovery processor", "error", err.Error(), "typeURL", config.TypeURL)
	}

	config.TypeURL = util.EndpointTypeUrl
	ep, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatalw("unable to create delta discovery processor", "error", err.Error(), "typeURL", config.TypeURL)
	}

	srv := &server{
		ctx:                  ctx,
		listenerProcessor:    lp,
		clusterProcessor:     cp,
		routeProcessor:       rp,
		scopedRouteProcessor: srp,
		endpointProcessor:    ep,
		log:                  log,
		dbhandle:             &config.DBHandle,
	}

	go srv.startIngest("/home/tallen/cds.json")
	go srv.startIngest("/home/tallen/lds.json")

	return srv
}

func (srv *server) startIngest(path string) {
	ctx, cancel := context.WithCancel(srv.ctx)
	defer cancel()

	watcher, err := watcher.NewFilesystemResourceWatcher(ctx, srv.log.Named("fs_watcher"), *srv.dbhandle)
	if err != nil {
		srv.log.Fatalw("failed to make fswatcher", "error", err.Error())
	}

	watcher.Start(path)

	<-srv.ctx.Done()
}

// A generic RPC stream.
type Stream interface {
	grpc.ServerStream

	Send(*discovery.DeltaDiscoveryResponse) error
	Recv() (*discovery.DeltaDiscoveryRequest, error)
}

// SotW stream handler for ADS server. This isn't supported.
func (srv *server) StreamAggregatedResources(stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer) error {
	_, _ = stream.Recv()
	return status.Error(codes.Unimplemented, "SotW ADS is only supported for gRPC")
}

func (srv *server) getProcessor(typeURL string) (*protocol.Processor, error) {
	switch typeURL {
	case util.ClusterTypeUrl:
		return srv.clusterProcessor, nil
	case util.ListenerTypeUrl:
		return srv.listenerProcessor, nil
	case util.EndpointTypeUrl:
		return srv.endpointProcessor, nil
	case util.ScopedRouteTypeUrl:
		return srv.scopedRouteProcessor, nil
	case util.RouteTypeUrl:
		return srv.routeProcessor, nil
	default:
		srv.log.Errorw("unknown type url encountered", "url", typeURL)
		return nil, fmt.Errorf("unknown type url %s", typeURL)
	}
}

// Delta stream handler for ADS server.
func (srv *server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	send := make(chan *discovery.DeltaDiscoveryResponse, 1)
	recv := make(chan *discovery.DeltaDiscoveryRequest, 1)
	defer close(recv)

	errchan := make(chan error)

	go srv.clientConnection(stream, send, errchan)

	for {
		ddr, err := stream.Recv()
		if err == io.EOF {
			// The client message stream has ended.
			return nil
		} else if err != nil {
			return err
		}

		p, err := srv.getProcessor(ddr.GetTypeUrl())
		if err != nil {
			return err
		}

		go p.ProcessDeltaDiscoveryRequest(stream.Context(), ddr, stream)
	}
}

func (srv *server) clientConnection(
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer,
	send chan *discovery.DeltaDiscoveryResponse,
	errchan chan error) {

	defer srv.log.Infow("closing client connection")

	for {
		select {
		// Response pending.
		case ddrsp := <-send:
			stream.Send(ddrsp)

		case <-srv.ctx.Done():
			return
		case <-stream.Context().Done():
			return
		}
	}
}
