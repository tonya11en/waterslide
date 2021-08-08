package server

import (
	"context"
	"fmt"
	"io"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
	"allen.gg/waterslide/pkg/server/protocol"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
}

// Creates a new server. If the DB filepath is empty, it will make an in-memory DB.
func NewServer(ctx context.Context, log *zap.SugaredLogger, dbFilepath string) *server {
	if log == nil {
		panic("passed in nil logger")
	}

	handleConfig := db.DatabaseHandleConfig{
		Filepath: dbFilepath,
		Log:      log,
	}

	handle, err := db.NewDatabaseHandle(ctx, handleConfig)
	if err != nil {
		log.Fatalw("failed to initialize database handle", "error", err.Error())
	}

	config := protocol.ProcessorConfig{
		Ctx:            ctx,
		Log:            log,
		ResourceStream: make(chan *discovery.Resource),
		Ingest:         &protocol.NoopIngest{},
		DBHandle:       handle,
	}

	config.TypeURL = util.ListenerTypeUrl
	lp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	config.TypeURL = util.ClusterTypeUrl
	cp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	config.TypeURL = util.RouteTypeUrl
	rp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	config.TypeURL = util.ScopedRouteTypeUrl
	srp, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	config.TypeURL = util.EndpointTypeUrl
	ep, err := protocol.NewDeltaDiscoveryProcessor(config)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	return &server{
		ctx:                  ctx,
		listenerProcessor:    lp,
		clusterProcessor:     cp,
		routeProcessor:       rp,
		scopedRouteProcessor: srp,
		endpointProcessor:    ep,
		log:                  log,
	}
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
	recv := make(chan *discovery.DeltaDiscoveryRequest, 1)
	send := make(chan *discovery.DeltaDiscoveryResponse, 1)
	errchan := make(chan error, 1)

	go srv.clientConnection(stream, recv, send, errchan)

	for {
		select {
		case err := <-errchan:
			srv.log.Errorw("returning error before closing client conn", "error", err.Error())
			return err
		default:
		}

		ddr, err := stream.Recv()
		if err == io.EOF {
			// The client message stream has ended.
			return nil
		} else if err != nil {
			return err
		}

		recv <- ddr
	}
}

func (srv *server) sendEmptyResponse(
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer,
	send chan *discovery.DeltaDiscoveryResponse, typeURL string) {

	go func() {
		send <- &discovery.DeltaDiscoveryResponse{
			TypeUrl: typeURL,
		}
	}()
}

func (srv *server) clientConnection(
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer,
	recv chan *discovery.DeltaDiscoveryRequest,
	send chan *discovery.DeltaDiscoveryResponse, errchan chan error) {

	defer srv.log.Errorw("closing client connection")

	s := protocol.NewClientStream(&stream, send)
	for {
		select {
		// Request received.
		case ddrq, ok := <-recv:
			if !ok {
				return
			}

			p, err := srv.getProcessor(ddrq.GetTypeUrl())
			if err != nil {
				// There was some issue getting the processor for the type, so we'll just send an empty
				// response back.
				srv.log.Errorw("scheduling empty response for bogus typeURL", "typeURL", ddrq.GetTypeUrl())
				srv.sendEmptyResponse(stream, send, ddrq.GetTypeUrl())
				continue
			}

			p.ProcessDeltaDiscoveryRequest(srv.ctx, ddrq, s)

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
