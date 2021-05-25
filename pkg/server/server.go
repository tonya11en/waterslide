package server

import (
	"context"
	"io"
	"log"

	"allen.gg/waterslide/pkg/server/protocol"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// An xDS delta protocol server.
type server struct {
	ctx               context.Context
	listenerProcessor *protocol.Processor
	clusterProcessor  *protocol.Processor
	routeProcessor    *protocol.Processor
	endpointProcessor *protocol.Processor
}

func NewServer(ctx context.Context) *server {
	return &server{
		ctx:               context.Background(),
		listenerProcessor: protocol.NewDeltaDiscoveryProcessor(ctx),
		clusterProcessor:  protocol.NewDeltaDiscoveryProcessor(ctx),
		routeProcessor:    protocol.NewDeltaDiscoveryProcessor(ctx),
		endpointProcessor: protocol.NewDeltaDiscoveryProcessor(ctx),
	}
}

// A generic RPC stream.
type Stream interface {
	grpc.ServerStream

	Send(*discovery.DeltaDiscoveryResponse) error
	Recv() (*discovery.DeltaDiscoveryRequest, error)
}

// SotW stream handler for ADS server.
func (srv *server) StreamAggregatedResources(stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer) error {
	_, _ = stream.Recv()
	return status.Error(codes.Unimplemented, "SotW ADS is only supported for gRPC")
}

func (srv *server) getProcessor(ddr *discovery.DeltaDiscoveryRequest) *protocol.Processor {
	switch url := ddr.GetTypeUrl(); url {
	case protocol.ClusterTypeUrl:
		return srv.clusterProcessor
	case protocol.ListenerTypeUrl:
		return srv.listenerProcessor
	case protocol.EndpointTypeUrl:
		return srv.endpointProcessor
	case protocol.RouteTypeUrl:
		return srv.routeProcessor
	default:
		log.Fatalf("unknown type url: %s", url)
	}

	// Make the compiler happy.
	return nil
}

// Delta stream handler for ADS server.
func (srv *server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	first := true
	for {
		ddr, err := stream.Recv()
		if err == io.EOF {
			// The client message stream has ended.
			return nil
		} else if err != nil {
			return err
		}

		if first {
			srv.getProcessor(ddr).ProcessInitialDeltaDiscoveryRequest(ddr, stream)
		} else {
			srv.getProcessor(ddr).ProcessSubsequentDeltaDiscoveryRequest(ddr)
		}
	}
}
