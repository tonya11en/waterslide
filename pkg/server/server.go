package server

import (
	"context"
	"fmt"
	"io"

	"allen.gg/waterslide/pkg/server/protocol"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
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
	log               *zap.SugaredLogger
}

func NewServer(ctx context.Context) *server {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}
	log := l.Sugar()

	lp, err := protocol.NewDeltaDiscoveryProcessor(ctx, log)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	cp, err := protocol.NewDeltaDiscoveryProcessor(ctx, log)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	rp, err := protocol.NewDeltaDiscoveryProcessor(ctx, log)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	ep, err := protocol.NewDeltaDiscoveryProcessor(ctx, log)
	if err != nil {
		log.Fatal("unable to create delta discovery processor", "error", err)
	}

	return &server{
		ctx:               context.Background(),
		listenerProcessor: lp,
		clusterProcessor:  cp,
		routeProcessor:    rp,
		endpointProcessor: ep,
		log:               log,
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

func (srv *server) getProcessor(ddr *discovery.DeltaDiscoveryRequest) (*protocol.Processor, error) {
	switch url := ddr.GetTypeUrl(); url {
	case protocol.ClusterTypeUrl:
		return srv.clusterProcessor, nil
	case protocol.ListenerTypeUrl:
		return srv.listenerProcessor, nil
	case protocol.EndpointTypeUrl:
		return srv.endpointProcessor, nil
	case protocol.RouteTypeUrl:
		return srv.routeProcessor, nil
	default:
		srv.log.Errorw("unknown type url encountered", "url", url)
		return nil, fmt.Errorf("unknown type url %s", url)
	}

	// Make the compiler happy.
	panic("inaccessible region")
}

// Delta stream handler for ADS server.
func (srv *server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	first := true
	subCh := make(chan *discovery.Resource)
	for {
		ddr, err := stream.Recv()
		if err == io.EOF {
			// The client message stream has ended.
			return nil
		} else if err != nil {
			return err
		}

		p, err := srv.getProcessor(ddr)
		if err != nil {
			return err
		}

		if first {
			p.ProcessInitialDeltaDiscoveryRequest(srv.ctx, ddr, &stream, subCh)
		} else {
			p.ProcessSubsequentDeltaDiscoveryRequest(srv.ctx, ddr, &stream, subCh)
		}
	}
}
