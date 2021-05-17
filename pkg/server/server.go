package server

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// An xDS delta protocol server.
type server struct {
}

func NewServer(ctx context.Context) *server {
	return &server{}
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

// Delta stream handler for ADS server.
func (srv *server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	for {
		ddr, err := stream.Recv()
		if err == io.EOF {
			// The client message stream has ended.
			return nil
		} else if err != nil {
			return err
		}

		err = srv.processDeltaDiscoveryRequest(ddr)
		if err != nil {
			return err
		}
	}
}

func (srv *server) processDeltaDiscoveryRequest(ddr *discovery.DeltaDiscoveryRequest) error {
	fmt.Println("@tallen", ddr.String())
	return nil
}
