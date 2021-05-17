package server

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// An xDS delta protocol server.
type server struct {
}

func NewServer(ctx context.Context) *server {
	return nil
}

// A generic RPC stream.
type Stream interface {
	grpc.ServerStream

	Send(*discovery.DeltaDiscoveryResponse) error
	Recv() (*discovery.DeltaDiscoveryRequest, error)
}

// SotW stream for ADS server.
func (srv *server) StreamAggregatedResources(stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer) error {
	return fmt.Errorf("SotW stream is not supported/implemented")
}

// Delta stream for ADS server.
func (srv *server) DeltaAggregatedResources(stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	return srv.adsStreamHandler(stream)
}

// Handler for the delta ADS stream.
func (srv *server) adsStreamHandler(stream Stream) error {
	log.Println("handling stream! It's alive!")
	return nil
}
