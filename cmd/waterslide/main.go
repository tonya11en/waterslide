package main

import (
	"context"
	"log"
	"net"

	"allen.gg/waterslide/pkg/server"
	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

const (
	listenPort = "8080"
)

func main() {
	log.Println("initializing")
	ctx := context.Background()

	log.Println("creating waterslide server")
	srv := server.NewServer(ctx)

	log.Println("creating gRPC server")
	grpcServer := grpc.NewServer()

	log.Println("registering waterslide server as aggregated discovery service")
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)

	log.Println("listening on", listenPort)
	lis, err := net.Listen("tcp", ":"+listenPort)
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln(err.Error())
	}
}
