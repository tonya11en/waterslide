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
	srv := server.NewServer(ctx)
	grpcServer := grpc.NewServer()

	log.Println("sup")

	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)

	log.Println("listening on", listenPort)
	lis, _ := net.Listen("tcp", ":"+listenPort)
	err := grpcServer.Serve(lis)
	if err != nil {
		panic(err.Error())
	}
}
