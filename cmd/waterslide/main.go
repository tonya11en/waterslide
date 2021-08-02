package main

import (
	"context"
	"flag"
	"net"

	"allen.gg/waterslide/pkg/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

var (
	listenPort = flag.String("port", "8080", "port the server listens on")
)

func main() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}
	log := l.Sugar()

	ctx := context.Background()

	log.Info("creating waterslide server")
	srv := server.NewServer(ctx, log)

	log.Info("creating gRPC server")
	grpcServer := grpc.NewServer()

	log.Info("registering waterslide server as aggregated discovery service")
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)

	log.Infow("listening", "port", *listenPort)
	lis, err := net.Listen("tcp", ":"+(*listenPort))
	if err != nil {
		log.Fatal(err.Error())
	}

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatal(err.Error())
	}
}
