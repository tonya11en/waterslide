package main

import (
	"context"
	"flag"
	"net"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/pkg/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

var (
	listenPort = flag.String("port", "8080", "port the server listens on")
	dbFilepath = flag.String("db_path", "/tmp/waterslide_db", "filepath to the database")
)

func main() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}
	log := l.Sugar()

	ctx := context.Background()

	log.Info("creating database handle")
	handleConfig := db.DatabaseHandleConfig{
		Filepath: *dbFilepath,
		Log:      log,
	}
	handle, err := db.NewDatabaseHandle(ctx, handleConfig)
	if err != nil {
		log.Fatalw("failed to initialize database handle", "error", err.Error())
	}

	log.Info("creating waterslide server")
	srv := server.NewServer(ctx, log, handle)

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
