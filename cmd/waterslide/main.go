package main

import (
	"context"
	"flag"
	"net"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/pkg/server"
)

var (
	listenPort = flag.String("port", "8080", "port the server listens on")
	dbFilepath = flag.String("db_path", "/tmp/waterslide_db", "filepath to the database")
	dbInMemory = flag.Bool("db_in_memory", false, "filepath to the database")
)

func main() {
	flag.Parse()

	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}
	log := l.Sugar()

	ctx := context.Background()

	handleConfig := db.DatabaseHandleConfig{
		Filepath:     *dbFilepath,
		Log:          log,
		InMemoryMode: *dbInMemory,
	}
	log.Infow("creating database handle",
		"filepath", handleConfig.Filepath,
		"in_memory_mode", handleConfig.InMemoryMode)
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
