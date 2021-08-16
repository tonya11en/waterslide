package server

import (
	ctx "context"
	"net"
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
	"github.com/stretchr/testify/assert"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

type testCfg struct {
	log    *zap.SugaredLogger
	srv    *server
	handle db.DatabaseHandle
}

func setup() *testCfg {
	l, err := zap.NewDevelopment()
	log := l.Sugar()
	if err != nil {
		panic(err.Error())
	}

	handleConfig := db.DatabaseHandleConfig{
		// Empty filepath makes the DB in-memory for the test.
		Filepath:     "",
		InMemoryMode: true,
		Log:          log,
	}

	handle, err := db.NewDatabaseHandle(ctx.Background(), handleConfig)
	if err != nil {
		log.Fatalw("failed to initialize database handle", "error", err.Error())
	}

	lis = bufconn.Listen(bufSize)
	srv := NewServer(ctx.Background(), log, handle)
	s := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(s, srv)

	go func() {
		err := s.Serve(lis)
		if err != nil {
			log.Fatal(err.Error())
		}
	}()

	return &testCfg{
		log:    log,
		srv:    srv,
		handle: handle,
	}
}

func makeAny(res proto.Message) *anypb.Any {
	ret, err := anypb.New(res)
	if err != nil {
		panic(err.Error())
	}
	return ret
}

func populateDB(cfg *testCfg) {
	// Clusters to populate the DB with.
	clusterResources := []*discovery.Resource{
		{
			Name:    "cluster_1",
			Version: "1",
			Resource: makeAny(
				&cluster.Cluster{
					Name: "test1_cluster_name",
				}),
		},
		{
			Name:    "cluster_2",
			Version: "1",
			Resource: makeAny(
				&cluster.Cluster{
					Name: "test2_cluster_name",
				}),
		},
		{
			Name:    "cluster_3",
			Version: "1",
			Resource: makeAny(
				&cluster.Cluster{
					Name: "test3_cluster_name",
				}),
		},
	}

	for _, res := range clusterResources {
		_, err := cfg.handle.Put(ctx.Background(), "", util.ClusterTypeUrl, res)
		if err != nil {
			panic(err.Error())
		}
	}
}

func bufDialer(ctx.Context, string) (net.Conn, error) {
	return lis.Dial()
}

// Creates a bufconn to use in the ADS clients. This makes it so that the
// network is not involved and we can quickly run tests deterministically.
func getBufconn(t *testing.T) *grpc.ClientConn {
	conn, err := grpc.DialContext(ctx.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

/**
* For an explanation and examples of how the gRPC client/server interaction works, see:
* https://grpc.io/docs/languages/go/basics/#client-side-streaming-rpc-1
 */

// Verify that the SotW wire protocol is not supported and returns an error.
func TestSotWNotSupported(t *testing.T) {
	cfg := setup()
	bc := getBufconn(t)
	client := discovery.NewAggregatedDiscoveryServiceClient(bc)

	stream, err := client.StreamAggregatedResources(ctx.Background())
	assert.Nil(t, err)

	req := &discovery.DiscoveryRequest{}
	cfg.log.Debugw("test")
	err = stream.Send(req)
	assert.Nil(t, err)

	_, err = stream.Recv()
	assert.Equal(t, status.Code(err), codes.Unimplemented)
	err = stream.CloseSend()
	assert.Nil(t, err)
}

func TestBogusResourcetype(t *testing.T) {
	cfg := setup()
	bc := getBufconn(t)
	client := discovery.NewAggregatedDiscoveryServiceClient(bc)

	stream, err := client.DeltaAggregatedResources(ctx.Background())
	assert.Nil(t, err)

	req := &discovery.DeltaDiscoveryRequest{
		TypeUrl: "bogus_type_url",
	}

	cfg.log.Infof("sending the bogus request", "req", req.String())
	err = stream.Send(req)
	cfg.log.Infof("done sending bogus request")
	assert.Nil(t, err)
	_, err = stream.Recv()
	assert.NotNil(t, err)
}

func TestResourceSub(t *testing.T) {
	cfg := setup()
	bc := getBufconn(t)
	client := discovery.NewAggregatedDiscoveryServiceClient(bc)
	stream, err := client.DeltaAggregatedResources(ctx.Background())
	assert.Nil(t, err)

	populateDB(cfg)

	req := &discovery.DeltaDiscoveryRequest{
		TypeUrl:                util.ClusterTypeUrl,
		ResourceNamesSubscribe: []string{"test1", "test2", "test3"},
	}

	cfg.log.Infow("sending the ddrq", "req", req.String())
	err = stream.Send(req)
	cfg.log.Infow("done sending ddrq")
	assert.Nil(t, err)

	ddrsp, err := stream.Recv()
	cfg.log.Infow("received response", "response", ddrsp.String())
	assert.Nil(t, err)
	assert.Equal(t, ddrsp.GetTypeUrl(), util.ClusterTypeUrl)
	assert.Equal(t, len(ddrsp.GetResources()), 3)
}

func TestNonexistentResourceSub(t *testing.T) {
	cfg := setup()
	bc := getBufconn(t)
	client := discovery.NewAggregatedDiscoveryServiceClient(bc)
	stream, err := client.DeltaAggregatedResources(ctx.Background())
	assert.Nil(t, err)

	req := &discovery.DeltaDiscoveryRequest{
		TypeUrl:                util.ClusterTypeUrl,
		ResourceNamesSubscribe: []string{"test1", "test2", "test3"},
	}

	cfg.log.Infow("sending the ddrq", "req", req.String())
	err = stream.Send(req)
	cfg.log.Infow("done sending ddrq")
	assert.Nil(t, err)

	ddrsp, err := stream.Recv()
	cfg.log.Infow("received response", "response", ddrsp.String())
	assert.Nil(t, err)
	assert.Equal(t, ddrsp.GetTypeUrl(), util.ClusterTypeUrl)
	assert.Equal(t, len(ddrsp.GetResources()), 3)
}
