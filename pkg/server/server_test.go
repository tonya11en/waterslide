package server

import (
	ctx "context"
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var lis *bufconn.Listener

func init() {
	const bufSize = 1024 * 1024

	lis = bufconn.Listen(bufSize)
	srv := NewServer(ctx.Background())
	s := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(s, srv)

	go func() {
		err := s.Serve(lis)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}()
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
	bc := getBufconn(t)
	client := discovery.NewAggregatedDiscoveryServiceClient(bc)

	stream, err := client.StreamAggregatedResources(ctx.Background())
	assert.Nil(t, err)

	req := &discovery.DiscoveryRequest{}
	err = stream.Send(req)
	assert.Nil(t, err)

	_, err = stream.Recv()
	assert.Equal(t, status.Code(err), codes.Unimplemented)
	err = stream.CloseSend()
	assert.Nil(t, err)
}
