package util

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	auth "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	runtime "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"allen.gg/waterslide/internal/db/flatbuffers/waterslide_bufs"
)

// Compares the resource versions and decides if the new resource is actually "newer". Note that
// this is not part of the xDS protocol, the version is just an arbitrary string.
func IsNewerVersion(new string, existing string) bool {
	if existing == "" {
		return true
	}

	newInt, err := strconv.Atoi(new)
	if err != nil {
		panic("bogus resource version error encountered when expecting incrementing version scheme: " + err.Error())
	}

	existingInt, err := strconv.Atoi(existing)
	if err != nil {
		panic("bogus resource version error encountered when expecting incrementing version scheme: " + err.Error())
	}

	return newInt > existingInt
}

// Unmarshals a resource proto from a flatbuffer resource.
func ResourceProtoFromFlat(fbuf *waterslide_bufs.Resource) (*discovery.Resource, error) {
	if fbuf == nil {
		return nil, fmt.Errorf("nil flatbuffer passed")
	}

	var res discovery.Resource
	err := proto.Unmarshal(fbuf.ResourceProto(), &res)
	return &res, err
}

// GetResourceName returns the resource name for a valid xDS response type.
func GetResourceName(res proto.Message) string {
	switch v := res.(type) {
	case *endpoint.ClusterLoadAssignment:
		return v.GetClusterName()
	case *cluster.Cluster:
		return v.GetName()
	case *route.RouteConfiguration:
		return v.GetName()
	case *listener.Listener:
		return v.GetName()
	case *auth.Secret:
		return v.GetName()
	case *runtime.Runtime:
		return v.GetName()
	case *core.TypedExtensionConfig:
		// This is a V3 proto, but this is the easiest workaround for the fact that there is no V2 proto.
		return v.GetName()
	default:
		panic("unhandled resource type")
	}
}

// Creates a resource proto from an Any type.
func CreateResource(anyRes *anypb.Any) (ret discovery.Resource, err error) {
	// TODO: use a real version.
	m, err := anyRes.UnmarshalNew()
	if err != nil {
		return
	}

	ret.Version = time.Now().String()
	ret.Resource = anyRes
	ret.Name = GetResourceName(m)

	return ret, err
}

// Creates a resource proto from raw bytes.
func CreateResourceFromBytes(b []byte) (*discovery.Resource, error) {
	var ret discovery.Resource
	err := proto.Unmarshal(b, &ret)
	return &ret, err
}

func MakeRandomNonce() string {
	b := make([]byte, NumNonceChars)
	_, err := rand.Read(b)
	if err != nil {
		panic("failed to read from rand")
	}
	return base64.StdEncoding.EncodeToString(b)
}
