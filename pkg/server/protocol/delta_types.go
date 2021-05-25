package protocol

import (
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

const (
	typeUrlPrefix   = "type.googleapis.com/"
	ClusterTypeUrl  = typeUrlPrefix + "envoy.config.cluster.v3.Cluster"
	ListenerTypeUrl = typeUrlPrefix + "envoy.config.listener.v3.Listener"
	RuntimeTypeUrl  = typeUrlPrefix + "envoy.service.runtime.v3.Runtime"
	EndpointTypeUrl = typeUrlPrefix + "envoy.service.endpoint.v3.ClusterLoadAssignment"
	RouteTypeUrl    = typeUrlPrefix + "envoy.config.route.v3.RouteConfiguration"
)

// Each gRPC stream will continuously consume resources from their own
// subscriber channel. If we want resources to make their way to a client, we
// push them into the corresponding subscriber channel.
type subscriberChannel chan *discovery.Resource

// Clients with a wildcard subscription are stored in these sets, rather than
// adding them to every resource in the resource map.
type wildcardSubscriptions map[subscriberChannel]struct{}

type resourceBundle struct {
	// The resource proto clients would receive.
	resource *discovery.Resource

	// Channels of subscribers to this resource.
	subscribers map[subscriberChannel]struct{}
}

// Map resource name to bundle.
type resourceMap map[string]resourceBundle
