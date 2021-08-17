package util

import (
	"time"
)

const (
	typeUrlPrefix      = "type.googleapis.com/"
	ClusterTypeUrl     = typeUrlPrefix + "envoy.config.cluster.v3.Cluster"
	ListenerTypeUrl    = typeUrlPrefix + "envoy.config.listener.v3.Listener"
	RuntimeTypeUrl     = typeUrlPrefix + "envoy.service.runtime.v3.Runtime"
	EndpointTypeUrl    = typeUrlPrefix + "envoy.service.endpoint.v3.ClusterLoadAssignment"
	RouteTypeUrl       = typeUrlPrefix + "envoy.config.route.v3.RouteConfiguration"
	ScopedRouteTypeUrl = typeUrlPrefix + "envoy.config.route.v3.ScopedRouteConfiguration"

	UpdateInterval       = 1 * time.Second
	UpdateIntervalJitter = 100 * time.Millisecond

	NumNonceChars = 16
)
