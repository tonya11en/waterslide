// Copyright 2020 Envoyproxy Authors
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
package main

import (
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes"

	bootstrap "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
)

type ResourceManager struct {
	unmarshaler *jsonpb.Unmarshaler

	configFilePath string

	bootstrapProto *bootstrap.Bootstrap

	version uint64
}

// Create a new resource manager struct.
func NewResourceManager(configPath string) (*ResourceManager, error) {
	rm := &ResourceManager{
		unmarshaler: &jsonpb.Unmarshaler{
			AllowUnknownFields: false,
			AnyResolver:        nil,
		},
		configFilePath: configPath,
		version:        0,
	}

	bp, err := rm.loadBootstrapFromFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("error loading bootstrap from file %s: %v+", configPath, err)
	}
	rm.bootstrapProto = bp

	return rm, nil
}

func (rm *ResourceManager) extractListeners(bs *bootstrap.Bootstrap) []types.Resource {
	listeners := []types.Resource{}
	for _, listenerObj := range bs.StaticResources.Listeners {
		listeners = append(listeners, listenerObj)
	}
	return listeners
}

func (rm *ResourceManager) extractRouteConfig(bs *bootstrap.Bootstrap) []types.Resource {
	routeConfig := []types.Resource{}
	for _, listenerObj := range bs.StaticResources.Listeners {
		for _, fchain := range listenerObj.FilterChains {
			for _, filter := range fchain.Filters {
				hcmConfig := &hcm.HttpConnectionManager{}
				err := ptypes.UnmarshalAny(filter.GetTypedConfig(), hcmConfig)
				if err != nil {
					l.Infof("Unable to parse type %s as HCM", filter.GetTypedConfig().TypeUrl)
					continue
				}

				routeConfig = append(routeConfig, hcmConfig.GetRouteConfig())
			}
		}
	}
	return routeConfig
}

func (rm *ResourceManager) extractClusters(bs *bootstrap.Bootstrap) []types.Resource {
	clusters := []types.Resource{}
	for _, c := range bs.StaticResources.Clusters {
		clusters = append(clusters, c)
	}
	return clusters
}

func (rm *ResourceManager) extractEndpoints(bs *bootstrap.Bootstrap) []types.Resource {
	endpoints := []types.Resource{}
	for _, c := range bs.StaticResources.Clusters {
		for _, ep := range c.LoadAssignment.Endpoints {
			for _, lbendpoint := range ep.LbEndpoints {
				endpoints = append(endpoints, lbendpoint.GetEndpoint())
			}
		}
	}
	return endpoints
}

func (rm *ResourceManager) loadBootstrapFromFile(file string) (*bootstrap.Bootstrap, error) {
	fstr, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	bs := bootstrap.Bootstrap{}
	err = jsonpb.UnmarshalString(string(fstr), &bs)
	if err != nil {
		return nil, err
	}

	return &bs, nil
}

func (rm *ResourceManager) GenerateSnapshot() cache.Snapshot {
	bp, err := rm.loadBootstrapFromFile(rm.configFilePath)
	if bp != nil && err != nil {
		l.Errorf("error loading bootstrap from file: %v+", err)
	}
	rm.version += 1
	version := strconv.Itoa(int(rm.version))

	return cache.NewSnapshot(
		version,
		[]types.Resource{},                     // endpoints
		rm.extractClusters(rm.bootstrapProto),  // clusters
		[]types.Resource{},                     // routes
		rm.extractListeners(rm.bootstrapProto), // listeners
		[]types.Resource{},                     // runtimes
		[]types.Resource{},                     // secrets
		//rm.extractEndpoints(rm.bootstrapProto),   // endpoints
		//rm.extractRouteConfig(rm.bootstrapProto), // routes
	)
}
