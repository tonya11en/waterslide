package protocol

import (
	"context"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/pkg/server/protocol/client_state"
)

const (
	CommonNamespace = "namespace"
)

// The processor handles discovery requests and tracks the state associated with each client stream.
type Processor struct {
	ctx            context.Context
	dbhandle       db.DatabaseHandle
	log            *zap.SugaredLogger
	typeURL        string
	clientStateMap client_state.ClientStateMapping
}

type ProcessorConfig struct {
	Ctx      context.Context
	Log      *zap.SugaredLogger
	TypeURL  string
	DBHandle db.DatabaseHandle
}

func NewDeltaDiscoveryProcessor(config ProcessorConfig) (*Processor, error) {
	p := &Processor{
		ctx:      config.Ctx,
		log:      config.Log,
		typeURL:  config.TypeURL,
		dbhandle: config.DBHandle,
		clientStateMap: client_state.ClientStateMapping{
			Log: config.Log,
		},
	}

	return p, nil
}

func (p *Processor) ProcessDeltaDiscoveryRequest(
	ctx context.Context,
	ddr *discovery.DeltaDiscoveryRequest,
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {

	state := p.clientStateMap.GetState(ctx, stream, p.dbhandle)
	p.log.Debugw("received delta discovery request",
		"nonce", ddr.GetResponseNonce(),
		"is_ack", isAck(ddr),
		"is_nack", isNack(ddr),
		"type_url", ddr.GetTypeUrl(),
		"error_detail", ddr.GetErrorDetail(),
		"initial_resource_versions", ddr.GetInitialResourceVersions(),
		"resource_names_subscribe", ddr.GetResourceNamesSubscribe(),
		"resource_names_unsubscribe", ddr.GetResourceNamesUnsubscribe())

	if isNack(ddr) {
		p.log.Errorw("NACK received", "nonce", ddr.GetResponseNonce(), "error_detail", ddr.GetErrorDetail())
		state.MarkNonceStale(ddr.GetResponseNonce())
		return
	}

	if isAck(ddr) {
		p.log.Debugw("ACK received", "nonce", ddr.GetResponseNonce())
		state.MarkNonceStale(ddr.GetResponseNonce())
		return
	}

	state.HaltFlushing()
	defer state.FlushAndResume()

	// Legacy behavior states that wildcard subscriptions may also come in the form of empty
	// subscribe/unsubscribe fields. We'll need to handle this here.
	if !isAck(ddr) && !isNack(ddr) &&
		len(ddr.GetResourceNamesSubscribe()) == 0 && len(ddr.GetResourceNamesUnsubscribe()) == 0 {
		p.log.Debugw("client subscribing to wildcard resource via legacy behavior (empty sub/unsub fields)")
		state.DoSubscribe(CommonNamespace, p.typeURL, "*")
	}

	// Unconditionally handle resource subscriptions.
	for _, subName := range ddr.GetResourceNamesSubscribe() {
		p.log.Debugw("client subscribing to resource", "resource", subName)
		state.DoSubscribe(CommonNamespace, p.typeURL, subName)
	}

	// Unconditionally handle resource subscriptions.
	for _, unsubName := range ddr.GetResourceNamesUnsubscribe() {
		p.log.Debugw("client unsubscribing from resource", "resource", unsubName)
		state.DoUnsubscribe(unsubName)
	}

}

func isAck(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() == nil
}

func isNack(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() != nil
}
