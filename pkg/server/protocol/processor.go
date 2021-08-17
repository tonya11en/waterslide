package protocol

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"math/rand"
	"sync/atomic"

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
	ctx                 context.Context
	dbhandle            db.DatabaseHandle
	currentNonceCounter uint64
	log                 *zap.SugaredLogger
	typeURL             string
	clientStateMap      client_state.ClientStateMapping
}

type ProcessorConfig struct {
	Ctx      context.Context
	Log      *zap.SugaredLogger
	TypeURL  string
	Ingest   Ingester
	DBHandle db.DatabaseHandle
}

func NewDeltaDiscoveryProcessor(config ProcessorConfig) (*Processor, error) {
	p := &Processor{
		ctx:                 config.Ctx,
		log:                 config.Log,
		currentNonceCounter: rand.Uint64(),
		typeURL:             config.TypeURL,
		dbhandle:            config.DBHandle,
		clientStateMap: client_state.ClientStateMapping{
			Log: config.Log,
		},
	}

	return p, nil
}

// Creates a new and unique nonce value.
func (p *Processor) newNonce() string {
	i := atomic.AddUint64(&p.currentNonceCounter, 1)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return base64.StdEncoding.EncodeToString(b)
}

func (p *Processor) ProcessDeltaDiscoveryRequest(
	ctx context.Context,
	ddr *discovery.DeltaDiscoveryRequest,
	stream discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) {

	state := p.clientStateMap.GetState(ctx, stream, p.dbhandle)
	p.log.Debugw("received delta discovery request",
		"nonce", ddr.GetResponseNonce(), "is_ack", isAck(ddr), "is_nack", isNack(ddr))

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

	if isNack(ddr) {
		p.log.Errorw("NACK received", "nonce", ddr.GetResponseNonce(), "error_detail", ddr.GetErrorDetail())
		state.MarkNonceStale(ddr.GetResponseNonce())
	}

	if isAck(ddr) {
		p.log.Debugw("ACK received", "nonce", ddr.GetResponseNonce())
		state.MarkNonceStale(ddr.GetResponseNonce())
	}
}

func isAck(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() == nil
}

func isNack(ddrq *discovery.DeltaDiscoveryRequest) bool {
	return len(ddrq.GetResponseNonce()) > 0 && ddrq.GetErrorDetail() != nil
}
