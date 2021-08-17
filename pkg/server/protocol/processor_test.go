package protocol

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"allen.gg/waterslide/internal/db"
	"allen.gg/waterslide/internal/util"
	"allen.gg/waterslide/pkg/server/ingest"
)

type testCfg struct {
	log       *zap.SugaredLogger
	processor *Processor
	handle    db.DatabaseHandle
}

func setup() *testCfg {
	l, err := zap.NewDevelopment()
	log := l.Sugar()
	if err != nil {
		panic(err.Error())
	}

	handleConfig := db.DatabaseHandleConfig{
		// Empty filepath makes the DB in-memory for the test.
		Filepath: "",
		Log:      log,
	}

	handle, err := db.NewDatabaseHandle(context.TODO(), handleConfig)
	if err != nil {
		log.Fatalw("failed to initialize database handle", "error", err.Error())
	}

	cfg := ProcessorConfig{
		Ctx:      context.TODO(),
		Log:      log,
		TypeURL:  util.ClusterTypeUrl,
		Ingest:   &ingest.TestIngest{},
		DBHandle: handle,
	}

	p, err := NewDeltaDiscoveryProcessor(cfg)
	if err != nil {
		panic(err.Error())
	}

	return &testCfg{
		log:       log,
		processor: p,
		handle:    handle,
	}
}

func TestArgsSet(t *testing.T) {
}
