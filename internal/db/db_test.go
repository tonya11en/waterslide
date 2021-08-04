package db

import (
	"context"
	"strconv"
	"testing"

	badger "github.com/dgraph-io/badger/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	log    *zap.SugaredLogger
	ctx    context.Context
)

func init() {
	var err error

	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}

	log = logger.Sugar()

	ctx = context.Background()
}

// Makes an in-memory DB for the tests.
func newTestDatabaseHandle() (DatabaseHandle, error) {
	var err error

	handle := &dbHandle{
		ctx: context.Background(),
		log: log,
	}

	opts := badger.DefaultOptions("").WithInMemory(true)
	handle.db, err = badger.Open(opts)

	return handle, err
}

func TestIncrementingVersionSchemeBehavior(t *testing.T) {
	handle, err := newTestDatabaseHandle()
	assert.Nil(t, err)

	turl := "typeurl"

	r0 := discovery.Resource{
		Name:    "test_res",
		Version: "0",
	}

	r1 := discovery.Resource{
		Name:    "test_res",
		Version: "1",
	}

	r2 := discovery.Resource{
		Name:    "test_res",
		Version: "2",
	}

	assert.Nil(t, handle.Put(ctx, &r1, turl))
	r, err := handle.Get(ctx, "test_res", turl)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "1", r.GetVersion())

	// The upgraded resource should successful overwrite r1.
	assert.Nil(t, handle.Put(ctx, &r2, turl))
	r, err = handle.Get(ctx, "test_res", turl)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "2", r.GetVersion())

	// By default, the incrementing version scheme is enabled, so r0 should not overwrite r2.
	assert.Nil(t, handle.Put(ctx, &r0, turl))
	r, err = handle.Get(ctx, "test_res", turl)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "2", r.GetVersion())

	// Disable incrementing version scheme.
	*incrementingVersionScheme = false

	assert.Nil(t, handle.Put(ctx, &r0, turl))
	r, err = handle.Get(ctx, "test_res", turl)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "0", r.GetVersion())
}

func TestPrefixScan(t *testing.T) {
	handle, err := newTestDatabaseHandle()
	assert.Nil(t, err)

	turl := "typeurl"

	for i := 0; i < 1000; i++ {
		x := strconv.Itoa(i)
		err := handle.Put(ctx, &discovery.Resource{
			Name:    "test_res_" + x,
			Version: "0",
		}, turl)
		assert.Nil(t, err)
	}

	all, err := handle.GetAll(ctx, "bogus_type_url")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(all))

	all, err = handle.GetAll(ctx, turl)
	assert.Nil(t, err)
	assert.Equal(t, 1000, len(all))
}
