package db

import (
	"context"
	"strconv"
	"testing"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"allen.gg/waterslide/internal/util"
)

const (
	testNamespace = "db_test_namespace"
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

func TestIncrementingVersionSchemeBehavior(t *testing.T) {
	cfg := DatabaseHandleConfig{
		Filepath:     "",
		InMemoryMode: true,
		Log:          log,
	}
	handle, err := NewDatabaseHandle(ctx, cfg)
	assert.Nil(t, err)

	turl := "typeurl"
	name := "test_res"

	r0 := discovery.Resource{
		Name:    name,
		Version: "0",
	}

	r1 := discovery.Resource{
		Name:    name,
		Version: "1",
	}

	r2 := discovery.Resource{
		Name:    name,
		Version: "2",
	}

	// Verify no errors when looking for non-existent key.
	r, err := handle.Get(ctx, testNamespace, turl, name)
	assert.Nil(t, err)
	assert.Nil(t, r)

	// Make the initial write of the key/val.
	gsn, err := handle.Put(ctx, testNamespace, turl, &r1)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), gsn)

	// Read back.
	r, err = handle.Get(ctx, testNamespace, turl, name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), r.Gsn())
	assert.Equal(t, []byte("1"), r.Version())

	res, err := util.ResourceProtoFromFlat(r)
	assert.Nil(t, err)
	assert.Equal(t, name, res.GetName())
	assert.Equal(t, "1", res.GetVersion())

	// The upgraded resource should successfully overwrite r1.
	gsn, err = handle.Put(ctx, testNamespace, turl, &r2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), gsn)

	// Read back.
	r, err = handle.Get(ctx, testNamespace, turl, name)
	assert.Nil(t, err)
	res, err = util.ResourceProtoFromFlat(r)
	assert.Nil(t, err)
	assert.Equal(t, name, res.GetName())
	assert.Equal(t, "2", res.GetVersion())

	// Verify conditional write behavior, so r0 should not overwrite r2.
	gsn, err = handle.ConditionalPut(ctx, testNamespace, turl, &r0, func(resourceVersion string) bool {
		return false
	})
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), gsn)

	r, err = handle.Get(ctx, testNamespace, turl, name)
	assert.Nil(t, err)
	res, err = util.ResourceProtoFromFlat(r)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", res.GetName())
	assert.Equal(t, "2", res.GetVersion())

	// Conditional write should succeed now.
	gsn, err = handle.ConditionalPut(ctx, testNamespace, turl, &r0, func(resourceVersion string) bool {
		return true
	})
	assert.Nil(t, err)
	assert.Equal(t, uint64(3), gsn)

	r, err = handle.Get(ctx, testNamespace, turl, name)
	assert.Nil(t, err)
	res, err = util.ResourceProtoFromFlat(r)
	assert.Nil(t, err)
	assert.Equal(t, "test_res", res.GetName())
	assert.Equal(t, "0", res.GetVersion())
}

func TestPrefixScan(t *testing.T) {
	cfg := DatabaseHandleConfig{
		Filepath:     "",
		InMemoryMode: true,
		Log:          log,
	}
	handle, err := NewDatabaseHandle(ctx, cfg)
	assert.Nil(t, err)

	turl := "typeurl"

	// The ordering of the scan is not guaranteed, since a stream iterator is used, so we need to
	// verify values are encountered when read back.
	vmap := make(map[string]struct{})

	for i := 0; i < 10; i++ {
		x := strconv.Itoa(i)
		_, err := handle.Put(ctx, testNamespace, turl, &discovery.Resource{
			Name:    "test_res_" + x,
			Version: x,
		})
		assert.Nil(t, err)
		vmap[x] = struct{}{}
	}

	all, err := handle.GetAll(ctx, testNamespace, "bogus_type_url")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(all))

	all, err = handle.GetAll(ctx, testNamespace, turl)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(all))

	for _, v := range all {
		res, err := util.ResourceProtoFromFlat(v)
		assert.Nil(t, err)

		x := res.GetVersion()
		delete(vmap, x)
		assert.Equal(t, "test_res_"+x, res.GetName())
	}

	assert.Equal(t, 0, len(vmap))
}

func makeRandomResource(b *testing.B, typeURL string) *discovery.Resource {
	b.StopTimer()
	defer b.StartTimer()

	return &discovery.Resource{
		Name:    util.MakeRandomNonce(),
		Version: util.MakeRandomNonce(),
		Aliases: []string{
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
			util.MakeRandomNonce(),
		},
	}
}

func BenchmarkInMemoryPut(b *testing.B) {
	// Use a non-verbose logger.
	logger, err := zap.NewProduction()
	log := logger.Sugar()
	if err != nil {
		panic(err.Error())
	}

	cfg := DatabaseHandleConfig{
		Filepath:     "",
		InMemoryMode: true,
		Log:          log,
	}

	handle, err := NewDatabaseHandle(ctx, cfg)
	if err != nil {
		panic(err.Error())
	}

	turl := "test_type_url"

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		res := makeRandomResource(b, turl)
		handle.Put(context.Background(), "", turl, res)
	}
}
