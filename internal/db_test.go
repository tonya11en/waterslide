package db

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/stretchr/testify/assert"
)

func initTestfile() string {
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		panic(err.Error())
	}

	return tmpfile.Name()
}

func TestIncrementingVersionSchemeBehavior(t *testing.T) {
	filename := initTestfile()
	defer os.Remove(filename)
	config := DatabaseHandleConfig{
		Filepath: filename,
	}

	handle, err := NewDatabaseHandle(context.Background(), config)
	assert.NotNil(t, err)

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

	assert.NotNil(t, handle.Put(&r1, turl))
	r, err := handle.Get("test_res", turl)
	assert.NotNil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "1", r.GetVersion())

	assert.NotNil(t, handle.Put(&r2, turl))
	r, err = handle.Get("test_res", turl)
	assert.NotNil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "2", r.GetVersion())

	// By default, the incrementing version scheme is enabled.
	assert.NotNil(t, handle.Put(&r0, turl))
	r, err = handle.Get("test_res", turl)
	assert.NotNil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "2", r.GetVersion())

	// Disable incrementing version scheme.
	*incrementingVersionScheme = false

	assert.NotNil(t, handle.Put(&r0, turl))
	r, err = handle.Get("test_res", turl)
	assert.NotNil(t, err)
	assert.Equal(t, "test_res", r.GetName())
	assert.Equal(t, "0", r.GetVersion())
}
