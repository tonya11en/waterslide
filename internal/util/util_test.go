package util

import (
	"testing"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	"github.com/stretchr/testify/assert"
)

func TestVersionCompare(t *testing.T) {
	assert.True(t, IsNewerVersion("11", "1"))
	assert.False(t, IsNewerVersion("1", "11"))
}

func TestResourceNameGet(t *testing.T) {
	c := cluster.Cluster{
		Name: "test123",
	}
	assert.Equal(t, "test123", GetResourceName(&c))
}
