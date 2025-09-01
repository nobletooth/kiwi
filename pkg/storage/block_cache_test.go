package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSharedCache(t *testing.T) {
	cache1 := getSharedCache()
	cache2 := getSharedCache()
	assert.Same(t, cache1, cache2, "Expected both calls to return the same cache instance")
}
