package storage

import (
	"flag"
	"fmt"
	"log/slog"
	"testing"

	"github.com/nobletooth/kiwi/pkg/cache"
	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/stretchr/testify/assert"
)

func TestGetSharedCache(t *testing.T) {
	cache1 := getSharedCache()
	cache2 := getSharedCache()
	assert.Same(t, cache1, cache2, "Expected both calls to return the same cache instance")
}

func TestNewBlockCache(t *testing.T) {
	t.Run("single_shard", func(t *testing.T) {
		assert.NoError(t, flag.Set("block_cache_shard_count", "1"))
		blockCache := newBlockCache()
		assert.NotNil(t, blockCache)
		_, isSingleShard := blockCache.internalCache.(*cache.HyperClock[dbCacheKey, *kiwipb.DataBlock])
		slog.Error(fmt.Sprintf("%T", blockCache.internalCache))
		assert.True(t, isSingleShard, "Expected single shard cache")
	})
	t.Run("multi_shard", func(t *testing.T) {
		assert.NoError(t, flag.Set("block_cache_shard_count", "10"))
		blockCache := newBlockCache()
		assert.NotNil(t, blockCache)
		_, isMultiShard := blockCache.internalCache.(*cache.Sharded[dbCacheKey, *kiwipb.DataBlock])
		assert.True(t, isMultiShard, "Expected multi shard cache")
	})
}
