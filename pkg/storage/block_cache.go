package storage

import (
	"context"
	"flag"
	"runtime"
	"sync"
	"time"

	"github.com/nobletooth/kiwi/pkg/cache"
	kiwipb "github.com/nobletooth/kiwi/proto"
)

var (
	// initCacheOnce ensures cache is reused across multiple SSTables.
	initCacheOnce sync.Once
	sharedCache   *BlockCache

	cacheCapacity = flag.Int("block_cache_capacity", 1000,
		"The maximum number of blocks to keep in the shared block cache.")
	cacheShardCount = flag.Int("block_cache_shard_count", runtime.NumCPU(),
		"The number of shards to keep in the block cache.")
	cacheTtl = flag.Duration("block_cache_ttl", 5*time.Minute,
		"The TTL for each block entry in the shared block cache.")
	cacheTickInterval = flag.Duration("block_cache_tick_interval", 1*time.Second,
		"The clock tick interval for the shared block cache.")
)

// dbCacheKey is the cache key for a data block in the BlockCache.
type dbCacheKey struct{ table, ssTableId, offset int64 }

// getSharedCache returns the singleton shared block cache instance.
func getSharedCache() *BlockCache {
	initCacheOnce.Do(func() { sharedCache = newBlockCache() })
	return sharedCache
}

// BlockCache is an in-memory cache that reduces disk reads for frequently accessed data blocks.
type BlockCache struct {
	internalCache cache.Layer[dbCacheKey, *kiwipb.DataBlock]
}

// newBlockCache instantiates a new BlockCache.
func newBlockCache() *BlockCache {
	var cacheLayer cache.Layer[dbCacheKey, *kiwipb.DataBlock]
	evictionCallback := func(k dbCacheKey, v *kiwipb.DataBlock) {
		// No special cleanup needed for DataBlock.
		// TODO: Export eviction metrics.
	}
	if *cacheShardCount > 1 {
		cacheLayer = cache.NewSharded(func() cache.Layer[dbCacheKey, *kiwipb.DataBlock] {
			return cache.NewHyperClock(context.Background(), *cacheCapacity, *cacheTickInterval, evictionCallback)
		}, *cacheShardCount)
	} else {
		cacheLayer = cache.NewHyperClock(context.Background(), *cacheCapacity, *cacheTickInterval, evictionCallback)
	}
	return &BlockCache{internalCache: cacheLayer}
}

func (p *BlockCache) Get(table, ssTableId, offset int64) (*kiwipb.DataBlock, bool) {
	return p.internalCache.Get(dbCacheKey{table: table, ssTableId: ssTableId, offset: offset})
}

func (p *BlockCache) Set(table, ssTableId, offset int64, block *kiwipb.DataBlock) {
	p.internalCache.Add(dbCacheKey{table: table, ssTableId: ssTableId, offset: offset}, block, *cacheTtl)
}
