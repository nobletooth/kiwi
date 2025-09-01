// Kiwi caches block reads to reduce IO operations for frequently accessed data blocks.
// Cache is enabled by default but users may decide to disable the cache or adjust its capacity.

package storage

import (
	"context"
	"flag"
	"runtime"
	"sync"
	"time"

	"github.com/nobletooth/kiwi/pkg/cache"
	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// initCacheOnce ensures cache is reused across multiple SSTables.
	initCacheOnce sync.Once
	sharedCache   *BlockCache

	cacheEnabled  = flag.Bool("enable_block_cache", true, "Enable the shared block cache.")
	cacheCapacity = flag.Int("block_cache_capacity", 5,
		"The maximum number of blocks to keep in the shared block cache; 0 or negative disables the cache.")
	cacheShardCount = flag.Int("block_cache_shard_count", runtime.NumCPU(),
		"The number of shards to keep in the block cache; 0 or negative disables the cache.")
	cacheTtl = flag.Duration("block_cache_ttl", 5*time.Minute,
		"The TTL for each block entry in the shared block cache.")
	cacheTickInterval = flag.Duration("block_cache_tick_interval", 1*time.Second,
		"The clock tick interval for the shared block cache.")

	cacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "block_cache_lookups_total",
		Help: "Total number of block cache lookups.",
	}, []string{"status" /* hit | miss */})
	cacheEvictedBlocks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "block_cache_evicted_blocks_total",
		Help: "Total number of block cache evictions.",
	})
	cacheEvictedKeys = promauto.NewCounter(prometheus.CounterOpts{
		Name: "block_cache_evicted_keys_total",
		Help: "Total number of block cache evictions.",
	})
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
	// newCache builds a new hyper clock cache according to configured flags.
	newCache := func() cache.Layer[dbCacheKey, *kiwipb.DataBlock] {
		return cache.NewHyperClock(context.Background(), *cacheCapacity, *cacheTickInterval,
			func(k dbCacheKey, v *kiwipb.DataBlock) {
				cacheEvictedBlocks.Inc()
				cacheEvictedKeys.Add(float64(len(v.Keys)))
			},
		)
	}

	var cacheLayer cache.Layer[dbCacheKey, *kiwipb.DataBlock] = cache.NewNoOp[dbCacheKey, *kiwipb.DataBlock]()
	if *cacheEnabled && *cacheCapacity > 0 && *cacheShardCount > 0 {
		if *cacheShardCount > 1 { // Sharded cache.
			cacheLayer = cache.NewSharded(newCache, *cacheShardCount)
		} else if *cacheShardCount == 1 { // Single shard cache.
			cacheLayer = newCache()
		}
	}

	return &BlockCache{internalCache: cacheLayer}
}

// Get retrieves a data block from the cache.
func (p *BlockCache) Get(table, ssTableId, offset int64) (*kiwipb.DataBlock, bool) {
	db, found := p.internalCache.Get(dbCacheKey{table: table, ssTableId: ssTableId, offset: offset})
	if found {
		cacheLookups.WithLabelValues("hit").Inc()
	} else {
		cacheLookups.WithLabelValues("miss").Inc()
	}
	return db, found
}

// Set adds a data block to the cache.
func (p *BlockCache) Set(table, ssTableId, offset int64, block *kiwipb.DataBlock) {
	p.internalCache.Add(dbCacheKey{table: table, ssTableId: ssTableId, offset: offset}, block, *cacheTtl)
}
