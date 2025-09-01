// This module implements cache sharding which distributes keys uniformly across cache shards. Since each thread-safe
// cache implementation has a mutex to avoid races between reads and writes, sharding helps by distributing the locks.
// In cases where there are multiple goroutines trying to read or write to the sharded cache, each goroutine can only
// lock the shard that their key belongs to and doesn't prevent other goroutines from accessing their intended keys.

package cache

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/nobletooth/kiwi/pkg/utils"
)

// ShardedCache is a cache implementation that distributes keys across multiple underlying cache instances (shards).
// This pattern is used to reduce lock contention and improve concurrency in high-traffic scenarios, as different keys
// can be accessed in parallel on different shards.
type ShardedCache[K comparable, V any] struct {
	shards []Cache[K, V]
	hash   func(key K) uint64 // Helps choose the shards index.
}

// NewShardedCache is the constructor for ShardedCache. It takes a cacheGenerator function, which is responsible for
// creating individual shard instances, and the desired number of shards (shardCount).
func NewShardedCache[K comparable, V any](cacheGenerator func() Cache[K, V], shardCount int) *ShardedCache[K, V] {
	// Ensure there is at least one shard.
	if shardCount <= 0 {
		utils.RaiseInvariant("shard", "negative_shard_count",
			"Invalid capacity has been given to sharded cache.", "shardCount", shardCount)
		shardCount = 1
	}
	shardedCache := &ShardedCache[K, V]{shards: make([]Cache[K, V], shardCount)}
	// Initialize shard instances.
	for i := range shardCount {
		shardedCache.shards[i] = cacheGenerator()
	}
	// Initialize the hash function once to use in getShard.
	switch any(*new(K)).(type) {
	case string:
		shardedCache.hash = func(key K) uint64 {
			// Use the FNV-1a hash algorithm, which is fast and provides good distribution.
			return xxhash.Sum64String(any(key).(string))
		}
	case int:
		shardedCache.hash = func(key K) uint64 {
			var b [8]byte
			// For numeric types, write their binary representation.
			// Since int's size is architecture-dependent, we should cast it to a fixed-size type before hashing.
			binary.LittleEndian.PutUint64(b[:], uint64(any(key).(int)))
			return xxhash.Sum64(b[:])
		}
	case uint:
		shardedCache.hash = func(key K) uint64 {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(any(key).(uint)))
			return xxhash.Sum64(b[:])
		}
	case int32:
		shardedCache.hash = func(key K) uint64 {
			var b [4]byte
			// Fixed-size numeric types can be written directly.
			binary.LittleEndian.PutUint32(b[:], uint32(any(key).(int32)))
			return xxhash.Sum64(b[:])
		}
	case uint32:
		shardedCache.hash = func(key K) uint64 {
			var b [4]byte
			binary.LittleEndian.PutUint32(b[:], any(key).(uint32))
			return xxhash.Sum64(b[:])
		}
	case int64:
		shardedCache.hash = func(key K) uint64 {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(any(key).(int64)))
			return xxhash.Sum64(b[:])
		}
	case uint64:
		shardedCache.hash = func(key K) uint64 {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], any(key).(uint64))
			return xxhash.Sum64(b[:])
		}
	case bool:
		shardedCache.hash = func(key K) uint64 {
			// For booleans, write a single byte (1 for true, 0 for false).
			if any(key).(bool) {
				return xxhash.Sum64([]byte{1})
			} else {
				return xxhash.Sum64([]byte{0})
			}
		}
	default:
		shardedCache.hash = func(key K) uint64 {
			// As a fallback for other types (like structs), use fmt.Sprintf. This is less performant but works for any
			// type that can be printed.
			return xxhash.Sum64String(fmt.Sprintf("%#v", key))
		}
	}
	return shardedCache
}

// getShard determines which shard a given key belongs to. It does this by hashing the key and using the modulo operator
// to map the hash value to a shard index.
func (c *ShardedCache[K, V]) getShard(key K) Cache[K, V] {
	return c.shards[c.hash(key)%uint64(len(c.shards))]
}

// Get finds the appropriate shard for the key and retrieves the value from it.
func (c *ShardedCache[K, V]) Get(key K) (V, bool /*found*/) {
	return c.getShard(key).Get(key)
}

// Add finds the appropriate shard for the key and adds the key-value pair to it.
func (c *ShardedCache[K, V]) Add(key K, value V, ttl time.Duration) /*evictionOccurred*/ bool {
	return c.getShard(key).Add(key, value, ttl)
}

// Keys aggregates the keys from all shards into a single slice. This can be a resource-intensive operation, as it
// requires iterating over every shard and collecting its keys.
func (c *ShardedCache[K, V]) Keys() []K {
	keys := make([]K, 0)
	for _, shard := range c.shards {
		keys = append(keys, shard.Keys()...)
	}
	return keys
}

// Purge clears all items from the cache by calling Purge on every shard.
func (c *ShardedCache[K, V]) Purge() {
	for _, shard := range c.shards {
		shard.Purge()
	}
}
