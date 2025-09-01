// Kiwi caches pages / lookup results in memory to avoid repeated calls to disk.
// This module provides an interface on caching, making single shard cache
// and multi shard caches have the same API.

package cache

import "time"

// Layer defines the interface for a generic key-value cache. This allows different cache implementations
// (e.g., LRU, LFU, simple map-based) to be used as shards within the Sharded.
type Layer[K comparable, V any] interface {
	// Get returns value from cache for given key and a boolean indicating whether key was found.
	Get(key K) (V, bool)
	// Add inserts a key-value pair into the cache with the given TTL. It returns true if an item was evicted.
	Add(key K, value V, ttl time.Duration) bool
	Keys() []K // Returns a slice of all keys currently in the cache.
	Purge()    // Removes all items from the cache.
}
