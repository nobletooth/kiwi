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

// NoOp is a cache layer that doesn't store any items.
// It is used when cache is disabled.
type NoOp[K comparable, V any] struct { // Implements Layer.
}

var _ Layer[int, int] = (*NoOp[int, int])(nil)

// NewNoOp returns a no-operation cache layer that does not store any items.
func NewNoOp[K comparable, V any]() *NoOp[K, V] {
	return &NoOp[K, V]{}
}

// Get always returns false, indicating the key is not found.
func (n *NoOp[K, V]) Get(key K) (V, bool) {
	var zero V
	return zero, false
}

// Add does nothing and always returns false, indicating no item was evicted.
func (n *NoOp[K, V]) Add(key K, value V, ttl time.Duration) bool {
	return false
}

// Keys always returns nil, as there are no keys stored.
func (n *NoOp[K, V]) Keys() []K {
	return nil
}

// Purge does nothing, as there are no items to remove.
func (n *NoOp[K, V]) Purge() {}
