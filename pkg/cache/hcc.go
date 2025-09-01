// This module implements an expirable CLOCK cache.
// Eviction Policy (CLOCK Algorithm):
// The cache uses a circular list of entries and a "hand" that sweeps over them. When the cache is full and a new item
// needs to be added, the hand checks the entry it's pointing to:
//   - If the entry's reference bit is 'true', it sets it to 'false' and moves to the next entry.
//     This gives the entry a "second chance".
//   - If the entry's reference bit is 'false', it evicts that entry and replaces it with the new one.
//
// Expiration Policy (TTL with Reaper):
// Entries are given a Time-To-Live (TTL). To manage expirations efficiently, entries are distributed to time-based
// 'buckets'. A background goroutine, the "reaper", periodically wakes up and clears one bucket of all its entries,
// effectively deleting items that have lived past their TTL. This avoids scanning the entire cache for expired items.

package cache

import (
	"context"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nobletooth/kiwi/pkg/types"
	"github.com/nobletooth/kiwi/pkg/utils"
)

// expirableClockCacheEntry represents a single entry in the cache. It contains the key-value pair, metadata for the
// clock algorithm, and expiration details.
type expirableClockCacheEntry[K comparable, V any] struct {
	key   K // The cache key for this entry.
	value V // The data stored for this key.
	// ref is the reference bit for the CLOCK algorithm. A value of 'true' indicates the entry has been recently
	// accessed and should be given a "second chance" before eviction. It's an atomic boolean to allow safe concurrent
	// access from Get and the eviction loop.
	ref       atomic.Bool
	expiresAt time.Time // The timestamp when this entry is considered expired.
}

// getTimeBucket rounds down the timestamp to the last timestamp that the reaper cleared given the tickInterval.
func getTimeBucket(timestamp time.Time, tickInterval time.Duration) time.Time {
	return time.Unix(0, (timestamp.UnixNano()/int64(tickInterval))*int64(tickInterval))
}

// HyperClock is a thread-safe, fixed-capacity, in-memory cache that combines the CLOCK (Second-Chance)
// eviction algorithm with a time-based expiration mechanism.
type HyperClock[K comparable, V any] struct {
	capacity int // Maximum number of entries the cache can hold.
	// hand is the "clock hand" that points to the next candidate for eviction in the circular list.
	hand  *types.LinkedListNode[*expirableClockCacheEntry[K, V]]
	index map[K]*types.LinkedListNode[*expirableClockCacheEntry[K, V]] // Provides lookup for an entry by its key.
	// circularBuffer allows the hand to sweep over keys for the CLOCK eviction.
	circularBuffer *types.LinkedList[*expirableClockCacheEntry[K, V]]
	// expiryBuckets indexes cache entries to allow expiring a batch of keys together.
	expiryBuckets map[time.Time]map[K]*types.LinkedListNode[*expirableClockCacheEntry[K, V]]
	tickInterval  time.Duration // Rate of reaper goroutine removing expired keys.
	reaperHand    time.Time     // Next bucket to be cleared by the reaper goroutine.
	// evictionCallback is an optional callback function that is executed when an entry is evicted. This function is run
	// on key eviction in Add or Purge functions, so it must not be calling any of the cache methods to avoid deadlocks.
	evictionCallback func(K, V)
	mux              sync.RWMutex // Provides thread-safety for concurrent operations on the cache.
}

// NewHyperClock is the constructor for HyperClock. It initializes the cache with the given capacity,
// eviction callback, and tick interval. It also starts the background reaper goroutine for handling expirations.
// NOTE: eviction callback function must not call any of the cache methods or else we'll be having a deadlock.
func NewHyperClock[K comparable, V any](ctx context.Context, capacity int, tickInterval time.Duration,
	evictionCallback func(K, V)) *HyperClock[K, V] {
	// Ensure capacity is at least 1.
	if capacity <= 0 {
		utils.RaiseInvariant("hcc", "negative_cache_capacity",
			"Invalid capacity has been given to clock cache.", "capacity", capacity)
		capacity = 1
	}
	clockCache := &HyperClock[K, V]{
		capacity:         capacity,
		index:            make(map[K]*types.LinkedListNode[*expirableClockCacheEntry[K, V]], capacity),
		circularBuffer:   new(types.LinkedList[*expirableClockCacheEntry[K, V]]),
		expiryBuckets:    make(map[time.Time]map[K]*types.LinkedListNode[*expirableClockCacheEntry[K, V]], capacity),
		tickInterval:     tickInterval,
		reaperHand:       getTimeBucket(time.Now(), tickInterval),
		evictionCallback: evictionCallback,
	}
	// Start the reaper goroutine in the background.
	go clockCache.reaper(ctx, tickInterval)
	return clockCache
}

// Get retrieves a value from the cache for a given key. If the key is found and the entry is not expired, it returns
// the value and true. Accessing an item with Get marks it as recently used by setting its reference bit to true.
func (c *HyperClock[K, V]) Get(key K) (V, bool /*found*/) {
	c.mux.RLock()
	defer c.mux.RUnlock()

	entry, keyExists := c.index[key]
	if !keyExists || time.Now().After(entry.Value.expiresAt) {
		// Entry is not found or has expired.
		return *new(V), false
	}
	// Mark the entry as referenced (give it a second chance).
	entry.Value.ref.Store(true)
	return entry.Value.value, true
}

func (c *HyperClock[K, V]) addEntryToExpiryBucket(entry *types.LinkedListNode[*expirableClockCacheEntry[K, V]]) {
	bucket := getTimeBucket(entry.Value.expiresAt, c.tickInterval)
	if _, bucketExists := c.expiryBuckets[bucket]; !bucketExists {
		c.expiryBuckets[bucket] = make(map[K]*types.LinkedListNode[*expirableClockCacheEntry[K, V]])
	}
	c.expiryBuckets[bucket][entry.Value.key] = entry
}

// Add inserts or updates a key-value pair in the cache. If the key already exists, its value and expiration are
// updated. If the cache is full, it evicts an old entry using the CLOCK algorithm. It returns true if an eviction
// occurred, and false otherwise.
func (c *HyperClock[K, V]) Add(key K, value V, ttl time.Duration) /*evictionOccurred*/ bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	// Update existing entry.
	if entry, keyExists := c.index[key]; keyExists {
		entryValue := entry.Value
		// Remove from the old time bucket before updating.
		delete(c.expiryBuckets[getTimeBucket(entryValue.expiresAt, c.tickInterval)], entryValue.key)
		// Update value, mark as referenced, and reset TTL.
		entryValue.value = value
		entryValue.ref.Store(false)
		entryValue.expiresAt = time.Now().Add(ttl)
		c.addEntryToExpiryBucket(entry)
		return false
	}

	// Add new entry (if cache is not full).
	if c.circularBuffer.Len() < c.capacity {
		entry := c.circularBuffer.PushBack(&expirableClockCacheEntry[K, V]{
			key:       key,
			value:     value,
			expiresAt: time.Now().Add(ttl),
		})
		c.addEntryToExpiryBucket(entry)
		c.index[key] = entry
		// Initialize clock hand if it's the first element.
		if c.hand == nil {
			c.hand = entry
		}
		return false
	}

	// Eviction loop (if cache is full). This loop implements the CLOCK (Second-Chance) algorithm.
	for {
		entry := c.hand
		entryValue := entry.Value
		// Find a victim: an entry that is either unreferenced OR expired.
		if !entryValue.ref.Load() || time.Now().After(entryValue.expiresAt) {
			// Evict this entry. Remove it from the index and its time bucket.
			delete(c.index, entryValue.key)
			delete(c.expiryBuckets[getTimeBucket(entryValue.expiresAt, c.tickInterval)], entryValue.key)
			evictedKey := entryValue.key
			evictedValue := entryValue.value
			// Replace the evicted entry's data with the new data in the same node.
			entryValue.key = key
			entryValue.value = value
			entryValue.ref.Store(false)
			entryValue.expiresAt = time.Now().Add(ttl)
			c.addEntryToExpiryBucket(entry)
			c.index[key] = entry
			// Advance the clock hand.
			next := entry.Next()
			if next == nil { // Wrap around to the front if at the end of the list.
				next = c.circularBuffer.Front()
			}
			c.hand = next
			if c.evictionCallback != nil {
				c.evictionCallback(evictedKey, evictedValue)
			}
			return true
		}
		// If the entry was referenced, give it a second chance by clearing its reference bit.
		entryValue.ref.Store(false)
		// Advance the clock hand to the next element.
		next := entry.Next()
		if next == nil { // Wrap around if at the end.
			next = c.circularBuffer.Front()
		}
		c.hand = next
	}
}

func (c *HyperClock[K, V]) Keys() []K {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return slices.Collect(maps.Keys(c.index))
}

func (c *HyperClock[K, V]) Purge() {
	c.mux.Lock()
	defer c.mux.Unlock()

	for key, bucket := range c.expiryBuckets {
		// Remove all entries from the bucket that is being cleared.
		for _, entryNode := range bucket {
			// Remove the entry from circular buffer and key index.
			evictedKey := entryNode.Value.key
			evictedValue := entryNode.Value.value
			delete(c.index, evictedKey)
			c.circularBuffer.Remove(entryNode)
			if c.evictionCallback != nil {
				c.evictionCallback(evictedKey, evictedValue)
			}
		}
		delete(c.expiryBuckets, key)
	}
	c.hand = nil
}

// reaper is a background goroutine that handles entry expiration. It wakes up at a regular interval and clears an
// entire bucket of entries that are presumed to have expired.
func (c *HyperClock[K, V]) reaper(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			{
				c.mux.Lock()
				// Clear all the expired buckets. There can be more than one expired bucket in case of high CPU usage.
				for c.reaperHand.Before(time.Now()) {
					if bucket, bucketExists := c.expiryBuckets[c.reaperHand]; bucketExists {
						// Remove all entries from the bucket that is being cleared.
						for _, entryNode := range bucket {
							// If the clock hand is pointing to an entry we are about to delete, we must advance it to
							// prevent it from pointing to a removed node.
							if c.hand == entryNode {
								next := entryNode.Next()
								if next == nil { // Wrap around if at the end.
									next = c.circularBuffer.Front()
								}
								c.hand = next
							}
							// Remove the entry from circular buffer and key index.
							delete(c.index, entryNode.Value.key)
							c.circularBuffer.Remove(entryNode)
						}
						delete(c.expiryBuckets, c.reaperHand)
					}
					// Advance the reaper hand to the next bucket for the next cycle.
					c.reaperHand = c.reaperHand.Add(c.tickInterval)
				}
				c.mux.Unlock()
			}
		}
	}
}
