package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHyperClock_AddAndGet(t *testing.T) {
	ctx := context.Background()
	clockCache := NewHyperClock[string, string](ctx, 5, time.Second /*tickInterval*/, nil /*evictionCallback*/)

	wasEvicted := clockCache.Add("key1", "value1", time.Minute)
	assert.False(t, wasEvicted, "Should not evict when cache is not full")

	val, found := clockCache.Get("key1")
	assert.True(t, found, "Should find key1")
	assert.Equal(t, "value1", val, "Should get correct value for key1")

	_, found = clockCache.Get("nonexistent")
	assert.False(t, found, "Should not find a non-existent key")
}

func TestHyperClock_UpdateKey(t *testing.T) {
	ctx := context.Background()
	clockCache := NewHyperClock[string, int](ctx, 2, time.Second /*tickInterval*/, nil /*evictionCallback*/)

	clockCache.Add("key1", 100, time.Minute)
	clockCache.Add("key2", 200, time.Minute)

	wasEvicted := clockCache.Add("key1", 999, time.Minute)
	assert.False(t, wasEvicted, "Should not evict on update")
	val, found := clockCache.Get("key1")
	assert.True(t, found, "Key should be present after update")
	assert.Equal(t, 999, val, "Value should be the updated value")

	_, found = clockCache.Get("key2")
	assert.True(t, found, "Other key should not be affected by an update")
}

func TestHyperClock_EvictionPolicy(t *testing.T) {
	ctx := context.Background()
	clockCache := NewHyperClock[int, string](ctx, 2, time.Second /*tickInterval*/, nil /*evictionCallback*/)

	// Fill the cache.
	clockCache.Add(1, "one", time.Minute)
	clockCache.Add(2, "two", time.Minute)

	// Add a third item, which should trigger an eviction since the cache is full.
	wasEvicted := clockCache.Add(3, "three", time.Minute)
	assert.True(t, wasEvicted, "Should evict when adding to a full cache")
	_, found := clockCache.Get(1)
	assert.False(t, found, "Item 1 should have been evicted")
	_, found = clockCache.Get(2)
	assert.True(t, found, "Item 2 should not be evicted")
	val, found := clockCache.Get(3)
	assert.True(t, found, "Item 3 should be in the cache")
	assert.Equal(t, "three", val, "Item 3 should have the correct value")

	// Adding another item should also trigger eviction.
	wasEvicted = clockCache.Add(4, "four", time.Minute)
	assert.True(t, wasEvicted, "Should evict when adding to a full cache")
	_, found = clockCache.Get(2)
	assert.False(t, found, "Item 2 should have been evicted")
	_, found = clockCache.Get(3)
	assert.True(t, found, "Item 3 should not be evicted")
	val, found = clockCache.Get(4)
	assert.True(t, found, "Item 4 should be in the cache")
	assert.Equal(t, "four", val, "Item 4 should have the correct value")
}

func TestHyperClock_EvictionCallback(t *testing.T) {
	var evictedKey int
	var evictedValue string
	var mu sync.Mutex

	evictionCallback := func(k int, v string) {
		mu.Lock()
		defer mu.Unlock()
		evictedKey = k
		evictedValue = v
	}

	ctx := context.Background()
	clockCache := NewHyperClock[int, string](ctx, 1, time.Second /*tickInterval*/, evictionCallback)

	// Fill the cache.
	clockCache.Add(10, "ten", time.Minute)

	// This Add will trigger the eviction of key 10.
	clockCache.Add(20, "twenty", time.Minute)

	time.Sleep(10 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 10, evictedKey, "Evicted key should be 10")
	assert.Equal(t, "ten", evictedValue, "Evicted value should be 'ten'")
}

func TestHyperClock_GetExpired(t *testing.T) {
	ctx := context.Background()
	clockCache := NewHyperClock[string, int](ctx, 5, time.Millisecond /*tickInterval*/, nil /*evictionCallback*/)

	clockCache.Add("key1", 1, 20*time.Millisecond)

	// Wait for the item to expire.
	time.Sleep(25 * time.Millisecond)

	_, found := clockCache.Get("key1")
	assert.False(t, found, "Should not find an expired item")
}

func TestHyperClock_Reaper(t *testing.T) {
	ctx := context.Background()
	clockCache := NewHyperClock[string, int](ctx, 10, time.Millisecond /*tickInterval*/, nil /*evictionCallback*/)

	clockCache.Add("key1", 1, 50*time.Millisecond)
	clockCache.Add("key2", 2, 60*time.Millisecond)

	// Wait long enough for the reaper to clear all buckets.
	time.Sleep(70 * time.Millisecond)

	// Verify items are gone by trying to Get them.
	_, found := clockCache.Get("key1")
	assert.False(t, found, "Key1 should have been removed by the reaper")
	_, found = clockCache.Get("key2")
	assert.False(t, found, "Key2 should have been removed by the reaper")
}

func TestHyperClock_Concurrency(t *testing.T) {
	numGoroutines := 50
	itemsPerGoroutine := 50

	ctx := context.Background()
	clockCache := NewHyperClock[string, int](ctx, 1000, time.Second /*tickInterval*/, nil /*evictionCallback*/)
	var wg sync.WaitGroup

	// Concurrent writers.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				clockCache.Add(fmt.Sprintf("key-%d-%d", goroutineID, j), goroutineID*100+j, time.Minute)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent readers.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				// We can't guarantee the key is still present due to evictions from other goroutines,
				// but if it is found, its value must be correct.
				if val, found := clockCache.Get(fmt.Sprintf("key-%d-%d", goroutineID, j)); found {
					assert.Equal(t, goroutineID*100+j, val, "Concurrent Get should return the correct value")
				}
			}
		}(i)
	}
	wg.Wait()
}
