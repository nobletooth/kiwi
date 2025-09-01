package cache

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeCache is a simple map-based implementation of the Cache interface for testing purposes. It is not thread-safe.
type fakeCache[K comparable, V any] struct {
	items map[K]V
}

// newFakeCache is the constructor for fakeCache.
func newFakeCache[K comparable, V any]() Cache[K, V] {
	return &fakeCache[K, V]{items: make(map[K]V)}
}

// Get retrieves a value from the mock cache.
func (m *fakeCache[K, V]) Get(key K) (V, bool /*found*/) {
	val, found := m.items[key]
	return val, found
}

// Add inserts a key-value pair into the mock cache. It always returns false because as it doesn't support eviction.
func (m *fakeCache[K, V]) Add(key K, value V, _ time.Duration) bool {
	m.items[key] = value
	return false
}

// Keys returns all keys from the mock cache.
func (m *fakeCache[K, V]) Keys() []K {
	return slices.Collect(maps.Keys(m.items))
}

// Purge removes all items from the mock cache.
func (m *fakeCache[K, V]) Purge() {
	m.items = make(map[K]V)
}

// TestShardedCache_AddAndGet verifies the basic Add and Get functionality.
func TestShardedCache_AddAndGet(t *testing.T) {
	sc := NewShardedCache(newFakeCache[string, int], 10)
	t.Run("Add and Get existing key", func(t *testing.T) {
		sc.Add("hello", 123, time.Second)

		got, found := sc.Get("hello")
		assert.True(t, found, "Expected to find key %q", "hello")
		assert.Equal(t, 123, got, "Expected value does not match")
	})
	t.Run("Get non-existent key", func(t *testing.T) {
		_, found := sc.Get("non-existent")
		assert.False(t, found, "Expected not to find key")
	})
}

// TestShardedCache_KeyTypes tests that different key types are hashed and handled correctly.
func TestShardedCache_KeyTypes(t *testing.T) {
	type testValue struct {
		Name string
		Age  int
	}
	for _, testCase := range []struct {
		name  string
		key   any
		value any
	}{
		{
			name:  "string key",
			key:   "my-string-key",
			value: "a string value",
		},
		{
			name:  "int key",
			key:   42,
			value: 999,
		},
		{
			name:  "bool key",
			key:   true,
			value: false,
		},
		{
			name:  "struct value",
			key:   testValue{Name: "Go", Age: 15},
			value: testValue{Name: "Go", Age: 15},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			switch key := testCase.key.(type) {
			case string:
				sc := NewShardedCache(newFakeCache[string, string], 8)
				sc.Add(key, testCase.value.(string), time.Second)
				got, found := sc.Get(key)
				assert.True(t, found)
				assert.Equal(t, testCase.value, got)
			case int:
				sc := NewShardedCache(newFakeCache[int, int], 8)
				sc.Add(key, testCase.value.(int), time.Second)
				got, found := sc.Get(key)
				assert.True(t, found)
				assert.Equal(t, testCase.value, got)
			case bool:
				sc := NewShardedCache(newFakeCache[bool, bool], 8)
				sc.Add(key, testCase.value.(bool), time.Second)
				got, found := sc.Get(key)
				assert.True(t, found)
				assert.Equal(t, testCase.value, got)
			case testValue:
				sc := NewShardedCache(newFakeCache[testValue, testValue], 8)
				sc.Add(key, testCase.value.(testValue), time.Second)
				got, found := sc.Get(key)
				assert.True(t, found)
				assert.Equal(t, testCase.value, got)
			}
		})
	}
}

func TestShardedCache_Keys(t *testing.T) {
	sc := NewShardedCache(newFakeCache[string, int], 4 /*shardCount*/)
	expectedKeys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for i, key := range expectedKeys {
		sc.Add(key, i, time.Second)
	}
	gotKeys := sc.Keys()
	assert.ElementsMatch(t, expectedKeys, gotKeys)
}

func TestShardedCache_Purge(t *testing.T) {
	sc := NewShardedCache(newFakeCache[int, string], 5)
	keysToAdd := []int{1, 10, 100, 1000}
	for _, key := range keysToAdd {
		sc.Add(key, "some value", time.Second)
	}
	assert.Len(t, sc.Keys(), len(keysToAdd), "Incorrect number of keys before purge")

	// Verify all keys are removed.
	sc.Purge()
	assert.Empty(t, sc.Keys(), "Expected keys to be empty after purge")
	_, found := sc.Get(keysToAdd[0])
	assert.False(t, found, "Expected key to be gone after purge")
}

// TestShardedCache_ShardingDistribution verifies that keys are distributed across multiple shards.
func TestShardedCache_ShardingDistribution(t *testing.T) {
	shardCount := 10
	sc := NewShardedCache(newFakeCache[string, int], shardCount)
	// keyCount should be large enough compared to shardCount so it becomes virtually impossible to have a shard with
	// less than 50% of `keyCount/shardCount` keys.
	keyCount := 100_000
	for i := range keyCount {
		sc.Add(fmt.Sprintf("key-%d", i), i, time.Second)
	}
	for _, shard := range sc.shards {
		assert.True(t, len(shard.Keys()) > keyCount/(2*shardCount),
			"Expected keys in each shard to be at least half the keys compared to the uniform distribution.")
	}
}

// TestShardedCache_ShardMapping tests the hash function mapping to each shard.
func TestShardedCache_ShardMapping(t *testing.T) {
	sc := NewShardedCache(newFakeCache[string, int], 10 /*shardCount*/)
	for i := range 10 {
		sc.Add(fmt.Sprintf("key-%d", i), i, time.Second)
	}
	assert.Empty(t, sc.shards[0].Keys())
	assert.ElementsMatch(t, []string{"key-6"}, sc.shards[1].Keys())
	assert.Empty(t, sc.shards[2].Keys())
	assert.ElementsMatch(t, []string{"key-0", "key-7"}, sc.shards[3].Keys())
	assert.ElementsMatch(t, []string{"key-1", "key-3"}, sc.shards[4].Keys())
	assert.Empty(t, sc.shards[5].Keys())
	assert.ElementsMatch(t, []string{"key-2", "key-5", "key-9"}, sc.shards[6].Keys())
	assert.ElementsMatch(t, []string{"key-4", "key-8"}, sc.shards[7].Keys())
	assert.Empty(t, sc.shards[8].Keys())
	assert.Empty(t, sc.shards[9].Keys())
}
