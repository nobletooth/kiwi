package storage

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList_EmptyGet(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	val, found := skipList.Get(42)
	assert.False(t, found, "Expected key to be missing.")
	assert.Zero(t, val, "Expected previous value to be zero value.")
}

// assertHasKey checks the given `skipList` contains the given `key` corresponding to given `expectedVal`.
func assertHasKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, expectedVal any) {
	t.Helper()
	val, found := skipList.Get(key)
	assert.Truef(t, found, "Expected key %s to be present.", fmt.Sprint(key))
	assert.Equal(t, expectedVal, val)
}

// setNewKey puts the given `key` and `value` into the `skipList` and asserts that the key was not present before.
func setNewKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, value V) {
	t.Helper()
	prevVal, found := skipList.Set(key, value)
	assert.Zero(t, prevVal, "Expected previous value to be zero value.")
	assert.Falsef(t, found, "Expected key %s to be new.", fmt.Sprint(key))
}

// updateExistingKey updates the `key` with `value` and asserts that the key was present before.
func updateExistingKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, value, expectedPrev V) {
	t.Helper()
	prevVal, found := skipList.Set(key, value)
	assert.Equal(t, expectedPrev, prevVal)
	assert.Truef(t, found, "Expected key %s to be already exist.", fmt.Sprint(key))
}

func TestSkipList_SetAndGet_Simple(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	setNewKey(t, skipList, 2, "two")
	setNewKey(t, skipList, 1, "one")
	setNewKey(t, skipList, 3, "three")

	assertHasKey(t, skipList, 1, "one")
	assertHasKey(t, skipList, 2, "two")
	assertHasKey(t, skipList, 3, "three")
}

func TestSkipList_UpdateValue(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	setNewKey(t, skipList, 10, "ten")
	updateExistingKey(t, skipList, 10, "TEN", "ten")
	assertHasKey(t, skipList, 10, "TEN")
}

func TestSkipList_Delete(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	// Deleting a missing key returns ErrKeyNotFound.
	prev, found := skipList.Delete(7)
	assert.Zero(t, prev, "Expected previous value to be zero value.")
	assert.Falsef(t, found, "Expected key %d to be missing.", 7)

	// Insert some and delete one
	for _, testCase := range []struct {
		k int
		v string
	}{{k: 1, v: "a"}, {k: 2, v: "b"}, {k: 3, v: "c"}} {
		setNewKey(t, skipList, testCase.k, testCase.v)
	}
	{ // Deleting twice should return ErrKeyNotFound.
		prev, found := skipList.Delete(2)
		assert.Equal(t, prev, "b")
		assert.True(t, found)
		_, found = skipList.Get(2)
		assert.False(t, found)
		prev, found = skipList.Delete(2)
		assert.Zero(t, prev)
		assert.False(t, found)
	}
	{ // Other keys remain.
		assertHasKey(t, skipList, 1, "a")
		assertHasKey(t, skipList, 3, "c")
	}
}

func TestSkipList_StringKeys(t *testing.T) {
	skipList := NewSkipList[string, int](cmp.Compare)
	setNewKey(t, skipList, "alpha", 1)
	setNewKey(t, skipList, "beta", 2)
	setNewKey(t, skipList, "gamma", 3)
	assertHasKey(t, skipList, "beta", 2)
}

func TestSkipList_BulkInsertAndGet(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	const samples = 200
	for i := 0; i < samples; i++ {
		setNewKey(t, skipList, i, fmt.Sprintf("val-%d", i))
	}
	for i := 0; i < samples; i++ {
		gotValue, found := skipList.Get(i)
		assert.True(t, found)
		assert.Equal(t, fmt.Sprintf("val-%d", i), gotValue)
	}
}

func TestSkipList_IterateCollect(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	// Insert in non-sorted order.
	setNewKey(t, skipList, 3, "three")
	setNewKey(t, skipList, 1, "one")
	setNewKey(t, skipList, 2, "two")

	{ // Keys should be in ascending order with matching values.
		gotPairs := slices.Collect(skipList.Iterate())
		assert.Equal(t, []Pair[int, string]{
			{Key: 1, Value: "one"},
			{Key: 2, Value: "two"},
			{Key: 3, Value: "three"},
		}, gotPairs)
	}
	{ // Updating a key should reflect in iteration.
		updateExistingKey(t, skipList, 2, "TWO", "two")
		pairs := slices.Collect(skipList.Iterate())
		assert.Equal(t, "TWO", pairs[1].Value)
	}
}

func TestSkipList_Scan(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	// Insert in non-sorted order.
	setNewKey(t, skipList, 5, "five")
	setNewKey(t, skipList, 3, "three")
	setNewKey(t, skipList, 1, "one")
	setNewKey(t, skipList, 2, "two")
	setNewKey(t, skipList, 4, "four")

	t.Run("scan_range", func(t *testing.T) {
		// The range end is exclusive, i.e. [2, 4).
		got := slices.Collect(skipList.ScanRange(2 /*start*/, 5 /*end*/))
		expected := []Pair[int, string]{{Key: 2, Value: "two"}, {Key: 3, Value: "three"}, {Key: 4, Value: "four"}}
		assert.Equal(t, expected, got)
	})
	t.Run("scan_from", func(t *testing.T) {
		// Query has no range end.
		got := slices.Collect(skipList.ScanFrom(3 /*start*/))
		expected := []Pair[int, string]{{Key: 3, Value: "three"}, {Key: 4, Value: "four"}, {Key: 5, Value: "five"}}
		assert.Equal(t, expected, got)
	})
}
