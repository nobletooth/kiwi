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
	_, err := skipList.Get(42)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestSkipList_CloseNoop(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	assert.NoError(t, skipList.Close())
}

// assertHasKey checks the given `skipList` contains the given `key` corresponding to given `expectedVal`.
func assertHasKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, expectedVal any) {
	t.Helper()
	gotValue, err := skipList.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedVal, gotValue)
}

// setNewKey puts the given `key` and `value` into the `skipList` and asserts that the key was not present before.
func setNewKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, value V) {
	t.Helper()
	exists, err := skipList.Set(key, value)
	assert.Falsef(t, exists, "Expected key %s to be new.", fmt.Sprint(key))
	assert.NoError(t, err)
}

// updateExistingKey updates the `key` with `value` and asserts that the key was present before.
func updateExistingKey[K any, V any](t *testing.T, skipList *SkipList[K, V], key K, value V) {
	t.Helper()
	exists, err := skipList.Set(key, value)
	assert.Truef(t, exists, "Expected key %s to be already exist.", fmt.Sprint(key))
	assert.NoError(t, err)
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
	updateExistingKey(t, skipList, 10, "TEN")
	assertHasKey(t, skipList, 10, "TEN")
}

func TestSkipList_Delete(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	// Deleting a missing key returns ErrKeyNotFound
	assert.ErrorIs(t, skipList.Delete(7), ErrKeyNotFound)

	// Insert some and delete one
	for _, testCase := range []struct {
		k int
		v string
	}{{k: 1, v: "a"}, {k: 2, v: "b"}, {k: 3, v: "c"}} {
		setNewKey(t, skipList, testCase.k, testCase.v)
	}
	assert.NoError(t, skipList.Delete(2))
	_, err := skipList.Get(2)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	// Deleting again should return ErrKeyNotFound.
	assert.ErrorIs(t, skipList.Delete(2), ErrKeyNotFound)
	// Other keys remain.
	assertHasKey(t, skipList, 1, "a")
	assertHasKey(t, skipList, 3, "c")
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
		gotValue, err := skipList.Get(i)
		assert.NoError(t, err)
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
		updateExistingKey(t, skipList, 2, "TWO")
		pairs := slices.Collect(skipList.Iterate())
		assert.Equal(t, "TWO", pairs[1].Value)
	}
}
