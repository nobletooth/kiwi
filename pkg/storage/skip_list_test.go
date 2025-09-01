package storage

import (
	"cmp"
	"fmt"
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

func assertHasKey[K cmp.Ordered, V any](t *testing.T, skipList *SkipList[K, V], key K, expectedVal any) {
	t.Helper()
	gotValue, err := skipList.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, expectedVal, gotValue)
}

func TestSkipList_SetAndGet_Simple(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	assert.NoError(t, skipList.Set(2, "two"))
	assert.NoError(t, skipList.Set(1, "one"))
	assert.NoError(t, skipList.Set(3, "three"))

	assertHasKey(t, skipList, 1, "one")
	assertHasKey(t, skipList, 2, "two")
	assertHasKey(t, skipList, 3, "three")
}

func TestSkipList_UpdateValue(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	assert.NoError(t, skipList.Set(10, "ten"))
	assert.NoError(t, skipList.Set(10, "TEN")) // update
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
		assert.NoError(t, skipList.Set(testCase.k, testCase.v))
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
	assert.NoError(t, skipList.Set("alpha", 1))
	assert.NoError(t, skipList.Set("beta", 2))
	assert.NoError(t, skipList.Set("gamma", 3))
	assertHasKey(t, skipList, "beta", 2)
}

func TestSkipList_BulkInsertAndGet(t *testing.T) {
	skipList := NewSkipList[int, string](cmp.Compare)
	const samples = 200
	for i := 0; i < samples; i++ {
		assert.NoError(t, skipList.Set(i, fmt.Sprintf("val-%d", i)))
	}
	for i := 0; i < samples; i++ {
		gotValue, err := skipList.Get(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%d", i), gotValue)
	}
}
