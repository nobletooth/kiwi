package storage

import (
	"bytes"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSTable ensures basic functionality of SSTable operations and their integration.
func TestSSTable(t *testing.T) {
	const tableId = 1
	resultFile := filepath.Join(t.TempDir(), strconv.Itoa(tableId), "test.sst")
	data := []BytePair{
		{Key: []byte("zed"), Value: []byte("editor")},
		{Key: []byte("apple"), Value: []byte("fruit")},
		{Key: []byte("carrot"), Value: []byte("vegetable")},
		{Key: []byte("banana"), Value: []byte("fruit")},
		{Key: []byte("zebra"), Value: []byte("mammal")},
		{Key: []byte("broccoli"), Value: []byte("vegetable")},
		{Key: []byte("cherry"), Value: []byte("fruit")},
		{Key: []byte("charlie"), Value: []byte("chaplin")},
		{Key: []byte("charlotte"), Value: []byte("female")},
		{Key: []byte("bruce"), Value: []byte("banner")},
	}
	// Ensure data is sorted by key before writing to SSTable.
	slices.SortFunc(data, func(a, b BytePair) int { return bytes.Compare(a.Key, b.Key) })
	err := writeSSTable( /*prevId*/ 0, data, resultFile)
	require.NoError(t, err)

	sst, err := NewSSTable(resultFile)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, sst.Close()) })

	assert.Equal(t, int64(tableId), sst.Table(), "Expected correct table ID")
	t.Run("existing_keys", func(t *testing.T) {
		for _, pair := range data {
			gotValue, err := sst.Get(pair.Key)
			assert.NoError(t, err)
			assert.Equal(t, pair.Value, gotValue)
		}
	})
	t.Run("non_existent_keys", func(t *testing.T) {
		for _, key := range [][]byte{[]byte("notfound"), []byte("never"), []byte("404")} {
			gotValue, err := sst.Get(key)
			assert.ErrorIs(t, err, ErrKeyNotFound)
			assert.Nil(t, gotValue, "Expected nil for non-existent key: %s", key)
		}
	})
	t.Run("bf_index_filled", func(t *testing.T) {
		for _, pair := range data {
			assert.True(t, sst.bloomFilter.Test(pair.Key), "Bloom filter should contain key: %s", pair.Key)
		}
	})
	t.Run("skip_index_filled", func(t *testing.T) {
		assert.Equal(t, "apple", string(sst.header.GetSkipIndex().GetFirstKeys()[0]),
			"First skip index key should be 'apple'")
		assert.Equal(t, "zed", string(sst.header.GetSkipIndex().GetLastKey()),
			"Last skip index key should be 'zed'")
	})
}
