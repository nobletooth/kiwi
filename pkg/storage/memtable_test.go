package storage

import (
	"testing"

	"github.com/nobletooth/kiwi/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMemTable_Get(t *testing.T) {
	memTable := NewMemTable()
	assert.NotNil(t, memTable)
	_ = memTable.Set([]byte("k"), []byte("v"))

	t.Run("existing_key", func(t *testing.T) {
		val, found := memTable.Get([]byte("k"))
		assert.True(t, found)
		assert.Equal(t, []byte("v"), val)
	})
	t.Run("non_existent_key", func(t *testing.T) {
		val, found := memTable.Get([]byte("non-existent"))
		assert.False(t, found)
		assert.Zero(t, val)
	})
}

func TestMemTable_Set(t *testing.T) {
	utils.SetTestFlag(t, "memtable_flush_size_bytes", "9")
	utils.SetTestFlag(t, "memtable_flush_size", "3")
	memTable := NewMemTable()
	assert.NotNil(t, memTable)

	{ // Set first key.
		shouldFlush := memTable.Set([]byte("a"), []byte("12"))
		// Entries   : 1 < 3
		// Held bytes: len("a") + len("12") = 3 < 9
		assert.False(t, shouldFlush)
		assert.Equal(t, 1, memTable.entries)
		assert.Equal(t, 3, memTable.heldBytes)
	}
	{ // Set second key.
		shouldFlush := memTable.Set([]byte("bb"), []byte("123"))
		// Entries   : 2 < 3
		// Held bytes: 3 + len("bb") + len("123") = 8 < 9
		assert.False(t, shouldFlush)
		assert.Equal(t, 2, memTable.entries)
		assert.Equal(t, 8, memTable.heldBytes)
	}
	{ // Set third key.
		shouldFlush := memTable.Set([]byte("ccc"), []byte("1234"))
		// Entries   : 3 == 3
		// Held bytes: 8 + len("ccc") + len("1234") = 15 > 9
		assert.True(t, shouldFlush)
		assert.Equal(t, 3, memTable.entries)
		assert.Equal(t, 15, memTable.heldBytes)
	}
	{ // Update existing key.
		shouldFlush := memTable.Set([]byte("bb"), []byte("12345"))
		// Entries   : 3 == 3
		// Held bytes: 15 + (len("12345") - len("123")) = 17 > 9
		assert.True(t, shouldFlush)
		assert.Equal(t, 3, memTable.entries)
		assert.Equal(t, 17, memTable.heldBytes)
	}
}

func TestMemTable_Delete(t *testing.T) {
	memTable := NewMemTable()
	assert.NotNil(t, memTable)
	// Set a couple of keys.
	_ = memTable.Set([]byte("a"), []byte("1"))
	_ = memTable.Set([]byte("b"), []byte("2"))
	assert.Equal(t, 2, memTable.entries)
	assert.Equal(t, 4, memTable.heldBytes)

	{ // Get should return the values
		v, found := memTable.Get([]byte("a"))
		assert.True(t, found)
		assert.Equal(t, []byte("1"), v)
		v, found = memTable.Get([]byte("b"))
		assert.True(t, found)
		assert.Equal(t, []byte("2"), v)
	}
	{ // Deleting non-existent key should return error; no side effects on tracked sizes.
		assert.False(t, memTable.Delete([]byte("non_existent")))
		assert.Equal(t, 2, memTable.entries)
		assert.Equal(t, 4, memTable.heldBytes)
	}
	{ // Delete one and verify it's gone; tracked sizes should shrink.
		assert.True(t, memTable.Delete([]byte("a")))
		_, found := memTable.Get([]byte("a"))
		assert.False(t, found)
		assert.Equal(t, 1, memTable.entries)
		assert.Equal(t, 2, memTable.heldBytes)
	}
	{ // Other key remains.
		v, found := memTable.Get([]byte("b"))
		assert.True(t, found)
		assert.Equal(t, []byte("2"), v)
	}
}
