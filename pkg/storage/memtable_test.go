package storage

import (
	"testing"

	"github.com/nobletooth/kiwi/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMemTable_Get(t *testing.T) {
	memTable, err := NewMemTable(0 /*prevTable*/, 1 /*table*/)
	assert.NoError(t, err)
	_ = memTable.Set([]byte("k"), []byte("v"))

	t.Run("existing_key", func(t *testing.T) {
		val, err := memTable.Get([]byte("k"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v"), val)
	})
	t.Run("non_existent_key", func(t *testing.T) {
		_, err := memTable.Get([]byte("non-existent"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})
}

func TestMemTable_Set(t *testing.T) {
	utils.SetTestFlag(t, "memtable_flush_size_bytes", "9")
	utils.SetTestFlag(t, "memtable_flush_size", "3")
	memTable, err := NewMemTable(0 /*prevTable*/, 1 /*table*/)
	assert.NoError(t, err)

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
	memTable, err := NewMemTable(0 /*prevTable*/, 1 /*table*/)
	assert.NoError(t, err)
	// Set a couple of keys.
	_ = memTable.Set([]byte("a"), []byte("1"))
	_ = memTable.Set([]byte("b"), []byte("2"))
	assert.Equal(t, 2, memTable.entries)
	assert.Equal(t, 4, memTable.heldBytes)

	{ // Get should return the values
		v, err := memTable.Get([]byte("a"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), v)
		v, err = memTable.Get([]byte("b"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("2"), v)
	}
	{ // Deleting non-existent key should return error; no side effects on tracked sizes.
		assert.ErrorIs(t, memTable.Delete([]byte("non_existent")), ErrKeyNotFound)
		assert.Equal(t, 2, memTable.entries)
		assert.Equal(t, 4, memTable.heldBytes)
	}
	{ // Delete one and verify it's gone; tracked sizes should shrink.
		assert.NoError(t, memTable.Delete([]byte("a")))
		_, err := memTable.Get([]byte("a"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		assert.Equal(t, 1, memTable.entries)
		assert.Equal(t, 2, memTable.heldBytes)
	}
	{ // Other key remains.
		v, err := memTable.Get([]byte("b"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("2"), v)
	}
}
