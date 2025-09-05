package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemTable_Get(t *testing.T) {
	memTable, err := NewMemTable(0 /*prevTable*/, 1 /*table*/)
	assert.NoError(t, err)
	assert.NoError(t, memTable.Set([]byte("k"), []byte("v")))

	t.Run("existing_key", func(t *testing.T) {
		val, err := memTable.Get([]byte("k"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v"), val)
	})
	t.Run("non_existent_key", func(t *testing.T) {
		_, err := memTable.Get([]byte("non-existent"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	assert.NoError(t, memTable.Close())
}

func TestMemTable_Delete_RemovesKey(t *testing.T) {
	memTable, err := NewMemTable(0 /*prevTable*/, 1 /*table*/)
	assert.NoError(t, err)
	// Set a couple of keys.
	assert.NoError(t, memTable.Set([]byte("a"), []byte("1")))
	assert.NoError(t, memTable.Set([]byte("b"), []byte("2")))

	{ // Get should return the values
		v, err := memTable.Get([]byte("a"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("1"), v)
		v, err = memTable.Get([]byte("b"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("2"), v)
	}
	{ // Delete one and verify it's gone.
		assert.NoError(t, memTable.Delete([]byte("a")))
		_, err := memTable.Get([]byte("a"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
	}
	{ // Other key remains.
		v, err := memTable.Get([]byte("b"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("2"), v)
	}
}
