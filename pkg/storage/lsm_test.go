package storage

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLSMTree(t *testing.T) {
	t.Run("empty_dir", func(t *testing.T) {
		lsm, err := NewLSMTree(t.TempDir(), 1 /*tableId*/)
		assert.NoError(t, err)
		assert.NotNil(t, lsm)
		assert.Equal(t, int64(1), lsm.table)
		assert.Nil(t, lsm.latestDiskTable, "Expected a nil SSTable tail")
		assert.Empty(t, lsm.diskTables, "Expected SSTables to be empty")
	})
	t.Run("non_empty_dir", func(t *testing.T) {
		dataDir := t.TempDir()
		table := int64(10)
		tableDir := filepath.Join(dataDir, strconv.FormatInt(table, 10 /*base*/))
		assert.NoError(t, writeSSTable(0 /*prevId*/, 1 /*nextId*/, filepath.Join(tableDir, "1.sst"), []BytePair{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: []byte("k3"), Value: []byte("v3")},
		}))
		assert.NoError(t, writeSSTable(1 /*prevId*/, 2 /*nextId*/, filepath.Join(tableDir, "2.sst"), []BytePair{
			{Key: []byte("k2"), Value: []byte("v1*")},
			{Key: []byte("k1"), Value: []byte("v1*")},
			{Key: []byte("k4"), Value: []byte("v4")},
		}))

		// Create table and make sure the SSTable chain is set up correctly.
		lsm, err := NewLSMTree(dataDir, table)
		assert.NoError(t, err)
		assert.NotNil(t, lsm)
		assert.Equal(t, table, lsm.table)
		// Check latest disk table.
		assert.Equal(t, table, lsm.latestDiskTable.table)
		assert.Equal(t, int64(2), lsm.latestDiskTable.header.GetId())
		// Check all read disk tables.
		assert.Len(t, lsm.diskTables, 2)
		assert.Equal(t, int64(0), lsm.diskTables[1].header.GetPrevPart())
		assert.Equal(t, int64(1), lsm.diskTables[1].header.GetId())
		assert.Equal(t, int64(1), lsm.diskTables[2].header.GetPrevPart())
		assert.Equal(t, int64(2), lsm.diskTables[2].header.GetId())
		assert.Same(t, lsm.latestDiskTable, lsm.diskTables[2])
	})
}
