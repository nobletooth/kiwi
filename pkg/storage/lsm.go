// Kiwi tables are stored in different formats, one is an LSM tree.
// Log structured merge tree (LSM tree) is a data structure that is optimized for write-heavy workloads.
// It consists of multiple levels of sorted tables, where each level is larger than the previous one.
// New data is first written to an in-memory table (memtable) and then flushed to disk as a sorted string
// table (SSTable). When the memtable is full, it is flushed to disk and a new memtable is created.
// Periodically, the SSTables are merged together to create larger SSTables, which helps to reduce the number of
// SSTables that need to be searched when reading data.

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/nobletooth/kiwi/pkg/utils"
)

// LSMTree represents a log-structured merge tree (LSM tree) for a specific Kiwi table (Redis db).
type LSMTree struct {
	table           int64        // The Kiwi table ID (Redis db number).
	mux             sync.RWMutex // Protects against race conditions.
	memTable        *MemTable    // Lookups are started from the memtable, and then disk tables.
	latestDiskTable *SSTable     // Disk lookups are started from the latest disk table.
	diskTables      map[ /*partId*/ int64]*SSTable
}

// NewLSMTree is the constructor for LSMTree.
// The given `dataDir` path would be used to store the entire table parts, i.e. the .sst files.
// Each LSM Tree would have its own subdirectory under `dataDir`, named as the table ID.
// For example, if `dataDir` is "/data/kiwi" and the table ID is 0, then the LSM tree would use `/data/kiwi/0`.
func NewLSMTree(dataDir string, table int64) (*LSMTree, error) {
	if table <= 0 {
		return nil, fmt.Errorf("expected positivive table id got %d", table)
	}

	// Make sure directory exists.
	dir := filepath.Join(dataDir, fmt.Sprint(table))
	if dirInfo, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("failed to create lsm tree directory %s: %v", dir, err)
			}
		} else {
			return nil, fmt.Errorf("failed to stat lsm tree directory %s: %v", dir, err)
		}
	} else if !dirInfo.IsDir() {
		return nil, fmt.Errorf("lsm tree path %s is not a directory", dir)
	}

	// Scan for existing .sst files inside the directory.
	diskTables := make(map[ /*partId*/ int64]*SSTable)
	prevPartIds := make(map[int64]struct{}) // To find the latest part.
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() { // Skip dirs.
			return nil
		}
		if filepath.Ext(path) != ".sst" { // Skip non-sst files.
			return nil
		}
		sst, err := NewSSTable(path)
		if err != nil {
			return err
		}
		diskTables[sst.header.GetId()] = sst
		prevPartIds[sst.header.GetPrevPart()] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan lsm tree directory %s: %v", dir, err)
	}

	// All SSTables would be the previous part of some other part, except the latest one.
	var latestDiskTable *SSTable
	for part, _ := range diskTables {
		if _, exists := prevPartIds[part]; !exists {
			if latestDiskTable != nil {
				tail := latestDiskTable.header.GetId()
				utils.RaiseInvariant("lsm", "multi_tail_lsm", "Multiple latest parts found in lsm tree directory.",
					"dir", dir, "partOne", tail, "partTwo", part)
				return nil, fmt.Errorf("multiple tails found in lsm tree directory %s: (%d,%d)", dir, tail, part)
			}
			latestDiskTable = diskTables[part]
		}
	}
	if latestDiskTable == nil && len(diskTables) > 0 {
		// This should never happen, unless the .sst files are corrupted or manually tampered with.
		utils.RaiseInvariant("lsm", "no_tail_lsm", "No latest part found in lsm tree directory.", "dir", dir)
		return nil, fmt.Errorf("no tail found in lsm tree directory %s", dir)
	}

	return &LSMTree{
		table:           table,
		mux:             sync.RWMutex{},
		memTable:        NewMemTable(),
		latestDiskTable: latestDiskTable,
		diskTables:      diskTables,
	}, nil
}
