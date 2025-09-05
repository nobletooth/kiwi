// Kiwi tables are stored in different formats, one is an LSM tree.
// Log structured merge tree (LSM tree) is a data structure that is optimized for write-heavy workloads.
// It consists of multiple levels of sorted tables, where each level is larger than the previous one.
// New data is first written to an in-memory table (memtable) and then flushed to disk as a sorted string
// table (SSTable). When the memtable is full, it is flushed to disk and a new memtable is created.
// Periodically, the SSTables are merged together to create larger SSTables, which helps to reduce the number of
// SSTables that need to be searched when reading data.

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"github.com/nobletooth/kiwi/pkg/utils"
)

// Opts represents options for storing a key-value pair.
type Opts uint8

// Toggled returns true if any of the given options are toggled in the current options.
func (o Opts) Toggled(opts Opts) bool {
	return o&opts != 0
}

const (
	// TombStone is when a key is deleted. We put a tombstone marker instead of actually deleting the key.
	// This is because in LSM tree, we cannot delete a key from disk immediately, as it may exist in multiple SSTables.
	// Instead, we mark the key as deleted with a tombstone, and during compaction, the key would be removed.
	TombStone Opts = 1 << iota
	// Expirable is when a key has an expiration time set; Expired keys would be removed during compaction.
	Expirable
)

// LSMTree represents a log-structured merge tree (LSM tree) for a specific Kiwi table (Redis db).
type LSMTree struct {
	table           int64        // The Kiwi table ID (Redis db number).
	dir             string       // Path where tables files are stored; ends with table.
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

	lsm := &LSMTree{
		table:           table,
		mux:             sync.RWMutex{},
		memTable:        NewMemTable(),
		latestDiskTable: latestDiskTable,
		diskTables:      diskTables,
		dir:             dir,
	}
	// Close SSTable file descriptors when the LSM tree is garbage collected.
	runtime.SetFinalizer(lsm, func(lsm *LSMTree) { _ = lsm.Close() })

	return lsm, nil
}

func (l *LSMTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("expected a non-empty key")
	}

	l.mux.RLock()
	defer l.mux.RUnlock()
	// First check the memtable.
	if val, exists := l.memTable.Get(key); exists {
		return val, nil
	}
	// Then check the disk tables, starting from the latest one.
	for partId := l.latestDiskTable.header.GetId(); partId > 0; {
		sst, exists := l.diskTables[partId]
		if !exists || sst == nil {
			utils.RaiseInvariant("lsm", "missing_part", "Missing part in LSM tree.", "table", l.table, "part", partId)
			return nil, fmt.Errorf("missing part %d in lsm tree for table %d", partId, l.table)
		}
		val, err := sst.Get(key)
		if errors.Is(err, ErrKeyNotFound) {
			partId = sst.header.GetPrevPart()
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get key from sstable %d: %v", partId, err)
		}
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// Set sets the given key-value pair in the LSM tree.
func (l *LSMTree) Set(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("expected a non-empty key")
	}

	l.mux.Lock()
	defer l.mux.Unlock()

	shouldFlush := l.memTable.Set(key, value)
	if !shouldFlush {
		return nil
	}

	// Memtable is full, flush it to disk.
	prevPartId := l.latestDiskTable.header.GetId()
	nextPartId := prevPartId + 1
	tablePath := filepath.Join(l.dir, fmt.Sprintf("%d.sst", nextPartId))
	if err := writeSSTable(prevPartId, nextPartId, slices.Collect(l.memTable.Pairs()), tablePath); err != nil {
		return fmt.Errorf("failed to flush memtable to disk: %v", err)
	}

	sst, err := NewSSTable(tablePath)
	if err != nil {
		return fmt.Errorf("failed to load newly created sstable %s: %v", tablePath, err)
	}
	if sst.header.GetId() != nextPartId || sst.header.GetPrevPart() != prevPartId {
		utils.RaiseInvariant("lsm", "invalid_part_ids", "Created sstable has invalid part ids.", "table", tablePath)
		return fmt.Errorf("newly created sstable %s has invalid part ids: got (%d<-%d), want (%d<-%d)",
			tablePath, sst.header.GetPrevPart(), sst.header.GetId(), prevPartId, nextPartId)
	}
	l.diskTables[nextPartId] = sst
	l.latestDiskTable = sst
	l.memTable = NewMemTable() // Reset memtable.
	return nil
}

// Close closes every SSTable in the LSM tree.
func (l *LSMTree) Close() error {
	if l == nil {
		return nil
	}

	l.mux.Lock()
	defer l.mux.Unlock()

	var errs error
	for _, sst := range l.diskTables {
		if sst == nil {
			continue
		}
		if err := sst.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}
