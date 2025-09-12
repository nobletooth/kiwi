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
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"github.com/nobletooth/kiwi/pkg/utils"
)

// LSMTree represents a log-structured merge tree (LSM tree) for a specific Kiwi table (Redis db).
type LSMTree struct { // Implements KeyValueHolder.
	table           int64        // The Kiwi table ID (Redis db number).
	dir             string       // Path where tables files are stored; ends with table.
	mux             sync.RWMutex // Protects against race conditions.
	memTable        *MemTable    // Lookups are started from the memtable, and then disk tables.
	latestDiskTable *SSTable     // Disk lookups are started from the latest disk table.
	diskTables      map[ /*partId*/ int64]*SSTable
}

var _ KeyValueHolder = (*LSMTree)(nil)

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
		if _, hasPrevPart := prevPartIds[part]; !hasPrevPart {
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

// lookupDiskTables finds the value of the given key. NOTE: Caller should acquire lock.
func (l *LSMTree) lookupDiskTables(key []byte) ([]byte, error) {
	// Before any memtable is flushed, there are no disk tables, hence we'd short circuit here.
	if l.latestDiskTable == nil {
		return nil, ErrKeyNotFound
	}

	// Since the latest parts contain the most recent values, we'll start our lookup from there.
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
			return nil, fmt.Errorf("failed to lookupDiskTables key from sstable %d: %v", partId, err)
		}
		return val, nil
	}

	return nil, ErrKeyNotFound
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
	// If not found in memory, we'll look it up from disk.
	return l.lookupDiskTables(key)
}

// flushMemTable flushes the currently held memTable to disk. NOTE: Caller should acquire lock.
func (l *LSMTree) flushMemTable() error {
	// Memtable is full, flush it to disk.
	prevPartId := int64(0)
	if l.latestDiskTable != nil {
		prevPartId = l.latestDiskTable.header.GetId()
	}
	nextPartId := prevPartId + 1
	tablePath := filepath.Join(l.dir, fmt.Sprintf("%d.sst", nextPartId))
	pairs := slices.Collect(l.memTable.Pairs())
	if len(pairs) == 0 {
		return nil
	}
	if err := writeSSTable(prevPartId, nextPartId, tablePath, pairs); err != nil {
		return fmt.Errorf("failed to write sstable to disk: %v", err)
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
	slog.Info("Flushed MemTable to disk.", "path", tablePath)
	return nil
}

// Set sets the given key-value pair in the LSM tree.
func (l *LSMTree) Set(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("expected a non-empty key")
	}

	l.mux.Lock()
	defer l.mux.Unlock()

	if shouldFlush := l.memTable.Set(key, value); shouldFlush {
		return l.flushMemTable()
	}

	return nil
}

// Swap stores the given key, value in the storage and returns the previous value corresponding to the key.
func (l *LSMTree) Swap(key, value []byte) ( /*previousValue*/ []byte, error) {
	l.mux.Lock()
	defer l.mux.Unlock()

	var (
		returnValue []byte
		found       = false
	)
	shouldFlush, foundOnMem, prevValue := l.memTable.Swap(key, value)
	// If the mem table contains the previous value, we won't need to go further and lookup on disk.
	if foundOnMem {
		returnValue = prevValue
		found = true
	} else {
		// Look up disk for the previous value.
		prevValueOnDisk, err := l.lookupDiskTables(key)
		if err == nil {
			returnValue = prevValueOnDisk
			found = true
		} else if !errors.Is(err, ErrKeyNotFound) { // Some unexpected error happened.
			return nil, fmt.Errorf("failed to swap key %v: %w", fmt.Sprint(key), err)
		}
	}

	if shouldFlush { // Flush memtable when we're done.
		if err := l.flushMemTable(); err != nil {
			return nil, err
		}
	}

	if !found {
		return nil, ErrKeyNotFound
	}

	return returnValue, nil
}

// Close closes every SSTable in the LSM tree.
func (l *LSMTree) Close() error {
	if l == nil {
		return nil
	}

	l.mux.Lock()
	defer l.mux.Unlock()

	slog.Info("Closing LSM tree instance.")
	var errs error
	if err := l.flushMemTable(); err != nil {
		errs = err
	}
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
