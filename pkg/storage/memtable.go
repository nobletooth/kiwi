package storage

import (
	"bytes"
	"errors"
	"flag"
	"sync"
)

var (
	memtableFlushSizeBytes = flag.Int("memtable_flush_size_bytes", 1<<10, /*1 KiB*/
		"Triggers mem tables flush when total key+value bytes reach this size.")
	memtableFlushSize = flag.Int("memtable_flush_size", 1_000,
		"Triggers mem table flush when number of key-value entries reaches this count.")
)

type Pair[K any, V any] struct {
	Key   K
	Value V
}

type BytePair Pair[[]byte /*key*/, []byte /*value*/]

// MemTable serves the latest key-value pairs in memory before they are flushed to disk.
type MemTable struct {
	// skipList allows fast lookup, insertion, and deletion of key-value pairs.
	skipList         *SkipList[[]byte /*key*/, []byte /*value*/]
	mux              sync.RWMutex // Protects against race conditions.
	table, prevTable int64        // Current and previous table IDs; used when flushing SSTables.
	entries          int          // Size is tracked for flush thresholds.
}

// NewMemTable is the constructor for MemTable.
func NewMemTable(prevTable, table int64) (*MemTable, error) {
	if prevTable == table {
		return nil, errors.New("prevTable and table must not be equal")
	}

	return &MemTable{
		mux:       sync.RWMutex{},
		skipList:  NewSkipList[[]byte /*key*/, []byte /*value*/](bytes.Compare),
		table:     table,
		prevTable: prevTable,
	}, nil
}

// Get returns the value for a given key, or an error if not found.
func (m *MemTable) Get(key []byte) ( /*value*/ []byte, error) {
	m.mux.RLock()
	defer m.mux.RUnlock()
	return m.skipList.Get(key)
}

// Set inserts or updates the value for a given key.
func (m *MemTable) Set(key, value []byte) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	// Determine if key exists to update size accounting correctly.
	// NOTE: Since skip list is initialized, we'll ignore `Set` returned error.
	alreadyExists, _ := m.skipList.Set(key, value)
	if !alreadyExists {
		m.entries++
	}

	return nil
}

func (m *MemTable) Delete(key []byte) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	err := m.skipList.Delete(key)
	if !errors.Is(err, ErrKeyNotFound) {
		m.entries--
	}

	return err
}

func (m *MemTable) Close() error {
	m.mux.Lock()
	defer m.mux.Unlock()

	return m.skipList.Close()
}
