package storage

import (
	"bytes"
	"errors"
	"flag"
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
	skipList           *SkipList[[]byte /*key*/, []byte /*value*/]
	entries, heldBytes int // Size is tracked for flush thresholds.
}

// NewMemTable is the constructor for MemTable.
func NewMemTable() *MemTable {
	return &MemTable{skipList: NewSkipList[[]byte /*key*/, []byte /*value*/](bytes.Compare), entries: 0, heldBytes: 0}
}

// Get returns the value for a given key, or an error if not found.
func (m *MemTable) Get(key []byte) ( /*value*/ []byte, error) {
	return m.skipList.Get(key)
}

// Set inserts or updates the value for a given key.
func (m *MemTable) Set(key, value []byte) /*shouldFlush*/ bool {
	// Determine if key exists to update size accounting correctly.
	// NOTE: Since skip list is initialized, we'll ignore `Set` returned error.
	prevVal, alreadyExists, _ := m.skipList.Set(key, value)
	if !alreadyExists {
		m.entries++
		m.heldBytes += len(key) + len(value)
	} else {
		m.heldBytes += len(value) - len(prevVal)
	}

	return m.entries >= *memtableFlushSize || m.heldBytes >= *memtableFlushSizeBytes
}

func (m *MemTable) Delete(key []byte) error {
	prevVal, err := m.skipList.Delete(key)
	if !errors.Is(err, ErrKeyNotFound) {
		m.entries--
		m.heldBytes -= len(key) + len(prevVal)
	}
	return err
}
