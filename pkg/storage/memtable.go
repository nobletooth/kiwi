package storage

import (
	"bytes"
	"flag"
	"iter"
)

var (
	memtableFlushSizeBytes = flag.Int("memtable_flush_size_bytes", 1<<10, /*1 KiB*/
		"Triggers mem tables flush when total key+value bytes reach this size.")
	memtableFlushSize = flag.Int("memtable_flush_size", 1_000,
		"Triggers mem table flush when number of key-value entries reaches this count.")
)

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

// Get returns the value for a given key.
func (m *MemTable) Get(key []byte) ( /*value*/ []byte, bool /*found*/) {
	return m.skipList.Get(key)
}

// Swap sets the given {key,value} pair, returning the previous value corresponding to the key.
func (m *MemTable) Swap(key, value []byte) (bool /*shouldFlush*/, bool /*found*/, []byte /*previousValue*/) {
	// Determine if key exists to update size accounting correctly.
	// NOTE: Since skip list is initialized, we'll ignore `Set` returned error.
	prevVal, found := m.skipList.Set(key, value)
	if !found { // New key.
		m.entries++
		m.heldBytes += len(key) + len(value)
	} else { // Updating existing key.
		m.heldBytes += len(value) - len(prevVal)
	}
	return m.entries >= *memtableFlushSize || m.heldBytes >= *memtableFlushSizeBytes, found, prevVal
}

// Set inserts or updates the value for a given key.
func (m *MemTable) Set(key, value []byte) /*shouldFlush*/ bool {
	shouldFlush, _, _ := m.Swap(key, value)
	return shouldFlush
}

func (m *MemTable) Delete(key []byte) /*found*/ bool {
	prevVal, found := m.skipList.Delete(key)
	if found {
		m.entries--
		m.heldBytes -= len(key) + len(prevVal)
	}
	return found
}

// Pairs returns an iterator over all key-value pairs in the memtable.
func (m *MemTable) Pairs() iter.Seq[BytePair] {
	it := m.skipList.Iterate()
	return func(yield func(BytePair) bool) {
		it(func(pair Pair[ /*key*/ []byte /*value*/, []byte]) bool {
			return yield(BytePair(pair))
		})
	}
}
