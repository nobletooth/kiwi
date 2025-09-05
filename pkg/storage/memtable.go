package storage

import "bytes"

// MemTable serves the latest key-value pairs in memory before they are flushed to disk.
type MemTable struct {
	// skipList allows fast lookup, insertion, and deletion of key-value pairs.
	skipList *SkipList[[]byte /*key*/, []byte /*value*/]
}

// NewMemTable is the constructor for MemTable.
func NewMemTable() *MemTable {
	return &MemTable{skipList: NewSkipList[[]byte /*key*/, []byte /*value*/](bytes.Compare)}
}

func (m *MemTable) Get(key []byte) ( /*value*/ []byte, error) {
	return m.skipList.Get(key)
}

func (m *MemTable) Set(key, value []byte) error {
	return m.skipList.Set(key, value)
}

func (m *MemTable) Delete(key []byte) error {
	return m.skipList.Delete(key)
}

func (m *MemTable) Close() error {
	return m.skipList.Close()
}
