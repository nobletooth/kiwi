package storage

import (
	"errors"
	"iter"
)

var ErrKeyNotFound = errors.New("key was not found")

// KeyValueHolder is a simple append-only storage interface.
type KeyValueHolder interface {
	// Get returns the corresponding value to the given `key` or else ErrKeyNotFound.
	Get(key []byte) ([]byte, error)
	// Set stores the given key, value pair in the storage, returning any errors encountered meanwhile.
	Set(key, value []byte) error
	// Swap returns the previous value of the key or ErrKeyNotFound if it didn't exist.
	Swap(key, value []byte) ( /*previousValue*/ []byte, error)
	// Close closes every held resource.
	Close() error
}

// RangeScanner extends KeyValueHolder with range scanning capabilities.
type RangeScanner interface {
	KeyValueHolder
	// Scan returns an iterator over key-value pairs within the given range [start, end).
	// If start is nil, scanning begins from the first key.
	// If end is nil, scanning continues to the last key.
	Scan(start, end []byte) iter.Seq[BytePair]
	// ScanPrefix returns an iterator over all key-value pairs with the given prefix.
	ScanPrefix(prefix []byte) iter.Seq[BytePair]
}
