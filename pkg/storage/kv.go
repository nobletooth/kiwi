package storage

import (
	"errors"
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
