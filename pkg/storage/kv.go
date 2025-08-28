package storage

import (
	"errors"
	"fmt"
)

var ErrKeyNotFound = errors.New("key was not found")

type KeyValueHolder interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string)
}

var _ KeyValueHolder = (*InMemoryKeyValueHolder)(nil)

type InMemoryKeyValueHolder struct { // Implements KeyValueHolder.
	data map[string]string
}

func (i *InMemoryKeyValueHolder) Get(key string) (string, error) {
	if i.data == nil {
		return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	if value, exists := i.data[key]; exists {
		return value, nil
	}

	return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
}

func (i *InMemoryKeyValueHolder) Set(key, value string) error {
	i.data[key] = value
	return nil
}

func (i *InMemoryKeyValueHolder) Delete(key string) {
	delete(i.data, key)
}
