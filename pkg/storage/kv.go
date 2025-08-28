package storage

import (
	"errors"
	"fmt"
	"log/slog"
)

var ErrKeyNotFound = errors.New("key was not found")

type KeyValueHolder interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Close() error
}

var _ KeyValueHolder = (*InMemoryKeyValueHolder)(nil)

type InMemoryKeyValueHolder struct { // Implements KeyValueHolder.
	data map[string]string
}

func NewInMemoryKeyValueHolder() *InMemoryKeyValueHolder {
	return &InMemoryKeyValueHolder{data: make(map[string]string)}
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

func (i *InMemoryKeyValueHolder) Delete(key string) error {
	if _, exists := i.data[key]; !exists {
		return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}
	delete(i.data, key)
	return nil
}

func (i *InMemoryKeyValueHolder) Close() error {
	slog.Info("Closing InMemoryKeyValueHolder")
	return nil
}
