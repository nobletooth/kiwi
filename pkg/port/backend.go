package port

import (
	"errors"
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/nobletooth/kiwi/pkg/storage"
)

var dataDir = flag.String("data_dir", "./data", "Directory to store the DB data files.")

// KiwiStorage is the Kiwi storage backend used by Kiwi ports, e.g. Redis.
type KiwiStorage struct {
	db storage.KeyValueHolder
}

// NewKiwiStorage creates a new KiwiStorage with the given number of databases.
func NewKiwiStorage() (*KiwiStorage, error) {
	if *dataDir == "" {
		return nil, errors.New("--data_dir flag is required")
	}
	// TODO: Allow support for multi tables (multi Redis DBs).
	db, err := storage.NewLSMTree(*dataDir, 0 /*table*/)
	if err != nil {
		return nil, fmt.Errorf("failed to create db: %w", err)
	}

	store := &KiwiStorage{db: db}
	runtime.SetFinalizer(store, func(store *KiwiStorage) { _ = store.Close() })
	return store, nil
}

// Get looks up the given `key` and returns its value or an error if not found.
func (ks *KiwiStorage) Get(key []byte) ([]byte, error) {
	packed, err := ks.db.Get(key)
	if err != nil {
		return nil, err
	}
	unpacked, err := unpack(packed)
	if err != nil {
		return nil, err
	}
	if unpacked.opt.Is(TombStone) || unpacked.isExpired() {
		return nil, storage.ErrKeyNotFound
	}
	return unpacked.value, nil
}

// Set puts the given `key` and `val` inside the storage.
func (ks *KiwiStorage) Set(key, val []byte) error {
	return ks.db.Set(key, unpackedValue{value: val}.pack())
}

// SetExpirable puts the given `key` and `val` inside the storage.
func (ks *KiwiStorage) SetExpirable(key, val []byte, ttl time.Duration) error {
	return ks.db.Set(key, unpackedValue{opt: Expirable, value: val, expiry: time.Now().Add(ttl)}.pack())
}

func (ks *KiwiStorage) Delete(key []byte) error {
	_, err := ks.db.Swap(key, tombstonePacked)
	return err
}

func (ks *KiwiStorage) Close() error {
	return ks.db.Close()
}
