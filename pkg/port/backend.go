package port

import (
	"errors"
	"flag"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/nobletooth/kiwi/pkg/storage"
	"github.com/nobletooth/kiwi/pkg/utils"
)

var dataDir = flag.String("data_dir", "./data", "Directory to store the DB data files.")

// KiwiStorage is the Kiwi storage backend used by Kiwi ports, e.g. Redis.
type KiwiStorage struct {
	mux sync.RWMutex
	db  storage.KeyValueHolder
}

// NewKiwiStorage creates a new KiwiStorage with the given number of databases.
func NewKiwiStorage() (*KiwiStorage, error) {
	if *dataDir == "" {
		return nil, errors.New("--data_dir flag is required")
	}
	// TODO: Allow support for multi tables (multi Redis DBs).
	db, err := storage.NewLSMTree(*dataDir, 1 /*table*/)
	if err != nil {
		return nil, fmt.Errorf("failed to create db: %w", err)
	}

	store := &KiwiStorage{db: db, mux: sync.RWMutex{}}
	runtime.SetFinalizer(store, func(store *KiwiStorage) { _ = store.Close() })
	return store, nil
}

// Get looks up the given `key` and returns its value or an error if not found.
func (ks *KiwiStorage) Get(key []byte) ([]byte, error) {
	ks.mux.RLock()
	defer ks.mux.RUnlock()

	packed, err := ks.db.Get(key)
	if err != nil {
		return nil, err
	}
	unpacked, err := unpack(packed)
	if err != nil {
		return nil, err
	}
	if unpacked.opt.is(TombStone) || unpacked.isExpired() {
		return nil, storage.ErrKeyNotFound
	}

	return unpacked.value, nil
}

type existenceCheck uint8

const (
	noCheck     existenceCheck = iota
	ifNotExists                // NX
	ifExists                   // XX
)

var allExistenceChecks = []existenceCheck{noCheck, ifExists, ifNotExists}

type SetCommand struct {
	key        []byte
	value      []byte
	expiryTime time.Time
	existence  existenceCheck
	keepTtl    bool // The Redis KEEPTTL option; overrides the `expiryTime`.
	get        bool // The Redis GET option; if true, should return the previous value.
}

type SetResult struct {
	previousValue    []byte // Only set if the command requires the previous value.
	hasPreviousValue bool   // If true, the `key` specified in SetCommand had a previous value.
	couldSet         bool   // If true, something was set in the storage.
	err              error
}

// Set executes the given `cmd` and returns the previous value if required.
func (ks *KiwiStorage) Set(cmd SetCommand) SetResult {
	if !slices.Contains(allExistenceChecks, cmd.existence) {
		utils.RaiseInvariant("backend", "unknown_set_existence_constraint",
			"Got an unknown existence constraint in the given set command.", "constraint", cmd.existence)
		return SetResult{err: fmt.Errorf("got unknwon set constraint '%d'", cmd.existence)}
	}

	ks.mux.Lock()
	defer ks.mux.Unlock()

	// Check if previous key-value pair needs to be retrieved.
	var prevValue []byte = nil
	hasPrevValue := false
	if cmd.existence != noCheck || cmd.keepTtl || cmd.get {
		value, err := ks.db.Get(cmd.key)
		if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
			return SetResult{err: fmt.Errorf("failed to get previous key: %w", err)}
		} else if !errors.Is(err, storage.ErrKeyNotFound) {
			prevValue = value
			hasPrevValue = true
		}
	}
	// Unpack the previously set value.
	var unpackedPrev unpackedValue
	if hasPrevValue {
		unpacked, err := unpack(prevValue)
		if err != nil {
			return SetResult{err: fmt.Errorf("failed to unpack previous value: %w", err)}
		}
		unpackedPrev = unpacked
		// Tombstones and expired keys should be treated as non-existent for NX/XX checks.
		if unpackedPrev.is(TombStone) || unpackedPrev.isExpired() {
			hasPrevValue = false
		}
	}

	// Build the unpacked value that's going to be set in the storage.
	valueToSet := unpackedValue{value: cmd.value}
	// KEEPTTL only copies the previous key expiry if it exists.
	if cmd.keepTtl && hasPrevValue && unpackedPrev.is(Expirable) && !unpackedPrev.isExpired() {
		valueToSet.opt = Expirable
		valueToSet.expiry = unpackedPrev.expiry
	} else if !cmd.expiryTime.IsZero() {
		valueToSet.opt = Expirable
		valueToSet.expiry = cmd.expiryTime
	}

	// Check whether we can set the value or not.
	couldSet := cmd.existence == noCheck || // Set any way.
		(cmd.existence == ifNotExists && !hasPrevValue) || // NX; Set only if not exists.
		(cmd.existence == ifExists && hasPrevValue) // XX; Set only if exists.
	if couldSet {
		if err := ks.db.Set(cmd.key, valueToSet.pack()); err != nil {
			return SetResult{err: fmt.Errorf("failed to set value: %w", err)}
		}
	}

	// Client wants the previous value returned.
	if cmd.get {
		return SetResult{
			previousValue:    prevValue,
			hasPreviousValue: hasPrevValue,
			couldSet:         couldSet,
			err:              nil,
		}
	}

	return SetResult{couldSet: couldSet, err: nil}
}

func (ks *KiwiStorage) Delete(key []byte) error {
	ks.mux.Lock()
	defer ks.mux.Unlock()
	_, err := ks.db.Swap(key, tombstonePacked)
	return err
}

func (ks *KiwiStorage) Close() error {
	ks.mux.Lock()
	defer ks.mux.Unlock()
	return ks.db.Close()
}
