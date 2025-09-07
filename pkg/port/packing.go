// Kiwi packs / unpacks values into three types:
// 1) Tombstone: A marker indicating that the key has been deleted.
// 2) Regular  : A normal key-value pair without expiration.
// 3) Expirable: A key-value pair with an expiration time.

package port

import (
	"encoding/binary"
	"errors"
	"time"
)

// Opts represents options for storing a key-value pair.
type Opts uint8

// Is returns true if any of the given options are toggled in the current options.
func (o Opts) Is(opts Opts) bool {
	return o&opts != 0
}

const (
	// TombStone is when a key is deleted. We put a tombstone marker instead of actually deleting the key.
	// This is because in LSM tree, we cannot delete a key from disk immediately, as it may exist in multiple SSTables.
	// Instead, we mark the key as deleted with a tombstone, and during compaction, the key would be removed.
	TombStone Opts = 1 << iota
	// Expirable is when a key has an expiration time set; Expired keys would be removed during compaction.
	Expirable
)

var (
	tombstoneUnpacked = unpackedValue{opt: TombStone}
	tombstonePacked   = tombstoneUnpacked.pack()
	emptyUnpacked     = unpackedValue{}
)

// unpackedValue represents a value that has been unpacked from the storage format.
type unpackedValue struct {
	opt    Opts
	value  []byte
	expiry time.Time
}

// unpack deserializes the packed byte slice into an unpackedValue struct.
func unpack(packed []byte) (unpackedValue, error) {
	if len(packed) == 0 {
		return emptyUnpacked, errors.New("value is emptyUnpacked")
	}
	opt := Opts(packed[0])
	if opt.Is(TombStone) {
		return tombstoneUnpacked, nil
	}

	var value []byte
	expiryTime := time.Time{}
	if opt.Is(Expirable) {
		if len(packed) < 1+8 {
			return emptyUnpacked, errors.New("value is too short to contain expiry")
		}
		expiryNs := int64(binary.BigEndian.Uint64(packed[len(packed)-8:]))
		expiryTime = time.Unix(0, expiryNs)
		value = packed[1 : len(packed)-8]
	} else {
		value = packed[1:]
	}

	return unpackedValue{opt: opt, value: value, expiry: expiryTime}, nil
}

// packs serializes the options and the value into a single byte slice.
func (uv unpackedValue) pack() []byte {
	outputSize := 1 + len(uv.value) // 1 byte for the options.
	if uv.opt.Is(Expirable) {
		outputSize += 8 // 8 bytes for the expiry time.
	}
	buffer := make([]byte, outputSize)
	buffer[0] = byte(uv.opt)
	if uv.opt.Is(TombStone) {
		return buffer
	}
	copy(buffer[1:], uv.value)
	if uv.opt.Is(Expirable) {
		binary.BigEndian.PutUint64(buffer[1+len(uv.value):], uint64(uv.expiry.UTC().UnixNano()))
	}
	return buffer
}

func (uv unpackedValue) isExpired() bool {
	if !uv.opt.Is(Expirable) {
		return false
	}
	return time.Now().After(uv.expiry)
}
