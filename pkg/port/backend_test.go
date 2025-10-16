package port

import (
	"errors"
	"testing"
	"time"

	"github.com/nobletooth/kiwi/pkg/config"
	"github.com/nobletooth/kiwi/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func TestKiwiStorage(t *testing.T) {
	config.SetTestFlag(t, "data_dir", t.TempDir())
	store, err := NewKiwiStorage()
	assert.NoError(t, err)

	t.Run("set", func(t *testing.T) {
		assert.NoError(t, store.Set(SetCommand{key: []byte("k1"), value: []byte("v1")}).err)
		assert.NoError(t, store.Set(SetCommand{key: []byte("k2"), value: []byte("v2")}).err)
		assert.NoError(t, store.Set(SetCommand{key: []byte("k3"), value: []byte("v3")}).err)
	})
	t.Run("get_existing_key", func(t *testing.T) {
		val, err := store.Get([]byte("k1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v1"), val)
	})
	t.Run("get_non_existent_key", func(t *testing.T) {
		_, err := store.Get([]byte("non_existent"))
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})
	t.Run("delete_existing_key", func(t *testing.T) {
		assert.NoError(t, store.Delete([]byte("k2")))
		val, err := store.Get([]byte("k2"))
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
		assert.Nil(t, val)
	})
	t.Run("delete_non_existent_key", func(t *testing.T) {
		assert.ErrorIs(t, store.Delete([]byte("random")), storage.ErrKeyNotFound)
	})
	t.Run("set_expirable", func(t *testing.T) {
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("kx1"),
			value:      []byte("vx1"),
			expiryTime: time.Now().Add(10 * time.Millisecond),
		}).err)
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("kx2"),
			value:      []byte("vx2"),
			expiryTime: time.Now().Add(1 * time.Hour),
		}).err)
		// Make sure "kx1" eventually expires.
		assert.Eventually(t, func() bool {
			_, err := store.Get([]byte("kx"))
			return errors.Is(err, storage.ErrKeyNotFound)
		}, time.Second, 10*time.Millisecond)
		// Even when "kx1" expired, the "kx2" still remains since it has a really long TTL.
		val, err := store.Get([]byte("kx2"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("vx2"), val)
	})

	// Tests for SET NX (set if not exists) semantics.
	t.Run("set_nx_on_non_existent_key", func(t *testing.T) {
		result := store.Set(SetCommand{
			key:       []byte("nx_key"),
			value:     []byte("nx_value"),
			existence: ifNotExists,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet, "Should set key when it doesn't exist with NX")

		val, err := store.Get([]byte("nx_key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("nx_value"), val)
	})

	t.Run("set_nx_on_existing_key", func(t *testing.T) {
		// First, set a key.
		assert.NoError(t, store.Set(SetCommand{key: []byte("existing_nx"), value: []byte("original")}).err)
		// Try to set with NX - should fail.
		result := store.Set(SetCommand{
			key:       []byte("existing_nx"),
			value:     []byte("new_value"),
			existence: ifNotExists,
		})
		assert.NoError(t, result.err)
		assert.False(t, result.couldSet, "Should NOT set key when it exists with NX")
		// Verify original value is unchanged.
		val, err := store.Get([]byte("existing_nx"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("original"), val)
	})

	// Tests for SET XX (set if exists) semantics.
	t.Run("set_xx_on_existing_key", func(t *testing.T) {
		// First, set a key.
		assert.NoError(t, store.Set(SetCommand{key: []byte("existing_xx"), value: []byte("original")}).err)
		// Update with XX - should succeed.
		result := store.Set(SetCommand{
			key:       []byte("existing_xx"),
			value:     []byte("updated"),
			existence: ifExists,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet, "Should set key when it exists with XX")

		val, err := store.Get([]byte("existing_xx"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("updated"), val)
	})

	t.Run("set_xx_on_non_existent_key", func(t *testing.T) {
		result := store.Set(SetCommand{
			key:       []byte("non_existent_xx"),
			value:     []byte("value"),
			existence: ifExists,
		})
		assert.NoError(t, result.err)
		assert.False(t, result.couldSet, "Should NOT set key when it doesn't exist with XX")

		// Verify key was not set.
		_, err := store.Get([]byte("non_existent_xx"))
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})

	// Tests for SET GET option (return previous value).
	t.Run("set_get_on_existing_key", func(t *testing.T) {
		// First, set a key.
		assert.NoError(t, store.Set(SetCommand{key: []byte("get_key"), value: []byte("old_value")}).err)
		// Set with GET option.
		result := store.Set(SetCommand{
			key:   []byte("get_key"),
			value: []byte("new_value"),
			get:   true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		assert.True(t, result.hasPreviousValue, "Should indicate previous value exists")
		// Verify new value is set.
		val, err := store.Get([]byte("get_key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("new_value"), val)
	})

	t.Run("set_get_on_non_existent_key", func(t *testing.T) {
		result := store.Set(SetCommand{
			key:   []byte("get_key_new"),
			value: []byte("value"),
			get:   true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		assert.False(t, result.hasPreviousValue, "Should indicate no previous value")
		assert.Nil(t, result.previousValue)
	})

	// Tests for SET KEEPTTL option.
	t.Run("set_keepttl_preserves_ttl", func(t *testing.T) {
		// Set a key with expiry.
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("ttl_key"),
			value:      []byte("original"),
			expiryTime: time.Now().Add(1 * time.Hour),
		}).err)
		// Update value while keeping TTL.
		result := store.Set(SetCommand{
			key:     []byte("ttl_key"),
			value:   []byte("updated"),
			keepTtl: true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		// Verify value is updated and key still has TTL (doesn't expire immediately).
		val, err := store.Get([]byte("ttl_key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("updated"), val)
	})

	t.Run("set_keepttl_on_non_existent_key", func(t *testing.T) {
		result := store.Set(SetCommand{
			key:     []byte("no_ttl_key"),
			value:   []byte("value"),
			keepTtl: true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		// Should set normally without TTL.
		val, err := store.Get([]byte("no_ttl_key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), val)
	})

	t.Run("set_keepttl_on_key_without_ttl", func(t *testing.T) {
		// Set a key without expiry.
		assert.NoError(t, store.Set(SetCommand{key: []byte("no_expiry"), value: []byte("original")}).err)
		// Update with KEEPTTL - should not add TTL.
		result := store.Set(SetCommand{
			key:     []byte("no_expiry"),
			value:   []byte("updated"),
			keepTtl: true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)

		val, err := store.Get([]byte("no_expiry"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("updated"), val)
	})

	t.Run("set_keepttl_on_expired_key", func(t *testing.T) {
		// Set a key with very short expiry.
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("expired_ttl"),
			value:      []byte("original"),
			expiryTime: time.Now().Add(10 * time.Millisecond),
		}).err)
		// Wait for expiry.
		time.Sleep(20 * time.Millisecond)
		// Try to set with KEEPTTL - should act like setting a new key.
		result := store.Set(SetCommand{
			key:     []byte("expired_ttl"),
			value:   []byte("new"),
			keepTtl: true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		// Should be retrievable without expiry.
		val, err := store.Get([]byte("expired_ttl"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("new"), val)
	})

	// Tests for combined options.
	t.Run("set_nx_with_get", func(t *testing.T) {
		// NX + GET on non-existent key - should set and return nil previous value.
		result := store.Set(SetCommand{
			key:       []byte("nx_get_new"),
			value:     []byte("value"),
			existence: ifNotExists,
			get:       true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		assert.False(t, result.hasPreviousValue)
		assert.Nil(t, result.previousValue)
		// NX + GET on existing key - should not set and return previous value.
		result = store.Set(SetCommand{
			key:       []byte("nx_get_new"),
			value:     []byte("new_value"),
			existence: ifNotExists,
			get:       true,
		})
		assert.NoError(t, result.err)
		assert.False(t, result.couldSet)
		assert.True(t, result.hasPreviousValue)
	})

	t.Run("set_xx_with_keepttl", func(t *testing.T) {
		// Set key with TTL.
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("xx_ttl"),
			value:      []byte("original"),
			expiryTime: time.Now().Add(1 * time.Hour),
		}).err)
		// XX + KEEPTTL - should update and keep TTL.
		result := store.Set(SetCommand{
			key:       []byte("xx_ttl"),
			value:     []byte("updated"),
			existence: ifExists,
			keepTtl:   true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)

		val, err := store.Get([]byte("xx_ttl"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("updated"), val)
	})

	t.Run("set_xx_with_get", func(t *testing.T) {
		// Set initial key.
		assert.NoError(t, store.Set(SetCommand{key: []byte("xx_get"), value: []byte("original")}).err)

		// XX + GET on existing key.
		result := store.Set(SetCommand{
			key:       []byte("xx_get"),
			value:     []byte("updated"),
			existence: ifExists,
			get:       true,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)
		assert.True(t, result.hasPreviousValue)

		// XX + GET on non-existent key.
		result = store.Set(SetCommand{
			key:       []byte("xx_get_missing"),
			value:     []byte("value"),
			existence: ifExists,
			get:       true,
		})
		assert.NoError(t, result.err)
		assert.False(t, result.couldSet)
		assert.False(t, result.hasPreviousValue)
	})

	t.Run("set_overwrites_expired_key", func(t *testing.T) {
		// Set key with very short expiry.
		assert.NoError(t, store.Set(SetCommand{
			key:        []byte("will_expire"),
			value:      []byte("original"),
			expiryTime: time.Now().Add(10 * time.Millisecond),
		}).err)

		// Wait for expiry.
		time.Sleep(20 * time.Millisecond)

		// Set new value.
		result := store.Set(SetCommand{
			key:   []byte("will_expire"),
			value: []byte("new_value"),
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)

		// Should get new value.
		val, err := store.Get([]byte("will_expire"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("new_value"), val)
	})

	t.Run("set_after_delete", func(t *testing.T) {
		// Set, delete, then set again.
		assert.NoError(t, store.Set(SetCommand{key: []byte("del_set"), value: []byte("v1")}).err)
		assert.NoError(t, store.Delete([]byte("del_set")))

		result := store.Set(SetCommand{
			key:   []byte("del_set"),
			value: []byte("v2"),
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet)

		val, err := store.Get([]byte("del_set"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v2"), val)
	})

	t.Run("set_nx_after_delete", func(t *testing.T) {
		// Set, delete, then NX should succeed.
		assert.NoError(t, store.Set(SetCommand{key: []byte("del_nx"), value: []byte("v1")}).err)
		assert.NoError(t, store.Delete([]byte("del_nx")))

		result := store.Set(SetCommand{
			key:       []byte("del_nx"),
			value:     []byte("v2"),
			existence: ifNotExists,
		})
		assert.NoError(t, result.err)
		assert.True(t, result.couldSet, "NX should succeed after delete")

		val, err := store.Get([]byte("del_nx"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("v2"), val)
	})

	t.Run("set_xx_after_delete", func(t *testing.T) {
		// Set, delete, then XX should fail.
		assert.NoError(t, store.Set(SetCommand{key: []byte("del_xx"), value: []byte("v1")}).err)
		assert.NoError(t, store.Delete([]byte("del_xx")))

		result := store.Set(SetCommand{
			key:       []byte("del_xx"),
			value:     []byte("v2"),
			existence: ifExists,
		})
		assert.NoError(t, result.err)
		assert.False(t, result.couldSet, "XX should fail after delete")

		_, err := store.Get([]byte("del_xx"))
		assert.ErrorIs(t, err, storage.ErrKeyNotFound)
	})
}
