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
}
