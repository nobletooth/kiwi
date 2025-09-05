package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLSMTree(t *testing.T) {
	t.Run("empty_dir", func(t *testing.T) {
		lsm, err := NewLSMTree(t.TempDir(), 1 /*tableId*/)
		assert.NoError(t, err)
		assert.NotNil(t, lsm)
	})
}
