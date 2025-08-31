package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSSTable ensures basic functionality of SSTable operations and their integration.
func TestSSTable(t *testing.T) {
	resultFile := filepath.Join(t.TempDir(), "test.sst")
	data := []Pair{
		{Key: []byte("apple"), Value: []byte("fruit")},
		{Key: []byte("carrot"), Value: []byte("vegetable")},
		{Key: []byte("banana"), Value: []byte("fruit")},
	}
	err := writeSSTable( /*prevId*/ 0, data, resultFile)
	require.NoError(t, err)
}

/*
aaB
aaC
bbD
bbE
*/

/*
aaa(B C)
aa(b c d f g o)
*/

/*
aa(aB, aC, b, c, d, f, g, o)
*/
