package iterator

import (
	"cmp"
	"iter"
	"slices"
	"testing"

	"github.com/nobletooth/kiwi/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMultiHead(t *testing.T) {
	s1 := slices.Values([]utils.Pair[string, int]{{"k1", 11}, {"k2", 21}, {"k3", 31}, {"k4", 41}})
	s2 := slices.Values([]utils.Pair[string, int]{{"k1", 12}, {"k2", 22}, {"k5", 52}, {"k6", 62}})
	s3 := slices.Values([]utils.Pair[string, int]{{"k1", 13}, {"k2", 23}, {"k4", 43}, {"k5", 53}})
	s4 := slices.Values([]utils.Pair[string, int]{{"k3", 34}})
	merged, err := MultiHead(cmp.Compare, []iter.Seq[utils.Pair[string, int]]{s1, s2, s3, s4})
	assert.NoError(t, err)

	got := slices.Collect(merged)
	expected := []utils.Pair[string, int]{{"k1", 11}, {"k2", 21}, {"k3", 31}, {"k4", 41}, {"k5", 52}, {"k6", 62}}
	assert.Equal(t, expected, got)
}
