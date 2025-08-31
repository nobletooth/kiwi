package storage

import (
	"testing"

	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/stretchr/testify/assert"
)

func TestCompressDataBlocks(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		pairs          []Pair
		expectedPrefix [][]byte
		expectedBlocks []*kiwipb.DataBlock
	}{
		{
			name:           "empty",
			pairs:          nil,
			expectedPrefix: nil,
			expectedBlocks: nil,
		},
		{ // There's no reason to split a single pair into multiple blocks.
			name:           "single pair",
			pairs:          []Pair{{Key: []byte{1, 2, 3}, Value: []byte{4, 5, 6}}},
			expectedPrefix: [][]byte{{}},
			expectedBlocks: []*kiwipb.DataBlock{{Keys: [][]byte{{1, 2, 3}}, Values: [][]byte{{4, 5, 6}}}},
		},
		{
			name: "multiple pairs with common prefix",
			pairs: []Pair{
				// Two following keys share prefix {1, 2}.
				{Key: []byte{1, 2, 3}, Value: []byte{4}},
				{Key: []byte{1, 2, 4}, Value: []byte{5}},
				// The next two don't share any prefix.
				{Key: []byte{1, 3, 5}, Value: []byte{6}},
				{Key: []byte{2, 3, 4}, Value: []byte{7}},
			},
			expectedPrefix: [][]byte{{1, 2}, {}},
			expectedBlocks: []*kiwipb.DataBlock{
				{Keys: [][]byte{{3}, {4}}, Values: [][]byte{{4}, {5}}},
				{Keys: [][]byte{{1, 3, 5}, {2, 3, 4}}, Values: [][]byte{{6}, {7}}},
			},
		},
		{ // Both {1,2,3} and {1,2} are common prefixes, but we should pick the shorter one (counter-intuitive).
			name: "non-trivial common prefix",
			pairs: []Pair{
				// Following keys share prefix {1, 2, 3}.
				{Key: []byte{1, 2, 3, 4}, Value: []byte{4}},
				{Key: []byte{1, 2, 3, 5}, Value: []byte{5}},
				{Key: []byte{1, 2, 3, 6}, Value: []byte{5}},
				// Following keys share prefix {1, 2}.
				{Key: []byte{1, 2, 1}, Value: []byte{1}},
				{Key: []byte{1, 2, 2}, Value: []byte{2}},
				{Key: []byte{1, 2, 3}, Value: []byte{3}},
				{Key: []byte{1, 2, 4}, Value: []byte{4}},
				{Key: []byte{1, 2, 5}, Value: []byte{5}},
				{Key: []byte{1, 2, 6}, Value: []byte{6}},
				{Key: []byte{1, 2, 7}, Value: []byte{7}},
				{Key: []byte{1, 2, 8}, Value: []byte{8}},
				{Key: []byte{1, 2, 9}, Value: []byte{9}},
			},
			expectedPrefix: [][]byte{{1, 2}},
			expectedBlocks: []*kiwipb.DataBlock{
				{ // The first block has prefix {1,2}, so we strip it from keys.
					Keys:   [][]byte{{3, 4}, {3, 5}, {3, 6}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}},
					Values: [][]byte{{4}, {5}, {5}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}},
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prefixes, blocks := compressDataBlocks(testCase.pairs)
			assert.Equal(t, testCase.expectedPrefix, prefixes)
			assert.Equal(t, testCase.expectedBlocks, blocks)
		})
	}
}

func TestLcpLen(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		a, b     []byte
		expected int
	}{
		{
			name: "both empty",
			a:    []byte{}, b: []byte{},
			expected: 0,
		},
		{
			name: "a empty",
			a:    []byte{}, b: []byte{1, 2, 3},
			expected: 0,
		},
		{
			name: "b empty",
			a:    []byte{1, 2, 3}, b: []byte{},
			expected: 0,
		},
		{
			name: "no common prefix",
			a:    []byte{1, 2, 3}, b: []byte{4, 5, 6},
			expected: 0,
		},
		{
			name: "partial common prefix",
			a:    []byte{1, 2, 3, 4, 5}, b: []byte{1, 2, 0, 4, 5},
			expected: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, lcpLen(testCase.a, testCase.b))
		})
	}
}
