package scan

import (
	"slices"
	"testing"

	"github.com/nobletooth/kiwi/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestMatchGlob(t *testing.T) {
	pairs := []utils.BytePair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("anotherkey"), Value: []byte("value3")},
	}

	for _, testCase := range []struct {
		name     string
		glob     string
		expected []utils.BytePair
	}{
		{
			name: "match all",
			glob: "*",
			expected: []utils.BytePair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("anotherkey"), Value: []byte("value3")},
			},
		},
		{
			name: "match with ?",
			glob: "key?",
			expected: []utils.BytePair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
		},
		{
			name: "match with * at the end",
			glob: "key*",
			expected: []utils.BytePair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
		},
		{
			name: "match with * at the beginning",
			glob: "*key",
			expected: []utils.BytePair{
				{Key: []byte("anotherkey"), Value: []byte("value3")},
			},
		},
		{
			name: "match with multiple *",
			glob: "*key*",
			expected: []utils.BytePair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("anotherkey"), Value: []byte("value3")},
			},
		},
		{
			name:     "no match",
			glob:     "nomatch",
			expected: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			seq := MatchGlob([]byte(testCase.glob), slices.Values(pairs))
			got := slices.Collect(seq)
			assert.Equal(t, testCase.expected, got)
		})
	}
}
