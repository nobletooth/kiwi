// Nothing to see here in this module. Couldn't find a better place for Pair.

package utils

type Pair[K any, V any] struct {
	Key   K
	Value V
}

type BytePair Pair[[]byte /*key*/, []byte /*value*/]
