// To store key-value pairs in the data block section, we first apply a prefix compression on it and each prefix
// group is then stored as a separate DataBlock entry in the file.

package storage

import (
	"slices"

	kiwipb "github.com/nobletooth/kiwi/proto"
)

type Pair struct{ Key, Value []byte }

// partitionToDataBlocks splits a sorted list of keys into optimal prefixed blocks.
// Each block stores one shared prefix and per-key suffixes, minimizing total bytes:
// sum(len(prefix(a)) + sum(len(suffixes(b))) over all blocks b.
//
// Assumes pairs are sorted by Key (lexicographically).
func partitionToDataBlocks(pairs []Pair) ([] /*prefix*/ []byte, []*kiwipb.DataBlock) {
	pairsNum := len(pairs)
	if pairsNum == 0 {
		return nil, nil
	}

	// Precompute adjacent longest common prefixes (LCP): lcpNext[i] = LCP(keys[i], keys[i+1]).
	lcpNext := make([]int, pairsNum-1)
	for i := 0; i+1 < pairsNum; i++ {
		lcpNext[i] = lcpLen(pairs[i].Key, pairs[i+1].Key)
	}

	// DP for maximum total savings.
	// Savings of a block [i..j] = (j - i) * LCP_all(i..j),
	// where LCP_all(i..j) = min(lcpNext[i..j-1]) and 0 for a singleton.
	dp := make([]int, pairsNum+1) // dp[pairsNum] = 0.
	end := make([]int, pairsNum)  // Best end index j for block starting at index `i`.
	for i := pairsNum - 1; i >= 0; i-- {
		best, bestJ := -1, i
		minL := 1<<31 - 1
		for j := i; j < pairsNum; j++ {
			var blockSave int
			if j == i {
				blockSave = 0
			} else {
				if l := lcpNext[j-1]; l < minL {
					minL = l
				}
				blockSave = (j - i) * minL
			}
			if total := blockSave + dp[j+1]; total > best {
				best = total
				bestJ = j
			}
		}
		dp[i] = best
		end[i] = bestJ
	}

	// Reconstruct blocks.
	var prefixes [][]byte
	var blocks []*kiwipb.DataBlock
	for i := 0; i < pairsNum; {
		j := end[i]
		// Compute LCP_all(i..j) for the chosen block.
		pLen := 0
		if j > i {
			pLen = slices.Min(lcpNext[i:j])
		}
		// Construct the DataBlock with prefix and suffixes.
		prefix := pairs[i].Key[:pLen]
		db := &kiwipb.DataBlock{Keys: make([][]byte, j-i+1), Values: make([][]byte, j-i+1)}
		for k := i; k <= j; k++ {
			db.Keys[k-i] = pairs[k].Key[pLen:] // Suffix slice.
			db.Values[k-i] = pairs[k].Value
		}
		prefixes = append(prefixes, prefix)
		blocks = append(blocks, db)
		i = j + 1
	}

	return prefixes, blocks
}

// lcpLen returns the length of the longest common prefix of keys `k1` and `k2`.
func lcpLen(k1, k2 []byte) int {
	biggerSize := len(k1)
	if len(k2) < biggerSize {
		biggerSize = len(k2)
	}

	// Compare by bytes. Case-sensitive, exact match.
	lcpSize := 0
	for lcpSize < biggerSize && k1[lcpSize] == k2[lcpSize] {
		lcpSize++
	}

	return lcpSize
}
