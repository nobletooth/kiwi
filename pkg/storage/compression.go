// To store key-value pairs in the data block section, we first apply a prefix compression on it and each prefix
// group is then stored as a separate DataBlock entry in the file.

package storage

import (
	"slices"

	kiwipb "github.com/nobletooth/kiwi/proto"
)

// lcpLen returns the length of the longest common prefix of keys k1 and k2.
func lcpLen(k1, k2 []byte) int {
	biggerSize := len(k1)
	if len(k2) < biggerSize {
		biggerSize = len(k2)
	}

	longestCommon := 0
	for longestCommon < biggerSize && k1[longestCommon] == k2[longestCommon] {
		longestCommon++
	}

	return longestCommon
}

// compressDataBlocks splits a sorted list of keys into optimal prefixed blocks.
// Each block stores one shared prefix and per-key suffixes, minimizing total bytes.
// Tie-breaker: on equal savings prefer fewer blocks (i.e., longer blocks).
func compressDataBlocks(pairs []BytePair) ([] /*prefix*/ []byte, []*kiwipb.DataBlock) {
	pairsNum := len(pairs)
	if pairsNum == 0 {
		return nil, nil
	}

	// Precompute adjacent LCP: lcpNext[i] = LCP(keys[i], keys[i+1]).
	lcpNext := make([]int, pairsNum-1)
	for i := 0; i+1 < pairsNum; i++ {
		lcpNext[i] = lcpLen(pairs[i].Key, pairs[i+1].Key)
	}

	// DP for maximum total savings with tie-break on block count.
	// Savings of a block [i..j] = (j - i) * min(lcpNext[i..j-1]), singleton => 0.
	dpSave := make([]int, pairsNum+1)   // Best savings from i to end.
	dpBlocks := make([]int, pairsNum+1) // Min blocks when achieving dpSave[i].
	end := make([]int, pairsNum)        // Chosen j for block starting at index i.

	for i := pairsNum - 1; i >= 0; i-- {
		bestSave := -1
		bestBlocks := int(^uint(0) >> 1) // max int
		bestJ := i

		minL := int(^uint(0) >> 1) // Running minimum of lcpNext.
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
			// candSave = total saved bytes if we choose current block [i..j],
			// plus the optimal savings for the remainder starting at j+1.
			candSave := blockSave + dpSave[j+1]
			// candBlocks = total number of blocks if we choose [i..j] (that's 1),
			// plus the number of blocks used for the remainder (dpBlocks[j+1]).
			// Used only to break ties in favor of fewer blocks.
			candBlocks := 1 + dpBlocks[j+1]
			if candSave > bestSave || (candSave == bestSave && candBlocks < bestBlocks) {
				bestSave = candSave
				bestBlocks = candBlocks
				bestJ = j
			}
		}
		dpSave[i] = bestSave
		dpBlocks[i] = bestBlocks
		end[i] = bestJ
	}

	// Reconstruct blocks.
	var prefixes [][]byte
	var blocks []*kiwipb.DataBlock
	for i := 0; i < pairsNum; {
		j := end[i]
		// LCP_all(i..j).
		predixLength := 0
		if j > i {
			predixLength = slices.Min(lcpNext[i:j])
		}
		// Construct compressed data block.
		prefix := pairs[i].Key[:predixLength]
		db := &kiwipb.DataBlock{
			Keys:   make([][]byte, j-i+1),
			Values: make([][]byte, j-i+1),
		}
		for k := i; k <= j; k++ {
			db.Keys[k-i] = pairs[k].Key[predixLength:] // Suffix.
			db.Values[k-i] = pairs[k].Value
		}
		prefixes = append(prefixes, prefix)
		blocks = append(blocks, db)
		i = j + 1
	}

	return prefixes, blocks
}
