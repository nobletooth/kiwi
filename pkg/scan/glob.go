// Kiwi checks keys against glob patterns after applying range scans; the following module implements glob matching.

package scan

import (
	"iter"

	"github.com/nobletooth/kiwi/pkg/utils"
	"v.io/v23/glob"
)

// MatchGlob matches the `pairs` stream with the given `glob` pattern.
func MatchGlob(pattern []byte, pairs iter.Seq[utils.BytePair]) iter.Seq[utils.BytePair] {
	// Parse the glob pattern.
	parsedPattern, err := glob.Parse(string(pattern))
	if err != nil { // If pattern is invalid, return empty sequence.
		return func(yield func(utils.BytePair) bool) {}
	}
	return func(yield func(utils.BytePair) bool) {
		for pair := range pairs {
			if parsedPattern.Head().Match(string(pair.Key)) {
				if !yield(pair) {
					return
				}
			}
		}
	}
}
