// Since Kiwi has multiple sorted structures (SSTables, MemTables, Cluster shards, etc.) it needs a way to
// iterate over different sources using constant memory usage; or else the server memory would run out while doing
// range scans / full table scans.
//
// This module implements a heap-based multi-way iterator that lazily yields from multiple underneath iterators.
// Keys pulled from multiple sequences are sorted by key and sequence priority; values pulled from lower priority
// sequences are discarded in case their key was already seen.

package scan

import (
	"container/heap"
	"errors"
	"iter"

	"github.com/nobletooth/kiwi/pkg/utils"
)

// heapElement represents a pulled item from sequences inside iterHeap.
type heapElement[K any, V any] struct {
	key    K
	val    V
	seqIdx int // The sequence index inside iterHeap.sequences that produced this element.
}

// iterHeap holds the iteration state over multiple iterators.
type iterHeap[K any, V any] struct { // Implements heap.Interface.
	compare  utils.CompareFn[K]
	elements []*heapElement[K, V] // The latest elements pulled from sequences; nil means not pulled yet.
}

var _ heap.Interface = (*iterHeap[int, int])(nil)

func (ih *iterHeap[K, V]) Len() int {
	return len(ih.elements)
}

// Less returns true when element[i] has a less key or less priority with equal keys.
func (ih *iterHeap[K, V]) Less(i, j int) bool {
	e1, e2 := ih.elements[i], ih.elements[j]
	if cmp := ih.compare(e1.key, e2.key); cmp == 0 {
		return ih.elements[i].seqIdx < ih.elements[j].seqIdx
	} else if cmp < 0 {
		return true
	} else {
		return false
	}
}

// Swap changes positions of elements at i and j.
func (ih *iterHeap[K, V]) Swap(i, j int) {
	ih.elements[i], ih.elements[j] = ih.elements[j], ih.elements[i]
}

// Push will add the given element `x` to the heap if it matches the desired type.
func (ih *iterHeap[K, V]) Push(x any) {
	if element, ok := x.(*heapElement[K, V]); !ok {
		utils.RaiseInvariant("merged_iterator", "pushed_invalid_type", "n item with invalid type was pushed to heap.")
	} else if element == nil {
		utils.RaiseInvariant("merged_iterator", "pushed_nil_element", "A nil element was pushed to iteration heap.")
		return
	} else if len(ih.elements) == cap(ih.elements) {
		utils.RaiseInvariant("merged_iterator", "exceeded_capacity",
			"An element was pushed while the capacity was full.", "cap", cap(ih.elements))
	} else {
		ih.elements = append(ih.elements, element)
	}
}

// Pop returns and removes the last element in the heap.
func (ih *iterHeap[K, V]) Pop() any {
	lastElement := ih.elements[len(ih.elements)-1]
	ih.elements = ih.elements[:len(ih.elements)-1]
	return lastElement
}

// TopKey returns the min-heap's minimum key without removing it. If not found, returns zero value.
func (ih *iterHeap[K, V]) TopKey() K {
	if len(ih.elements) > 0 {
		return ih.elements[0].key
	}
	return *new(K)
}

// MultiHead allows multi-way iteration over a list of increasing iterators with different priorities.
// Incoming items from sequences are merged together with their key (K), and higher priorities are
// selected while other are discarded. Note: Sequences are expected to be increasing.
func MultiHead[Seq iter.Seq[utils.Pair[K, V]], K any, V any](cmp utils.CompareFn[K], sequences []Seq) (Seq, error) {
	if cmp == nil {
		return nil, errors.New("expected a non-nil comparison function")
	}
	if len(sequences) == 0 {
		return nil, errors.New("expected a non-empty sequences")
	}

	// Allocate the first element as the root node, pull and stop functions per each given sequence.
	// Sequences would be pulled from in the order of their priority.
	it := &iterHeap[K, V]{compare: cmp, elements: make([]*heapElement[K, V], 0, len(sequences))}
	pull := make([]func() (utils.Pair[K, V], bool), 0)
	stop := make([]func(), 0)
	for _, seq := range sequences {
		pullFn, stopFn := iter.Pull(iter.Seq[utils.Pair[K, V]](seq))
		firstElem, hasAny := pullFn()
		if !hasAny { // Sequence has no elements. Would be skipped entirely.
			stopFn()
			continue
		}
		heap.Push(it, &heapElement[K, V]{key: firstElem.Key, val: firstElem.Value, seqIdx: len(pull)})
		pull = append(pull, pullFn)
		stop = append(stop, stopFn)
	}

	// Next moves the iterator forward and returns the next pair returned by the iterator.
	next := func() utils.Pair[K, V] {
		topElement := heap.Pop(it).(*heapElement[K, V])
		nextElement, hasNext := pull[topElement.seqIdx]()
		if hasNext { // Next element in the sequence enter the heap.
			heap.Push(it, &heapElement[K, V]{key: nextElement.Key, val: nextElement.Value, seqIdx: topElement.seqIdx})
		} else { // No elements left in the sequence, stop pulling.
			stop[topElement.seqIdx]()
		}
		return utils.Pair[K, V]{Key: topElement.key, Value: topElement.val}
	}

	return func(yield func(utils.Pair[K, V]) bool) {
		if it.Len() == 0 {
			return
		}
		// Stop all underlying sequences once iteration is done.
		defer func() {
			for _, stopFn := range stop {
				stopFn()
			}
		}()
		// Return the first element (minimum key with the highest priority.
		nextElement := next()
		if !yield(nextElement) {
			return
		}
		// Discard values with the same key but lower priority.
		for it.Len() > 0 {
			// Discard lower priority values of the same key.
			if cmp := it.compare(it.TopKey(), nextElement.Key); cmp == 0 {
				next()
				continue
			}
			// New keys should be streamed.
			nextElement = next()
			if !yield(nextElement) {
				return
			}
		}
	}, nil
}
