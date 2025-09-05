// Package storage provides storage-related data structures and utilities.
//
// This file implements a generic SkipList. A skip list maintains multiple
// forward-pointer layers over a sorted linked list. Each key may be promoted
// to higher levels with probability p, forming express lanes that let searches
// skip over large ranges. Operations start at the highest populated level and
// descend when advancing would overshoot the target key.
//
// Properties
// - Expected time complexity for Get/Set/Delete: O(log n)
// - Space complexity: O(n)
// - Probabilistic balancing controlled by promotion probability p (default 0.25)
// - Deterministic iteration order by key using per-level forward pointers
package storage

import (
	"iter"
	"math/rand"
	"time"
)

// CompareFn defines a three-way comparison for keys of type K.
// It must return a negative value if x < y, 0 if x == y, and a positive value if x > y.
type CompareFn[K any] func(x, y K) int

// skipListNode represents a node in the skip list.
type skipListNode[K any, V any] struct {
	key      K
	value    V
	forwards []*skipListNode[K, V] // forward pointers per level (0..level-1)
}

// SkipList is a probabilistically balanced ordered map over keys of any type.
// Key ordering is defined by a user-supplied CompareFn passed to the constructor.
// The structure maintains up to maxLevel layers; each node appears in level i
// with probability p^i (independently), enabling logarithmic expected search.
type SkipList[K any, V any] struct {
	head            *skipListNode[K, V]
	level, maxLevel int
	p               float64 // Probability that a node is promoted to the next level.
	rnd             *rand.Rand
	compare         CompareFn[K]
}

// NewSkipList creates a new empty skip list.
// The compare function defines the total ordering over keys.
// Defaults: maxLevel=16, p=0.25.
func NewSkipList[K any, V any](compare CompareFn[K]) *SkipList[K, V] {
	if compare == nil {
		panic("SkipList requires a non-nil compare function")
	}
	const defaultMaxLevel = 16
	const defaultP = 0.25
	return &SkipList[K, V]{
		head:     &skipListNode[K, V]{forwards: make([]*skipListNode[K, V], defaultMaxLevel)},
		level:    1,
		maxLevel: defaultMaxLevel,
		p:        defaultP,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
		compare:  compare,
	}
}

// randomLevel generates a random level based on the skip list's probability p.
func (s *SkipList[K, V]) randomLevel() int {
	lvl := 1
	for lvl < s.maxLevel && s.rnd.Float64() < s.p {
		lvl++
	}
	return lvl
}

// Get returns the value for key and a boolean to indicate if it was found.
// It traverses from the highest populated level down to level 0, advancing at
// each level while the next key is still less than the target key.
func (s *SkipList[K, V]) Get(key K) (V, bool /*found*/) {
	if s == nil || s.head == nil {
		return *new(V), false
	}
	node := s.head
	// Traverse from top level down to level 0.
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := node.forwards[lvl]; next != nil && s.compare(next.key, key) < 0; next = node.forwards[lvl] {
			node = next
		}
	}
	// Candidate is at level 0 forward from node.
	node = node.forwards[0]
	if node != nil && s.compare(node.key, key) == 0 {
		return node.value, true
	}
	return *new(V), false
}

// Set inserts a new key/value or updates an existing one.
// It records the immediate predecessors per level during the search, then
// either updates in place or splices in a new node of random level.
func (s *SkipList[K, V]) Set(key K, value V) (V /*previousVal*/, bool /*found*/) {
	// Track the last nodes before the position at each level.
	update := make([]*skipListNode[K, V], s.maxLevel)
	node := s.head
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := node.forwards[lvl]; next != nil && s.compare(next.key, key) < 0; next = node.forwards[lvl] {
			node = next
		}
		update[lvl] = node
	}
	// Check if the key already exists at level 0.
	if next := node.forwards[0]; next != nil && s.compare(next.key, key) == 0 {
		prevVal := next.value
		next.value = value
		return prevVal, true
	}
	// Insert a new node with a random level.
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	newNode := &skipListNode[K, V]{key: key, value: value, forwards: make([]*skipListNode[K, V], lvl)}
	for i := 0; i < lvl; i++ {
		newNode.forwards[i] = update[i].forwards[i]
		update[i].forwards[i] = newNode
	}

	return *new(V), false
}

// Delete removes key from the list and returns its previous value if found.
// It finds predecessors at each level and rewires forward pointers to skip the target, then trims empty top levels.
func (s *SkipList[K, V]) Delete(key K) (V /*previousVal*/, bool /*found*/) {
	if s == nil || s.head == nil {
		return *new(V), false
	}
	update := make([]*skipListNode[K, V], s.maxLevel)
	node := s.head
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := node.forwards[lvl]; next != nil && s.compare(next.key, key) < 0; next = node.forwards[lvl] {
			node = next
		}
		update[lvl] = node
	}
	target := node.forwards[0]
	if target == nil || s.compare(target.key, key) != 0 {
		return *new(V), false
	}
	prevVal := target.value
	// Rewire forward pointers to remove target.
	for i := 0; i < s.level; i++ {
		if update[i].forwards[i] == target {
			update[i].forwards[i] = target.forwards[i]
		}
	}
	// Decrease level if the top levels are now empty.
	for s.level > 1 && s.head.forwards[s.level-1] == nil {
		s.level--
	}
	return prevVal, true
}

// Iterate returns an iterator over all key/value pairs in ascending key order.
// Usage: pairs := slices.Collect(skipList.Iterate())
func (s *SkipList[K, V]) Iterate() iter.Seq[Pair[K, V]] {
	return func(yield func(Pair[K, V]) bool) {
		if s == nil || s.head == nil {
			return
		}
		for noteIter := s.head.forwards[0]; noteIter != nil; noteIter = noteIter.forwards[0] {
			if !yield(Pair[K, V]{Key: noteIter.key, Value: noteIter.value}) {
				return
			}
		}
	}
}
