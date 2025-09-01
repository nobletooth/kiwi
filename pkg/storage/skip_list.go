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
	"cmp"
	"errors"
	"math/rand"
	"time"
)

// skipListNode represents a node in the skip list.
type skipListNode[K cmp.Ordered, V any] struct {
	key      K
	value    V
	forwards []*skipListNode[K, V] // forward pointers per level (0..level-1)
}

// SkipList is a probabilistically balanced ordered map over comparable keys.
// Keys are strictly ordered using cmp.Compare from the standard library.
// The structure maintains up to maxLevel layers; each node appears in level i
// with probability p^i (independently), enabling logarithmic expected search.
type SkipList[K cmp.Ordered, V any] struct {
	head            *skipListNode[K, V]
	level, maxLevel int
	p               float64 // Probability that a node is promoted to the next level.
	rnd             *rand.Rand
}

// NewSkipList creates a new empty skip list.
// Defaults: maxLevel=16, p=0.25.
func NewSkipList[K cmp.Ordered, V any]() *SkipList[K, V] {
	const defaultMaxLevel = 16
	const defaultP = 0.25
	return &SkipList[K, V]{
		head:     &skipListNode[K, V]{forwards: make([]*skipListNode[K, V], defaultMaxLevel)},
		level:    1,
		maxLevel: defaultMaxLevel,
		p:        defaultP,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
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

// Get returns the value for key or ErrKeyNotFound if the key is absent.
// It traverses from the highest populated level down to level 0, advancing at
// each level while the next key is still less than the target key.
func (s *SkipList[K, V]) Get(key K) (V, error) {
	var zero V
	if s == nil || s.head == nil {
		return zero, ErrKeyNotFound
	}
	n := s.head
	// Traverse from top level down to level 0.
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := n.forwards[lvl]; next != nil && cmp.Compare(next.key, key) < 0; next = n.forwards[lvl] {
			n = next
		}
	}
	// Candidate is at level 0 forward from n.
	n = n.forwards[0]
	if n != nil && cmp.Compare(n.key, key) == 0 {
		return n.value, nil
	}
	return zero, ErrKeyNotFound
}

// Set inserts a new key/value or updates an existing one.
// It records the immediate predecessors per level during the search, then
// either updates in place or splices in a new node of random level.
func (s *SkipList[K, V]) Set(key K, value V) error {
	if s == nil || s.head == nil {
		return errors.New("skip list not initialized")
	}
	// Track the last nodes before the position at each level.
	update := make([]*skipListNode[K, V], s.maxLevel)
	node := s.head
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := node.forwards[lvl]; next != nil && cmp.Compare(next.key, key) < 0; next = node.forwards[lvl] {
			node = next
		}
		update[lvl] = node
	}
	// Check if the key already exists at level 0.
	if next := node.forwards[0]; next != nil && cmp.Compare(next.key, key) == 0 {
		next.value = value
		return nil
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
	return nil
}

// Delete removes key from the list or returns ErrKeyNotFound.
// It finds predecessors at each level and rewires forward pointers to skip the
// target node, then trims empty top levels.
func (s *SkipList[K, V]) Delete(key K) error {
	if s == nil || s.head == nil {
		return ErrKeyNotFound
	}
	update := make([]*skipListNode[K, V], s.maxLevel)
	n := s.head
	for lvl := s.level - 1; lvl >= 0; lvl-- {
		for next := n.forwards[lvl]; next != nil && cmp.Compare(next.key, key) < 0; next = n.forwards[lvl] {
			n = next
		}
		update[lvl] = n
	}
	target := n.forwards[0]
	if target == nil || cmp.Compare(target.key, key) != 0 {
		return ErrKeyNotFound
	}
	for i := 0; i < s.level; i++ {
		if update[i].forwards[i] == target {
			update[i].forwards[i] = target.forwards[i]
		}
	}
	// Decrease level if the top levels are now empty.
	for s.level > 1 && s.head.forwards[s.level-1] == nil {
		s.level--
	}
	return nil
}

// Close releases no resources to free for now.
func (s *SkipList[K, V]) Close() error {
	return nil
}
