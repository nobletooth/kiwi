package cache

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

// assertLinkedListEqualsSlice makes sure the list elements match the linked list elements.
func assertLinkedListEqualsSlice[V comparable](t *testing.T, expected []V, list *linkedList[V]) {
	t.Helper()

	assert.Equal(t, len(expected), list.Len(), "List length mismatch")

	if len(expected) == 0 {
		assert.Nil(t, list.Front(), "Empty list should have nil Front()")
		assert.Nil(t, list.Back(), "Empty list should have nil Back()")
		return
	}

	// Check head and tail values.
	assert.NotNil(t, list.Front())
	assert.NotNil(t, list.Back())
	assert.Equal(t, expected[0], list.Front().Value, "Front() value mismatch")
	assert.Equal(t, expected[len(expected)-1], list.Back().Value, "Back() value mismatch")

	// Forward iteration.
	var forwardResult []V
	for node := list.Front(); node != nil; node = node.Next() {
		forwardResult = append(forwardResult, node.Value)
	}
	assert.Equal(t, expected, forwardResult, "Forward iteration mismatch")

	// Backward iteration.
	var backwardResult []V
	for node := list.Back(); node != nil; node = node.Prev() {
		backwardResult = append(backwardResult, node.Value)
	}
	// Reverse the backward result to compare with expected.
	slices.Reverse(backwardResult)
	assert.Equal(t, expected, backwardResult, "Backward iteration mismatch")
}

func TestLinkedList_Push(t *testing.T) {
	t.Run("PushBack", func(t *testing.T) {
		list := new(linkedList[int])
		list.PushBack(1)
		assertLinkedListEqualsSlice(t, []int{1}, list)
		list.PushBack(2)
		assertLinkedListEqualsSlice(t, []int{1, 2}, list)
		list.PushBack(3)
		assertLinkedListEqualsSlice(t, []int{1, 2, 3}, list)
	})

	t.Run("PushFront", func(t *testing.T) {
		list := new(linkedList[int])
		list.PushFront(1)
		assertLinkedListEqualsSlice(t, []int{1}, list)
		list.PushFront(2)
		assertLinkedListEqualsSlice(t, []int{2, 1}, list)
		list.PushFront(3)
		assertLinkedListEqualsSlice(t, []int{3, 2, 1}, list)
	})

	t.Run("Mixed Push", func(t *testing.T) {
		list := new(linkedList[int])
		list.PushBack(2)
		list.PushFront(1)
		list.PushBack(3)
		assertLinkedListEqualsSlice(t, []int{1, 2, 3}, list)
	})
}

func TestLinkedList_Remove(t *testing.T) {
	// Helper to create a list for testing removal.
	newLinkedListWithNodes := func(nodeCount int) (*linkedList[int], []*linkedListNode[int]) {
		list := new(linkedList[int])
		nodes := make([]*linkedListNode[int], nodeCount)
		for i := 1; i <= nodeCount; i++ {
			nodes[i-1] = list.PushBack(i)
		}
		return list, nodes
	}

	t.Run("Remove from middle", func(t *testing.T) {
		list, nodes := newLinkedListWithNodes(5)
		// Remove 3 (node at index 2).
		list.Remove(nodes[2])
		assertLinkedListEqualsSlice(t, []int{1, 2, 4, 5}, list)

		// Check that the neighbors of the removed node are correctly linked.
		assert.Equal(t, nodes[3], nodes[1].Next(), "Node 2's next should be node 4")
		assert.Equal(t, nodes[1], nodes[3].Prev(), "Node 4's prev should be node 2")
	})

	t.Run("Remove head", func(t *testing.T) {
		list, nodes := newLinkedListWithNodes(5)
		list.Remove(nodes[0]) // Remove 1.
		assertLinkedListEqualsSlice(t, []int{2, 3, 4, 5}, list)
	})

	t.Run("Remove tail", func(t *testing.T) {
		list, nodes := newLinkedListWithNodes(5)
		list.Remove(nodes[4]) // Remove 5.
		assertLinkedListEqualsSlice(t, []int{1, 2, 3, 4}, list)
	})

	t.Run("Remove until empty", func(t *testing.T) {
		list, nodes := newLinkedListWithNodes(5)
		for i := 0; i < len(nodes); i++ {
			list.Remove(nodes[i])
		}
		assertLinkedListEqualsSlice(t, []int{}, list)
	})

	t.Run("Remove the only element", func(t *testing.T) {
		list := new(linkedList[int])
		node := list.PushBack(1)
		list.Remove(node)
		assertLinkedListEqualsSlice(t, []int{}, list)
	})
}
