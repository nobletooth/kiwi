package cache

// linkedListNode represents a node in the doubly linked list.
type linkedListNode[V any] struct {
	next  *linkedListNode[V]
	prev  *linkedListNode[V]
	Value V
}

// Next returns the next node in the list.
func (n *linkedListNode[V]) Next() *linkedListNode[V] {
	return n.next
}

// Prev returns the previous node in the list.
func (n *linkedListNode[V]) Prev() *linkedListNode[V] {
	return n.prev
}

// linkedList represents a doubly linked list.
type linkedList[V any] struct {
	head *linkedListNode[V]
	tail *linkedListNode[V]
	size int
}

// Len returns the number of elements in the list.
func (l *linkedList[V]) Len() int {
	return l.size
}

// Front returns the first node of the list or nil if the list is empty.
func (l *linkedList[V]) Front() *linkedListNode[V] {
	return l.head
}

// Back returns the last node of the list or nil if the list is empty.
func (l *linkedList[V]) Back() *linkedListNode[V] {
	return l.tail
}

// Remove removes a node from the list.
func (l *linkedList[V]) Remove(n *linkedListNode[V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		// Node is the head.
		l.head = n.next
	}

	if n.next != nil {
		n.next.prev = n.prev
	} else {
		// Node is the tail.
		l.tail = n.prev
	}

	// Clean up the removed node's pointers.
	n.next = nil
	n.prev = nil

	l.size--
}

// PushFront adds a new value to the front of the list.
func (l *linkedList[V]) PushFront(v V) *linkedListNode[V] {
	n := &linkedListNode[V]{Value: v, next: l.head}
	if l.head != nil {
		l.head.prev = n
	} else { // List was empty.
		l.tail = n
	}
	l.head = n
	l.size++
	return n
}

// PushBack adds a new value to the back of the list.
func (l *linkedList[V]) PushBack(v V) *linkedListNode[V] {
	n := &linkedListNode[V]{Value: v, prev: l.tail}
	if l.tail != nil {
		l.tail.next = n
	} else {
		// List was empty.
		l.head = n
	}
	l.tail = n
	l.size++
	return n
}
