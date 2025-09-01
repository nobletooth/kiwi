package cache

// LinkedListNode represents a node in the doubly linked list.
type LinkedListNode[V any] struct {
	next  *LinkedListNode[V]
	prev  *LinkedListNode[V]
	Value V
}

// Next returns the next node in the list.
func (n *LinkedListNode[V]) Next() *LinkedListNode[V] {
	return n.next
}

// Prev returns the previous node in the list.
func (n *LinkedListNode[V]) Prev() *LinkedListNode[V] {
	return n.prev
}

// LinkedList represents a doubly linked list.
type LinkedList[V any] struct {
	head *LinkedListNode[V]
	tail *LinkedListNode[V]
	size int
}

// Len returns the number of elements in the list.
func (l *LinkedList[V]) Len() int {
	return l.size
}

// Front returns the first node of the list or nil if the list is empty.
func (l *LinkedList[V]) Front() *LinkedListNode[V] {
	return l.head
}

// Back returns the last node of the list or nil if the list is empty.
func (l *LinkedList[V]) Back() *LinkedListNode[V] {
	return l.tail
}

// Remove removes a node from the list.
func (l *LinkedList[V]) Remove(n *LinkedListNode[V]) {
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
func (l *LinkedList[V]) PushFront(v V) *LinkedListNode[V] {
	n := &LinkedListNode[V]{Value: v, next: l.head}
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
func (l *LinkedList[V]) PushBack(v V) *LinkedListNode[V] {
	n := &LinkedListNode[V]{Value: v, prev: l.tail}
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
