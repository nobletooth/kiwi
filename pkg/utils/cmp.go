package utils

// CompareFn defines a three-way comparison for keys of type T.
// It must return a negative value if x < y, 0 if x == y, and a positive value if x > y.
type CompareFn[T any] func(x, y T) int

// IsZero checks if a value is the zero value for its type
func IsZero[T any](v T, compare CompareFn[T]) bool {
	var zero T
	return compare(v, zero) == 0
}
