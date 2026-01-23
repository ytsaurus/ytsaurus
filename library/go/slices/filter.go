package slices

// Filter reduces slice values using given function.
// It operates with a copy of given slice
func Filter[S ~[]T, T any](s S, fn func(T) bool) S {
	if len(s) == 0 {
		return s
	}
	result := make([]T, 0, len(s))
	for _, v := range s {
		if fn(v) {
			result = append(result, v)
		}
	}
	return result
}

// Reduce is like Filter, but modifies original slice.
func Reduce[S ~[]T, T any](s S, fn func(T) bool) S {
	if len(s) == 0 {
		return s
	}
	var p int
	for _, v := range s {
		if fn(v) {
			s[p] = v
			p++
		}
	}
	return s[:p]
}
