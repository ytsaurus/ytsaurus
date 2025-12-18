package slices

type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// SumTransform transforms given slice values to Number and sums them up
func SumTransform[S ~[]T, T any, N Number](s S, fn func(T) N) N {
	if len(s) == 0 || fn == nil {
		return 0
	}

	var sum N
	for _, v := range s {
		sum += fn(v)
	}
	return sum
}
