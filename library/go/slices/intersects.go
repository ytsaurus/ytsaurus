package slices

// Intersection returns intersection for slices of various built-in types.
//
// Note that this function does not perform deduplication on result slice,
// expect duplicate entries to be present in it.
func Intersection[E comparable](a, b []E) []E {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}

	p, s := a, b
	if len(b) > len(a) {
		p, s = b, a
	}

	m := make(map[E]struct{}, len(s))
	for _, i := range s {
		m[i] = struct{}{}
	}

	res := make([]E, 0, len(m))
	for _, v := range p {
		if _, exists := m[v]; exists {
			res = append(res, v)
		}
	}

	if len(res) == 0 {
		return nil
	}

	return res
}

// IntersectStrings returns intersection of two string slices
// Deprecated: use Intersection instead.
var IntersectStrings = Intersection[string]

// IntersectInts returns intersection of two int slices
// Deprecated: use Intersection instead.
var IntersectInts = Intersection[int]
