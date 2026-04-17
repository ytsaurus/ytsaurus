package slices

import (
	"cmp"
	"slices"
	"sort"
)

// Dedup removes duplicate values from slice.
// It will alter original non-empty slice, consider copy it beforehand.
func Dedup[E cmp.Ordered](s []E) []E {
	if len(s) < 2 {
		return s
	}

	slices.Sort(s)

	cur, tmp := s[0], s[:1]
	for i := 1; i < len(s); i++ {
		if s[i] != cur {
			tmp = append(tmp, s[i])
			cur = s[i]
		}
	}

	return tmp
}

// DedupBools removes duplicate values from bool slice.
// It will alter original non-empty slice, consider copy it beforehand.
func DedupBools(a []bool) []bool {
	if len(a) < 2 {
		return a
	}

	sort.Slice(a, func(i, j int) bool { return a[i] != a[j] })

	cur, tmp := a[0], a[:1]
	for i := 1; i < len(a); i++ {
		if a[i] != cur {
			tmp = append(tmp, a[i])
			cur = a[i]
		}
	}

	return tmp
}

// DedupStrings removes duplicate values from string slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupStrings = Dedup[string]

// DedupInt64s removes duplicate values from int64 slice.
// It will alter original non-empty slice, consider copy it beforehand.
// Deprecated: use Dedup instead.
var DedupInt64s = Dedup[int64]
