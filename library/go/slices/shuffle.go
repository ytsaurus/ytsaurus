package slices

import (
	"math/rand"
)

// Shuffle shuffles values in slice using given or pseudo-random source.
// It will alter original non-empty slice, consider copy it beforehand.
func Shuffle[E any](a []E, src rand.Source) []E {
	if len(a) < 2 {
		return a
	}
	shuffle(src)(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})
	return a
}

func shuffle(src rand.Source) func(n int, swap func(i, j int)) {
	shuf := rand.Shuffle
	if src != nil {
		shuf = rand.New(src).Shuffle
	}
	return shuf
}
