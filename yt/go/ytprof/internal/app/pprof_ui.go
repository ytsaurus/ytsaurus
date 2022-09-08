package app

import (
	"time"

	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
)

var _ driver.Fetcher = (*profileFetcher)(nil)
var _ driver.FlagSet = (*flags)(nil)

type profileFetcher struct {
	profile *profile.Profile
}

func (p *profileFetcher) Fetch(s string, d, t time.Duration) (*profile.Profile, string, error) {
	return p.profile, s, nil
}

// flags implements the plugin.FlagSet interface.
// origin: https://github.com/google/pprof/blob/70bd9ae97f40a5e0ef03e61bec8e90f3a5732191/internal/driver/driver_test.go#L297
type flags struct {
	bools       map[string]bool
	ints        map[string]int
	floats      map[string]float64
	strings     map[string]string
	args        []string
	stringLists map[string][]string
}

func (flags) ExtraUsage() string { return "" }

func (flags) AddExtraUsage(eu string) {}

func (f flags) Bool(s string, d bool, c string) *bool {
	if b, ok := f.bools[s]; ok {
		return &b
	}
	return &d
}

func (f flags) Int(s string, d int, c string) *int {
	if i, ok := f.ints[s]; ok {
		return &i
	}
	return &d
}

func (f flags) Float64(s string, d float64, c string) *float64 {
	if g, ok := f.floats[s]; ok {
		return &g
	}
	return &d
}

func (f flags) String(s, d, c string) *string {
	if t, ok := f.strings[s]; ok {
		return &t
	}
	return &d
}

func (f flags) StringList(s, d, c string) *[]*string {
	if t, ok := f.stringLists[s]; ok {
		// convert slice of strings to slice of string pointers before returning.
		tp := make([]*string, len(t))
		for i, v := range t {
			tp[i] = &v
		}
		return &tp
	}
	return &[]*string{}
}

func (f flags) Parse(func()) []string {
	return f.args
}

func baseFlags() flags {
	return flags{
		bools:  map[string]bool{},
		ints:   map[string]int{},
		floats: map[string]float64{},
		strings: map[string]string{
			"http":      "localhost:80",
			"symbolize": "None",
		},
		args: []string{"http://example.com/pprof"}, // dummy url to trigger fetching
	}
}
