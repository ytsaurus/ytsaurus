package jsonschema

import "testing"

func TestQuote(t *testing.T) {
	got, want := quote(`abc"def'ghi`), `'abc"def\'ghi'`
	if got != want {
		t.Fatalf("got: %s want: %s", got, want)
	}
}
