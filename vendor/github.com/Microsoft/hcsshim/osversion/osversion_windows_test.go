package osversion

import (
	"fmt"
	"testing"
)

func TestOSVersionString(t *testing.T) {
	v := OSVersion{
		Version:      809042555,
		MajorVersion: 123,
		MinorVersion: 2,
		Build:        12345,
	}
	expected := "the version is: 123.2.12345"
	actual := fmt.Sprintf("the version is: %s", v)
	if actual != expected {
		t.Errorf("expected: %q, got: %q", expected, actual)
	}
}
