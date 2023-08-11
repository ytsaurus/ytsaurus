package cli

import (
	"os"
	"testing"
)

const envYTBinaryPath = "YT_BINARY_PATH"

// setupYTBinary sets YT binary path and required env variables.
func (r *runner) setupYTBinary(t *testing.T) {
	path := os.Getenv(envYTBinaryPath)
	if path == "" {
		t.Skipf("Skipping testing as there is no yt binary found in %q.", envYTBinaryPath)
	}
	r.ytBinaryPath = path
}
