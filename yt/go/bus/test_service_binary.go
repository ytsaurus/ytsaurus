package bus

import (
	"os"
	"testing"
)

const envTestServiceBinaryPath = "TEST_SERVICE_BINARY_PATH"

func GetTestServiceBinary(t *testing.T) string {
	path := os.Getenv(envTestServiceBinaryPath)
	if path == "" {
		t.Skipf("Skipping testing as there is no test service binary found in %q.", envTestServiceBinaryPath)
	}
	return path
}
