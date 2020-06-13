package joberror

import (
	"os"
	"testing"
)

func TestJobError(t *testing.T) {
	if os.Getenv("AUTOCHECK") != "" {
		return
	}

	//_, err := os.Create(yatest.OutputPath(ytrecipe.CrashJobFileMarker))
	//require.NoError(t, err)
}
