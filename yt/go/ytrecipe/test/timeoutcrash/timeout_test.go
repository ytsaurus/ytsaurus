package timeoutcrash

import (
	"os"
	"testing"
	"time"
)

func TestOK(t *testing.T) {

}

func TestTimeout(t *testing.T) {
	t.Logf("test started")

	if os.Getenv("AUTOCHECK") != "" {
		time.Sleep(time.Hour)
	}
}
