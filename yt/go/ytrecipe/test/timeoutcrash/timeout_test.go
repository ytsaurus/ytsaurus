package timeoutcrash

import (
	"testing"
	"time"
)

func TestOK(t *testing.T) {

}

func TestTimeout(t *testing.T) {
	t.Logf("test started")
	time.Sleep(time.Hour)
}
