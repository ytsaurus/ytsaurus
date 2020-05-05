package timeoutcrash

import (
	"testing"
	"time"
)

func TestTimeout(t *testing.T) {
	time.Sleep(time.Second * 30)
}
