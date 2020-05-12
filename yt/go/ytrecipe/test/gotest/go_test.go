package gotest

import (
	"testing"
	"time"
)

func TestRecipeEnvironment(t *testing.T) {
	time.Sleep(time.Second * 10)

	t.Skipf("test is working")
}
