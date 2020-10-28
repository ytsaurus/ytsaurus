package gotest

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecipeEnvironment(t *testing.T) {
	time.Sleep(time.Second * 10)

	t.Skipf("test is working")
}

func TestFDQN(t *testing.T) {
	cmd := exec.Command("/bin/hostname", "-f")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr
	require.NoError(t, cmd.Run())
}
