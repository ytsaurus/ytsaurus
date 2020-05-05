package sudo

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSudo(t *testing.T) {
	cmd := exec.Command("groups")
	cmd.Stdout = os.Stderr
	require.NoError(t, cmd.Run())

	cmd = exec.Command("sudo", "whoami")
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
}
