package cli

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

type runner struct {
	ytBinaryPath string
	EnvVariables map[string]string

	env *helpers.Env
	t   *testing.T
}

func NewRunner(env *helpers.Env, t *testing.T) runner {
	r := runner{
		EnvVariables: map[string]string{
			"YT_TEST_USER": "root",
		},
		env: env,
		t:   t,
	}
	r.setupYTBinary(t)
	return r
}

func (r *runner) RunYTWithOutput(args ...string) ([]byte, error) {
	r.env.L.Debug("running command", log.Strings("args", args))
	require.GreaterOrEqual(r.t, len(args), 1)

	cmd := exec.Command(r.ytBinaryPath, args...)
	cmd.Env = os.Environ()
	for key, value := range r.EnvVariables {
		cmd.Env = append(cmd.Env, key+"="+value)
	}

	output, err := cmd.CombinedOutput()
	r.env.L.Debug("command finished", log.ByteString("output", output))
	return output, err
}

func (r *runner) RunYT(args ...string) error {
	_, err := r.RunYTWithOutput(args...)
	return err
}
