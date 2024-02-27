package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/chyt/controller/test/cli"
	"go.ytsaurus.tech/yt/chyt/controller/test/helpers"
)

func TestCLISimple(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	r := cli.NewRunner(env, t)

	r.EnvVariables["CHYT_CTL_ADDRESS"] = c.Endpoint

	err := r.RunYT("clickhouse", "ctl", "-h")
	require.NoError(t, err)

	alias := helpers.GenerateAlias()
	err = r.RunYT("clickhouse", "ctl", "create", alias)
	require.NoError(t, err)

	ok, err := env.YT.NodeExists(env.Ctx, env.StrawberryRoot.Child(alias), nil)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCLIControllerUnavailable(t *testing.T) {
	t.Parallel()

	env := helpers.PrepareEnv(t)
	r := cli.NewRunner(env, t)

	r.EnvVariables["CHYT_CTL_ADDRESS"] = "http://this.address.does.not.exists"

	// Should fail since ctl service is unavailable.
	err := r.RunYT("clickhouse", "ctl", "-h")
	require.Error(t, err)

	// Other commands should not depend on controller address.
	err = r.RunYT("list", "/")
	require.NoError(t, err)
}

func TestCLIYsonOutput(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	r := cli.NewRunner(env, t)

	r.EnvVariables["CHYT_CTL_ADDRESS"] = c.Endpoint

	output, err := r.RunYTWithOutput("clickhouse", "ctl", "exists", "this_clique_does_not_exist")
	require.NoError(t, err)
	require.Equal(t, []byte("%false\n"), output)
}

func TestCLIListMutuallyExclusiveGroup(t *testing.T) {
	t.Parallel()

	env, c := helpers.PrepareAPI(t)
	r := cli.NewRunner(env, t)

	r.EnvVariables["CHYT_CTL_ADDRESS"] = c.Endpoint

	err := r.RunYT("clickhouse", "ctl", "list", "--attribute", "creator", "--attributes", "[start_time]")
	require.Error(t, err)

	err = r.RunYT("clickhouse", "ctl", "list")
	require.NoError(t, err)

	err = r.RunYT("clickhouse", "ctl", "list", "--attribute", "creator", "--attribute", "start_time")
	require.NoError(t, err)

	err = r.RunYT("clickhouse", "ctl", "list", "--attributes", "[start_time]")
	require.NoError(t, err)
}
