package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type MyGoodBaseConfig struct {
	Config
	SomeUserCustomField int
}

type MyGoodConfig struct {
	MyGoodBaseConfig
	AnotherUserCustomField string
}

type BadConfig1 struct {
	Foo string
	Bar int
}

func TestResolveConfig(t *testing.T) {
	var config Config
	config.Config.WorkDir = "/work/dir/of/config"
	resolved, err := resolveAppConfig(&config)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config", resolved.WorkDir)

	var goodBaseConfig MyGoodBaseConfig
	goodBaseConfig.WorkDir = "/work/dir/of/config2"
	resolved, err = resolveAppConfig(&goodBaseConfig)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config2", resolved.WorkDir)

	var goodConfig MyGoodConfig
	goodConfig.WorkDir = "/work/dir/of/config3"
	resolved, err = resolveAppConfig(&goodConfig)
	require.NoError(t, err)
	require.Equal(t, "/work/dir/of/config3", resolved.WorkDir)

	var badConfig1 BadConfig1
	_, err = resolveAppConfig(&badConfig1)
	require.Error(t, err, "bad user config type")
}
