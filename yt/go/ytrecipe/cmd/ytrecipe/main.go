package main

import (
	"fmt"

	"github.com/spf13/pflag"

	"a.yandex-team.ru/library/go/test/recipe"
	"a.yandex-team.ru/library/go/test/yatest"
)

type ytRecipe struct{}

func (y ytRecipe) Start() error {
	if flagJobRun == "" {
		return fmt.Errorf("--job-run flag is required")
	}

	recipe.SetEnv("YTRECIPE_CONFIG_PATH", yatest.SourcePath(flagConfigPath))
	recipe.SetEnv("TEST_COMMAND_WRAPPER", yatest.BuildPath(flagJobRun))
	return nil
}

func (y ytRecipe) Stop() error {
	return nil
}

var (
	flagConfigPath string
	flagJobRun     string
)

func main() {
	pflag.StringVar(&flagConfigPath, "config-path", "", "")
	pflag.StringVar(&flagJobRun, "job-run", "", "")

	recipe.Run(ytRecipe{})
}
