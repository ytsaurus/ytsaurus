package ytexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/pflag"

	"a.yandex-team.ru/yt/go/yterrors"
)

var (
	flagConfig string
)

func do() error {
	pflag.StringVar(&flagConfig, "config", "", "path to json files with config")
	pflag.Parse()

	configJS, err := ioutil.ReadFile(flagConfig)
	if err != nil {
		return err
	}

	var config Config
	if err := json.Unmarshal(configJS, &config); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	exec, err := New(config)
	if err != nil {
		return err
	}

	if err := exec.Run(context.Background()); err != nil {
		return err
	}

	_, err = exec.ReadOutputs(context.Background())
	return err
}

func Main() {
	if err := do(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", yterrors.FromError(err))
		os.Exit(1)
	}
}
