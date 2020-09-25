package ytexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/pflag"

	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yterrors"
)

var (
	flagJSONConfig string
	flagYSONConfig string
)

func do() error {
	pflag.StringVar(&flagJSONConfig, "config", "", "path to json config")
	pflag.StringVar(&flagYSONConfig, "config-yson", "", "path to yson config")
	pflag.Parse()

	var config Config

	switch {
	case flagJSONConfig != "":
		configJS, err := ioutil.ReadFile(flagJSONConfig)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(configJS, &config); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}

	case flagYSONConfig != "":
		configYS, err := ioutil.ReadFile(flagYSONConfig)
		if err != nil {
			return err
		}

		if err := yson.Unmarshal(configYS, &config); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}

	default:
		return fmt.Errorf("either --config or --config-yson must be specified")
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
