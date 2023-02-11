package ytexec

import (
	"context"
	"encoding/json"
	"fmt"
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
		configJS, err := os.ReadFile(flagJSONConfig)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(configJS, &config); err != nil {
			return fmt.Errorf("invalid config: %w", err)
		}

	case flagYSONConfig != "":
		configYS, err := os.ReadFile(flagYSONConfig)
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

	ctx := context.Background()

	if err := exec.Run(ctx); err != nil {
		return err
	}

	_, err = exec.ReadOutputs(ctx)
	return err
}

func Main() {
	if err := do(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", yterrors.FromError(err))
		os.Exit(1)
	}
}
