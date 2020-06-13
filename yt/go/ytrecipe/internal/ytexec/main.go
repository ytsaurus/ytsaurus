package ytexec

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/pflag"
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

	return exec.Run(context.Background())
}

func Main() {
	if err := do(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
	}
}
