package main

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"

	"a.yandex-team.ru/library/go/core/log"
	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ytlog"
	"a.yandex-team.ru/yt/go/ytprof/internal/app"
)

var (
	flagLogsDir     string
	flagLogToStderr bool
	flagConfigPath  string
	flagConfigJSON  string
)

func newLogger(name string) *logzap.Logger {
	if flagLogToStderr {
		return ytlog.Must()
	}

	l, _, err := ytlog.NewSelfrotate(filepath.Join(flagLogsDir, name+".log"))
	if err != nil {
		panic(err)
	}

	return l
}

func readConfig() (config app.Config, err error) {
	if flagConfigPath != "" {
		var js []byte
		js, err = os.ReadFile(flagConfigPath)
		if err != nil {
			return
		}

		err = yaml.Unmarshal(js, &config)
		return
	} else {
		err = json.Unmarshal([]byte(flagConfigJSON), &config)
		return
	}
}

func main() {
	pflag.StringVar(&flagLogsDir, "log-dir", "/logs", "path to the log directory")
	pflag.BoolVar(&flagLogToStderr, "log-to-stderr", false, "write logs to stderr")
	pflag.StringVar(&flagConfigPath, "config", "", "path to the yaml config")
	pflag.StringVar(&flagConfigJSON, "config-json", "", "json config")
	pflag.Parse()

	l := newLogger("ytprof")

	config, err := readConfig()
	if err != nil {
		l.Fatal("failed to read config", log.Error(err))
	}
	l.Debug("reading config succeeded",
		log.Int("query_limit", config.QueryLimit),
		log.String("http_endpoint", config.HTTPEndpoint),
		log.String("proxy", config.Proxy),
		log.String("folder_path", config.FolderPath),
		log.String("proxy", config.Proxy))

	_ = app.NewApp(l, config)
	select {}
}
