package app

import (
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/go/ytlog"
)

func stderrLogger() *logzap.Logger {
	conf := zap.NewDevelopmentConfig()
	conf.Level.SetLevel(zap.DebugLevel)
	conf.Sampling = nil
	conf.DisableStacktrace = true
	conf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	conf.OutputPaths = []string{"stderr"}

	logger, err := logzap.New(conf)
	if err != nil {
		panic(err)
	}
	return logger
}

func withName(l *logzap.Logger, name string) *logzap.Logger {
	return &logzap.Logger{L: l.L.Named(name)}
}

func newLogger(name string, stderr bool) (l *logzap.Logger) {
	if stderr {
		l = stderrLogger()
	} else {
		foo, _, err := ytlog.NewSelfrotate(filepath.Join(".", name+".log"))
		if err != nil {
			panic(err)
		}
		l = foo
	}

	return withName(l, name)
}
