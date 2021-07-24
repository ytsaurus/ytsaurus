package main

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
)

func StderrLogger() *logzap.Logger {
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
