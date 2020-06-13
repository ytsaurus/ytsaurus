package job

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	zaplog "a.yandex-team.ru/library/go/core/log/zap"
)

var jobLogConfig = zap.Config{
	Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
	Encoding:         "console",
	EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
	OutputPaths:      []string{"stderr"},
	ErrorOutputPaths: []string{"stderr"},
}

func init() {
	jobLogConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
}

func (j *Job) initLog() error {
	l, err := zaplog.New(jobLogConfig)
	if err != nil {
		return err
	}

	j.L = l
	return nil
}
