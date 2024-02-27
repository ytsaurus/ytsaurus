package ytlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/library/go/core/log/zap/asynczap"
	"go.ytsaurus.tech/yt/go/ytlog/selfrotate"
)

// New returns synchronous stderr logger configured with YT defaults.
func New() (*logzap.Logger, error) {
	conf := zap.NewProductionConfig()
	conf.Level.SetLevel(zap.DebugLevel)
	conf.Sampling = nil
	conf.DisableStacktrace = true
	conf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	conf.OutputPaths = []string{"stderr"}

	return logzap.New(conf)
}

// Must does the same as New but panics on error.
func Must() *logzap.Logger {
	l, err := New()
	if err != nil {
		panic(err)
	}
	return l
}

var defaultRotationOptions = selfrotate.Options{
	MaxKeep:        0,
	MaxSize:        0,
	MinFreeSpace:   0.05,
	Compress:       selfrotate.CompressDelayed,
	RotateInterval: selfrotate.RotateHourly,
}

type (
	logLevel struct {
		level zapcore.Level
	}

	Option interface {
		isOption()
	}
)

func (logLevel) isOption() {}

func WithLogLevel(level zapcore.Level) Option {
	return logLevel{level: level}
}

// NewSelfrotate returns logger configured with YT defaults.
func NewSelfrotate(logPath string, options ...Option) (l *logzap.Logger, stop func(), err error) {
	rotateOptions := defaultRotationOptions
	rotateOptions.Name = logPath

	w, err := selfrotate.New(rotateOptions)
	if err != nil {
		return nil, nil, err
	}

	encoder := zap.NewProductionEncoderConfig()
	encoder.EncodeTime = zapcore.ISO8601TimeEncoder

	level := zap.DebugLevel
	for _, opt := range options {
		switch v := opt.(type) {
		case logLevel:
			level = v.level
		}
	}

	core := asynczap.NewCore(zapcore.NewJSONEncoder(encoder), w, level, asynczap.Options{})

	stop = func() {
		core.Stop()
		_ = w.Close()
	}

	// TODO(prime@): make callerSkip from library/go/core/log public.
	zl := zap.New(core, zap.AddCallerSkip(1), zap.AddCaller())
	return &logzap.Logger{L: zl}, stop, nil
}
