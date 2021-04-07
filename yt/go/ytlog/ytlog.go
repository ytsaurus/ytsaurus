package ytlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/core/log/zap/asynczap"
	"a.yandex-team.ru/yt/go/ytlog/selfrotate"
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

// NewSelfrotate returns logger configured with YT defaults.
func NewSelfrotate(logPath string) (l *logzap.Logger, stop func(), err error) {
	options := defaultRotationOptions
	options.Name = logPath

	w, err := selfrotate.New(options)
	if err != nil {
		return nil, nil, err
	}

	encoder := zap.NewProductionEncoderConfig()
	encoder.EncodeTime = zapcore.ISO8601TimeEncoder

	core := asynczap.NewCore(zapcore.NewJSONEncoder(encoder), w, zap.DebugLevel, asynczap.Options{})

	stop = func() {
		core.Stop()
		_ = w.Close()
	}

	// TODO(prime@): make callerSkip from library/go/core/log public.
	zl := zap.New(core, zap.AddCallerSkip(1), zap.AddCaller())
	return &logzap.Logger{L: zl}, stop, nil
}
