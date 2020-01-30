package ytlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/core/log/zap/asynczap"
	"a.yandex-team.ru/yt/go/ytlog/selfrotate"
)

var defaultRotationOptions = selfrotate.Options{
	MaxKeep:        0,
	MaxSize:        0,
	MinFreeSpace:   0.05,
	Compress:       selfrotate.CompressDelayed,
	RotateInterval: selfrotate.RotateHourly,
}

// New returns logger configured with YT defaults.
func New(logPath string) (l *logzap.Logger, stop func(), err error) {
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
