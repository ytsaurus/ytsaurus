package yttest

import (
	"bytes"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	zaplog "go.ytsaurus.tech/library/go/core/log/zap"
)

// testingWriter is a WriteSyncer that writes to the given testing.TB.
type testingWriter struct {
	t testing.TB

	mu      sync.Mutex
	stopped bool
}

func (w *testingWriter) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stopped = true
}

func (w *testingWriter) Write(p []byte) (n int, err error) {
	n = len(p)

	// Strip trailing newline because t.Log always adds one.
	p = bytes.TrimRight(p, "\n")

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.stopped {
		w.t.Logf("%s", p)
	}

	return n, nil
}

func (w *testingWriter) Sync() error {
	return nil
}

func NewLogger(t testing.TB) (logger *zaplog.Logger, stop func()) {
	writer := &testingWriter{t: t}

	l := zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig()),
			writer,
			zapcore.DebugLevel,
		),
		zap.AddCallerSkip(1))

	return &zaplog.Logger{L: l}, func() {
		writer.Stop()
	}
}
