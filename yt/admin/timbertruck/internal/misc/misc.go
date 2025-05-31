package misc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.ytsaurus.tech/library/go/core/buildinfo"
)

const (
	defaultBufferSize = 32768
)

var logrotatingLoggerLock sync.Mutex
var logrotatingLogger *slog.Logger

type LogrotatingFile struct {
	path       string
	bufferSize int
	file       io.WriteCloser
	lock       sync.Mutex
}

func SetLogrotatingLogger(logger *slog.Logger) {
	logrotatingLoggerLock.Lock()
	defer logrotatingLoggerLock.Unlock()

	logrotatingLogger = logger
}

func getLogrotatingLogger() *slog.Logger {
	logrotatingLoggerLock.Lock()
	defer logrotatingLoggerLock.Unlock()

	return logrotatingLogger
}

type loggingStarted struct{}

var LoggingStartedKey loggingStarted

func LogLoggingStarted(logger *slog.Logger) {
	ctx := context.WithValue(context.Background(), &LoggingStartedKey, 1)
	logger.InfoContext(ctx, "Logging started", "version", buildinfo.Info.ProgramVersion)
}

// Open new file and register SIGHUP handler to reopen it.
// Meant to be used as io.Writer for slog.Handler
func NewLogrotatingFile(path string, bufferSize int, reopenLogFileInterval time.Duration) (result io.WriteCloser, err error) {
	if bufferSize == 0 {
		bufferSize = defaultBufferSize
	}
	logrotating := &LogrotatingFile{
		path:       path,
		bufferSize: bufferSize,
	}
	err = logrotating.reopen()
	if err != nil {
		return
	}

	ch := make(chan os.Signal, 16)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for {
			reopenLogFile(logrotating, ch, reopenLogFileInterval)
		}
	}()
	result = logrotating
	return
}

func reopenLogFile(logrotating *LogrotatingFile, ch <-chan os.Signal, reopenLogFileInterval time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), reopenLogFileInterval)
	defer cancel()

	select {
	case <-ch:
	case <-ctx.Done():
		log.Default().Printf("Context expired after %v: %v. Reopening log file.", reopenLogFileInterval, ctx.Err().Error())
	}

	err := logrotating.reopen()
	if err != nil {
		log.Default().Printf("Failed to reopen log file: %v", err)
		return
	}
	logger := getLogrotatingLogger()
	if logger != nil {
		LogLoggingStarted(logger)
	}
}

func (r *LogrotatingFile) Write(p []byte) (n int, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.file == nil {
		return 0, errors.New("file is closed")
	}
	n, err = r.file.Write(p)
	return
}

func (r *LogrotatingFile) reopen() (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	oldFile := r.file

	file, err := os.OpenFile(r.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	r.file = NewBufferedWriter(file, r.bufferSize)

	if oldFile != nil {
		err = oldFile.Close()
	}

	return
}

func (r *LogrotatingFile) Close() (err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.file != nil {
		err = r.file.Close()
	}
	return err
}

// BufferedWriter is a buffered wrapper over any io.WriteCloser with periodic flushing.
type BufferedWriter struct {
	target        io.WriteCloser
	buf           *bufio.Writer
	mu            sync.Mutex
	flushInterval time.Duration

	stopFlush chan struct{}
	wg        sync.WaitGroup
}

func NewBufferedWriter(target io.WriteCloser, bufSize int) *BufferedWriter {
	bw := &BufferedWriter{
		target:        target,
		buf:           bufio.NewWriterSize(target, bufSize),
		flushInterval: 5 * time.Second,
		stopFlush:     make(chan struct{}),
	}
	bw.wg.Add(1)
	go bw.flushLoop()
	return bw
}

func (bw *BufferedWriter) Write(p []byte) (int, error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.buf.Write(p)
}

func (bw *BufferedWriter) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.buf.Flush()
}

func (bw *BufferedWriter) flushLoop() {
	defer bw.wg.Done()
	tick := time.NewTicker(bw.flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-bw.stopFlush:
			return
		case <-tick.C:
			_ = bw.Flush() // best effort
		}
	}
}

func (bw *BufferedWriter) Close() error {
	close(bw.stopFlush)
	bw.wg.Wait()
	_ = bw.Flush()
	return bw.target.Close()
}
