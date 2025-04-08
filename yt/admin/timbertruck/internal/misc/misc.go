package misc

import (
	"context"
	"errors"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.ytsaurus.tech/library/go/core/buildinfo"
)

var logrotatingLoggerLock sync.Mutex
var logrotatingLogger *slog.Logger

type LogrotatingFile struct {
	path string
	file *os.File
	lock sync.Mutex
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
func NewLogrotatingFile(path string) (result io.WriteCloser, err error) {
	logrotating := &LogrotatingFile{
		path: path,
	}
	err = logrotating.reopen()
	if err != nil {
		return
	}

	ch := make(chan os.Signal, 16)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for range ch {
			err = logrotating.reopen()
			if err != nil {
				log.Default().Printf("Failed to reopen log file: %v", err)
				continue
			}
			logger := getLogrotatingLogger()
			if logger != nil {
				LogLoggingStarted(logger)
			}
		}
	}()
	result = logrotating
	return
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

	r.file, err = os.OpenFile(r.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}

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
