package misc

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.ytsaurus.tech/library/go/core/log"
)

func NewSlogArcadiaAdapter(l *slog.Logger) log.Logger {
	return &slogArcadiaAdapter{l}
}

type slogArcadiaAdapter struct {
	logger *slog.Logger
}

// Logger implements log.Fmt.
func (s *slogArcadiaAdapter) Logger() log.Logger {
	return s
}

func fieldsToArgs(fields ...log.Field) []any {
	args := []any{}
	for i := range fields {
		args = append(args, fields[i].Key())
		args = append(args, fields[i].Any())
	}
	return args
}

func (s *slogArcadiaAdapter) Debug(msg string, fields ...log.Field) {
	s.logger.Debug(msg, fieldsToArgs(fields...)...)
}

// Debugf implements log.Logger.
func (s *slogArcadiaAdapter) Debugf(format string, args ...interface{}) {
	s.logger.Debug(fmt.Sprintf(format, args...))
}

// Error implements log.Logger.
func (s *slogArcadiaAdapter) Error(msg string, fields ...log.Field) {
	s.logger.Error(msg, fieldsToArgs(fields...)...)
}

// Errorf implements log.Logger.
func (s *slogArcadiaAdapter) Errorf(format string, args ...interface{}) {
	s.logger.Error(fmt.Sprintf(format, args...))
}

// Fatal implements log.Logger.
func (s *slogArcadiaAdapter) Fatal(msg string, fields ...log.Field) {
	s.logger.Log(context.Background(), slog.LevelError+4, msg, fieldsToArgs(fields...)...)
	os.Exit(1)
}

// Fatalf implements log.Logger.
func (s *slogArcadiaAdapter) Fatalf(format string, args ...interface{}) {
	s.logger.Log(context.Background(), slog.LevelError+4, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// Fmt implements log.Logger.
func (s *slogArcadiaAdapter) Fmt() log.Fmt {
	return s
}

// Info implements log.Logger.
func (s *slogArcadiaAdapter) Info(msg string, fields ...log.Field) {
	s.logger.Info(msg, fieldsToArgs(fields...)...)
}

// Infof implements log.Logger.
func (s *slogArcadiaAdapter) Infof(format string, args ...interface{}) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

// Structured implements log.Logger.
func (s *slogArcadiaAdapter) Structured() log.Structured {
	return s
}

// Trace implements log.Logger.
func (s *slogArcadiaAdapter) Trace(msg string, fields ...log.Field) {
	s.logger.Log(context.Background(), slog.LevelDebug-4, msg, fieldsToArgs(fields...)...)
}

// Tracef implements log.Logger.
func (s *slogArcadiaAdapter) Tracef(format string, args ...interface{}) {
	s.logger.Log(context.Background(), slog.LevelDebug-4, fmt.Sprintf(format, args...))
}

// Warn implements log.Logger.
func (s *slogArcadiaAdapter) Warn(msg string, fields ...log.Field) {
	s.logger.Warn(msg, fieldsToArgs(fields...)...)
}

// Warnf implements log.Logger.
func (s *slogArcadiaAdapter) Warnf(format string, args ...interface{}) {
	s.logger.Warn(fmt.Sprintf(format, args...))
}

// WithName implements log.Logger.
func (s *slogArcadiaAdapter) WithName(name string) log.Logger {
	return NewSlogArcadiaAdapter(s.logger.With("name", name))
}
