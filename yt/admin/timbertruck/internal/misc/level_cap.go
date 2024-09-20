package misc

import (
	"context"
	"log/slog"

	"go.ytsaurus.tech/library/go/core/log"
)

type levelCappingHandler struct {
	underlying slog.Handler
}

func NewLevelCappingLogger(logger *slog.Logger, componentName string) *slog.Logger {
	attrs := []slog.Attr{
		slog.String("component", componentName),
	}
	return slog.New(levelCappingHandler{logger.Handler().WithAttrs(attrs)})
}

func NewArcadiaLevelCappingLogger(logger *slog.Logger, componentName string) log.Logger {
	return newSlogArcadiaAdapter(NewLevelCappingLogger(logger, componentName))
}

func (l levelCappingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return l.underlying.Enabled(ctx, level)
}

func (l levelCappingHandler) Handle(ctx context.Context, record slog.Record) error {
	record = record.Clone()
	record.Add("component_level", record.Level)

	if record.Level >= slog.LevelError {
		record.Level = slog.LevelWarn
	}
	return l.underlying.Handle(ctx, record)
}

func (l levelCappingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return levelCappingHandler{
		underlying: l.underlying.WithAttrs(attrs),
	}
}

func (l levelCappingHandler) WithGroup(name string) slog.Handler {
	return levelCappingHandler{
		underlying: l.underlying.WithGroup(name),
	}
}
