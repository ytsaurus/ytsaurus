package zap

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.ytsaurus.tech/library/go/core/log/ctxlog"
)

type ctxField struct {
	ctx context.Context
}

// MarshalLogObject implements zapcore.ObjectMarshaler to append context fields directly to encoder in a lazy manner
func (c ctxField) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	fields := ctxlog.ContextFields(c.ctx)
	for _, f := range fields {
		zapifyField(f).AddTo(encoder)
	}
	return nil
}

// Context creates a log field from context - all fields bound with ctxlog.WithFields will be added.
func Context(ctx context.Context) zap.Field {
	return zap.Field{
		Key:       "",
		Type:      zapcore.InlineMarshalerType,
		Interface: ctxField{ctx: ctx},
	}
}

// RawContext creates a log field with raw context.
// Unlike Context(), this field uses SkipType which prevents it from being marshaled into the log output,
// making it available only to logger plugins that can access the raw field data.
func RawContext(ctx context.Context) zap.Field {
	return zap.Field{
		Key:       "",
		Type:      zapcore.SkipType,
		Interface: ctx,
	}
}
