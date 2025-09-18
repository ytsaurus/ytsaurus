package otelzap

import "go.uber.org/zap/zapcore"

type formatType int

const (
	HexStringFmt formatType = iota
	Base64StringFmt
	BinaryFmt
)

type otelEncoderTraceIDDescription struct {
	TraceIDKey string
	Format     formatType
}

type otelEncoderSpanIDDescription struct {
	SpanIDKey string
	Format    formatType
}

type otelEncoderConfig struct {
	zapcore.EncoderConfig
	traceIDDescription otelEncoderTraceIDDescription
	spanIDDescription  otelEncoderSpanIDDescription
}

type cfgOptions func(cfg *otelEncoderConfig)

// NewOtelEncoderConfig constructs otel encoder config
func NewOtelEncoderConfig(cfg zapcore.EncoderConfig, options ...cfgOptions) otelEncoderConfig {
	newCfg := otelEncoderConfig{
		EncoderConfig: cfg,
	}

	for _, opt := range options {
		opt(&newCfg)
	}

	return newCfg
}

func WithTraceIDDescription(key string, format formatType) cfgOptions {
	return func(cfg *otelEncoderConfig) {
		cfg.traceIDDescription = otelEncoderTraceIDDescription{
			TraceIDKey: key,
			Format:     format,
		}
	}
}

func WithSpanIDDescription(key string, format formatType) cfgOptions {
	return func(cfg *otelEncoderConfig) {
		cfg.spanIDDescription = otelEncoderSpanIDDescription{
			SpanIDKey: key,
			Format:    format,
		}
	}
}
