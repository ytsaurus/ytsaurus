package otelzap

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestOtelEncoderStructure(t *testing.T) {
	now := time.Now().UTC()
	enc, _ := NewOtelEncoder(NewOtelEncoderConfig(zapcore.EncoderConfig{}))
	enc.AddString("some_key", "some_value")
	buf, err := enc.EncodeEntry(zapcore.Entry{
		Time:    now,
		Message: "test message",
	}, nil)
	assert.NoError(t, err)

	// Checking the structure through convertion to proto
	protoOtelLogData := unmarshalLogData(t, buf)
	assert.Len(t, protoOtelLogData.ResourceLogs, 1)
	assert.NotNil(t, protoOtelLogData.ResourceLogs[0].Resource)
	assert.Len(t, protoOtelLogData.ResourceLogs[0].ScopeLogs, 1)
	assert.Len(t, protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords, 1)

	protoOtelLogRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	assert.Equal(t, now.UTC().UnixNano(), int64(protoOtelLogRecord.TimeUnixNano))
	assert.NotEmpty(t, protoOtelLogRecord.SeverityText, "Missing severity text")
	assert.NotZero(t, protoOtelLogRecord.SeverityNumber, "Missing severity number")
	assert.Equal(t, "test message", protoOtelLogRecord.Body.GetStringValue())
	assert.Len(t, protoOtelLogRecord.Attributes, 1)
	assert.Equal(t, "some_key", protoOtelLogRecord.Attributes[0].Key)
	assert.Equal(t, "some_value", protoOtelLogRecord.Attributes[0].Value.GetStringValue())
}

func TestOtelEncoderConfigurations(t *testing.T) {
	now := time.Now().UTC()
	t.Run("CustomLevel", func(t *testing.T) {
		cfg := zapcore.EncoderConfig{
			EncodeLevel: func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString("LEVEL_" + l.String())
			},
		}
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, err := enc.EncodeEntry(zapcore.Entry{Time: now, Level: zapcore.ErrorLevel}, nil)
		assert.NoError(t, err)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Equal(t, "LEVEL_error", logRecord.SeverityText)
	})

	t.Run("CustomTime", func(t *testing.T) {
		now := time.Now().UTC()
		cfg := zapcore.EncoderConfig{
			EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(t.Format(time.RFC1123Z))
			},
		}
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, err := enc.EncodeEntry(zapcore.Entry{Time: now}, []zapcore.Field{zap.Time("custom_time", now)})
		assert.NoError(t, err)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Equal(t, now.UTC().UnixNano(), int64(logRecord.TimeUnixNano))
		assert.Len(t, logRecord.Attributes, 1)
		assert.Equal(t, "custom_time", logRecord.Attributes[0].Key)
		assert.Equal(t, now.UTC().Format(time.RFC1123Z), logRecord.Attributes[0].Value.GetStringValue())
	})

	t.Run("CustomDuration", func(t *testing.T) {
		cfg := zapcore.EncoderConfig{
			EncodeDuration: func(t time.Duration, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(t.String())
			},
		}
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, err := enc.EncodeEntry(zapcore.Entry{Time: now}, []zapcore.Field{zap.Duration("custom_duration", time.Second)})
		assert.NoError(t, err)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Len(t, logRecord.Attributes, 1)
		assert.Equal(t, "custom_duration", logRecord.Attributes[0].Key)
		assert.Equal(t, "1s", logRecord.Attributes[0].Value.GetStringValue())
	})

	t.Run("CustomName", func(t *testing.T) {
		cfg := zapcore.EncoderConfig{
			EncodeName: func(name string, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(fmt.Sprintf("customize_%s", name))
			},
		}

		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, err := enc.EncodeEntry(zapcore.Entry{Time: now, LoggerName: "logger"}, nil)
		assert.NoError(t, err)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		scopeLog := protoOtelLogData.ResourceLogs[0].ScopeLogs[0]
		assert.Equal(t, "customize_logger", scopeLog.Scope.Name)
	})

	t.Run("Caller", func(t *testing.T) {
		cfg := zapcore.EncoderConfig{
			CallerKey:    "caller",
			EncodeCaller: zapcore.ShortCallerEncoder,
		}
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, _ := enc.EncodeEntry(zapcore.Entry{
			Time: now,
			Caller: zapcore.EntryCaller{
				Defined: true,
				File:    "test.go",
				Line:    42,
			},
		}, nil)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Len(t, logRecord.Attributes, 1)
		assert.Equal(t, "caller", logRecord.Attributes[0].Key)
		assert.Equal(t, "test.go:42", logRecord.Attributes[0].Value.GetStringValue())
	})

	t.Run("Stacktrace", func(t *testing.T) {
		cfg := zapcore.EncoderConfig{
			StacktraceKey: "stack",
		}
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(cfg))
		buf, _ := enc.EncodeEntry(zapcore.Entry{
			Time:  now,
			Stack: "test stack",
		}, nil)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Len(t, logRecord.Attributes, 1)
		assert.Equal(t, "stack", logRecord.Attributes[0].Key)
		assert.Equal(t, "test stack", logRecord.Attributes[0].Value.GetStringValue())
	})
}

func TestOtelEncoderSpecialFields(t *testing.T) {
	now := time.Now().UTC()

	hexTraceId := "4c6d0e0b404bd39c0fa23954e72e4e4e"
	hexSpanId := "aacfdfbc350db30f"

	bytesTraceId, _ := hex.DecodeString(hexTraceId)
	bytesSpanId, _ := hex.DecodeString(hexSpanId)

	base64TraceId := base64.StdEncoding.EncodeToString(bytesTraceId)
	base64SpanId := base64.StdEncoding.EncodeToString(bytesSpanId)

	rawBase64TraceId := base64.RawStdEncoding.EncodeToString(bytesTraceId)
	rawBase64SpanId := base64.RawStdEncoding.EncodeToString(bytesSpanId)

	tests := []struct {
		name               string
		encoderOptions     []cfgOptions
		fields             []zapcore.Field
		expectedTraceIdKey string
		expectedHexTraceId string
		expectedHexSpanId  string
		expectedAttributes map[string]string
	}{
		{
			name: "HexFormat",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", HexStringFmt),
				WithSpanIDDescription("span_id", HexStringFmt),
			},
			fields: []zapcore.Field{
				zap.String("trace_id", hexTraceId),
				zap.String("span_id", hexSpanId),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "HexFormat(ByteString)",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", HexStringFmt),
				WithSpanIDDescription("span_id", HexStringFmt),
			},
			fields: []zapcore.Field{
				zap.ByteString("trace_id", []byte(hexTraceId)),
				zap.ByteString("span_id", []byte(hexSpanId)),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "Base64Format",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", Base64StringFmt),
				WithSpanIDDescription("span_id", Base64StringFmt),
			},
			fields: []zapcore.Field{
				zap.String("trace_id", base64TraceId),
				zap.String("span_id", base64SpanId),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "Base64Format(ByteString)",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", Base64StringFmt),
				WithSpanIDDescription("span_id", Base64StringFmt),
			},
			fields: []zapcore.Field{
				zap.ByteString("trace_id", []byte(base64TraceId)),
				zap.ByteString("span_id", []byte(base64SpanId)),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "RawBase64Format",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", Base64StringFmt),
				WithSpanIDDescription("span_id", Base64StringFmt),
			},
			fields: []zapcore.Field{
				zap.String("trace_id", rawBase64TraceId),
				zap.String("span_id", rawBase64SpanId),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "BinaryFormat",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("trace_id", BinaryFmt),
				WithSpanIDDescription("span_id", BinaryFmt),
			},
			fields: []zapcore.Field{
				zap.Binary("trace_id", bytesTraceId),
				zap.Binary("span_id", bytesSpanId),
			},
			expectedHexTraceId: hexTraceId,
			expectedHexSpanId:  hexSpanId,
			expectedAttributes: map[string]string{},
		},
		{
			name: "UnknownKeys",
			encoderOptions: []cfgOptions{
				WithTraceIDDescription("@trace_id", Base64StringFmt),
				WithSpanIDDescription("@span_id", HexStringFmt),
			},
			fields: []zapcore.Field{
				zap.String("trace_id", hexTraceId),
				zap.String("span_id", hexSpanId),
			},
			expectedHexTraceId: "",
			expectedHexSpanId:  "",
			expectedAttributes: map[string]string{
				"trace_id": hexTraceId,
				"span_id":  hexSpanId,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			otelEncodeCfg := NewOtelEncoderConfig(zapcore.EncoderConfig{}, tt.encoderOptions...)

			enc, _ := NewOtelEncoder(otelEncodeCfg)
			buf, _ := enc.EncodeEntry(
				zapcore.Entry{
					Time: now,
				},
				tt.fields,
			)

			// Checking the structure through convertion to proto
			protoOtelLogData := unmarshalLogData(t, buf)
			logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
			assert.Len(t, logRecord.Attributes, len(tt.expectedAttributes))
			assert.Equal(t, tt.expectedHexTraceId, hex.EncodeToString(logRecord.TraceId))
			assert.Equal(t, tt.expectedHexSpanId, hex.EncodeToString(logRecord.SpanId))

			protoAttrs := make(map[string]string)
			for _, attr := range logRecord.Attributes {
				protoAttrs[attr.Key] = attr.Value.GetStringValue()
			}
			assert.EqualValues(t, tt.expectedAttributes, protoAttrs)
		})
	}
}

func TestOtelEncoderEdgeCases(t *testing.T) {
	t.Run("EmptyFields", func(t *testing.T) {
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(zapcore.EncoderConfig{}))
		buf, err := enc.EncodeEntry(zapcore.Entry{}, nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, buf.Bytes())
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(zapcore.EncoderConfig{}))
		buf, err := enc.EncodeEntry(zapcore.Entry{
			Message: "test\n\r\t\"\\",
		}, nil)
		assert.NoError(t, err)
		assert.Contains(t, buf.String(), `test\n\r\t\"\\`)
	})

	t.Run("BytesWithProtoTrascoding", func(t *testing.T) {
		enc, _ := NewOtelEncoder(NewOtelEncoderConfig(zapcore.EncoderConfig{}))
		buf, err := enc.EncodeEntry(zapcore.Entry{
			Time:    time.Now().UTC(),
			Message: "test message",
		}, []zapcore.Field{zap.Binary("binary", []byte("test"))})
		assert.NoError(t, err)

		// Checking the structure through convertion to proto
		protoOtelLogData := unmarshalLogData(t, buf)
		logRecord := protoOtelLogData.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		assert.Len(t, logRecord.Attributes, 1)
		assert.Equal(t, []byte("test"), logRecord.Attributes[0].Value.GetBytesValue())
	})
}

// Helpers
func unmarshalLogData(t *testing.T, buf *buffer.Buffer) *v1.LogsData {
	var protoOtelLogData v1.LogsData
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}
	err := unmarshaler.Unmarshal(buf.Bytes(), &protoOtelLogData)
	assert.NoError(t, err, "JSON does not confirm to OTLP schema")

	return &protoOtelLogData
}
