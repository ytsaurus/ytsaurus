package otelzap

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var otelPool = sync.Pool{New: func() interface{} {
	return &otelEncoder{}
}}

type otelEncoder struct {
	cfg  otelEncoderConfig
	pool buffer.Pool
	buf  *buffer.Buffer

	attrsEncoder *attributesEncoder

	specialFields otelEncoderSpecialFields
}

type otelEncoderSpecialFields struct {
	traceId string
	spanId  string
}

// NewOtelEncoder constructs otel encoder with config
func NewOtelEncoder(cfg otelEncoderConfig) (zapcore.Encoder, error) {
	enc := newOtelEncoder(cfg)
	return enc, nil
}

func newOtelEncoder(cfg otelEncoderConfig) *otelEncoder {
	pool := buffer.NewPool()
	return &otelEncoder{
		cfg:          cfg,
		pool:         pool,
		buf:          pool.Get(),
		attrsEncoder: newAttributesEncoder(cfg.EncoderConfig),
	}
}

func (enc *otelEncoder) Clone() zapcore.Encoder {
	clone := enc.clone()
	_, _ = clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *otelEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()
	defer func() {
		final.buf = nil
		final.attrsEncoder = nil
		otelPool.Put(final)
	}()

	final.writeOtelLogStructure(ent, fields)
	return final.buf, nil
}

// zapcore.ObjectEncoder methods
func (enc *otelEncoder) AddArray(key string, marshaler zapcore.ArrayMarshaler) error {
	if enc.checkSpecialField(key, marshaler) {
		return nil
	}
	return enc.attrsEncoder.AddArray(key, marshaler)
}
func (enc *otelEncoder) AddObject(key string, marshaler zapcore.ObjectMarshaler) error {
	if enc.checkSpecialField(key, marshaler) {
		return nil
	}
	return enc.attrsEncoder.AddObject(key, marshaler)
}
func (enc *otelEncoder) AddBinary(key string, value []byte) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddBinary(key, value)
}
func (enc *otelEncoder) AddByteString(key string, value []byte) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddByteString(key, value)
}
func (enc *otelEncoder) AddBool(key string, value bool) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddBool(key, value)
}
func (enc *otelEncoder) AddComplex128(key string, value complex128) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddComplex128(key, value)
}
func (enc *otelEncoder) AddComplex64(key string, value complex64) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddComplex64(key, value)
}
func (enc *otelEncoder) AddDuration(key string, value time.Duration) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddDuration(key, value)
}
func (enc *otelEncoder) AddFloat64(key string, value float64) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddFloat64(key, value)
}
func (enc *otelEncoder) AddFloat32(key string, value float32) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddFloat32(key, value)
}
func (enc *otelEncoder) AddInt(key string, value int) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddInt(key, value)
}
func (enc *otelEncoder) AddInt64(key string, value int64) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddInt64(key, value)
}
func (enc *otelEncoder) AddInt32(key string, value int32) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddInt32(key, value)
}
func (enc *otelEncoder) AddInt16(key string, value int16) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddInt16(key, value)
}
func (enc *otelEncoder) AddInt8(key string, value int8) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddInt8(key, value)
}
func (enc *otelEncoder) AddString(key, value string) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddString(key, value)
}
func (enc *otelEncoder) AddTime(key string, value time.Time) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddTime(key, value)
}
func (enc *otelEncoder) AddUint(key string, value uint) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUint(key, value)
}
func (enc *otelEncoder) AddUint64(key string, value uint64) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUint64(key, value)
}
func (enc *otelEncoder) AddUint32(key string, value uint32) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUint32(key, value)
}
func (enc *otelEncoder) AddUint16(key string, value uint16) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUint16(key, value)
}
func (enc *otelEncoder) AddUint8(key string, value uint8) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUint8(key, value)
}
func (enc *otelEncoder) AddUintptr(key string, value uintptr) {
	if enc.checkSpecialField(key, value) {
		return
	}
	enc.attrsEncoder.AddUintptr(key, value)
}
func (enc *otelEncoder) AddReflected(key string, value interface{}) error {
	if enc.checkSpecialField(key, value) {
		return nil
	}
	return enc.attrsEncoder.AddReflected(key, value)
}
func (enc *otelEncoder) OpenNamespace(key string) {
	enc.attrsEncoder.OpenNamespace(key)
}

// zapcore.PrimitiveArrayEncoder methods for simple encoding all fields as string
func (enc *otelEncoder) AppendBool(val bool)             { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendByteString(val []byte)     { writeString(enc.buf, string(val)) }
func (enc *otelEncoder) AppendComplex128(val complex128) { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendComplex64(val complex64)   { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendFloat64(val float64)       { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendFloat32(val float32)       { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendInt(val int)               { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendInt64(val int64)           { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendInt32(val int32)           { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendInt16(val int16)           { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendInt8(val int8)             { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendString(val string)         { writeString(enc.buf, val) }
func (enc *otelEncoder) AppendUint(val uint)             { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendUint64(val uint64)         { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendUint32(val uint32)         { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendUint16(val uint16)         { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendUint8(val uint8)           { writeString(enc.buf, fmt.Sprint(val)) }
func (enc *otelEncoder) AppendUintptr(val uintptr)       { writeString(enc.buf, fmt.Sprint(val)) }

// Helpers
func (enc *otelEncoder) clone() *otelEncoder {
	clone := otelPool.Get().(*otelEncoder)
	clone.cfg = enc.cfg
	clone.pool = enc.pool
	clone.buf = enc.pool.Get()
	clone.attrsEncoder = enc.attrsEncoder.clone()
	clone.specialFields = enc.specialFields
	return clone
}

func (enc *otelEncoder) zapLevelToOtelSeverityNumber(level zapcore.Level) int64 {
	switch level {
	case zapcore.DebugLevel:
		return 5 // DEBUG
	case zapcore.InfoLevel:
		return 9 // INFO
	case zapcore.WarnLevel:
		return 13 // WARN
	case zapcore.ErrorLevel:
		return 17 // ERROR
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return 21 // FATAL
	default:
		return 0 // UNSPECIFIED
	}
}

func (enc *otelEncoder) writeOtelLogStructure(ent zapcore.Entry, fields []zapcore.Field) {
	enc.buf.AppendByte('{')
	writeKey(enc.buf, "resourceLogs")
	enc.buf.AppendByte('[')
	enc.buf.AppendByte('{')

	writeKey(enc.buf, "resource")
	enc.buf.AppendByte('{')
	enc.buf.AppendByte('}')
	enc.buf.AppendByte(',')
	enc.writeScopeLogs(ent, fields)

	enc.buf.AppendByte('}')
	enc.buf.AppendByte(']')
	enc.buf.AppendByte('}')
}

func (enc *otelEncoder) writeScopeLogs(ent zapcore.Entry, fields []zapcore.Field) {
	writeKey(enc.buf, "scopeLogs")
	enc.buf.AppendByte('[')
	enc.buf.AppendByte('{')

	writeKey(enc.buf, "scope")
	enc.buf.AppendByte('{')
	writeKey(enc.buf, "name")
	if enc.cfg.EncodeName != nil {
		enc.cfg.EncodeName(ent.LoggerName, enc)
	} else {
		// Fallback to FullNameEncoder
		zapcore.FullNameEncoder(ent.LoggerName, enc)
	}
	enc.buf.AppendByte('}')
	enc.buf.AppendByte(',')
	enc.writeLogRecords(ent, fields)

	enc.buf.AppendByte('}')
	enc.buf.AppendByte(']')
}

func (enc *otelEncoder) writeLogRecords(ent zapcore.Entry, fields []zapcore.Field) {
	writeKey(enc.buf, "logRecords")
	enc.buf.AppendByte('[')
	enc.buf.AppendByte('{')

	// Add timeUnixNano from ent.Time
	// It is impossible to use cfg.EncodeTime, because of otel standart
	writeKey(enc.buf, "timeUnixNano")
	enc.buf.AppendByte('"')
	enc.buf.AppendInt(ent.Time.UnixNano())
	enc.buf.AppendByte('"')
	enc.buf.AppendByte(',')

	// Add severityNumber and severityText from ent.Level
	writeKey(enc.buf, "severityNumber")
	enc.buf.AppendInt(enc.zapLevelToOtelSeverityNumber(ent.Level))
	enc.buf.AppendByte(',')
	writeKey(enc.buf, "severityText")
	if enc.cfg.EncodeLevel != nil {
		enc.cfg.EncodeLevel(ent.Level, enc)
	} else {
		writeString(enc.buf, ent.Level.String())
	}
	enc.buf.AppendByte(',')

	// Add body as string value
	writeKey(enc.buf, "body")
	enc.buf.AppendByte('{')
	writeKey(enc.buf, otelStringValue)
	writeString(enc.buf, ent.Message)
	enc.buf.AppendByte('}')

	// Add all attributes
	enc.writeAttributes(ent, fields)

	// Process special fields after processing all incoming fields
	enc.writeSpecialFields()

	enc.buf.AppendByte('}')
	enc.buf.AppendByte(']')
}

func (enc *otelEncoder) writeAttributes(ent zapcore.Entry, fields []zapcore.Field) {
	for _, field := range fields {
		// Enrich attributes from all incoming fields
		field.AddTo(enc)
	}

	// Add caller from zapcore.Entry to attributes
	if ent.Caller.Defined && enc.cfg.CallerKey != "" {
		if enc.cfg.EncodeCaller != nil {
			enc.attrsEncoder.addValue(enc.cfg.CallerKey, func() { enc.cfg.EncodeCaller(ent.Caller, enc.attrsEncoder) })
		} else {
			enc.attrsEncoder.AddString(enc.cfg.CallerKey, ent.Caller.String())
		}
	}

	// Add stacktrace from zapcore.Entry to attributes
	if ent.Stack != "" && enc.cfg.StacktraceKey != "" {
		enc.attrsEncoder.AddString(enc.cfg.StacktraceKey, ent.Stack)
	}

	// Flush not empty attributes to root buffer
	if enc.attrsEncoder.buf.Len() > 0 {
		writeKey(enc.buf, "attributes")
		enc.buf.AppendByte('[')
		attrsBuf, _ := enc.attrsEncoder.encode()
		_, _ = enc.buf.Write(attrsBuf.Bytes())
		attrsBuf.Free()
		enc.buf.AppendByte(']')
	}
}

func (enc *otelEncoder) writeSpecialFields() {
	if enc.specialFields.traceId != "" {
		writeKey(enc.buf, "traceId")
		writeString(enc.buf, enc.specialFields.traceId)
	}

	if enc.specialFields.spanId != "" {
		writeKey(enc.buf, "spanId")
		writeString(enc.buf, enc.specialFields.spanId)
	}
}

func (enc *otelEncoder) checkSpecialField(key string, value interface{}) bool {
	switch key {
	case enc.cfg.traceIDDescription.TraceIDKey:
		enc.specialFields.traceId = enc.processSpecialField(value, enc.cfg.traceIDDescription.Format)
	case enc.cfg.spanIDDescription.SpanIDKey:
		enc.specialFields.spanId = enc.processSpecialField(value, enc.cfg.spanIDDescription.Format)
	default:
		return false
	}
	return true
}

func (enc *otelEncoder) processSpecialField(value interface{}, format formatType) string {
	if format == BinaryFmt {
		switch v := value.(type) {
		case []byte:
			return base64.RawStdEncoding.EncodeToString(v)
		default:
			// Unexpected field type
			return ""
		}
	}

	var strVal string
	switch v := value.(type) {
	case string:
		strVal = v
	case []byte:
		strVal = string(v)
	default:
		// Unexpected field type
		return ""
	}

	switch format {
	case HexStringFmt:
		if bytes, err := hex.DecodeString(strVal); err == nil {
			return base64.RawStdEncoding.EncodeToString(bytes)
		}
	case Base64StringFmt:
		return strVal
	}

	return ""
}
