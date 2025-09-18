package otelzap

import (
	"encoding/base64"
	"math"
	"sync"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var attributesPool = sync.Pool{New: func() interface{} {
	return &attributesEncoder{}
}}

type attributesEncoder struct {
	cfg            zapcore.EncoderConfig
	pool           buffer.Pool
	buf            *buffer.Buffer
	openNamespaces int
}

func newAttributesEncoder(cfg zapcore.EncoderConfig) *attributesEncoder {
	pool := buffer.NewPool()
	return &attributesEncoder{
		cfg:  cfg,
		pool: pool,
		buf:  pool.Get(),
	}
}

// zapcore.ObjectEncoder methods
func (enc *attributesEncoder) addValue(key string, valWriteCallback func()) {
	writeElementSeparator(enc.buf)
	enc.buf.AppendByte('{')
	writeKey(enc.buf, otelKey)
	writeString(enc.buf, key)
	enc.buf.AppendByte(',')
	writeKey(enc.buf, otelValue)
	valWriteCallback()
	enc.buf.AppendByte('}')
}

func (enc *attributesEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	enc.addValue(key, func() { _ = enc.AppendArray(arr) })
	return nil
}
func (enc *attributesEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	oldOpenedNamespaces := enc.openNamespaces
	enc.openNamespaces = 0
	enc.addValue(key, func() { _ = enc.AppendObject(obj) })
	enc.CloseOpenedNamespaces()
	enc.openNamespaces = oldOpenedNamespaces
	return nil
}
func (enc *attributesEncoder) AddBinary(key string, val []byte) {
	enc.addValue(key, func() { enc.appendValue(func() { enc.writeBytesValue(val) }) })
}
func (enc *attributesEncoder) AddByteString(key string, val []byte) {
	enc.addValue(key, func() { enc.AppendByteString(val) })
}
func (enc *attributesEncoder) AddBool(key string, val bool) {
	enc.addValue(key, func() { enc.AppendBool(val) })
}
func (enc *attributesEncoder) AddComplex128(key string, val complex128) {
	enc.addValue(key, func() { enc.AppendComplex128(val) })
}
func (enc *attributesEncoder) AddComplex64(key string, val complex64) {
	enc.addValue(key, func() { enc.AppendComplex64(val) })
}
func (enc *attributesEncoder) AddDuration(key string, val time.Duration) {
	enc.addValue(key, func() { enc.AppendDuration(val) })
}
func (enc *attributesEncoder) AddFloat64(key string, val float64) {
	enc.addValue(key, func() { enc.AppendFloat64(val) })
}
func (enc *attributesEncoder) AddFloat32(key string, val float32) {
	enc.addValue(key, func() { enc.AppendFloat32(val) })
}
func (enc *attributesEncoder) AddInt(key string, val int) {
	enc.addValue(key, func() { enc.AppendInt(val) })
}
func (enc *attributesEncoder) AddInt64(key string, val int64) {
	enc.addValue(key, func() { enc.AppendInt64(val) })
}
func (enc *attributesEncoder) AddInt32(key string, val int32) {
	enc.addValue(key, func() { enc.AppendInt32(val) })
}
func (enc *attributesEncoder) AddInt16(key string, val int16) {
	enc.addValue(key, func() { enc.AppendInt16(val) })
}
func (enc *attributesEncoder) AddInt8(key string, val int8) {
	enc.addValue(key, func() { enc.AppendInt8(val) })
}
func (enc *attributesEncoder) AddString(key, val string) {
	enc.addValue(key, func() { enc.AppendString(val) })
}
func (enc *attributesEncoder) AddTime(key string, val time.Time) {
	enc.addValue(key, func() { enc.AppendTime(val) })
}
func (enc *attributesEncoder) AddUint(key string, val uint) {
	enc.addValue(key, func() { enc.AppendUint(val) })
}
func (enc *attributesEncoder) AddUint64(key string, val uint64) {
	enc.addValue(key, func() { enc.AppendUint64(val) })
}
func (enc *attributesEncoder) AddUint32(key string, val uint32) {
	enc.addValue(key, func() { enc.AppendUint32(val) })
}
func (enc *attributesEncoder) AddUint16(key string, val uint16) {
	enc.addValue(key, func() { enc.AppendUint16(val) })
}
func (enc *attributesEncoder) AddUint8(key string, val uint8) {
	enc.addValue(key, func() { enc.AppendUint8(val) })
}
func (enc *attributesEncoder) AddUintptr(key string, val uintptr) {
	enc.addValue(key, func() { enc.AppendUintptr(val) })
}
func (enc *attributesEncoder) AddReflected(key string, val interface{}) error {
	// Not implemented
	return nil
}
func (enc *attributesEncoder) OpenNamespace(key string) {
	writeKey(enc.buf, key)
	enc.buf.AppendByte('{')
	enc.openNamespaces++
}
func (enc *attributesEncoder) CloseOpenedNamespaces() {
	for range enc.openNamespaces {
		enc.buf.AppendByte('}')
	}
	enc.openNamespaces = 0
}

// zapcore.ArrayEncoder methods
func (enc *attributesEncoder) appendValue(valWriteCallback func()) {
	writeElementSeparator(enc.buf)
	enc.buf.AppendByte('{')
	valWriteCallback()
	enc.buf.AppendByte('}')
}

func (enc *attributesEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	enc.appendValue(func() { enc.writeArrayValue(arr) })
	return nil
}
func (enc *attributesEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	oldOpenedNamespaces := enc.openNamespaces
	enc.openNamespaces = 0
	enc.appendValue(func() { enc.writeObjectValue(obj) })
	enc.CloseOpenedNamespaces()
	enc.openNamespaces = oldOpenedNamespaces
	return nil
}
func (enc *attributesEncoder) AppendBool(val bool) {
	enc.appendValue(func() { enc.writeBoolValue(val) })
}
func (enc *attributesEncoder) AppendByteString(val []byte) {
	enc.appendValue(func() { enc.writeStringValue(string(val)) })
}
func (enc *attributesEncoder) AppendComplex128(val complex128) {
	enc.appendValue(func() { enc.writeComplexValue(val, 64) })
}
func (enc *attributesEncoder) AppendComplex64(val complex64) {
	enc.appendValue(func() { enc.writeComplexValue(complex128(val), 32) })
}
func (enc *attributesEncoder) AppendFloat64(val float64) {
	enc.appendValue(func() { enc.writeFloatValue(val, 64) })
}
func (enc *attributesEncoder) AppendFloat32(val float32) {
	enc.appendValue(func() { enc.writeFloatValue(float64(val), 32) })
}
func (enc *attributesEncoder) AppendInt(val int) {
	enc.appendValue(func() { enc.writeIntValue(int64(val)) })
}
func (enc *attributesEncoder) AppendInt64(val int64) {
	enc.appendValue(func() { enc.writeIntValue(val) })
}
func (enc *attributesEncoder) AppendInt32(val int32) {
	enc.appendValue(func() { enc.writeIntValue(int64(val)) })
}
func (enc *attributesEncoder) AppendInt16(val int16) {
	enc.appendValue(func() { enc.writeIntValue(int64(val)) })
}
func (enc *attributesEncoder) AppendInt8(val int8) {
	enc.appendValue(func() { enc.writeIntValue(int64(val)) })
}
func (enc *attributesEncoder) AppendString(val string) {
	enc.appendValue(func() { enc.writeStringValue(val) })
}
func (enc *attributesEncoder) AppendUint(val uint) {
	enc.appendValue(func() { enc.writeUintValue(uint64(val)) })
}
func (enc *attributesEncoder) AppendUint64(val uint64) {
	enc.appendValue(func() { enc.writeUintValue(val) })
}
func (enc *attributesEncoder) AppendUint32(val uint32) {
	enc.appendValue(func() { enc.writeUintValue(uint64(val)) })
}
func (enc *attributesEncoder) AppendUint16(val uint16) {
	enc.appendValue(func() { enc.writeUintValue(uint64(val)) })
}
func (enc *attributesEncoder) AppendUint8(val uint8) {
	enc.appendValue(func() { enc.writeUintValue(uint64(val)) })
}
func (enc *attributesEncoder) AppendUintptr(val uintptr) {
	enc.appendValue(func() { enc.writeUintValue(uint64(val)) })
}
func (enc *attributesEncoder) AppendDuration(val time.Duration) {
	if enc.cfg.EncodeDuration != nil {
		enc.cfg.EncodeDuration(val, enc)
	} else {
		// Default for otel logs
		enc.AppendInt64(val.Nanoseconds())
	}
}
func (enc *attributesEncoder) AppendTime(val time.Time) {
	if enc.cfg.EncodeTime != nil {
		enc.cfg.EncodeTime(val, enc)
	} else {
		// Default for otel logs
		enc.AppendInt64(val.UnixNano())
	}
}
func (enc *attributesEncoder) AppendReflected(val interface{}) error {
	// Not implemented
	return nil
}

// Helpers
func (enc *attributesEncoder) clone() *attributesEncoder {
	clone := attributesPool.Get().(*attributesEncoder)
	clone.cfg = enc.cfg
	clone.pool = enc.pool
	clone.buf = enc.pool.Get()
	clone.openNamespaces = enc.openNamespaces

	_, _ = clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *attributesEncoder) encode() (*buffer.Buffer, error) {
	final := enc.clone()
	defer func() {
		final.buf = nil
		final.openNamespaces = 0
		attributesPool.Put(final)
	}()

	return final.buf, nil
}

func (enc *attributesEncoder) writeBoolValue(val bool) {
	writeKey(enc.buf, otelBoolValue)
	enc.buf.AppendBool(val)
}

func (enc *attributesEncoder) writeIntValue(val int64) {
	writeKey(enc.buf, otelIntValue)
	enc.buf.AppendByte('"')
	enc.buf.AppendInt(val)
	enc.buf.AppendByte('"')
}

func (enc *attributesEncoder) writeUintValue(val uint64) {
	writeKey(enc.buf, otelIntValue)
	enc.buf.AppendByte('"')
	enc.buf.AppendUint(val)
	enc.buf.AppendByte('"')
}

func (enc *attributesEncoder) writeFloatValue(val float64, bitSize int) {
	if math.IsInf(val, 0) || math.IsNaN(val) {
		writeKey(enc.buf, otelStringValue)
	} else {
		writeKey(enc.buf, otelDoubleValue)
	}
	switch {
	case math.IsNaN(val):
		enc.buf.AppendString(`"NaN"`)
	case math.IsInf(val, 1):
		enc.buf.AppendString(`"+Inf"`)
	case math.IsInf(val, -1):
		enc.buf.AppendString(`"-Inf"`)
	default:
		enc.buf.AppendFloat(val, bitSize)
	}
}

func (enc *attributesEncoder) writeComplexValue(val complex128, precision int) {
	writeKey(enc.buf, otelStringValue)
	r, i := float64(real(val)), float64(imag(val))
	enc.buf.AppendByte('"')
	enc.buf.AppendFloat(r, precision)
	if i >= 0 {
		enc.buf.AppendByte('+')
	}
	enc.buf.AppendFloat(i, precision)
	enc.buf.AppendByte('i')
	enc.buf.AppendByte('"')
}

func (enc *attributesEncoder) writeStringValue(val string) {
	writeKey(enc.buf, otelStringValue)
	enc.buf.AppendByte('"')
	safeWriteString(enc.buf, val)
	enc.buf.AppendByte('"')
}

func (enc *attributesEncoder) writeBytesValue(val []byte) {
	writeKey(enc.buf, otelBytesValue)
	enc.buf.AppendByte('"')
	base64Enc := base64.NewEncoder(base64.RawStdEncoding, enc.buf)
	_, _ = base64Enc.Write(val)
	_ = base64Enc.Close()
	enc.buf.AppendByte('"')
}

func (enc *attributesEncoder) writeArrayValue(val zapcore.ArrayMarshaler) {
	writeKey(enc.buf, otelArrayValue)
	enc.buf.AppendByte('{')
	writeKey(enc.buf, otelValues)
	enc.buf.AppendByte('[')
	err := val.MarshalLogArray(enc)
	if err != nil {
		enc.AppendString(err.Error())
	}
	enc.buf.AppendByte(']')
	enc.buf.AppendByte('}')
}

func (enc *attributesEncoder) writeObjectValue(val zapcore.ObjectMarshaler) {
	writeKey(enc.buf, otelObjectValue)
	enc.buf.AppendByte('{')
	writeKey(enc.buf, otelValues)
	enc.buf.AppendByte('[')
	err := val.MarshalLogObject(enc)
	if err != nil {
		enc.AppendString(err.Error())
	}
	enc.buf.AppendByte(']')
	enc.buf.AppendByte('}')
}
