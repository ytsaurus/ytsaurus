package otelzap

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "go.opentelemetry.io/proto/otlp/common/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestAttributesEncoderTypes(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name          string
		addedFields   []zapcore.Field
		expectedAttrs map[string]any
	}{
		{
			name: "IntTypes",
			addedFields: []zapcore.Field{
				zap.Int8("int8", 3),
				zap.Int16("int16", -15),
				zap.Int32("int32", 156),
				zap.Int64("int64", -12345),
				zap.Uint8("uint8", 1),
				zap.Uint16("uint16", 45),
				zap.Uint32("uint32", 534),
				zap.Uint64("uint64", 99999999),
			},
			expectedAttrs: map[string]any{
				"int8":   map[string]any{"intValue": "3"},
				"int16":  map[string]any{"intValue": "-15"},
				"int32":  map[string]any{"intValue": "156"},
				"int64":  map[string]any{"intValue": "-12345"},
				"uint8":  map[string]any{"intValue": "1"},
				"uint16": map[string]any{"intValue": "45"},
				"uint32": map[string]any{"intValue": "534"},
				"uint64": map[string]any{"intValue": "99999999"},
			},
		},
		{
			name: "FloatTypes",
			addedFields: []zapcore.Field{
				zap.Float32("float32", 3.14),
				zap.Float32("float32_nan", float32(math.NaN())),
				zap.Float32("float32_-inf", float32(math.Inf(-1))),
				zap.Float32("float32_+inf", float32(math.Inf(+1))),
				zap.Float64("float64", 3.14),
				zap.Float64("float64_nan", math.NaN()),
				zap.Float64("float64_-inf", math.Inf(-1)),
				zap.Float64("float64_+inf", math.Inf(+1)),
			},
			expectedAttrs: map[string]any{
				"float32":      map[string]any{"doubleValue": 3.14},
				"float32_nan":  map[string]any{"stringValue": "NaN"},
				"float32_-inf": map[string]any{"stringValue": "-Inf"},
				"float32_+inf": map[string]any{"stringValue": "+Inf"},
				"float64":      map[string]any{"doubleValue": 3.14},
				"float64_nan":  map[string]any{"stringValue": "NaN"},
				"float64_-inf": map[string]any{"stringValue": "-Inf"},
				"float64_+inf": map[string]any{"stringValue": "+Inf"},
			},
		},
		{
			name: "ComplexTypes",
			addedFields: []zapcore.Field{
				zap.Complex64("complex64_pos_i", 5+3i),
				zap.Complex64("complex64_neg_i", -5-3i),
				zap.Complex128("complex128_pos_i", -5+3i),
				zap.Complex128("complex128_neg_i", -3i),
			},
			expectedAttrs: map[string]any{
				"complex64_pos_i":  map[string]any{"stringValue": "5+3i"},
				"complex64_neg_i":  map[string]any{"stringValue": "-5-3i"},
				"complex128_pos_i": map[string]any{"stringValue": "-5+3i"},
				"complex128_neg_i": map[string]any{"stringValue": "0-3i"},
			},
		},
		{
			name: "BoolType",
			addedFields: []zapcore.Field{
				zap.Bool("bool", true),
			},
			expectedAttrs: map[string]any{
				"bool": map[string]any{"boolValue": true},
			},
		},
		{
			name: "StringTypes",
			addedFields: []zapcore.Field{
				zap.String("string", "string"),
				zap.ByteString("byte_string", []byte{'b', 'y', 't', 'e', '_', 's', 't', 'r', 'i', 'n', 'g'}),
				zap.String("utf8_string", "utf8_текст"),
				zap.ByteString("utf8_byte_string", []byte{'u', 't', 'f', '8', '_', 0xd1, 0x82, 0xd0, 0xb5, 0xd0, 0xba, 0xd1, 0x81, 0xd1, 0x82}),
			},
			expectedAttrs: map[string]any{
				"string":           map[string]any{"stringValue": "string"},
				"byte_string":      map[string]any{"stringValue": "byte_string"},
				"utf8_string":      map[string]any{"stringValue": "utf8_текст"},
				"utf8_byte_string": map[string]any{"stringValue": "utf8_текст"},
			},
		},
		{
			name: "BytesType",
			addedFields: []zapcore.Field{
				zap.Binary("bytes", []byte{0, 1, 2, 3, '\t', '\n', '\r'}),
			},
			expectedAttrs: map[string]any{
				"bytes": map[string]any{"bytesValue": "AAECAwkKDQ"},
			},
		},
		{
			name: "TimeTypes",
			addedFields: []zapcore.Field{
				zap.Time("time", now),
				zap.Duration("duration", 12345*time.Second),
			},
			expectedAttrs: map[string]any{
				"time":     map[string]any{"intValue": strconv.FormatInt(now.UnixNano(), 10)},
				"duration": map[string]any{"intValue": "12345000000000"},
			},
		},
		{
			name: "ErrorType",
			addedFields: []zapcore.Field{
				zap.Error(errors.New("some awful error")),
			},
			expectedAttrs: map[string]any{
				"error": map[string]any{"stringValue": "some awful error"},
			},
		},
		{
			name: "ArrayTypes",
			addedFields: []zapcore.Field{
				zap.Strings("strings", []string{"str1", "str2"}),
				zap.Ints("ints", []int{1, 2, 3}),
				zap.Bools("bools", []bool{true, false}),
				zap.Float32s("float32s", []float32{1.1, float32(math.NaN()), float32(math.Inf(-1))}),
				zap.Float64s("float64s", []float64{math.Inf(1), 2.2, 3.3}),
			},
			expectedAttrs: map[string]any{
				"strings": map[string]any{"arrayValue": map[string]any{"values": []any{
					map[string]any{"stringValue": "str1"}, map[string]any{"stringValue": "str2"}}}},
				"ints": map[string]any{"arrayValue": map[string]any{"values": []any{
					map[string]any{"intValue": "1"}, map[string]any{"intValue": "2"}, map[string]any{"intValue": "3"}}}},
				"bools": map[string]any{"arrayValue": map[string]any{"values": []any{
					map[string]any{"boolValue": true}, map[string]any{"boolValue": false}}}},
				"float32s": map[string]any{"arrayValue": map[string]any{"values": []any{
					map[string]any{"doubleValue": 1.1}, map[string]any{"stringValue": "NaN"}, map[string]any{"stringValue": "-Inf"}}}},
				"float64s": map[string]any{"arrayValue": map[string]any{"values": []any{
					map[string]any{"stringValue": "+Inf"}, map[string]any{"doubleValue": 2.2}, map[string]any{"doubleValue": 3.3}}}},
			},
		},
		{
			name: "ObjectTypes",
			addedFields: []zapcore.Field{
				zap.Object("root_object", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
					enc.AddString("key1", "val1")
					enc.AddInt("key2", 42)
					_ = enc.AddObject("child_object", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
						_ = enc.AddArray("key3", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
							enc.AppendBool(true)
							return nil
						}))
						return nil
					}))
					return nil
				})),
			},
			expectedAttrs: map[string]any{
				"root_object": map[string]any{
					"kvlistValue": map[string]any{
						"values": []any{
							map[string]any{"key": "key1", "value": map[string]any{"stringValue": "val1"}},
							map[string]any{"key": "key2", "value": map[string]any{"intValue": "42"}},
							map[string]any{"key": "child_object", "value": map[string]any{"kvlistValue": map[string]any{"values": []any{
								map[string]any{"key": "key3", "value": map[string]any{"arrayValue": map[string]any{"values": []any{map[string]any{"boolValue": true}}}}},
							}}}},
						}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := newAttributesEncoder(zapcore.EncoderConfig{})
			for _, field := range tt.addedFields {
				field.AddTo(enc)
			}
			encBuf, err := enc.encode()
			assert.NoError(t, err)

			pool := buffer.NewPool()
			attrsBuf := pool.Get()

			attrsBuf.AppendByte('[')
			_, _ = attrsBuf.Write(encBuf.Bytes())
			attrsBuf.AppendByte(']')

			var attrs []any
			assert.NoError(t, json.Unmarshal(attrsBuf.Bytes(), &attrs))
			assert.EqualValues(t, tt.expectedAttrs, getAttributes(attrs))

			unmarshaler := protojson.UnmarshalOptions{
				DiscardUnknown: false,
			}
			for _, attr := range attrs {
				byteAttr, err := json.Marshal(attr)
				assert.NoError(t, err)

				var protoAttr v1.KeyValue
				err = unmarshaler.Unmarshal(byteAttr, &protoAttr)
				assert.NoError(t, err, "Attribute JSON does not conform to OTLP KeyValue schema")
				assert.Equal(t, attr.(map[string]any)["key"], protoAttr.Key)
			}
		})
	}
}

func TestAttributesEncoderConfig(t *testing.T) {
	t.Run("EncodeDuration", func(t *testing.T) {
		enc := newAttributesEncoder(zapcore.EncoderConfig{
			EncodeDuration: func(d time.Duration, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(d.String())
			},
		})
		enc.AddDuration("duration", time.Second)

		encBuf, err := enc.encode()
		assert.NoError(t, err)

		pool := buffer.NewPool()
		attrsBuf := pool.Get()

		attrsBuf.AppendByte('[')
		_, _ = attrsBuf.Write(encBuf.Bytes())
		attrsBuf.AppendByte(']')

		var attrs []any
		assert.NoError(t, json.Unmarshal(attrsBuf.Bytes(), &attrs))
		assert.Equal(t, "1s", getAttributes(attrs)["duration"].(map[string]any)["stringValue"])
	})

	t.Run("EncodeTime", func(t *testing.T) {
		now := time.Now().UTC()

		enc := newAttributesEncoder(zapcore.EncoderConfig{
			EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
				enc.AppendString(t.Format(time.RFC1123Z))
			},
		})
		enc.AddTime("time", now)

		encBuf, err := enc.encode()
		assert.NoError(t, err)

		pool := buffer.NewPool()
		attrsBuf := pool.Get()

		attrsBuf.AppendByte('[')
		_, _ = attrsBuf.Write(encBuf.Bytes())
		attrsBuf.AppendByte(']')

		var attrs []any
		assert.NoError(t, json.Unmarshal(attrsBuf.Bytes(), &attrs))
		assert.Equal(t, now.UTC().Format(time.RFC1123Z), getAttributes(attrs)["time"].(map[string]any)["stringValue"])
	})
}

func getAttributes(attrs []any) map[string]any {
	result := make(map[string]any)
	for _, attr := range attrs {
		a := attr.(map[string]any)
		result[a["key"].(string)] = a["value"].(map[string]any)
	}
	return result
}
