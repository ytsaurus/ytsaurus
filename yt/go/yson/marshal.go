package yson

import (
	"bytes"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"time"
)

type EncoderOptions struct {
	SupportYPAPIMaps bool
}

// Encoder writes YSON to output stream.
type Encoder struct {
	opts *EncoderOptions

	w *Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: NewWriter(w)}
}

func NewEncoderWriter(w *Writer) *Encoder {
	return &Encoder{w: w}
}

// Marshaler is an interface implemented by types that can encode themselves to YSON.
type Marshaler interface {
	MarshalYSON() ([]byte, error)
}

// StreamMarshaler is an interface implemented by types that can encode themselves to YSON.
type StreamMarshaler interface {
	MarshalYSON(*Writer) error
}

func (e *Encoder) Encode(value any) (err error) {
	err = encodeAny(e.w, value, e.opts)
	if err != nil {
		return
	}

	err = e.w.w.Flush()
	return
}

// Marshal returns YSON encoding of value.
//
// Marshal traverses value recursively.
//
// If value implements Marshaler interface, Marshal calls its MarshalYSON method to produce YSON.
//
// If value implements StreamMarshaler interface, Marshal calls its MarshalYSON method to produce YSON.
//
// Otherwise, the following default encoding is used.
//
// Boolean values are encoded as YSON booleans.
//
// Floating point values are encoded as YSON float64.
//
// Unsigned integer types uint8, uint16, uint32, uint64 and uint are encoded as unsigned YSON integers.
//
// Signed integer types int8, int16, int32, int64 and int are encoded as signed YSON integers.
//
// string and []byte values are encoded as YSON strings. Note that YSON strings are always binary.
//
// Slice values are encoded as YSON lists (with exception of []byte).
//
// Struct values are encoded as YSON maps by default. Encoding of each struct field can be customized by format string
// stored under the "yson" key in the field's tag.
//
//	// Field appears in YSON under key "my_field".
//	Field int `yson:"my_field"`
//
//	// Field appears as attribute with name "my_attr".
//	Field int `yson:"my_attr,attr"`
//
//	// Field encoding completely replaces encoding of the whole struct.
//	// Other fields annotated as ",attr" are encoded as attributes preceding the value.
//	// All other fields are ignored.
//	Field int `yson:",value"`
//
//	// Field is skipped if empty.
//	Field int `yson:",omitempty"'
//
//	// Field is ignored by this package
//	Field int `yson:"-"`
//
//	// Field appears in YSON under key "-"
//	Field int `yson:"-,"`
//
// Map values are encoded as YSON maps. The map's key type must either be a
// string, implement encoding.TextMarshaler, or implement encoding.BinaryMarshaler.
// The map keys are used as YSON map keys by applying the following rules:
//   - keys of any string type are used directly
//   - encoding.TextMarshalers are marshaled
//   - encoding.BinaryMarshalers are marshaled
//
// Pointer values are encoded as the value pointed to. A nil pointer encodes as the YSON entity value.
//
// Values implementing encoding.TextMarshaler and encoding.BinaryMarshaler interface are encoded as YSON strings.
//
// Interface values are encoded as the value contained in the interface. A nil interface value encodes as the YSON entity value.
func Marshal(value any) ([]byte, error) {
	return MarshalFormat(value, FormatText)
}

func MarshalFormat(value any, format Format) ([]byte, error) {
	var buf bytes.Buffer
	writer := NewWriterFormat(&buf, format)
	encoder := Encoder{w: writer}
	err := encoder.Encode(value)
	return buf.Bytes(), err
}

func MarshalOptions(value any, opts *EncoderOptions) ([]byte, error) {
	var buf bytes.Buffer
	writer := NewWriterFormat(&buf, FormatText)
	encoder := Encoder{w: writer, opts: opts}
	err := encoder.Encode(value)
	return buf.Bytes(), err
}

func encodeAny(w *Writer, value any, opts *EncoderOptions) (err error) {
	vv := reflect.ValueOf(value)
	if value == nil || vv.Kind() == reflect.Ptr && vv.IsNil() {
		w.Entity()
		return w.Err()
	}

	switch vv := value.(type) {
	case bool:
		w.Bool(vv)
	case *bool:
		if vv != nil {
			w.Bool(*vv)
		} else {
			w.Entity()
		}

	case int8:
		w.Int64(int64(vv))
	case int16:
		w.Int64(int64(vv))
	case int32:
		w.Int64(int64(vv))
	case int64:
		w.Int64(vv)
	case int:
		w.Int64(int64(vv))

	case *int8:
		if vv != nil {
			w.Int64(int64(*vv))
		} else {
			w.Entity()
		}
	case *int16:
		if vv != nil {
			w.Int64(int64(*vv))
		} else {
			w.Entity()
		}
	case *int32:
		if vv != nil {
			w.Int64(int64(*vv))
		} else {
			w.Entity()
		}
	case *int64:
		if vv != nil {
			w.Int64(*vv)
		} else {
			w.Entity()
		}
	case *int:
		if vv != nil {
			w.Int64(int64(*vv))
		} else {
			w.Entity()
		}

	case uint8:
		w.Uint64(uint64(vv))
	case uint16:
		w.Uint64(uint64(vv))
	case uint32:
		w.Uint64(uint64(vv))
	case uint64:
		w.Uint64(vv)
	case uint:
		w.Uint64(uint64(vv))

	case *uint8:
		if vv != nil {
			w.Uint64(uint64(*vv))
		} else {
			w.Entity()
		}
	case *uint16:
		if vv != nil {
			w.Uint64(uint64(*vv))
		} else {
			w.Entity()
		}
	case *uint32:
		if vv != nil {
			w.Uint64(uint64(*vv))
		} else {
			w.Entity()
		}
	case *uint64:
		if vv != nil {
			w.Uint64(*vv)
		} else {
			w.Entity()
		}
	case *uint:
		if vv != nil {
			w.Uint64(uint64(*vv))
		} else {
			w.Entity()
		}

	case string:
		w.String(vv)
	case *string:
		if vv != nil {
			w.String(*vv)
		} else {
			w.Entity()
		}

	case []byte:
		w.Bytes(vv)
	case *[]byte:
		if vv != nil {
			w.Bytes(*vv)
		} else {
			w.Entity()
		}

	case float32:
		w.Float64(float64(vv))
	case float64:
		w.Float64(vv)

	case *float32:
		if vv != nil {
			w.Float64(float64(*vv))
		} else {
			w.Entity()
		}
	case *float64:
		if vv != nil {
			w.Float64(*vv)
		} else {
			w.Entity()
		}

	case RawValue:
		w.RawNode([]byte(vv))

	case map[string]any:
		w.BeginMap()
		for k, item := range vv {
			w.MapKeyString(k)

			if err = encodeAny(w, item, opts); err != nil {
				return err
			}
		}
		w.EndMap()

	case Marshaler:
		var raw []byte
		raw, err = vv.MarshalYSON()
		if err != nil {
			return
		}
		w.RawNode(raw)

	case StreamMarshaler:
		if err = vv.MarshalYSON(w); err != nil {
			return err
		}

	case Duration:
		w.Int64(int64(time.Duration(vv) / time.Millisecond))

	case *Duration:
		if vv != nil {
			w.Int64(int64(time.Duration(*vv) / time.Millisecond))
		} else {
			w.Entity()
		}

	case encoding.TextMarshaler:
		var text []byte
		if text, err = vv.MarshalText(); err != nil {
			return err
		}
		w.Bytes(text)

	case encoding.BinaryMarshaler:
		var bin []byte
		if bin, err = vv.MarshalBinary(); err != nil {
			return err
		}
		w.Bytes(bin)

	default:
		err = encodeReflect(w, reflect.ValueOf(vv), opts)
		if err != nil {
			return err
		}
	}

	return w.Err()
}

func encodeReflect(w *Writer, value reflect.Value, opts *EncoderOptions) error {
	switch value.Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		w.Int64(value.Int())
		return w.Err()

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		w.Uint64(value.Uint())
		return w.Err()

	case reflect.String:
		w.String(value.String())
		return w.Err()

	case reflect.Struct:
		return encodeReflectStruct(w, value, opts)
	case reflect.Slice:
		return encodeReflectSlice(w, value, opts)
	case reflect.Array:
		return encodeReflectSlice(w, value, opts)
	case reflect.Ptr:
		if value.IsNil() {
			w.Entity()
			return w.Err()
		}

		return encodeAny(w, value.Elem().Interface(), opts)
	case reflect.Map:
		return encodeReflectMap(w, value, false, opts)
	}

	return fmt.Errorf("yson: type %T not supported", value.Interface())
}

var (
	textMarshalerType   = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	binaryMarshalerType = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
)

func encodeReflectMap(w *Writer, value reflect.Value, attrs bool, opts *EncoderOptions) (err error) {
	switch value.Type().Key().Kind() {
	case reflect.String:
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if opts == nil || !opts.SupportYPAPIMaps {
			return fmt.Errorf("yson: unsupported map key type")
		}
		return encodeReflectYPAPIMap(w, value, opts)
	default:
		switch {
		case value.Type().Key().Implements(textMarshalerType),
			value.Type().Key().Implements(binaryMarshalerType):
		default:
			return fmt.Errorf("yson: unsupported map key type")
		}
	}

	if !attrs {
		w.BeginMap()
	} else {
		w.BeginAttrs()
	}

	mr := value.MapRange()
	for mr.Next() {
		if err := encodeMapKey(w, mr.Key()); err != nil {
			return err
		}

		if err = encodeAny(w, mr.Value().Interface(), opts); err != nil {
			return err
		}
	}

	if !attrs {
		w.EndMap()
	} else {
		w.EndAttrs()
	}
	return w.Err()
}

func encodeReflectYPAPIMap(w *Writer, value reflect.Value, opts *EncoderOptions) (err error) {
	w.BeginList()

	mr := value.MapRange()
	for mr.Next() {
		w.BeginMap()

		w.MapKeyString("key")
		if err = encodeAny(w, mr.Key().Interface(), opts); err != nil {
			return err
		}

		w.MapKeyString("value")
		if err = encodeAny(w, mr.Value().Interface(), opts); err != nil {
			return err
		}

		w.EndMap()
	}

	w.EndList()

	return w.Err()
}

func encodeMapKey(w *Writer, v reflect.Value) error {
	if v.Kind() == reflect.String {
		w.MapKeyString(v.String())
		return nil
	}

	if tm, ok := v.Interface().(encoding.TextMarshaler); ok {
		buf, err := tm.MarshalText()
		if err != nil {
			return err
		}
		w.MapKeyBytes(buf)
		return nil
	}

	if bm, ok := v.Interface().(encoding.BinaryMarshaler); ok {
		buf, err := bm.MarshalBinary()
		if err != nil {
			return err
		}
		w.MapKeyBytes(buf)
		return nil
	}

	panic("yson: unsupported map key type")
}

func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func encodeReflectStruct(w *Writer, value reflect.Value, opts *EncoderOptions) (err error) {
	t := getStructType(value)

	encodeMapFragment := func(fields []*field) (err error) {
		for _, field := range fields {
			fieldValue, ok, _ := fieldByIndex(value, field.index, false)
			if !ok {
				continue
			}

			if field.omitempty && isZeroValue(fieldValue) {
				continue
			}

			w.MapKeyString(field.name)
			if err = encodeAny(w, fieldValue.Interface(), opts); err != nil {
				return
			}
		}

		return
	}

	if t.attributes != nil {
		w.BeginAttrs()

		if err = encodeMapFragment(t.attributes); err != nil {
			return
		}

		w.EndAttrs()
	} else if t.attrs != nil {
		if err = encodeReflectMap(w, value.FieldByIndex(t.attrs.index), true, opts); err != nil {
			return
		}
	}

	if t.value != nil {
		return encodeAny(w, value.FieldByIndex(t.value.index).Interface(), opts)
	}

	w.BeginMap()

	if err = encodeMapFragment(t.fields); err != nil {
		return
	}

	w.EndMap()

	return w.Err()
}

func encodeReflectSlice(w *Writer, value reflect.Value, opts *EncoderOptions) error {
	w.BeginList()
	for i := 0; i < value.Len(); i++ {
		if err := encodeAny(w, value.Index(i).Interface(), opts); err != nil {
			return err
		}
	}
	w.EndList()

	return w.Err()
}
