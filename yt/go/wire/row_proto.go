package wire

import (
	"math"

	"google.golang.org/protobuf/encoding/protowire"

	"go.ytsaurus.tech/library/go/core/xerrors"
)

const (
	rowProtoVersion = 0

	rowProtoTypeMin       = 0x00
	rowProtoTypeTheBottom = 0x01
	rowProtoTypeMax       = 0xEF
)

var (
	// ErrMalformedRowProto indicates that input is not one complete version-0 row proto blob.
	ErrMalformedRowProto = xerrors.New("wire: malformed row proto")

	// ErrUnsupportedRowProto indicates that a row proto version, value type, or row feature is unsupported.
	ErrUnsupportedRowProto = xerrors.New("wire: unsupported row proto")
)

type rowProtoError struct {
	category error
	cause    error
}

func (e *rowProtoError) Error() string {
	return e.cause.Error()
}

func (e *rowProtoError) Unwrap() error {
	return e.cause
}

func (e *rowProtoError) Is(target error) bool {
	return target == e.category
}

func classifyRowProtoError(category error, cause error) error {
	return &rowProtoError{
		category: category,
		cause:    cause,
	}
}

// MarshalRowProto serializes a single unversioned row into the compact blob that YT
// stores in bytes fields of proto messages (C++ ToProto/SerializeRowToBuffer). It is
// not a protobuf message or the MarshalRowset format. A nil row is serialized as zero bytes.
func MarshalRowProto(row Row) ([]byte, error) {
	if row == nil {
		return nil, nil
	}

	size, err := estimateRowProtoSize(row)
	if err != nil {
		return nil, err
	}
	return AppendRowProto(make([]byte, 0, size), row)
}

// AppendRowProto appends the compact row proto blob to dst. A nil row appends no bytes.
func AppendRowProto(dst []byte, row Row) ([]byte, error) {
	if row == nil {
		return dst, nil
	}

	size, err := estimateRowProtoSize(row)
	if err != nil {
		return dst, err
	}
	if size > int(^uint(0)>>1)-len(dst) {
		return dst, classifyRowProtoError(
			ErrIntegerOverflow,
			xerrors.Errorf("wire: serialized row is too large"))
	}
	if size > cap(dst)-len(dst) {
		buf := make([]byte, len(dst), len(dst)+size)
		copy(buf, dst)
		dst = buf
	}

	dst = protowire.AppendVarint(dst, rowProtoVersion)
	dst = protowire.AppendVarint(dst, uint64(len(row)))
	for i := range row {
		dst = appendRowProtoValue(dst, &row[i])
	}
	return dst, nil
}

func estimateRowProtoSize(row Row) (int, error) {
	valueCountSize, err := rowProtoVarUint32Size(uint64(len(row)))
	if err != nil {
		return 0, xerrors.Errorf("wire: invalid value count: %w", err)
	}

	maxInt := uint64(^uint(0) >> 1)
	size := uint64(protowire.SizeVarint(rowProtoVersion)) + valueCountSize

	for i := range row {
		valueSize, err := estimateRowProtoValueSize(&row[i])
		if err != nil {
			return 0, err
		}
		if valueSize > maxInt-size {
			return 0, classifyRowProtoError(
				ErrIntegerOverflow,
				xerrors.Errorf("wire: serialized row is too large"))
		}
		size += valueSize
	}
	return int(size), nil
}

func estimateRowProtoValueSize(v *Value) (uint64, error) {
	if v.Aggregate {
		return 0, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: aggregate value of column %d is not supported", v.ID))
	}

	size := uint64(protowire.SizeVarint(uint64(v.ID)) + protowire.SizeVarint(uint64(v.Type.Code())))
	switch v.Type {
	case TypeNull:
	case TypeInt64:
		size += uint64(protowire.SizeVarint(protowire.EncodeZigZag(v.Int64())))
	case TypeUint64:
		size += uint64(protowire.SizeVarint(v.Uint64()))
	case TypeBool:
		size++
	case TypeFloat64:
		size += 8
	case TypeBytes, TypeAny, TypeComposite:
		length := uint64(len(v.Bytes()))
		lengthSize, err := rowProtoVarUint32Size(length)
		if err != nil {
			return 0, xerrors.Errorf("wire: invalid value length of column %d: %w", v.ID, err)
		}
		size += lengthSize + length
	default:
		return 0, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: unsupported value type %#x", v.Type.Code()))
	}
	return size, nil
}

func rowProtoVarUint32Size(value uint64) (uint64, error) {
	if value > math.MaxUint32 {
		return 0, classifyRowProtoError(
			ErrIntegerOverflow,
			xerrors.Errorf("%d exceeds uint32", value))
	}
	return uint64(protowire.SizeVarint(value)), nil
}

func appendRowProtoValue(dst []byte, v *Value) []byte {
	dst = protowire.AppendVarint(dst, uint64(v.ID))
	dst = protowire.AppendVarint(dst, uint64(v.Type.Code()))

	switch v.Type {
	case TypeNull:
	case TypeInt64:
		dst = protowire.AppendVarint(dst, protowire.EncodeZigZag(v.Int64()))
	case TypeUint64:
		dst = protowire.AppendVarint(dst, v.Uint64())
	case TypeBool:
		if v.Bool() {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
	case TypeFloat64:
		dst = protowire.AppendFixed64(dst, math.Float64bits(v.Float64()))
	case TypeBytes, TypeAny, TypeComposite:
		dst = protowire.AppendBytes(dst, v.Bytes())
	}
	return dst
}

// UnmarshalRowProto deserializes the compact row blob stored in proto bytes fields.
// It is not a protobuf message or the UnmarshalRowset format. Zero-length input decodes to a nil row.
// Zero-length blob values decode to non-nil empty slices because the format does not distinguish them from nil.
// Boolean values consume one raw byte, and only 0x01 decodes as true.
// Decoded Bytes, Any, and Composite values are copied and never alias data, unlike UnmarshalRowset.
func UnmarshalRowProto(data []byte) (Row, error) {
	if len(data) == 0 {
		return nil, nil
	}

	d := rowProtoDecoder{buf: data}

	version, err := d.varUint32("version")
	if err != nil {
		return nil, err
	}
	if version != rowProtoVersion {
		return nil, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: unversioned row does not support version %d", version))
	}

	count, err := d.varUint32("value count")
	if err != nil {
		return nil, err
	}
	maxStructuralCount := uint64(len(d.buf)) / 2
	if count > maxStructuralCount {
		return nil, classifyRowProtoError(
			ErrMalformedRowProto,
			xerrors.Errorf(
				"wire: value count %d exceeds structural maximum %d for %d remaining bytes",
				count,
				maxStructuralCount,
				len(d.buf)))
	}

	row := make(Row, 0, int(count))
	for i := uint64(0); i < count; i++ {
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		row = append(row, v)
	}
	if len(d.buf) != 0 {
		return nil, classifyRowProtoError(
			ErrMalformedRowProto,
			xerrors.Errorf("wire: trailing data after row: %d bytes", len(d.buf)))
	}
	return row, nil
}

type rowProtoDecoder struct {
	buf []byte
}

func (d *rowProtoDecoder) varint(what string) (uint64, error) {
	v, n := protowire.ConsumeVarint(d.buf)
	if n < 0 {
		return 0, classifyRowProtoError(
			ErrMalformedRowProto,
			xerrors.Errorf("wire: malformed %s: %w", what, protowire.ParseError(n)))
	}
	d.buf = d.buf[n:]
	return v, nil
}

func (d *rowProtoDecoder) varUint32(what string) (uint64, error) {
	v, err := d.varint(what)
	if err != nil {
		return 0, err
	}
	if v > math.MaxUint32 {
		return 0, classifyRowProtoError(
			ErrMalformedRowProto,
			xerrors.Errorf("wire: %s %d exceeds uint32", what, v))
	}
	return v, nil
}

func (d *rowProtoDecoder) value() (Value, error) {
	rawID, err := d.varUint32("column id")
	if err != nil {
		return Value{}, err
	}
	id := uint16(rawID)

	rawType, err := d.varUint32("value type")
	if err != nil {
		return Value{}, err
	}
	switch rawType {
	case rowProtoTypeMin, rowProtoTypeMax, rowProtoTypeTheBottom:
		return Value{}, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: sentinel value type %#x is not supported", rawType))
	}
	if rawType > uint64(^uint8(0)) {
		return Value{}, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: unknown value type %#x", rawType))
	}

	valueType, err := ValueTypeFromCode(uint8(rawType))
	if err != nil {
		return Value{}, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: unknown value type: %w", err))
	}

	switch valueType {
	case TypeNull:
		return NewNull(id), nil

	case TypeInt64:
		raw, err := d.varint("int64 value")
		if err != nil {
			return Value{}, err
		}
		return NewInt64(id, protowire.DecodeZigZag(raw)), nil

	case TypeUint64:
		raw, err := d.varint("uint64 value")
		if err != nil {
			return Value{}, err
		}
		return NewUint64(id, raw), nil

	case TypeBool:
		if len(d.buf) == 0 {
			return Value{}, classifyRowProtoError(
				ErrMalformedRowProto,
				xerrors.Errorf("wire: truncated boolean value of column %d", id))
		}
		raw := d.buf[0]
		d.buf = d.buf[1:]
		return NewBool(id, raw == 1), nil

	case TypeFloat64:
		raw, n := protowire.ConsumeFixed64(d.buf)
		if n < 0 {
			return Value{}, classifyRowProtoError(
				ErrMalformedRowProto,
				xerrors.Errorf("wire: malformed double value: %w", protowire.ParseError(n)))
		}
		d.buf = d.buf[n:]
		return NewFloat64(id, math.Float64frombits(raw)), nil

	case TypeBytes, TypeAny, TypeComposite:
		blob, err := d.blob(valueType)
		if err != nil {
			return Value{}, err
		}
		switch valueType {
		case TypeBytes:
			return NewBytes(id, blob), nil
		case TypeAny:
			return NewAny(id, blob), nil
		default:
			return NewComposite(id, blob), nil
		}

	default:
		return Value{}, classifyRowProtoError(
			ErrUnsupportedRowProto,
			xerrors.Errorf("wire: unsupported value type %#x", rawType))
	}
}

func (d *rowProtoDecoder) blob(valueType ValueType) ([]byte, error) {
	size, err := d.varUint32("value length")
	if err != nil {
		return nil, err
	}
	if size > uint64(len(d.buf)) {
		return nil, classifyRowProtoError(
			ErrMalformedRowProto,
			xerrors.Errorf("wire: truncated value of type %v: want %d bytes, have %d", valueType, size, len(d.buf)))
	}
	blob := make([]byte, int(size))
	copy(blob, d.buf[:int(size)])
	d.buf = d.buf[int(size):]
	return blob, nil
}
