package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Rowset wire format:
//
// Rowset is encoded as 8 byte uint64 specifying row count, followed by a sequence of rows.
// Each row starts with uint64 specifying number of values. Special value -1 encodes nil row.
// Single value header is packed into 8 bytes with layout of TUnversionedValue structure.
// Encoding of the value depends of the value type.
//   - Null is omitted from the stream.
//   - Basic types are encoded into 8 bytes.
//   - String is encoded using variable number of bytes.
//
// All atomic elements of the stream are padded by 8 bytes alignment.
type writer struct {
	buf []byte
	pos int
}

const (
	padding          = 8
	maxRowsPerRowset = 5 * 1024 * 1024
	maxValuesPerRow  = 1024
	maxValueLength   = 16 * 1024 * 1024
	nullRowMarker    = math.MaxUint64
)

func (w *writer) writeUint64(i uint64) {
	binary.LittleEndian.PutUint64(w.buf[w.pos:w.pos+8], i)
	w.pos += padding
}

func computePadding(i int) int {
	return (padding - (i % padding)) % padding
}

func (w *writer) writeRaw(b []byte) {
	padding := computePadding(len(b))
	copy(w.buf[w.pos:w.pos+len(b)], b)
	w.pos += len(b) + padding
}

func (w *writer) writeValue(v Value) {
	var header [8]byte
	binary.LittleEndian.PutUint16(header[0:2], v.ID)
	header[2] = uint8(v.Type) + typeOffset
	if v.Aggregate {
		header[3] = 1
	}
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(v.blob)))

	copy(w.buf[w.pos:w.pos+8], header[:])
	w.pos += 8

	switch v.Type {
	case TypeInt64, TypeUint64, TypeFloat64, TypeBool:
		w.writeUint64(v.scalar)
	case TypeBytes, TypeAny:
		w.writeRaw(v.blob)
	default:
	}
}

func computeWireSize(rowset []Row) (int, error) {
	size := 0
	if len(rowset) > maxRowsPerRowset {
		return 0, fmt.Errorf("too many rows in rowset: actual=%d, limit=%d", len(rowset), maxRowsPerRowset)
	}
	size += 8

	for i, row := range rowset {
		if len(row) > maxValuesPerRow {
			return 0, fmt.Errorf("too many values in row #%d: actual=%d, limit=%d", i, len(row), maxValuesPerRow)
		}
		size += 8

		for _, value := range row {
			if err := value.Validate(); err != nil {
				return 0, err
			}

			size += value.wireSize()
		}
	}

	return size, nil
}

func MarshalRowset(rowset []Row) ([]byte, error) {
	size, err := computeWireSize(rowset)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	w := writer{buf: buf}

	w.writeUint64(uint64(len(rowset)))
	for _, row := range rowset {
		if row == nil {
			w.writeUint64(nullRowMarker)
			continue
		}

		w.writeUint64(uint64(len(row)))
		for _, value := range row {
			w.writeValue(value)
		}
	}

	return buf, nil
}
