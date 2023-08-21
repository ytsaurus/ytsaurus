// Package wire implements subset of YT wire protocol used by RPC proxy.
package wire

import (
	"encoding/binary"
	"fmt"
)

type reader struct {
	buf []byte
	pos int
}

func (r *reader) readUint64() (i uint64) {
	i = binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8])
	r.pos += 8
	return
}

func (r *reader) checkSize(size int) (err error) {
	if len(r.buf)-r.pos < size {
		return fmt.Errorf("rowset is truncated: read_size=%d, rowset_size=%d", size, len(r.buf))
	}

	return nil
}

func (r *reader) readValue() (v Value, err error) {
	if err = r.checkSize(8); err != nil {
		return
	}

	v.ID = binary.LittleEndian.Uint16(r.buf[r.pos : r.pos+2])
	v.Type = ValueType(r.buf[r.pos+2] - typeOffset)
	v.Aggregate = r.buf[r.pos+3] != 0
	rawLength := binary.LittleEndian.Uint32(r.buf[r.pos+4 : r.pos+8])

	r.pos += 8

	switch v.Type {
	case TypeInt64, TypeUint64, TypeFloat64, TypeBool:
		if err = r.checkSize(8); err != nil {
			return
		}

		v.scalar = r.readUint64()
	case TypeBytes, TypeAny:
		if rawLength > maxValueLength {
			err = fmt.Errorf("value is too long: actual=%d, limit=%d", rawLength, maxValueLength)
			return
		}
		length := int(rawLength)

		paddedLength := length + computePadding(length)
		if err = r.checkSize(paddedLength); err != nil {
			return
		}

		v.blob = r.buf[r.pos : r.pos+length]
		r.pos += paddedLength
	case TypeNull:
	default:
		err = fmt.Errorf("invalid wire type 0x%02x", int(v.Type)+typeOffset)
		return
	}

	return
}

// UnmarshalRowset decodes rowset from YT wire format.
//
// Returned values of type Bytes and Any point directly into the input buffer.
func UnmarshalRowset(b []byte) (rows []Row, err error) {
	r := reader{buf: b}

	if err = r.checkSize(8); err != nil {
		return
	}
	rawRowCount := r.readUint64()
	if rawRowCount > maxRowsPerRowset {
		err = fmt.Errorf("too many rows in rowset: actual=%d, limit=%d", rawRowCount, maxRowsPerRowset)
		return
	}
	rowCount := int(rawRowCount)
	rows = make([]Row, rowCount)

	for i := 0; i < rowCount; i++ {
		if err = r.checkSize(8); err != nil {
			return
		}

		rawValueCount := r.readUint64()
		if rawValueCount == nullRowMarker {
			continue
		}
		if rawValueCount > maxValuesPerRow {
			err = fmt.Errorf("too many values in row #%d: actual=%d, limit=%d", i, rawValueCount, maxValuesPerRow)
			return
		}
		valueCount := int(rawValueCount)

		row := make(Row, valueCount)
		for j := 0; j < valueCount; j++ {
			row[j], err = r.readValue()
			if err != nil {
				return
			}
		}

		rows[i] = row
	}

	return
}
