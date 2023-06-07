package yson

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Format is YSON representation.
type Format int

const (
	FormatBinary Format = iota
	FormatText
	FormatPretty
)

func (f Format) String() string {
	switch f {
	case FormatPretty:
		return "pretty"
	case FormatText:
		return "text"
	case FormatBinary:
		return "binary"
	}

	panic("invalid format")
}

// StreamKind is kind of top level value.
type StreamKind int

const (
	StreamNode StreamKind = iota
	StreamListFragment
	StreamMapFragment
)

func (s StreamKind) String() string {
	switch s {
	case StreamNode:
		return "node"
	case StreamListFragment:
		return "list_fragment"
	case StreamMapFragment:
		return "map_fragment"
	}

	panic("invalid stream kind")
}

// Writer writes YSON stream to underlying byte stream.
//
// Example usage:
//
//	var buf bytes.Buffer
//	w := yson.NewWriter(&buf)
//
//	w.BeginMap()
//	w.MapKeyString("a")
//	w.Bool(false)
//	w.EndMap()
//
//	if err := w.Flush(); err != nil {
//	    return err
//	}
//
//	yson := buf.Bytes()
//
// Most users should not use Writer directly. Use higher level Marshal() function instead.
//
// Writer ignores all calls after the first error.
type Writer struct {
	w   *bufio.Writer
	err error

	format Format

	notFirst bool
	afterKey bool

	validator streamValidator
}

type WriterConfig struct {
	Format Format
	Kind   StreamKind
}

func NewWriterConfig(inner io.Writer, c WriterConfig) (w *Writer) {
	w = &Writer{w: bufio.NewWriter(inner)}
	w.format = c.Format
	w.validator = newValidator(c.Kind)
	return
}

func NewWriterFormat(inner io.Writer, format Format) (w *Writer) {
	return NewWriterConfig(inner, WriterConfig{format, StreamNode})
}

// NewWriter returns new writer configured with text format.
func NewWriter(inner io.Writer) *Writer {
	return NewWriterFormat(inner, FormatText)
}

func (w *Writer) indent() {
	if w.format != FormatPretty {
		return
	}

	if w.afterKey {
		w.afterKey = false
		return
	}

	if !w.notFirst {
		w.notFirst = true
	} else {
		w.err = w.w.WriteByte('\n')
		if w.err != nil {
			return
		}
	}

	depth := w.validator.depth()
	for i := 0; i < depth; i++ {
		_, w.err = w.w.WriteString("    ")
		if w.err != nil {
			return
		}
	}
}

func (w *Writer) BeginAttrs() {
	w.indent()
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventBeginAttrs)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('<')
}

func (w *Writer) EndAttrs() {
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventEndAttrs)
	if w.err != nil {
		return
	}

	w.indent()
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('>')
}

func (w *Writer) BeginMap() {
	w.indent()
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventBeginMap)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('{')
}

func (w *Writer) EndMap() {
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventEndMap)
	if w.err != nil {
		return
	}

	w.indent()
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('}')
	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) str(s string) {
	switch w.format {
	case FormatText, FormatPretty:
		if !needEscapeString(s) {
			_, w.err = w.w.WriteString(s)
		} else {
			if w.err = w.w.WriteByte('"'); w.err != nil {
				return
			}

			if _, w.err = w.w.Write(escapeC([]byte(s))); w.err != nil {
				return
			}

			if w.err = w.w.WriteByte('"'); w.err != nil {
				return
			}
		}

	case FormatBinary:
		var bits [binary.MaxVarintLen64]byte
		n := binary.PutVarint(bits[:], int64(len(s)))

		w.err = w.w.WriteByte(binaryString)
		if w.err != nil {
			return
		}

		_, w.err = w.w.Write(bits[:n])
		if w.err != nil {
			return
		}

		_, w.err = w.w.WriteString(s)
	}
}

func (w *Writer) MapKeyString(k string) {
	w.indent()
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventKey)
	if w.err != nil {
		return
	}

	w.str(k)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('=')
	w.afterKey = true
}

func (w *Writer) bytes(b []byte) {
	switch w.format {
	case FormatText, FormatPretty:
		if !needEscape(b) {
			_, w.err = w.w.Write(b)
		} else {
			if w.err = w.w.WriteByte('"'); w.err != nil {
				return
			}

			if _, w.err = w.w.Write(escapeC(b)); w.err != nil {
				return
			}

			if w.err = w.w.WriteByte('"'); w.err != nil {
				return
			}
		}

	case FormatBinary:
		var bits [binary.MaxVarintLen64]byte
		n := binary.PutVarint(bits[:], int64(len(b)))

		w.err = w.w.WriteByte(binaryString)
		if w.err != nil {
			return
		}

		_, w.err = w.w.Write(bits[:n])
		if w.err != nil {
			return
		}

		_, w.err = w.w.Write(b)
	}
}

func (w *Writer) MapKeyBytes(k []byte) {
	w.indent()
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventKey)
	if w.err != nil {
		return
	}

	w.bytes(k)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('=')
	w.afterKey = true
}

func (w *Writer) BeginList() {
	w.indent()
	if w.err != nil {
		return
	}

	_, w.err = w.validator.pushEvent(EventBeginList)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('[')
}

func (w *Writer) EndList() {
	var colon bool
	colon, w.err = w.validator.pushEvent(EventEndList)
	if w.err != nil {
		return
	}

	w.indent()
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte(']')
	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) String(s string) {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	w.str(s)
	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) Bytes(b []byte) {
	if b == nil {
		w.Entity()
		return
	}

	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	w.bytes(b)
	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) Int64(i int64) {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	switch w.format {
	case FormatText, FormatPretty:
		_, w.err = fmt.Fprintf(w.w, "%d", i)

	case FormatBinary:
		w.err = w.w.WriteByte(binaryInt)
		if w.err != nil {
			return
		}

		var bits [binary.MaxVarintLen64]byte
		n := binary.PutVarint(bits[:], i)
		_, w.err = w.w.Write(bits[:n])
	}

	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}

}

func (w *Writer) Uint64(u uint64) {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	switch w.format {
	case FormatText, FormatPretty:
		_, w.err = fmt.Fprintf(w.w, "%du", u)

	case FormatBinary:
		w.err = w.w.WriteByte(binaryUint)
		if w.err != nil {
			return
		}

		var bits [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(bits[:], u)
		_, w.err = w.w.Write(bits[:n])
	}

	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) Float64(f float64) {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	switch w.format {
	case FormatText, FormatPretty:
		if math.IsNaN(f) {
			_, w.err = w.w.WriteString("%nan")
		} else if math.IsInf(f, 1) {
			_, w.err = w.w.WriteString("%inf")
		} else if math.IsInf(f, -1) {
			_, w.err = w.w.WriteString("%-inf")
		} else {
			_, w.err = fmt.Fprintf(w.w, "%f", f)
		}

	case FormatBinary:
		w.err = w.w.WriteByte(binaryFloat)
		if w.err != nil {
			return
		}

		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(f))
		_, w.err = w.w.Write(bits[:])
	}

	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) Bool(b bool) {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	switch w.format {
	case FormatText, FormatPretty:
		if b {
			_, w.err = w.w.WriteString("%true")
		} else {
			_, w.err = w.w.WriteString("%false")
		}

	case FormatBinary:
		if b {
			w.err = w.w.WriteByte(binaryTrue)
		} else {
			w.err = w.w.WriteByte(binaryFalse)
		}
	}

	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func (w *Writer) Entity() {
	w.indent()
	if w.err != nil {
		return
	}

	var colon bool
	colon, w.err = w.validator.pushEvent(EventLiteral)
	if w.err != nil {
		return
	}

	w.err = w.w.WriteByte('#')
	if w.err != nil {
		return
	}

	if colon {
		w.err = w.w.WriteByte(';')
	}
}

func pipe(w *Writer, r *Reader) error {
	for {
		event, err := r.Next(false)
		if err != nil {
			return err
		}

		switch event {
		case EventBeginAttrs:
			w.BeginAttrs()
		case EventEndAttrs:
			w.EndAttrs()

		case EventBeginMap:
			w.BeginMap()
		case EventEndMap:
			w.EndMap()
		case EventKey:
			w.MapKeyBytes(r.Bytes())

		case EventBeginList:
			w.BeginList()
		case EventEndList:
			w.EndList()

		case EventLiteral:
			switch r.Type() {
			case TypeEntity:
				w.Entity()
			case TypeBool:
				w.Bool(r.Bool())
			case TypeInt64:
				w.Int64(r.Int64())
			case TypeUint64:
				w.Uint64(r.Uint64())
			case TypeFloat64:
				w.Float64(r.Float64())
			case TypeString:
				w.Bytes(r.Bytes())
			}

		case EventEOF:
			return nil
		}
	}
}

func (w *Writer) RawNode(raw []byte) {
	if w.err != nil {
		return
	}

	switch w.format {
	case FormatText, FormatPretty:
		r := NewReaderFromBytes(raw)
		w.err = pipe(w, r)

	case FormatBinary:
		var colon bool
		colon, w.err = w.validator.pushEvent(EventLiteral)
		if w.err != nil {
			return
		}

		if w.err = Valid(raw); w.err != nil {
			return
		}

		_, w.err = w.w.Write(raw)
		if w.err != nil {
			return
		}

		if colon {
			w.err = w.w.WriteByte(';')
		}
	}
}

func (w *Writer) Any(v any) {
	if w.err != nil {
		return
	}

	w.err = encodeAny(w, v, nil)
}

func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.w.Flush()
	if w.err != nil {
		return w.err
	}

	w.err = w.validator.eof()
	return w.err
}

func (w *Writer) Err() error {
	return w.err
}
