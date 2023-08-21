package yson

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"unsafe"
)

type Event int

const (
	EventBeginList Event = iota
	EventEndList
	EventBeginAttrs
	EventEndAttrs
	EventBeginMap
	EventEndMap
	EventLiteral
	EventKey
	EventEOF
)

// Type is a logical YSON type.
type Type int

const (
	// TypeEntity is YSON equivalent to nil.
	TypeEntity Type = iota
	TypeBool
	TypeString
	TypeInt64
	TypeUint64
	TypeFloat64
	TypeList
	TypeMap
)

func (t Type) String() string {
	switch t {
	case TypeEntity:
		return "entity"
	case TypeBool:
		return "bool"
	case TypeString:
		return "string"
	case TypeInt64:
		return "int64"
	case TypeUint64:
		return "uint64"
	case TypeFloat64:
		return "float64"
	case TypeList:
		return "list"
	case TypeMap:
		return "map"
	default:
		return ""
	}
}

const decoderBufferSize = 16 * 1024

// Reader provides streaming access to YSON node.
type Reader struct {
	r io.Reader
	s scanner

	buf  []byte
	keep int // first byte to keep, -1 == keep only the last byte
	pos  int // next byte to scan
	end  int // end of data inside buf

	undo      bool
	undoEvent Event

	currentType   Type
	currentString []byte
	currentInt    int64
	currentUint   uint64
	currentBool   bool
	currentFloat  float64
}

// Type returns type of the last read literal.
func (r *Reader) Type() Type {
	return r.currentType
}

func (r *Reader) Bytes() []byte {
	return r.currentString
}

func (r *Reader) String() string {
	return string(r.currentString)
}

func (r *Reader) Int64() int64 {
	return r.currentInt
}

func (r *Reader) Uint64() uint64 {
	return r.currentUint
}

func (r *Reader) Bool() bool {
	return r.currentBool
}

func (r *Reader) Float64() float64 {
	return r.currentFloat
}

type RawValue []byte

func NewReaderKind(r io.Reader, kind StreamKind) *Reader {
	d := &Reader{
		r:   r,
		buf: make([]byte, decoderBufferSize),
	}

	d.s.reset(kind)
	return d
}

func NewReader(r io.Reader) *Reader {
	return NewReaderKind(r, StreamNode)
}

func NewReaderKindFromBytes(b []byte, kind StreamKind) *Reader {
	d := &Reader{buf: b, end: len(b)}
	d.s.reset(kind)
	return d
}

func NewReaderFromBytes(b []byte) *Reader {
	return NewReaderKindFromBytes(b, StreamNode)
}

func unsafeBytesToString(bytes []byte) (s string) {
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	str := (*reflect.StringHeader)(unsafe.Pointer(&s))
	str.Data = slice.Data
	str.Len = slice.Len
	return
}

func (r *Reader) decodeLastLiteral() (err error) {
	switch r.s.lastLiteral {
	case literalEntity:
		r.currentType = TypeEntity
	case literalInt:
		r.currentType = TypeInt64
		r.currentInt, err = strconv.ParseInt(unsafeBytesToString(r.buf[r.keep:r.pos]), 10, 64)
	case literalUint:
		r.currentType = TypeUint64
		r.currentUint, err = strconv.ParseUint(unsafeBytesToString(r.buf[r.keep:r.pos-1]), 10, 64)
	case literalBool:
		r.currentType = TypeBool
		r.currentBool = r.buf[r.keep+1] == 't'
	case literalFloat:
		r.currentType = TypeFloat64
		if r.buf[r.keep] != '%' {
			r.currentFloat, err = strconv.ParseFloat(unsafeBytesToString(r.buf[r.keep:r.pos]), 64)
		} else {
			switch r.buf[r.keep+1] {
			case 'n':
				r.currentFloat = math.NaN()
			case 'i', '+':
				r.currentFloat = math.Inf(1)
			case '-':
				r.currentFloat = math.Inf(-1)
			}
		}
	case literalIdentifier:
		r.currentType = TypeString
		r.currentString = r.buf[r.keep:r.pos]
	case literalString:
		r.currentType = TypeString
		r.currentString = unescapeC(r.buf[r.keep+1 : r.pos-1])
	case literalBinaryString:
		r.currentType = TypeString
		r.currentString = r.buf[r.keep+1+r.s.varintSize : r.pos]
	case literalBinaryInt:
		r.currentType = TypeInt64
		r.currentInt, _ = binary.Varint(r.buf[r.keep+1 : r.pos])
	case literalBinaryUint:
		r.currentType = TypeUint64
		r.currentUint, _ = binary.Uvarint(r.buf[r.keep+1 : r.pos])
	case literalBinaryFalse:
		r.currentType = TypeBool
		r.currentBool = false
	case literalBinaryTrue:
		r.currentType = TypeBool
		r.currentBool = true
	case literalBinaryFloat:
		r.currentType = TypeFloat64
		bit := binary.LittleEndian.Uint64(r.buf[r.keep+1 : r.pos])
		r.currentFloat = math.Float64frombits(bit)

	default:
		err = &SyntaxError{Message: "unexpected literal type"}
	}

	return
}

// Undo last call to Next.
//
// It is not possible to undo call to Next(true).
func (r *Reader) Undo(event Event) {
	switch event {
	case EventLiteral:
		r.undoEvent = event
		r.undo = true

	case EventBeginAttrs:
		r.s.undo(scanBeginAttrs)
		r.pos--

	case EventBeginMap:
		r.s.undo(scanBeginMap)
		r.pos--

	case EventBeginList:
		r.s.undo(scanBeginList)
		r.pos--

	default:
		panic(fmt.Sprintf("can't undo %d", event))
	}
}

// Next returns next event from yson stream.
func (r *Reader) Next(skipAttributes bool) (Event, error) {
	if r.undo {
		r.undo = false
		return r.undoEvent, nil
	}

	r.keep = -1
	op, err := r.scanWhile(scanSkipSpace)
	if err != nil {
		return 0, err
	}

	r.redo()
	if op == scanBeginAttrs && skipAttributes {
		_, err = r.scanUntilDepth(len(r.s.parseState) - 1)
		if err != nil {
			return 0, err
		}

		r.redo()
		op, err = r.scanWhile(scanSkipSpace)
		if err != nil {
			return 0, err
		}

		r.redo()
	}

	switch op {
	case scanBeginLiteral, scanBeginKey:
		var event Event
		if op == scanBeginLiteral {
			event = EventLiteral
		} else {
			event = EventKey
		}

		r.keep = r.pos - 1
		_, err = r.scanWhile(scanContinue)
		if err != nil {
			return 0, err
		}

		err = r.decodeLastLiteral()
		if err != nil {
			return 0, err
		}

		return event, nil

	case scanBeginAttrs:
		return EventBeginAttrs, nil
	case scanEndAttrs:
		return EventEndAttrs, nil
	case scanBeginList:
		r.currentType = TypeList
		return EventBeginList, nil
	case scanEndList:
		return EventEndList, nil
	case scanBeginMap:
		r.currentType = TypeMap
		return EventBeginMap, nil
	case scanEndMap:
		return EventEndMap, nil
	case scanEnd:
		return EventEOF, nil
	}

	panic(fmt.Sprintf("unreachable state %c", op))
}

// NextListItem returns true if next event in the stream is end of list.
func (r *Reader) NextListItem() (ok bool, err error) {
	r.keep = -1
	op, err := r.scanWhile(scanSkipSpace)
	if err != nil {
		return false, err
	}

	ok = op != scanEndList && op != scanEnd
	return
}

// NextKey returns true if next event in the stream is a key.
//
// If next event is a key, NextKey consumes it.
func (r *Reader) NextKey() (ok bool, err error) {
	r.keep = -1
	op, err := r.scanWhile(scanSkipSpace)
	if err != nil {
		return false, err
	}

	if op != scanBeginKey {
		return false, nil
	}

	r.keep = r.pos
	r.redo()
	_, err = r.scanWhile(scanContinue)
	if err != nil {
		return false, err
	}

	err = r.decodeLastLiteral()
	if err != nil {
		return false, err
	}

	return true, nil
}

// NextRawValue reads next YSON value from stream.
//
// Returned slice is valid only until next call to the reader.
func (r *Reader) NextRawValue() ([]byte, error) {
	if r.undo {
		r.undo = false
		return r.buf[r.keep:r.pos], nil
	}

	r.keep = -1
	op, err := r.scanWhile(scanSkipSpace)
	if err != nil {
		return nil, err
	}

	r.keep = r.pos
	if op == scanBeginAttrs {
		_, err = r.scanUntilDepth(len(r.s.parseState) - 1)
		if err != nil {
			return nil, err
		}

		r.redo()
		op, err = r.scanWhile(scanSkipSpace)
		if err != nil {
			return nil, err
		}
	}

	r.redo()
	if op == scanBeginLiteral {
		_, err = r.scanWhile(scanContinue)
		if err != nil {
			return nil, err
		}
	} else {
		_, err = r.scanUntilDepth(len(r.s.parseState) - 1)
		if err != nil {
			return nil, err
		}
		r.redo()
	}

	return r.buf[r.keep:r.pos], nil
}

func (r *Reader) readMore() error {
	// NOTE: if r.r == nil buffer we are using user provided buffer.
	// In that case we are not allowed to modify it.

	if r.keep == -1 {
		r.pos = 0
		r.end = 0
	}

	if r.r != nil && r.keep != -1 && r.end-r.keep < r.keep && r.keep != 0 {
		copy(r.buf, r.buf[r.keep:r.end])
		r.pos -= r.keep
		r.end -= r.keep
		r.keep = 0
	}

	if r.r != nil && r.end > len(r.buf)/2 {
		r.buf = append(r.buf, make([]byte, len(r.buf))...)
	}

	if r.r != nil {
		n, err := r.r.Read(r.buf[r.end:])
		r.end += n

		if n > 0 && err == io.EOF {
			err = nil
		}
		return err
	}

	return io.EOF
}

// CheckFinish consumes the rest of input.
func (r *Reader) CheckFinish() error {
	_, err := r.scanUntil(func(op opcode, eof bool) bool {
		return eof
	})
	return err
}

func (r *Reader) redo() {
	r.s.step(&r.s, 0)
	r.pos++
}

func (r *Reader) scanUntilDepth(depth int) (opcode, error) {
	return r.scanUntil(func(op opcode, eof bool) bool {
		return len(r.s.parseState) == depth
	})
}

func (r *Reader) scanWhile(expectedOp opcode) (opcode, error) {
	return r.scanUntil(func(op opcode, eof bool) bool {
		return expectedOp != op
	})
}

// scanUntil runs scanner on input until stop() returns true.
//
// Reader is left in a state pointing at the byte that caused stopping.
//
// When the end of input is reached, stop() is queried for the last time. It could either accept input termination,
// or refuse it. It that case, the error is raised.
func (r *Reader) scanUntil(stop func(op opcode, eof bool) bool) (opcode, error) {
	for {
		if r.pos == r.end {
			err := r.readMore()
			if err == io.EOF {
				op := r.s.eof()
				if op == scanError {
					return scanError, r.s.err
				} else if stop(op, true) {
					return op, nil
				} else {
					return scanError, err
				}
			} else if err != nil {
				return scanError, err
			}
		}

		var op opcode
		if !r.s.redo && r.s.binaryLength != 0 { // Bypass stateBinaryCountdown, skip bytes directly.
			skipBytes := r.s.binaryLength
			if int64(r.end-r.pos) < skipBytes {
				skipBytes = int64(r.end - r.pos)
			}

			r.s.binaryLength -= skipBytes
			r.pos += int(skipBytes) - 1

			if r.s.binaryLength == 0 {
				r.s.step = stateEndValue
			}

			op = scanContinue
		} else {
			op = r.s.step(&r.s, r.buf[r.pos])
		}

		if op == scanError {
			return scanError, r.s.err
		} else if stop(op, false) {
			r.s.undo(op)
			return op, nil
		}

		r.pos++
	}
}
