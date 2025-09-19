package otelzap

import (
	"unicode/utf8"

	"go.uber.org/zap/buffer"
)

const (
	_hex = "0123456789abcdef"

	otelKey    = "key"
	otelValue  = "value"
	otelValues = "values"

	otelBoolValue   = "boolValue"
	otelIntValue    = "intValue"
	otelDoubleValue = "doubleValue"
	otelStringValue = "stringValue"
	otelBytesValue  = "bytesValue"
	otelArrayValue  = "arrayValue"
	otelObjectValue = "kvlistValue"
)

func safeWriteStringLike[S []byte | string](
	appendTo func(*buffer.Buffer, S),
	decodeRune func(S) (rune, int),
	buf *buffer.Buffer,
	s S,
) {
	last := 0
	for i := 0; i < len(s); {
		if s[i] >= utf8.RuneSelf {
			r, size := decodeRune(s[i:])
			if r != utf8.RuneError || size != 1 {
				i += size
				continue
			}
			appendTo(buf, s[last:i])
			buf.AppendString(`\ufffd`)
			i++
			last = i
		} else {
			if s[i] >= 0x20 && s[i] != '\\' && s[i] != '"' {
				i++
				continue
			}
			appendTo(buf, s[last:i])
			switch s[i] {
			case '\\', '"':
				buf.AppendByte('\\')
				buf.AppendByte(s[i])
			case '\n':
				buf.AppendByte('\\')
				buf.AppendByte('n')
			case '\r':
				buf.AppendByte('\\')
				buf.AppendByte('r')
			case '\t':
				buf.AppendByte('\\')
				buf.AppendByte('t')
			default:
				buf.AppendString(`\u00`)
				buf.AppendByte(_hex[s[i]>>4])
				buf.AppendByte(_hex[s[i]&0xF])
			}
			i++
			last = i
		}
	}
	appendTo(buf, s[last:])
}

func safeWriteString(buf *buffer.Buffer, s string) {
	safeWriteStringLike(
		(*buffer.Buffer).AppendString,
		utf8.DecodeRuneInString,
		buf,
		s,
	)
}

func safeWriteByteString(buf *buffer.Buffer, s []byte) {
	safeWriteStringLike(
		(*buffer.Buffer).AppendBytes,
		utf8.DecodeRune,
		buf,
		s,
	)
}

func writeElementSeparator(buf *buffer.Buffer) {
	last := buf.Len() - 1
	if last < 0 {
		return
	}
	switch buf.Bytes()[last] {
	case '{', '[', ':', ',', ' ':
		return
	default:
		buf.AppendByte(',')
		buf.AppendByte(' ')
	}
}

func writeString(buf *buffer.Buffer, s string) {
	buf.AppendByte('"')
	safeWriteString(buf, s)
	buf.AppendByte('"')
}

func writeKey(buf *buffer.Buffer, key string) {
	writeElementSeparator(buf)
	writeString(buf, key)
	buf.AppendByte(':')
	buf.AppendByte(' ')
}
