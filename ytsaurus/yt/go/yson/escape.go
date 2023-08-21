package yson

import "unicode/utf8"

func hexDigit(value int) byte {
	if value < 10 {
		return '0' + byte(value)
	}

	return 'A' + byte(value) - 10
}

func octDigit(value int) byte {
	return '0' + byte(value)
}

func isPrintable(b byte) bool {
	return b >= 32 && b <= 126
}

func isHexDigit(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'A' && b <= 'F') || (b >= 'a' && b <= 'f')
}

func fromHex(b byte) byte {
	switch {
	case b >= '0' && b <= '9':
		return b - '0'
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	}

	panic("not a hex")
}

func fromOct(b byte) byte {
	if b >= '0' && b < '8' {
		return b - '0'
	}

	panic("not an oct")
}

func isOctDigit(b byte) bool {
	return b >= '0' && b <= '7'
}

func isDecDigit(b byte) bool {
	return '0' <= b && b <= '9'
}

func isAlpha(b byte) bool {
	return ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z')
}

func needEscape(b []byte) bool {
	if len(b) == 0 {
		return true
	}

	if !isAlpha(b[0]) {
		return true
	}

	for i := 0; i < len(b); i++ {
		if !isAlpha(b[i]) && !isDecDigit(b[i]) {
			return true
		}
	}

	return false
}

func needEscapeString(s string) bool {
	return needEscape([]byte(s))
}

func allHex(b []byte) bool {
	for _, bb := range b {
		if !isHexDigit(bb) {
			return false
		}
	}

	return true
}

func escapeByte(b, nextByte byte, nextRune rune) (escaped [4]byte, n int, encodedAsRune bool) {
	switch {
	case b == '"':
		escaped[0] = '\\'
		escaped[1] = '"'
		n = 2

	case b == '\\':
		escaped[0] = '\\'
		escaped[1] = '\\'
		n = 2

	case isPrintable(b):
		escaped[0] = b
		n = 1

	case b == '\r':
		escaped[0] = '\\'
		escaped[1] = 'r'
		n = 2

	case b == '\n':
		escaped[0] = '\\'
		escaped[1] = 'n'
		n = 2

	case b == '\t':
		escaped[0] = '\\'
		escaped[1] = 't'
		n = 2

	case b < 8 && !isOctDigit(nextByte):
		escaped[0] = '\\'
		escaped[1] = octDigit(int(b))
		n = 2

	case isHexDigit(nextByte):
		escaped[0] = '\\'
		escaped[1] = octDigit(int(b&0300) >> 6)
		escaped[2] = octDigit(int(b&0070) >> 3)
		escaped[3] = octDigit(int(b&0007) >> 0)
		n = 4

	case nextRune != utf8.RuneError:
		n = utf8.EncodeRune(escaped[:], nextRune)
		encodedAsRune = true

	case !isHexDigit(nextByte):
		escaped[0] = '\\'
		escaped[1] = 'x'
		escaped[2] = hexDigit(int(b&0xF0) >> 4)
		escaped[3] = hexDigit(int(b&0x0F) >> 0)
		n = 4

	default:
		escaped[0] = '\\'
		escaped[1] = octDigit(int(b&0300) >> 6)
		escaped[2] = octDigit(int(b&0070) >> 3)
		escaped[3] = octDigit(int(b&0007) >> 0)
		n = 4
	}

	return
}

func escapeC(b []byte) (escaped []byte) {
	for i, w := 0, 0; i < len(b); i += w {
		var nextByte byte
		if i+1 < len(b) {
			nextByte = b[i+1]
		}

		nextRune, runeWidth := utf8.DecodeRune(b[i:])

		buf, n, encodedAsRune := escapeByte(b[i], nextByte, nextRune)
		escaped = append(escaped, buf[:n]...)

		w = 1
		if encodedAsRune {
			w = runeWidth
		}
	}

	return
}

func unescapeC(b []byte) (unescaped []byte) {
	unescaped = []byte{}
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			unescaped = append(unescaped, b[i])
			continue
		}

		if i+1 == len(b) {
			return
		}

		i++
		switch b[i] {
		default:
			unescaped = append(unescaped, b[i])

		case 'a':
			unescaped = append(unescaped, '\a')
		case 'b':
			unescaped = append(unescaped, '\b')
		case 'f':
			unescaped = append(unescaped, '\f')
		case 'n':
			unescaped = append(unescaped, '\n')
		case 'r':
			unescaped = append(unescaped, '\r')
		case 't':
			unescaped = append(unescaped, '\t')
		case 'v':
			unescaped = append(unescaped, '\v')

		case 'u':
			if i+4 < len(b) && allHex(b[i+1:i+5]) {
				var r rune
				for j := 0; j < 4; j++ {
					r = r<<4 + rune(fromHex(b[i+j+1]))
				}

				var buf [16]byte
				n := utf8.EncodeRune(buf[:], r)
				unescaped = append(unescaped, buf[:n]...)

				i += 4
			} else {
				unescaped = append(unescaped, b[i])
			}

		case 'U':
			if i+8 < len(b) && allHex(b[i+1:i+9]) {
				var r rune
				for j := 0; j < 8; j++ {
					r = r<<4 + rune(fromHex(b[i+1+j]))
				}

				var buf [16]byte
				n := utf8.EncodeRune(buf[:], r)
				unescaped = append(unescaped, buf[:n]...)

				i += 8
			} else {
				unescaped = append(unescaped, b[i])
			}

		case 'x':
			if i+2 < len(b) && allHex(b[i+1:i+3]) {
				c := fromHex(b[i+1])
				c = c<<4 + fromHex(b[i+2])
				i += 2
				unescaped = append(unescaped, c)
			} else {
				unescaped = append(unescaped, b[i])
			}

		case '0', '1', '2', '3', '4', '5', '6', '7':
			max := 2
			if b[i] == '0' || b[i] == '1' || b[i] == '2' || b[i] == '3' {
				max = 3
			}

			c := fromOct(b[i])

			for j := 2; j <= max && i+1 < len(b); j++ {
				if !isOctDigit(b[i+1]) {
					break
				}

				i++
				c = (c << 3) + fromOct(b[i])
			}

			unescaped = append(unescaped, c)
		}
	}

	return
}
