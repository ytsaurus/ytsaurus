package yson

import (
	"encoding/binary"
)

const maxYSONDepth = 256

type opcode byte

const (
	scanContinue     opcode = 'c' // uninteresting byte
	scanBeginLiteral opcode = 'o' // begin value literal
	scanBeginKey     opcode = 'k' // begin key of attributes or map
	scanBeginAttrs   opcode = '<' // begin attributes
	scanEndAttrs     opcode = '>' // end attributes
	scanBeginMap     opcode = '{' // begin map
	scanEndMap       opcode = '}' // end map
	scanBeginList    opcode = '[' // begin list
	scanEndList      opcode = ']' // end list
	scanSkipSpace    opcode = 's' // space byte

	scanEnd   opcode = 'e'
	scanError opcode = 'x'
)

type parseState int

const (
	parseMapKey parseState = iota
	parseMapValue
	parseListValue
	parseAttrKey
	parseAttrValue
)

type literalKind byte

const (
	literalEntity     literalKind = '#'
	literalInt        literalKind = 'i'
	literalUint       literalKind = 'u'
	literalBool       literalKind = 'b'
	literalFloat      literalKind = 'f'
	literalIdentifier literalKind = 'a'
	literalString     literalKind = '"'

	literalBinaryString literalKind = '1'
	literalBinaryInt    literalKind = '2'
	literalBinaryUint   literalKind = '6'
	literalBinaryFalse  literalKind = '4'
	literalBinaryTrue   literalKind = '5'
	literalBinaryFloat  literalKind = '3'
)

const (
	binaryString byte = 1
	binaryInt    byte = 2
	binaryFloat  byte = 3
	binaryFalse  byte = 4
	binaryTrue   byte = 5
	binaryUint   byte = 6
)

// scanner is a state machine capable of parsing YSON input.
//
// scanner consumes one byte at a time and returns interesting events
// (such as start/end of literal value) to the user.
//
// See "Lexical Scanning in Go - Rob Pike" https://www.youtube.com/watch?v=HxaD_trXwRE
type scanner struct {
	// top level scanner state is encoded as function
	step        func(*scanner, byte) opcode
	stateEndTop func(*scanner, byte) opcode

	parseState   []parseState                // stack or open maps, lists and attribute maps
	endTop       bool                        // reached end of tol-level value
	fragment     bool                        // decoded stream is fragment
	binaryLength int64                       // length of currently processed binary chunk
	stringLength [binary.MaxVarintLen64]byte // scratch buffer for decoding string length
	inVarint     bool                        // whether we are inside varint or not
	varintSize   int                         // current varint size up to this point
	lastLiteral  literalKind                 // type of the last decoded literal

	err error

	redo      bool
	redoCode  opcode
	redoState func(*scanner, byte) opcode
}

func (s *scanner) reset(kind StreamKind) {
	s.step = stateBeginValue
	s.stateEndTop = stateEndTop
	s.parseState = s.parseState[0:0]

	s.err = nil
	s.redo = false

	switch kind {
	case StreamNode:
	case StreamMapFragment:
		s.fragment = true
		s.endTop = true
		s.pushParseState(parseMapKey)
	case StreamListFragment:
		s.fragment = true
		s.endTop = true
		s.pushParseState(parseListValue)
	}
}

func (s *scanner) eof() opcode {
	if s.err != nil {
		return scanError
	}

	if s.endTop {
		return scanEnd
	}

	if s.binaryLength != 0 || s.inVarint {
		s.err = &SyntaxError{"unexpected end of YSON input"}
		return scanError
	}

	op := s.step(s, ' ')
	if op == scanError {
		s.err = &SyntaxError{"unexpected end of YSON input"}
		return scanError
	} else if s.endTop {
		return scanEnd
	}

	if s.err == nil {
		s.err = &SyntaxError{"unexpected end of YSON input"}
	}
	return scanError
}

// Provides a means of undoing a single call to step().
func (s *scanner) undo(scanCode opcode) {
	if s.redo {
		panic("yson: invalid use of scanner")
	}
	s.redoCode = scanCode
	s.redoState = s.step
	s.step = stateRedo
	s.redo = true
}

func (s *scanner) error(_ byte, where string) opcode {
	s.err = &SyntaxError{where}
	s.step = stateError
	return scanError
}

func stateRedo(s *scanner, c byte) opcode {
	s.redo = false
	s.step = s.redoState
	return s.redoCode
}

func stateError(s *scanner, c byte) opcode {
	return scanError
}

func isSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}

	return false
}

func isIdentifierBegin(c byte) bool {
	if 'a' <= c && c <= 'z' {
		return true
	}
	if 'A' <= c && c <= 'Z' {
		return true
	}
	if c == '_' {
		return true
	}

	return false
}

func isIdentifierContinuation(c byte) bool {
	if 'a' <= c && c <= 'z' {
		return true
	}
	if 'A' <= c && c <= 'Z' {
		return true
	}
	if '0' <= c && c <= '9' {
		return true
	}
	if c == '_' || c == '.' || c == '-' {
		return true
	}

	return false
}

func stateBeginKey(s *scanner, c byte) opcode {
	switch {
	case c == '"':
		s.step = stateString
		return scanBeginKey
	case c == binaryString:
		s.inVarint = true
		s.varintSize = 0
		s.step = stateDecodeStringLength
		s.lastLiteral = literalBinaryString
		return scanBeginKey
	case isIdentifierBegin(c):
		s.step = stateIdentifier
		return scanBeginKey
	}

	return s.error(c, "looking for beginning of key")
}

func stateBeginValue(s *scanner, c byte) opcode {
	if isSpace(c) {
		return scanSkipSpace
	}

	if c == '<' {
		s.endTop = false
		s.step = stateBeginAttrs
		s.pushParseState(parseAttrKey)
		return scanBeginAttrs
	}

	return stateBeginValueNoAttrs(s, c)
}

func stateBeginValueNoAttrs(s *scanner, c byte) opcode {
	if isSpace(c) {
		return scanSkipSpace
	}

	s.endTop = false
	switch c {
	case '{':
		s.step = stateBeginMap
		s.pushParseState(parseMapKey)
		return scanBeginMap
	case '[':
		s.step = stateBeginList
		s.pushParseState(parseListValue)
		return scanBeginList
	case '"':
		s.step = stateString
		return scanBeginLiteral
	case '%':
		s.step = stateStartKeyword
		return scanBeginLiteral
	case '#':
		s.lastLiteral = literalEntity
		s.step = stateEndValue
		return scanBeginLiteral

	case '+':
		s.step = state1
		return scanBeginLiteral
	case '-':
		s.step = state1
		return scanBeginLiteral
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		s.step = state1
		return scanBeginLiteral
	case binaryString:
		s.inVarint = true
		s.varintSize = 0
		s.step = stateDecodeStringLength
		s.lastLiteral = literalBinaryString
		return scanBeginLiteral
	case binaryInt:
		s.inVarint = true
		s.varintSize = 0
		s.step = stateSkipVarint
		s.lastLiteral = literalBinaryInt
		return scanBeginLiteral
	case binaryFloat:
		s.binaryLength = 8
		s.step = stateBinaryCountdown
		s.lastLiteral = literalBinaryFloat
		return scanBeginLiteral
	case binaryTrue:
		s.lastLiteral = literalBinaryTrue
		s.step = stateEndValue
		return scanBeginLiteral
	case binaryFalse:
		s.lastLiteral = literalBinaryFalse
		s.step = stateEndValue
		return scanBeginLiteral
	case binaryUint:
		s.inVarint = true
		s.varintSize = 0
		s.step = stateSkipVarint
		s.lastLiteral = literalBinaryUint
		return scanBeginLiteral
	}

	if isIdentifierBegin(c) {
		s.step = stateIdentifier
		return scanBeginLiteral
	}

	return s.error(c, "looking for beginning of value")
}

func stateBinaryCountdown(s *scanner, c byte) opcode {
	s.binaryLength--

	if s.binaryLength == 0 {
		s.step = stateEndValue
	}

	return scanContinue
}

func stateString(s *scanner, c byte) opcode {
	if c == '"' {
		s.lastLiteral = literalString
		s.step = stateEndValue
		return scanContinue
	}

	if c == '\\' {
		s.step = stateStringSlash
		return scanContinue
	}

	return scanContinue
}

func stateStringSlash(s *scanner, c byte) opcode {
	s.step = stateString
	return scanContinue
}

func stateIdentifier(s *scanner, c byte) opcode {
	if isIdentifierContinuation(c) {
		return scanContinue
	}

	s.lastLiteral = literalIdentifier
	return stateEndValue(s, c)
}

func stateStartKeyword(s *scanner, c byte) opcode {
	switch c {
	case 't':
		s.step = stateT
		return scanContinue
	case 'f':
		s.step = stateF
		return scanContinue
	case 'n':
		s.step = stateN
		return scanContinue
	case 'i':
		s.step = stateI
		return scanContinue
	case '+', '-':
		s.step = statePreI
		return scanContinue
	}

	return s.error(c, "in literal boolean (expecting 't' or 'f')")
}

func statePreI(s *scanner, c byte) opcode {
	if c == 'i' {
		s.step = stateI
		return scanContinue
	}

	return s.error(c, "in literal inf (expecting 'i')")
}

func stateI(s *scanner, c byte) opcode {
	if c == 'n' {
		s.step = stateIN
		return scanContinue
	}

	return s.error(c, "in literal inf (expecting 'n')")
}

func stateIN(s *scanner, c byte) opcode {
	if c == 'f' {
		s.lastLiteral = literalFloat
		s.step = stateEndValue
		return scanContinue
	}

	return s.error(c, "in literal inf (expecting 'f')")
}

func stateN(s *scanner, c byte) opcode {
	if c == 'a' {
		s.step = stateNA
		return scanContinue
	}

	return s.error(c, "in literal nan (expecting 'a')")
}

func stateNA(s *scanner, c byte) opcode {
	if c == 'n' {
		s.lastLiteral = literalFloat
		s.step = stateEndValue
		return scanContinue
	}

	return s.error(c, "in literal nan (expecting 'n')")
}

func state1(s *scanner, c byte) opcode {
	if '0' <= c && c <= '9' {
		return scanContinue
	}

	if c == 'u' {
		s.lastLiteral = literalUint
		s.step = stateEndValue
		return scanContinue
	}

	if c == '.' {
		s.step = state1dot
		return scanContinue
	}

	if c == 'e' || c == 'E' {
		s.step = state1e
		return scanContinue
	}

	s.lastLiteral = literalInt
	return stateEndValue(s, c)
}

func state1e(s *scanner, c byte) opcode {
	if c == '+' || c == '-' {
		s.step = state1eSign
		return scanContinue
	}

	if '0' <= c && c <= '9' {
		s.step = state1e1
		return scanContinue
	}

	return s.error(c, "in literal float (expecting number after 'e' or 'E')")
}

func state1eSign(s *scanner, c byte) opcode {
	if '0' <= c && c <= '9' {
		s.step = state1e1
		return scanContinue
	}

	return s.error(c, "in literal float (expecting number after 'e' or 'E')")
}

func state1e1(s *scanner, c byte) opcode {
	if '0' <= c && c <= '9' {
		return scanContinue
	}

	s.lastLiteral = literalFloat
	return stateEndValue(s, c)
}

func state1dot(s *scanner, c byte) opcode {
	if '0' <= c && c <= '9' {
		return scanContinue
	}

	if c == 'e' || c == 'E' {
		s.step = state1e
		return scanContinue
	}

	s.lastLiteral = literalFloat
	return stateEndValue(s, c)
}

func stateT(s *scanner, c byte) opcode {
	if c == 'r' {
		s.step = stateTr
		return scanContinue
	}

	return s.error(c, "in literal true (expecting 'r')")
}

func stateTr(s *scanner, c byte) opcode {
	if c == 'u' {
		s.step = stateTru
		return scanContinue
	}

	return s.error(c, "in literal true (expecting 'u')")
}

func stateTru(s *scanner, c byte) opcode {
	if c == 'e' {
		s.lastLiteral = literalBool
		s.step = stateEndValue
		return scanContinue
	}

	return s.error(c, "in literal true (expecting 'e')")
}

func stateF(s *scanner, c byte) opcode {
	if c == 'a' {
		s.step = stateFa
		return scanContinue
	}

	return s.error(c, "in literal false (expecting 'a')")
}

func stateFa(s *scanner, c byte) opcode {
	if c == 'l' {
		s.step = stateFal
		return scanContinue
	}

	return s.error(c, "in literal false (expecting 'l')")
}

func stateFal(s *scanner, c byte) opcode {
	if c == 's' {
		s.step = stateFals
		return scanContinue
	}

	return s.error(c, "in literal false (expecting 's')")
}

func stateFals(s *scanner, c byte) opcode {
	if c == 'e' {
		s.lastLiteral = literalBool
		s.step = stateEndValue
		return scanContinue
	}

	return s.error(c, "in literal false (expecting 'e')")
}

func (s *scanner) popParseState() {
	s.parseState = s.parseState[:len(s.parseState)-1]
}

func (s *scanner) pushParseState(state parseState) {
	s.parseState = append(s.parseState, state)
	if len(s.parseState) > maxYSONDepth {
		s.step = stateError
		s.err = &SyntaxError{"nesting depth limit reached"}
	}
}

func stateBeginMap(s *scanner, c byte) opcode {
	if isSpace(c) {
		return scanSkipSpace
	}
	s.endTop = false

	if c == '}' {
		s.step = stateEndValue
		s.popParseState()
		return scanEndMap
	}

	return stateBeginKey(s, c)
}

func stateBeginList(s *scanner, c byte) opcode {
	if isSpace(c) {
		return scanSkipSpace
	}
	s.endTop = false

	if c == ']' {
		s.step = stateEndValue
		s.popParseState()
		return scanEndList
	}

	s.step = stateBeginValue
	return stateBeginValue(s, c)
}

func stateBeginAttrs(s *scanner, c byte) opcode {
	if isSpace(c) {
		return scanSkipSpace
	}

	if c == '>' {
		s.step = stateBeginValueNoAttrs
		s.popParseState()
		return scanEndAttrs
	}

	return stateBeginKey(s, c)
}

func stateDecodeStringLength(s *scanner, c byte) opcode {
	s.stringLength[s.varintSize] = c
	s.varintSize++

	if c&0x80 == 0 {
		var n int
		s.binaryLength, n = binary.Varint(s.stringLength[0:s.varintSize])
		if n != s.varintSize || s.binaryLength < 0 {
			return s.error(c, "in binary string length")
		}

		s.inVarint = false
		if s.binaryLength > 0 {
			s.step = stateBinaryCountdown
		} else {
			s.step = stateEndValue
		}
		return scanContinue
	}

	if s.varintSize >= binary.MaxVarintLen64 {
		return s.error(c, "in varint (size > 10)")
	}
	return scanContinue
}

func stateSkipVarint(s *scanner, c byte) opcode {
	if c&0x80 == 0 {
		s.inVarint = false
		s.step = stateEndValue
		return scanContinue
	}

	s.varintSize++
	if s.varintSize > binary.MaxVarintLen64 {
		return s.error(c, "in varint (size > 10)")
	}
	return scanContinue
}

func stateEndValue(s *scanner, c byte) opcode {
	n := len(s.parseState)
	if n == 0 {
		s.endTop = true
		s.step = s.stateEndTop
		return s.stateEndTop(s, c)
	} else if s.fragment && n == 1 && s.parseState[n-1] != parseMapKey {
		s.endTop = true
	}

	ps := s.parseState[n-1]
	if ps == parseMapKey || ps == parseAttrKey {
		if s.lastLiteral != literalString && s.lastLiteral != literalIdentifier && s.lastLiteral != literalBinaryString {
			return s.error(c, "in map key (expecting string literal)")
		}
	}

	if isSpace(c) {
		s.step = stateEndValue
		return scanSkipSpace
	}

	switch ps {
	case parseMapKey:
		if c == '=' {
			s.parseState[n-1] = parseMapValue
			s.step = stateBeginValue
			return scanSkipSpace
		}
		return s.error(c, "after map key")
	case parseMapValue:
		if c == ';' {
			s.parseState[n-1] = parseMapKey
			s.step = stateBeginMap
			return scanSkipSpace
		}
		if c == '}' {
			if len(s.parseState) == 1 && s.fragment {
				return s.error(c, "in map fragment")
			}

			s.popParseState()
			s.step = stateEndValue
			return scanEndMap
		}
		return s.error(c, "after map value")
	case parseListValue:
		if c == ';' {
			s.step = stateBeginList
			return scanSkipSpace
		}
		if c == ']' {
			if len(s.parseState) == 1 && s.fragment {
				return s.error(c, "in map fragment")
			}

			s.popParseState()
			s.step = stateEndValue
			return scanEndList
		}
		return s.error(c, "after list element")
	case parseAttrKey:
		if c == '=' {
			s.parseState[n-1] = parseAttrValue
			s.step = stateBeginValue
			return scanSkipSpace
		}
		return s.error(c, "after attribute key")
	case parseAttrValue:
		if c == ';' {
			s.parseState[n-1] = parseAttrKey
			s.step = stateBeginAttrs
			return scanSkipSpace
		}
		if c == '>' {
			s.popParseState()
			s.step = stateBeginValueNoAttrs
			return scanEndAttrs
		}
		return s.error(c, "after attribute value")
	}

	panic("unreachable")
}

func stateEndYPathLiteral(s *scanner, c byte) opcode {
	if !isSpace(c) && c != ']' && c != ',' && c != ')' && c != '}' {
		return s.error(c, "after top-level value")
	}

	s.step = stateEndTop
	return scanEnd
}

func stateEndTop(s *scanner, c byte) opcode {
	if !isSpace(c) {
		return s.error(c, "after top-level value")
	}

	return scanEnd
}
