package ypath

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

func isHex(b byte) bool {
	return ('a' <= b && b <= 'f') || ('A' <= b && b <= 'F') || ('0' <= b && b <= '9')
}

func isSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}

	return false
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}

type stateFn func(*lexer) stateFn

type lexer struct {
	pos   int
	input []byte

	err     error
	attrs   []byte
	tokens  [][]byte
	columns []string
	ranges  []Range
}

func (l *lexer) consume(n int) {
	l.pos += n
	l.input = l.input[n:]
}

func (l *lexer) run(input []byte) error {
	l.input = input
	state := stateMaybeAttrs
	for state != nil {
		state = state(l)
	}
	return l.err
}

func (l *lexer) error(msg string) {
	l.err = fmt.Errorf("ypath: %s", msg)
}

func stateMaybeAttrs(l *lexer) stateFn {
	var n int
	n, l.err = yson.SliceYPathAttrs(l.input)
	if l.err != nil {
		l.err = xerrors.Errorf("ypath: invalid attribute syntax: %v", l.err)
		return nil
	}

	l.attrs = l.input[:n]
	l.consume(n)

	return statePath
}

func statePath(l *lexer) stateFn {
	if len(l.input) == 0 {
		return nil
	}

	if l.input[0] == '{' {
		return stateColumns
	}

	if l.input[0] == '[' {
		return stateRanges
	}

	var i int
loop:
	for i = 0; i < len(l.input); i++ {
		switch l.input[i] {
		case '\\':
			if i+1 == len(l.input) {
				l.error("unterminated escape sequence")
				return nil
			}

			switch l.input[i+1] {
			case 'x':
				if i+3 >= len(l.input) {
					l.error("unterminated escape sequence")
					return nil
				}

				if !isHex(l.input[i+2]) || !isHex(l.input[i+3]) {
					l.error("invalid escape sequence")
					return nil
				}

				i += 3
				continue

			case '/', '@', '&', '*', '\\', '[', '{':
				i++
				continue

			default:
				l.error("invalid escape sequence")
				return nil
			}

		case '/':
			if i != 0 {
				break loop
			}

		case '{', '[':
			break loop

		default:
		}
	}

	l.tokens = append(l.tokens, l.input[:i])
	l.consume(i)

	return statePath
}

func stateColumns(l *lexer) stateFn {
	if len(l.input) == 0 {
		return nil
	}

	if l.input[0] != '{' {
		l.error("missing columns")
		return nil
	}

	i := 1
	l.columns = []string{}

loop:
	for {
		if i >= len(l.input) {
			l.error("unterminated column list")
			return nil
		}

		if isSpace(l.input[i]) {
			i++
			continue
		}

		if l.input[i] == '}' {
			i++
			break
		}

		var column []byte
		var n int
		if n, l.err = yson.SliceYPathString(l.input[i:], &column); l.err != nil {
			return nil
		}

		l.columns = append(l.columns, string(column))
		i += n

		if i >= len(l.input) {
			l.error("unterminated column list")
			return nil
		}

		switch l.input[i] {
		case ',':
			i++
			continue loop
		case '}':
			i++
			break loop

		default:
			l.error("malformed column list")
			return nil
		}
	}

	l.consume(i)
	if len(l.input) == 0 {
		return nil
	}

	return stateRanges
}

func stateRanges(l *lexer) stateFn {
	var i int
	skipSpace := func() {
		for {
			if i >= len(l.input) {
				break
			}

			if !isSpace(l.input[i]) {
				break
			}

			i++
		}
	}

	skipSpace()
	l.consume(i)

	if len(l.input) == 0 {
		return nil
	}

	if l.input[0] != '[' {
		l.error("syntax error")
		return nil
	}

	i = 1
	l.ranges = []Range{}

	readLimit := func() *ReadLimit {
		switch l.input[i] {
		case '#':
			start := i + 1
			end := start
			for end < len(l.input) && isDigit(l.input[end]) {
				end++
			}

			if start == end {
				l.error("invalid row index")
				return nil
			}

			rowIndex, err := strconv.ParseInt(string(l.input[start:end]), 10, 64)
			if err != nil {
				l.err = xerrors.Errorf("ypath: invalid row index: %v", err)
			}

			i = end
			return &ReadLimit{RowIndex: &rowIndex}

		case '(':
			i++
			composite := []any{}

			for {
				skipSpace()
				if i >= len(l.input) {
					l.error("eof in composite key")
					return nil
				}

				if l.input[i] == ')' {
					i++
					return &ReadLimit{Key: composite}
				}

				var value any
				n, err := yson.SliceYPathValue(l.input[i:], &value)
				if err != nil {
					l.err = xerrors.Errorf("ypath: invalid key: %v", err)
					return nil
				}

				composite = append(composite, value)
				i += n

				skipSpace()
				if i >= len(l.input) {
					l.error("eof in composite key")
					return nil
				}

				if l.input[i] == ',' {
					i++
				} else if l.input[i] != ')' {
					l.error("invalid composite key")
					return nil
				}
			}

		default:
			var value any
			n, err := yson.SliceYPathValue(l.input[i:], &value)
			if err != nil {
				l.err = xerrors.Errorf("ypath: invalid key: %v", err)
				return nil
			}

			i += n
			key := Key(value)
			return &key
		}
	}

	var r Range
	var colon bool

loop:
	for {
		skipSpace()

		if i >= len(l.input) {
			l.error("unterminated ranges list")
			return nil
		}

		switch l.input[i] {
		case ']':
			i++
			l.ranges = append(l.ranges, r)
			break loop

		case ':':
			i++
			r.Lower = r.Exact
			r.Exact = nil
			colon = true
			continue loop

		case ',':
			i++
			l.ranges = append(l.ranges, r)
			r = Range{}
			colon = false
			continue loop

		default:
			if colon {
				r.Upper = readLimit()
			} else {
				r.Exact = readLimit()
			}

			if l.err != nil {
				return nil
			}
		}
	}

	l.consume(i)
	return stateSkipSpace
}

func stateSkipSpace(l *lexer) stateFn {
	for i := 0; i < len(l.input); i++ {
		if !isSpace(l.input[i]) {
			l.error("unexpected symbol after suffix")
			return nil
		}
	}

	return nil
}

func SplitTokens(path string) (tokens []string, err error) {
	var l lexer
	if err = l.run([]byte(path)); err != nil {
		return
	}

	for _, t := range l.tokens {
		tokens = append(tokens, string(t))
	}

	return
}

// Split splits path into parent and a child component.
//
//	//home/prime/table -> //home/prime /table
//	#a-b-c-d/@attr     -> #a-b-c-d     /@attr
//
// If path refers to cypress root or object id, Split sets parent to path and child to an empty string.
//
// Path attributes, column selectors and range selectors are stripped from provided path.
func Split(path Path) (parent Path, child string, err error) {
	var tokens []string
	tokens, err = SplitTokens(path.String())
	if err != nil {
		return
	}

	if len(tokens) > 1 {
		child = tokens[len(tokens)-1]
		tokens = tokens[:len(tokens)-1]
	}

	parent = Path(strings.Join(tokens, ""))
	return
}

// PathsUpToRoot returns list of paths referring to the nodes on the path to the root.
//
// Starting node and root are included.
func PathsUpToRoot(path Path) ([]Path, error) {
	tokens, err := SplitTokens(string(path))
	if err != nil {
		return nil, err
	}

	var paths []Path
	for i := len(tokens); i != 0; i-- {
		paths = append(paths, Path(strings.Join(tokens[:i], "")))
	}
	return paths, nil
}

// Parse parses prefix and suffix of the path in simple form.
func Parse(path string) (p *Rich, err error) {
	var l lexer
	if err = l.run([]byte(path)); err != nil {
		return
	}

	if len(l.tokens) == 0 {
		err = xerrors.New("ypath: empty path")
		return
	}

	ypath := Path(bytes.Join(l.tokens, nil))

	p = &Rich{}

	if len(l.attrs) != 0 {
		attrs := l.attrs
		attrs = append(attrs, '"', '"')
		if err = yson.Unmarshal(attrs, p); err != nil {
			err = xerrors.Errorf("ypath: invalid attribute syntax: %v", err)
			return
		}
	}

	p.Path = ypath
	p.Columns = l.columns
	p.Ranges = l.ranges
	return
}
