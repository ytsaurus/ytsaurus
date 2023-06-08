package yson

import (
	"fmt"

	"golang.org/x/xerrors"
)

// SliceYPathAttrs splits ypath into attributes and path.
//
// SliceYPathAttrs does not validate that path is a correct ypath.
func SliceYPathAttrs(ypath []byte) (n int, err error) {
	var s scanner
	s.reset(StreamNode)

	var hasAttrs bool
	var hasSpace bool
	var depth int
	for i := 0; i < len(ypath); i++ {
		c := ypath[i]
		if depth == 0 && (c == '#' || c == '/') {
			if !hasAttrs && hasSpace {
				return 0, fmt.Errorf("ypath: unexpected space in the beginning")
			}

			return i, nil
		}

		op := s.step(&s, ypath[i])
		switch op {
		case scanError:
			err = xerrors.New("ypath: invalid format")
			return

		case scanBeginAttrs:
			hasAttrs = true
			depth++

		case scanEndAttrs, scanEndList, scanEndMap:
			depth--

		case scanSkipSpace:
			hasSpace = true

		default:
			if depth == 0 {
				err = xerrors.New("ypath: invalid format")
				return
			}

			if op == scanBeginList || op == scanBeginMap {
				depth++
			}
		}
	}

	err = xerrors.New("ypath: invalid format")
	return
}

func SliceYPathString(ypath []byte, str *[]byte) (n int, err error) {
	var value any
	n, err = SliceYPathValue(ypath, &value)
	if err != nil {
		return
	}

	if s, ok := value.([]byte); ok {
		*str = s
	} else {
		err = xerrors.New("ypath: type")
	}

	return
}

// SliceYPathValue decodes single YSON value from beginning of ypath.
//
// Returned value might point into the buffer.
func SliceYPathValue(ypath []byte, value *any) (n int, err error) {
	var r Reader
	r.s.reset(StreamNode)
	r.s.stateEndTop = stateEndYPathLiteral
	r.buf = ypath

	for i := 0; i < len(ypath); i++ {
		c := ypath[i]

		op := r.s.step(&r.s, c)
		switch op {
		case scanBeginLiteral:
			r.keep = i
		case scanEnd:
			r.pos = i
			if err = r.decodeLastLiteral(); err != nil {
				return
			}

			switch r.currentType {
			case TypeEntity:
			case TypeInt64:
				*value = r.currentInt
			case TypeUint64:
				*value = r.currentUint
			case TypeBool:
				*value = r.currentBool
			case TypeFloat64:
				*value = r.currentFloat
			case TypeString:
				*value = r.currentString
			default:
				err = xerrors.New("ypath: invalid format")
			}

			n = i
			return

		case scanSkipSpace, scanContinue:
		default:
			err = xerrors.New("ypath: invalid format")
			return
		}
	}

	err = xerrors.New("ypath: invalid format")
	return
}
