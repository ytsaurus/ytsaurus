package yson

import (
	"golang.org/x/xerrors"
)

// SliceYPath splits ypath into attributes and path.
//
// SliceYPath does not validate that path is a correct ypath.
func SliceYPath(ypath []byte) (attrs, path []byte, err error) {
	var s scanner
	s.reset(StreamNode)

	var depth int
	for i := 0; i < len(ypath); i++ {
		c := ypath[i]
		if depth == 0 && (c == '#' || c == '/') {
			attrs = ypath[:i]
			path = ypath[i:]
			return
		}

		op := s.step(&s, ypath[i])
		switch op {
		case scanError:
			err = xerrors.New("ypath: invalid format")
			return

		case scanBeginAttrs:
			depth++

		case scanEndAttrs, scanEndList, scanEndMap:
			depth--

		case scanSkipSpace:

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
