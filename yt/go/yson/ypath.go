package yson

import "github.com/pkg/errors"

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
			err = errors.New("ypath: invalid format")
			return

		case scanBeginAttrs:
			depth++

		case scanEndAttrs, scanEndList, scanEndMap:
			depth--

		case scanSkipSpace:

		default:
			if depth == 0 {
				err = errors.New("ypath: invalid format")
				return
			}

			if op == scanBeginList || op == scanBeginMap {
				depth++
			}
		}
	}

	err = errors.New("ypath: invalid format")
	return
}
