package yson

type streamState int

const (
	streamExpectValue streamState = iota
	streamExpectKey
	streamExpectValueNoAttrs

	streamOpenAttrs
	streamOpenList
	streamOpenMap

	streamListFragment
	streamMapFragment
)

type streamValidator []streamState

func newValidator(s StreamKind) streamValidator {
	switch s {
	case StreamNode:
		return streamValidator{streamExpectValue}
	case StreamListFragment:
		return streamValidator{streamListFragment, streamExpectValue}
	case StreamMapFragment:
		return streamValidator{streamMapFragment, streamExpectKey}
	}

	panic("invalid stream kind")
}

func (v *streamValidator) depth() int {
	if len(*v) == 0 {
		return 0
	}

	if (*v)[0] == streamListFragment || (*v)[0] == streamMapFragment {
		return len(*v) - 2
	}

	return len(*v) - 1
}

func (v *streamValidator) eof() error {
	if len(*v) == 0 {
		return nil
	}

	if len(*v) == 2 {
		first := (*v)[0]
		second := (*v)[1]

		if first == streamListFragment && second == streamExpectValue {
			return nil
		}

		if first == streamMapFragment && second == streamExpectKey {
			return nil
		}
	}

	return ErrInvalidNesting
}

func (v *streamValidator) pushEvent(event Event) (semicolon bool, err error) {
	if len(*v) == 0 {
		err = ErrInvalidNesting
		return
	}

	pop := func() {
		*v = (*v)[:len(*v)-1]
	}

	become := func(state streamState) {
		(*v)[len(*v)-1] = state
	}

	finishValue := func() {
		if len(*v) == 1 {
			*v = nil
			return
		}

		semicolon = true

		where := (*v)[len(*v)-2]
		switch where {
		case streamOpenAttrs, streamOpenMap, streamMapFragment:
			(*v)[len(*v)-1] = streamExpectKey

		case streamOpenList, streamListFragment:
			(*v)[len(*v)-1] = streamExpectValue
		}
	}

	top := (*v)[len(*v)-1]
	switch event {
	case EventBeginAttrs:
		if top != streamExpectValue {
			err = ErrInvalidNesting
			return
		}

		become(streamOpenAttrs)
		*v = append(*v, streamExpectKey)

	case EventBeginList:
		if top != streamExpectValue && top != streamExpectValueNoAttrs {
			err = ErrInvalidNesting
			return
		}

		become(streamOpenList)
		*v = append(*v, streamExpectValue)

	case EventBeginMap:
		if top != streamExpectValue && top != streamExpectValueNoAttrs {
			err = ErrInvalidNesting
			return
		}

		become(streamOpenMap)
		*v = append(*v, streamExpectKey)

	case EventKey:
		if top != streamExpectKey {
			err = ErrInvalidNesting
			return
		}

		become(streamExpectValue)

	case EventLiteral:
		if top != streamExpectValue && top != streamExpectValueNoAttrs {
			err = ErrInvalidNesting
			return
		}

		finishValue()

	case EventEndAttrs:
		if top != streamExpectKey {
			err = ErrInvalidNesting
			return
		}

		if len(*v) == 1 {
			err = ErrInvalidNesting
			return
		}

		pop()
		if (*v)[len(*v)-1] != streamOpenAttrs {
			err = ErrInvalidNesting
			return
		}
		become(streamExpectValueNoAttrs)

	case EventEndList:
		if top != streamExpectValue {
			err = ErrInvalidNesting
			return
		}

		if len(*v) < 2 {
			err = ErrInvalidNesting
			return
		}

		pop()
		finishValue()

	case EventEndMap:
		if top != streamExpectKey {
			err = ErrInvalidNesting
			return
		}

		if len(*v) < 2 {
			err = ErrInvalidNesting
			return
		}

		pop()
		finishValue()
	}

	return
}

// Valid checks that byte sequence is a valid YSON node.
func Valid(data []byte) error {
	return valid(data, StreamNode)
}

// ValidListFragment checks that byte sequence is a valid YSON list fragment.
func ValidListFragment(data []byte) error {
	return valid(data, StreamListFragment)
}

// ValidMapFragment checks that byte sequence is a valid YSON map fragment.
func ValidMapFragment(data []byte) error {
	return valid(data, StreamMapFragment)
}

func valid(data []byte, kind StreamKind) error {
	var s scanner
	s.reset(kind)

	for _, c := range data {
		op := s.step(&s, c)

		// TODO(prime@): check string and number literals
		if op == scanError {
			return s.err
		}
	}

	op := s.eof()
	if op == scanError {
		return s.err
	}

	return nil
}
