package yterrors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/yson"
)

//go:generate yt-gen-error-code -yt-root ../../yt -out error_code.go
type ErrorCode int

// Error is an implementation of built-in go error interface.
//
// YT errors are designed to be easily transferred over network and
// between different languages. Because of this, Error is a concrete
// type and not an interface.
//
// YT error consists of error code, error message, list of attributes
// and a list of inner errors.
//
// Since YT error might contain arbitrary nested tree structure, user
// should traverse the whole tree when searching for a specific error
// condition.
//
// Error supports brief and full formatting using %v and %+v format specifiers.
type Error struct {
	Code       ErrorCode      `yson:"code" json:"code"`
	Message    string         `yson:"message" json:"message"`
	Attributes map[string]any `yson:"attributes,omitempty" json:"attributes,omitempty"`

	// NOTE: Unwrap() always returns last inner error.
	InnerErrors []*Error `yson:"inner_errors,omitempty" json:"inner_errors,omitempty"`

	// Original error, saved during conversion.
	origError error
}

// ContainsErrorCode returns true iff any of the nested errors has ErrorCode equal to errorCode.
//
// ContainsErrorCode invokes errors.As internally. It is safe to pass arbitrary error value to this function.
func ContainsErrorCode(err error, code ErrorCode) bool {
	return FindErrorCode(err, code) != nil
}

// ContainsResolveError is convenient helper for checking ResolveError.
func ContainsResolveError(err error) bool {
	return ContainsErrorCode(err, CodeResolveError)
}

// ContainsAlreadyExistsError is convenient helper for checking AlreadyExistsError.
func ContainsAlreadyExistsError(err error) bool {
	return ContainsErrorCode(err, CodeAlreadyExists)
}

// FindErrorCode examines error and all nested errors, returning first YT error with given error code.
func FindErrorCode(err error, code ErrorCode) *Error {
	if err == nil {
		return nil
	}

	var ytErr *Error
	if ok := xerrors.As(err, &ytErr); ok {
		if code == ytErr.Code {
			return ytErr
		}

		for _, nested := range ytErr.InnerErrors {
			if ytErr = FindErrorCode(nested, code); ytErr != nil {
				return ytErr
			}
		}
	}

	return nil
}

// ContainsMessageRE returns true iff any of the nested errors has message matching provided regex.
//
// Use of this function is strongly discouraged, use ContainsErrorCode when possible.
func ContainsMessageRE(err error, messageRE *regexp.Regexp) bool {
	if err == nil {
		return false
	}

	var ytErr *Error
	if ok := xerrors.As(err, &ytErr); !ok {
		return false
	}

	var matchRecursive func(*Error) bool
	matchRecursive = func(ytErr *Error) bool {
		if messageRE.MatchString(ytErr.Message) {
			return true
		}

		for _, nested := range ytErr.InnerErrors {
			if matchRecursive(nested) {
				return true
			}
		}

		return false
	}

	return matchRecursive(ytErr)
}

func (yt *Error) HasAttr(name string) bool {
	_, ok := yt.Attributes[name]
	return ok
}

func (yt *Error) AddAttr(name string, value any) {
	if yt.Attributes == nil {
		yt.Attributes = map[string]any{}
	}

	yt.Attributes[name] = value
}

func (yt *Error) Error() string {
	if yt.origError != nil {
		return yt.origError.Error()
	}

	return fmt.Sprint(yt)
}

func (yt *Error) Format(s fmt.State, v rune) { xerrors.FormatError(yt, s, v) }

func (yt *Error) FormatError(p xerrors.Printer) (next error) {
	p.Printf("%s", uncapitalize(yt.Message))

	printAttrs := func(e *Error) {
		maxLen := 0
		if e.Code != 1 {
			maxLen = 4
		}

		for name := range e.Attributes {
			if len(name) > maxLen {
				maxLen = len(name)
			}
		}

		padding := func(name string) string {
			return strings.Repeat(" ", maxLen-len(name))
		}

		if e.Code != 1 {
			p.Printf("  code: %s%d\n", padding("code"), e.Code)
		}

		var names []string
		for name := range e.Attributes {
			names = append(names, name)
		}
		sort.Strings(names)

		formatAttr := func(v any) string {
			b, _ := yson.MarshalFormat(v, yson.FormatText)
			return string(b)
		}

		for _, name := range names {
			p.Printf("  %s: %s%s\n", name, padding(name), formatAttr(e.Attributes[name]))
		}
	}

	var visit func(*Error)
	visit = func(e *Error) {
		p.Printf("%s\n", uncapitalize(e.Message))
		printAttrs(e)

		for _, inner := range e.InnerErrors {
			visit(inner)
		}
	}

	if p.Detail() {
		p.Printf("\n")
		printAttrs(yt)

		for _, inner := range yt.InnerErrors {
			visit(inner)
		}
	} else {
		// Recursing only into the last inner error, since user asked for brief error.
		if len(yt.InnerErrors) != 0 {
			return yt.InnerErrors[len(yt.InnerErrors)-1]
		}
	}

	return nil
}

type ErrorAttr struct {
	Name  string
	Value any
}

func Attr(name string, value any) ErrorAttr {
	return ErrorAttr{Name: name, Value: value}
}

// Err creates new error of type Error.
//
// NOTE: when passing multiple inner errors, only the last one will be accessible by errors.Is and errors.As.
func Err(args ...any) error {
	err := new(Error)
	err.Code = 1
	err.Message = "Error"

	for _, arg := range args {
		switch v := arg.(type) {
		case ErrorCode:
			err.Code = v
		case string:
			err.Message = capitalize(v)
		case *Error:
			err.InnerErrors = append(err.InnerErrors, v)
		case ErrorAttr:
			err.AddAttr(v.Name, v.Value)
		case error:
			err.InnerErrors = append(err.InnerErrors, FromError(v).(*Error))
		default:
			panic(fmt.Sprintf("can't create yt.Error from type %T", arg))
		}
	}

	return err
}

var (
	_ json.Unmarshaler = (*Error)(nil)
)

// UnmarshalJSON copies json deserialization logic from C++ code.
func (yt *Error) UnmarshalJSON(b []byte) error {
	d := json.NewDecoder(bytes.NewBuffer(b))
	d.UseNumber()

	type ytError Error
	if err := d.Decode((*ytError)(yt)); err != nil {
		return err
	}

	yt.Message = decodeNonASCII(yt.Message)
	if yt.Attributes != nil {
		yt.Attributes = fixStrings(yt.Attributes, decodeNonASCII).(map[string]any)
	}
	return nil
}

// MarshalJSON copies json serialization logic from C++ code.
//
// All non-ascii bytes inside strings are interpreted as runes and encoded
// using utf8 encoding (even if they already form a valid utf-8 sequence of bytes).
func (yt *Error) MarshalJSON() ([]byte, error) {
	type ytError Error

	fixed := ytError(*yt)
	fixed.Message = encodeNonASCII(fixed.Message)

	if fixed.Attributes != nil {
		attrsJS, err := json.Marshal(yt.Attributes)
		if err != nil {
			return nil, err
		}
		var attrs any
		if err := json.Unmarshal(attrsJS, &attrs); err != nil {
			return nil, err
		}

		fixed.Attributes = fixStrings(attrs, encodeNonASCII).(map[string]any)
	}

	return json.Marshal(fixed)
}
