package yterrors

import (
	"errors"
	"strings"
	"unicode"
)

// Converter is interface that might be implemented by error type that wish to implement custom conversion to *Error.
type Converter interface {
	YTError() *Error
}

// FromError converts any error to YT error, if it not already YT error.
//
// If err implement Converter interface, FromError calls YTError() method of err and returns the result.
//
// Nested errors are converted to nested YT errors.
//
// err is stored internally and is accessible using errors.As and errors.Is. Original error is discarded during serialization.
//
// If err is already a YT error, returns err unmodified.
func FromError(err error) error {
	switch v := err.(type) {
	case *Error:
		return v

	case Converter:
		return v.YTError()

	case nil:
		return nil

	default:
		return convertError(v)
	}
}

func convertError(err error) *Error {
	yt := new(Error)
	yt.Code = 1
	yt.Message = err.Error()
	yt.origError = err

	if inner := errors.Unwrap(err); inner != nil {
		yt.Message = strings.TrimSuffix(yt.Message, inner.Error())
		yt.Message = strings.TrimSuffix(yt.Message, ": ")

		yt.InnerErrors = []*Error{FromError(inner).(*Error)}
	}

	yt.Message = capitalize(yt.Message)
	return yt
}

func (yt *Error) Is(target error) bool {
	if yt.origError == nil {
		return false
	}

	return errors.Is(yt.origError, target)
}

func (yt *Error) As(target any) bool {
	if yt.origError == nil {
		return false
	}

	return errors.As(yt.origError, target)
}

// Unwrap always returns last inner error.
func (yt *Error) Unwrap() error {
	if len(yt.InnerErrors) == 0 {
		return nil
	}

	return yt.InnerErrors[len(yt.InnerErrors)-1]
}

// capitalize converts go style error message to YT style.
func capitalize(msg string) string {
	if len(msg) == 0 {
		return msg
	}

	runes := []rune(msg)
	for i, r := range runes {
		if r == ' ' || i+1 == len(runes) {
			runes[0] = unicode.ToUpper(runes[0])
			break
		}

		if r == ':' {
			break
		}
	}

	for i, r := range runes {
		if r != ':' {
			continue
		}

		if i+2 >= len(runes) {
			continue
		}

		if runes[i+1] == ' ' {
			runes[i+2] = unicode.ToUpper(runes[i+2])
		}
	}
	return string(runes)
}

// uncapitalize converts YT style error message to go style.
func uncapitalize(msg string) string {
	if len(msg) == 0 {
		return msg
	}

	runes := []rune(msg)
	if len(runes) == 1 || !unicode.IsUpper(runes[1]) {
		runes[0] = unicode.ToLower(runes[0])
	}

	for i, r := range runes {
		if r != ';' {
			continue
		}

		if i+2 >= len(runes) {
			continue
		}

		if runes[i+1] == ' ' {
			if i+3 == len(runes) || !unicode.IsUpper(runes[i+3]) {
				runes[i+2] = unicode.ToLower(runes[i+2])
			}
		}
	}
	return string(runes)
}
