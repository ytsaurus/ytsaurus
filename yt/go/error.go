package yt

import "fmt"

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
// Error type is immutable.
type Error struct {
	errorCode ErrorCode
	message   string

	attrs []ErrorAttr

	innerErrors []*Error
}

func ContainsErrorCode(err error, errorCode ErrorCode) bool {
	if ytErr, ok := err.(*Error); ok {
		if errorCode == ytErr.errorCode {
			return true
		}

		for _, nested := range ytErr.innerErrors {
			if ContainsErrorCode(nested, errorCode) {
				return true
			}
		}
	}

	return false
}

func (yt *Error) Error() string {
	panic("not implemented")
}

type ErrorAttr struct {
	Name  string
	Value interface{}
}

func Err(args ...interface{}) error {
	err := new(Error)
	err.errorCode = 1

	for _, arg := range args {
		switch v := arg.(type) {
		case ErrorAttr:
			err.attrs = append(err.attrs, v)
		case ErrorCode:
			err.errorCode = v
		case string:
			err.message = v
		case *Error:
			err.innerErrors = append(err.innerErrors, v)
		case error:
			err.innerErrors = append(err.innerErrors, &Error{
				errorCode: 1,
				message:   v.Error(),
			})

		default:
			panic(fmt.Sprintf("can't create yt.Error from type %T", arg))
		}
	}

	return err
}
