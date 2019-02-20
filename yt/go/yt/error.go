package yt

import (
	"fmt"

	"a.yandex-team.ru/yt/go/yson"
	"golang.org/x/xerrors"
)

type ErrorCode int

func (e ErrorCode) MarshalYSON(w *yson.Writer) error {
	w.Int64(int64(e))
	return w.Err()
}

func (e *ErrorCode) UnmarshalYSON(r *yson.Reader) (err error) {
	var code int
	err = (&yson.Decoder{r}).Decode(&code)
	*e = ErrorCode(code)
	return
}

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
type Error struct {
	Code        ErrorCode              `yson:"code" json:"code"`
	Message     string                 `yson:"message" json:"message"`
	InnerErrors []*Error               `yson:"inner_errors" json:"inner_errors"`
	Attributes  map[string]interface{} `yson:"attributes" json:"attributes"`
}

func ContainsErrorCode(err error, errorCode ErrorCode) bool {
	var ytErr *Error
	if ok := xerrors.As(err, &ytErr); ok {
		if errorCode == ytErr.Code {
			return true
		}

		for _, nested := range ytErr.InnerErrors {
			if ContainsErrorCode(nested, errorCode) {
				return true
			}
		}
	}

	return false
}

func (yt *Error) Error() string {
	if yt.Code == 1 {
		return fmt.Sprintf("yt: message=%q", yt.Message)
	} else {
		return fmt.Sprintf("yt: code=%d, message=%q", yt.Code, yt.Message)
	}
}

type ErrorAttr struct {
	Name  string
	Value interface{}
}

func Err(args ...interface{}) error {
	err := new(Error)
	err.Code = 1

	for _, arg := range args {
		switch v := arg.(type) {
		case ErrorCode:
			err.Code = v
		case string:
			err.Message = v
		case *Error:
			err.InnerErrors = append(err.InnerErrors, v)
		case ErrorAttr:
			if err.Attributes == nil {
				err.Attributes = map[string]interface{}{}
			}

			err.Attributes[v.Name] = v.Value
		case error:
			err.InnerErrors = append(err.InnerErrors, &Error{
				Code:    1,
				Message: v.Error(),
			})
		default:
			panic(fmt.Sprintf("can't create yt.Error from type %T", arg))
		}
	}

	return err
}
