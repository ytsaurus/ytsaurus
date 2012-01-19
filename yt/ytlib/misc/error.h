#pragma once

#include "common.h"
#include "enum.h"
#include "property.h"

#include <ytlib/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#undef GetMessage // Fucking WinAPI

class TError
{
    DEFINE_BYVAL_RO_PROPERTY(int, Code);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Message);

public:
    TError()
        : Code_(OK)
    { }

    explicit TError(const Stroka& message)
        : Code_(Fail)
        , Message_(message)
    { }

    TError(const TError& other)
        : Code_(other.Code_)
        , Message_(other.Message_)
    { }

    TError(int code, const Stroka& message)
        : Code_(code)
        , Message_(message)
    { }

    bool IsOK() const
    {
        return Code_ == OK;
    }

    Stroka ToString() const
    {
        switch (Code_) {
            case OK:
                return "OK";
            case Fail:
                return Message_;
            default:
                return Sprintf("(%d): %s", Code_, ~Message_);
        }
    }

    enum
    {
        OK = 0,
        Fail = INT_MAX
    };
};

typedef TFuture<TError> TAsyncError;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TValueOrError
    : public TError
{
    DEFINE_BYREF_RW_PROPERTY(T, Value);

public:
    TValueOrError()
    { }

    TValueOrError(const T& value)
        : Value_(value)
    { }

    TValueOrError(T&& value)
        : Value_(MoveRV(value))
    { }

    TValueOrError(const TValueOrError<T>& other)
        : TError(other)
        , Value_(other.Value_)
    { }

    TValueOrError(const TError& other)
        : TError(other)
    { }

    TValueOrError(int code, const Stroka& message)
        : TError(code, message)
    { }

    template <class TOther>
    TValueOrError(const TValueOrError<TOther>& other)
        : TError(other)
        , Value_(other.Value())
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
