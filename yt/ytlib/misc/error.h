#pragma once

#include "common.h"
#include "enum.h"
#include "property.h"

#include "../actions/future.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
        if (Code_ == OK) {
            return "OK";
        } else {
            return Sprintf("(%d): %s", Code_, ~Message_);
        }
    }

    static const int OK;
    static const int Fail;
};

typedef TFuture<TError> TAsyncError;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TValuedError
    : public TError
{
    DEFINE_BYREF_RW_PROPERTY(T, Value);

public:
    TValuedError()
    { }

    explicit TValuedError(const Stroka& message)
        : TError(message)
    { }

    TValuedError(const T& value)
        : Value_(value)
    { }

    TValuedError(T&& value)
        : Value_(MoveRV(value))
    { }

    TValuedError(const TValuedError<T>& other)
        : TError(other)
        , Value_(other.Value_)
    { }

    TValuedError(const TError& other)
        : TError(other)
    { }

    TValuedError(int code, const Stroka& message)
        : TError(code, message)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
