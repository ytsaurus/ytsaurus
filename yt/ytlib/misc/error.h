#pragma once

#include "common.h"
#include "enum.h"
#include "property.h"

#include <ytlib/misc/error.pb.h>
#include <ytlib/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#undef GetMessage // Fucking WinAPI

class TError
{
    DEFINE_BYVAL_RO_PROPERTY(int, Code);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Message);

public:
    TError();

    explicit TError(const Stroka& message);

    TError(const TError& other);

    TError(int code, const Stroka& message);

    bool IsOK() const;

    Stroka ToString() const;

    NProto::TError ToProto() const;
    static TError FromProto(const NProto::TError& protoError);

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
