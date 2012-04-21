#pragma once

#include "common.h"
#include "enum.h"
#include "property.h"

#include <ytlib/misc/error.pb.h>
#include <ytlib/actions/future.h>
#include <ytlib/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#undef GetMessage // Fucking WinAPI

class TError
{
    DEFINE_BYVAL_RO_PROPERTY(int, Code);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Message);

public:
    TError();
    TError(const TError& other);

    explicit TError(const Stroka& message);
    TError(const char* format, ...);

    TError(int code, const Stroka& message);
    TError(int code, const char* format, ...);

    bool IsOK() const;

    Stroka ToString() const;

    NProto::TError ToProto() const;
    static TError FromProto(const NProto::TError& protoError);

    void ToYson(NYTree::IYsonConsumer* consumer) const;
    static TError FromYson(NYTree::INodePtr node);

    enum
    {
        OK = 0, 
        Fail = INT_MAX
    };
};

typedef TFuture<TError> TAsyncError;
typedef TPromise<TError> TAsyncErrorPromise;

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

template <>
class TValueOrError<void>
    : public TError
{
public:
    TValueOrError()
    { }

    TValueOrError(const TValueOrError<void>& other)
        : TError(other)
    { }

    TValueOrError(const TError& other)
        : TError(other)
    { }

    TValueOrError(int code, const Stroka& message)
        : TError(code, message)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
