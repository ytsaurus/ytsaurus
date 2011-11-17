#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

// By convention, RPC errors have negative error codes.
DECLARE_ENUM(EErrorCode,
    ((OK)(0))
    ((TransportError)(-1))
    ((ProtocolError)(-2))
    ((NoSuchService)(-3))
    ((NoSuchMethod)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
);

////////////////////////////////////////////////////////////////////////////////

class TError
{
public:
    TError()
    { }

    TError(int code, const Stroka& message)
        : Code_(code)
        , Message_(message)
    { }

    int GetCode() const
    {
        return Code_;
    }

    Stroka GetMessage() const
    {
        return Message_;
    }

    bool IsOK() const
    {
        return Code_ == EErrorCode::OK;   
    }

    bool IsRpcError() const
    {
        return Code_ < EErrorCode::OK;
    }

    bool IsServiceError() const
    {
        return Code_ > EErrorCode::OK;
    }

    Stroka ToString() const
    {
        return Sprintf("(%d): %s", Code_, ~Message_);
    }

private:
    int Code_;
    Stroka Message_;
};

typedef TFuture<TError> TAsyncError;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
