#pragma once

#include "../misc/common.h"
#include "../misc/enum.h"
#include "../misc/property.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

// By convention, RPC errors have negative error codes.
DECLARE_ENUM(EErrorCode,
    ((OK)(0))
    ((TransportError)(-1))
    ((ProtocolError)(-2))
    ((NoSuchService)(-3))
    ((NoSuchVerb)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
);

////////////////////////////////////////////////////////////////////////////////

class TError
{
    DEFINE_BYVAL_RO_PROPERTY(int, Code);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Message);

public:
    TError()
        : Code_(EErrorCode::OK)
    { }

    TError(int code, const Stroka& message)
        : Code_(code)
        , Message_(message)
    { }

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
        if (Code_ == EErrorCode::OK) {
            return "OK";
        } else {
            return Sprintf("(%d): %s", Code_, ~Message_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: get rid of this
#ifdef _win_
#undef GetMessage
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
