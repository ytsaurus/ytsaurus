#pragma once

#include <ytlib/misc/error.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

// By convention, TError::OK = 0 and RPC errors have negative error codes.
DECLARE_ENUM(EErrorCode,
    ((TransportError)(-1))
    ((ProtocolError)(-2))
    ((NoSuchService)(-3))
    ((NoSuchVerb)(-4))
    ((Timeout)(-5))
    ((ServiceError)(-6))
    ((Unavailable)(-7))
    ((PoisonPill)(-8))
);

////////////////////////////////////////////////////////////////////////////////

bool IsRpcError(const TError& error);
bool IsServiceError(const TError& error);
bool IsRetriableError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

//! Represents an error that has occurred during serving an RPC request.
class TServiceException 
    : public yexception
{
public:
    //! Initializes a new instance.
    explicit TServiceException(int code)
        : Code_(code)
    { }

    explicit TServiceException(const TError& error)
        : Code_(error.GetCode())
    {
        *this << error.ToString();
    }

    //! Gets the error code.
    TError GetError() const
    {
        return TError(Code_, what());
    }

protected:
    int Code_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
