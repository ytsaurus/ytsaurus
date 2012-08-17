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

} // namespace NRpc
} // namespace NYT
