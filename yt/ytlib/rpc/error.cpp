#include "stdafx.h"
#include "error.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool IsRpcError(const TError& error)
{
    return error.GetCode() < TError::OK;
}

bool IsServiceError(const TError& error)
{
    return error.GetCode() > TError::OK;
}

bool IsRetriableError(const TError& error)
{
    auto code = error.GetCode();
    return
        code == EErrorCode::TransportError ||
        code == EErrorCode::Timeout ||
        code == EErrorCode::Unavailable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
