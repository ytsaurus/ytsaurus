#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TRequestId NullRequestId(0, 0, 0, 0);

bool IsRpcError(const TError& error)
{
    auto code = error.GetCode();
    return code > 1 && code <= 100;
}

bool IsRetriableError(const TError& error)
{
    auto code = error.GetCode();
    return code == EErrorCode::TransportError ||
           code == EErrorCode::Timeout ||
           code == EErrorCode::Unavailable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
