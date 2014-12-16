#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId(0, 0, 0, 0);

bool IsRetriableError(const TError& error)
{
    auto code = error.GetCode();
    return code == EErrorCode::TransportError ||
           code == EErrorCode::Timeout ||
           code == EErrorCode::Unavailable;
}

bool IsChannelFailureError(const TError& error)
{
    auto code = error.GetCode();
    return code == EErrorCode::TransportError ||
           code == EErrorCode::Timeout ||
           code == EErrorCode::Unavailable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
