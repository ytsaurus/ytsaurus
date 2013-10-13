#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;

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
