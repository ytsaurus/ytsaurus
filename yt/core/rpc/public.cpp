#include "stdafx.h"
#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;
const TMutationId NullMutationId;

bool IsRetriableError(const TError& error)
{
    auto code = error.GetCode();
    return code == NRpc::EErrorCode::TransportError ||
           code == NRpc::EErrorCode::Unavailable ||
           code == NYT::EErrorCode::Timeout;
}

bool IsChannelFailureError(const TError& error)
{
    auto code = error.GetCode();
    return code == NRpc::EErrorCode::TransportError ||
           code == NRpc::EErrorCode::Unavailable ||
           code == NYT::EErrorCode::Timeout;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
