#include "helpers.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

TError CreateChaosLeaseNotKnownError(TChaosLeaseId chaosLeaseId)
{
    return TError(
        NChaosClient::EErrorCode::ChaosLeaseNotKnown,
        "Chaos lease is not known")
        << TErrorAttribute("chaos_lease_id", chaosLeaseId);
}

void ThrowChaosLeaseNotKnown(TChaosLeaseId chaosLeaseId)
{
    THROW_ERROR(CreateChaosLeaseNotKnownError(chaosLeaseId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
