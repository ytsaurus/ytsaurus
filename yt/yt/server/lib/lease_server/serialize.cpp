#include "serialize.h"

#include "lease_manager.h"

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

void TLeaseGuardSerializer::Save(TSaveContext& context, const ILeaseGuardPtr& guard)
{
    using NYT::Save;

    YT_VERIFY(guard->IsPersistent());
    Save(context, guard->GetLeaseId());
}

void TLeaseGuardSerializer::Load(TLoadContext& context, ILeaseGuardPtr& guard)
{
    using NYT::Load;

    auto leaseId = Load<TLeaseId>(context);
    auto* lease = context.GetLeaseManager()->GetLease(leaseId);
    guard = lease->GetPersistentLeaseGuardOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
