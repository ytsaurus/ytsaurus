#include "chaos_lease.h"

#include "serialize.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

bool TChaosLease::IsRoot() const
{
    return !RootId_;
}

bool TChaosLease::IsNormalState() const
{
    return State_ == EChaosLeaseState::Normal;
}

void TChaosLease::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, RootId_);
    Save(context, ParentId_);
    Save(context, NestedLeaseIds_);
    Save(context, Timeout_);
    Save(context, State_);
}

void TChaosLease::Load(TLoadContext& context)
{
    using NYT::Load;

    if (context.GetVersion() >= EChaosReign::IntroduceChaosLeaseManager) {
        Load(context, RootId_);
    }

    Load(context, ParentId_);
    Load(context, NestedLeaseIds_);
    Load(context, Timeout_);
    Load(context, State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
