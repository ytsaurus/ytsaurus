#include "chaos_lease.h"

#include "serialize.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

void TChaosLease::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ParentId_);
    Save(context, NestedLeases_);
    Save(context, Timeout_);
    Save(context, State_);
}

void TChaosLease::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, ParentId_);
    Load(context, NestedLeases_);
    Load(context, Timeout_);
    Load(context, State_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
