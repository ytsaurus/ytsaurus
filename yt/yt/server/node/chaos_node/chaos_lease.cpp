#include "chaos_lease.h"

#include "serialize.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

void TChaosLease::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ParentId_);
    Save(context, Timeout_);
}

void TChaosLease::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, ParentId_);
    Load(context, Timeout_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
