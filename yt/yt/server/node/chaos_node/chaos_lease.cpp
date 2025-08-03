#include "chaos_lease.h"

#include "serialize.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

void TChaosLease::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, LastPingTimestamp_);
}

void TChaosLease::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, LastPingTimestamp_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
