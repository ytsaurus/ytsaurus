#include "stdafx.h"
#include "leader_channel.h"

#include <ytlib/rpc/roaming_channel.h>

namespace NYT {
namespace NElection {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannel::TPtr> OnLeaderFound(TLeaderLookup::TResult result)
{
    if (result.Id == NElection::InvalidPeerId) {
        return TError("Unable to determine the leader");
    } 

    return CreateBusChannel(result.Address);
}

} // namespace <anonymous>

IChannel::TPtr CreateLeaderChannel(TLeaderLookup::TConfigPtr config)
{
    auto leaderLookup = New<TLeaderLookup>(config);
    return CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannel::TPtr> > {
            return leaderLookup->GetLeader().Apply(BIND(&OnLeaderFound));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
