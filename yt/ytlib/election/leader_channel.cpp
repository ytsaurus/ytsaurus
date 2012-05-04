#include "stdafx.h"
#include "leader_channel.h"

#include <ytlib/rpc/roaming_channel.h>

namespace NYT {
namespace NElection {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannelPtr> OnLeaderFound(TLeaderLookup::TResult result)
{
    if (result.Id == NElection::InvalidPeerId) {
        return TError("Unable to determine the leader");
    } 

    return CreateBusChannel(result.Address);
}

} // namespace

IChannelPtr CreateLeaderChannel(TLeaderLookup::TConfigPtr config)
{
    auto leaderLookup = New<TLeaderLookup>(config);
    return CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannelPtr> > {
            return leaderLookup->GetLeader().Apply(BIND(&OnLeaderFound));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
