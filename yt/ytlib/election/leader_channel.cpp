#include "stdafx.h"
#include "leader_channel.h"

#include <ytlib/rpc/roaming_channel.h>
#include <ytlib/rpc/bus_channel.h>

#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>

namespace NYT {
namespace NElection {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannelPtr> OnLeaderFound(  
    TMasterDiscovery::TConfigPtr config,
    TMasterDiscovery::TResult result)
{
    if (result.Id == NElection::InvalidPeerId) {
        return TError("Unable to determine the leader");
    } 

    auto clientConfig = New<TTcpBusClientConfig>();
    clientConfig->Address = result.Address;
    clientConfig->Priority = config->ConnectionPriority;
    auto client = CreateTcpBusClient(clientConfig);
    return CreateBusChannel(client);
}

} // namespace

IChannelPtr CreateLeaderChannel(TMasterDiscovery::TConfigPtr config)
{
    auto masterDiscovery = New<TMasterDiscovery>(config);
    return CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannelPtr> > {
            return masterDiscovery->GetLeader().Apply(BIND(
                &OnLeaderFound,
                config));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
