#include "stdafx.h"
#include "leader_channel.h"

#include "config.h"
#include "master_discovery.h"

#include <ytlib/rpc/roaming_channel.h>
#include <ytlib/rpc/bus_channel.h>

#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannelPtr> OnLeaderFound(
    TMasterDiscoveryConfigPtr config,
    TMasterDiscovery::TResult result)
{
    if (!result.Address) {
        return TError(
            EErrorCode::Unavailable,
            "Unable to determine the leader");
    }

    auto clientConfig = New<TTcpBusClientConfig>();
    clientConfig->Address = *result.Address;
    clientConfig->Priority = config->ConnectionPriority;
    auto client = CreateTcpBusClient(clientConfig);
    return CreateBusChannel(client);
}

} // namespace

IChannelPtr CreateLeaderChannel(TMasterDiscoveryConfigPtr config)
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

} // namespace NMetaState
} // namespace NYT
