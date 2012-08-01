#include "stdafx.h"
#include "master_channel.h"
#include "config.h"
#include "master_discovery.h"
#include "private.h"

#include <ytlib/rpc/roaming_channel.h>
#include <ytlib/rpc/bus_channel.h>

#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TValueOrError<IChannelPtr> OnPeerFound(
    const Stroka& role,
    TMasterDiscoveryConfigPtr config,
    TMasterDiscovery::TResult result)
{
    if (!result.Address) {
        return TError(
            EErrorCode::Unavailable,
            "%s cannot be located",
            ~role);
    }

    LOG_INFO("%s located at %s", ~role, ~result.Address.Get());

    auto clientConfig = New<TTcpBusClientConfig>();
    clientConfig->Address = result.Address.Get();
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
                &OnPeerFound,
                "Leader",
                config));
        }));
}

IChannelPtr CreateMasterChannel(TMasterDiscoveryConfigPtr config)
{
    auto masterDiscovery = New<TMasterDiscovery>(config);
    return CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TValueOrError<IChannelPtr> > {
            return masterDiscovery->GetMaster().Apply(BIND(
                &OnPeerFound,
                "Master",
                config));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
