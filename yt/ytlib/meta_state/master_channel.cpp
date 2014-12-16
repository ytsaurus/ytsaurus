#include "stdafx.h"
#include "master_channel.h"
#include "config.h"
#include "master_discovery.h"
#include "private.h"

#include <core/rpc/roaming_channel.h>
#include <core/rpc/bus_channel.h>
#include <core/rpc/retrying_channel.h>

#include <core/bus/config.h>
#include <core/bus/tcp_client.h>

namespace NYT {
namespace NMetaState {

using namespace NRpc;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorOr<IChannelPtr> OnPeerFound(
    const Stroka& role,
    TMasterDiscoveryConfigPtr config,
    TMasterDiscovery::TResult result)
{
    if (!result.Address) {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "No %s found",
            ~role);
    }

    LOG_INFO("Found %s at %s", ~role, ~result.Address.Get());

    auto clientConfig = New<TTcpBusClientConfig>();
    clientConfig->Address = result.Address.Get();
    clientConfig->Priority = config->ConnectionPriority;
    auto client = CreateTcpBusClient(clientConfig);
    return CreateBusChannel(client);
}

} // namespace

IChannelPtr CreateLeaderChannel(
    TMasterDiscoveryConfigPtr config,
    TCallback<bool(const TError&)> isRetriableError)
{
    auto masterDiscovery = New<TMasterDiscovery>(config);
    auto roamingChannel = CreateRoamingChannel(
        config->RpcTimeout,
        BIND([=] () -> TFuture< TErrorOr<IChannelPtr> > {
            return masterDiscovery->GetLeader().Apply(BIND(
                &OnPeerFound,
                "leader",
                config));
        }));
    return CreateRetryingChannel(config, roamingChannel, isRetriableError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
