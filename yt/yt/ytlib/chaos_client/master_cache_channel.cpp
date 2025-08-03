#include "master_cache_channel.h"

#include <yt/yt/client/chaos_client/config.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>

namespace NYT::NChaosClient {

using namespace NRpc;
using namespace NApi::NNative;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr int StickyGroupSize = 3;

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateChaosCacheChannel(
    const IConnectionPtr& connection,
    TChaosCacheChannelConfigPtr config)
{
    auto channelFactory = connection->GetChannelFactory();
    auto endpointDescription = std::string("ChaosCache");
    auto endpointAttributes = ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("chaos_cache").Value(true)
        .EndMap());
    auto channel = CreateBalancingChannel(
        config,
        std::move(channelFactory),
        endpointDescription,
        std::move(endpointAttributes));
    channel = CreateRetryingChannel(
        std::move(config),
        std::move(channel));
    return channel;
}

void SetChaosCacheStickyGroupBalancingHint(
    const TReplicationCardId& replicationCardId,
    NRpc::NProto::TBalancingExt* balancingHeaderExt)
{
    balancingHeaderExt->set_enable_stickiness(true);
    balancingHeaderExt->set_sticky_group_size(StickyGroupSize);
    balancingHeaderExt->set_balancing_hint(THash<TReplicationCardId>()(replicationCardId));
}

void SetChaosCacheCachingHeader(
    TDuration expireAfterSuccessfulUpdateTime,
    TDuration expireAfterFailedUpdateTime,
    TReplicationEra refreshEra,
    NYTree::NProto::TCachingHeaderExt* cachingHeaderExt)
{
    cachingHeaderExt->set_expire_after_successful_update_time(ToProto(expireAfterSuccessfulUpdateTime));
    cachingHeaderExt->set_expire_after_failed_update_time(ToProto(expireAfterFailedUpdateTime));
    if (refreshEra != InvalidReplicationEra) {
        cachingHeaderExt->set_refresh_revision(ToProto(refreshEra));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
