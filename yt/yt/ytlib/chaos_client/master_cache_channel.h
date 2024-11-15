#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IChannelPtr CreateChaosCacheChannel(
    const NApi::NNative::IConnectionPtr& connection,
    TChaosCacheChannelConfigPtr config);

void SetChaosCacheStickyGroupBalancingHint(
    const TReplicationCardId& replicationCardId,
    NRpc::NProto::TBalancingExt* balancingHeaderExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
