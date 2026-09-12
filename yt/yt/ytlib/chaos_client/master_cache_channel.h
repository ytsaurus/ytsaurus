#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpc::NProto {

////////////////////////////////////////////////////////////////////////////////

class TBalancingExt;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NProto

namespace NYT::NYTree::NProto {

////////////////////////////////////////////////////////////////////////////////

class TCachingHeaderExt;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NProto

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IChannelPtr CreateChaosCacheChannel(
    const NApi::NNative::IConnectionPtr& connection,
    TChaosCacheChannelConfigPtr config);

void SetChaosCacheStickyGroupBalancingHint(
    const TReplicationCardId& replicationCardId,
    NRpc::NProto::TBalancingExt* balancingHeaderExt);

void SetChaosCacheCachingHeader(
    TDuration expireAfterSuccessfulUpdateTime,
    TDuration expireAfterFailedUpdateTime,
    TReplicationEra refreshEra,
    NYTree::NProto::TCachingHeaderExt* cachingHeaderExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
