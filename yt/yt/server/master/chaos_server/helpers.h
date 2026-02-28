#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> GetMinimalTabletCount(std::vector<TErrorOr<int>> tabletCounts);

TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(
    const NApi::NNative::IConnectionPtr& connection,
    NChaosClient::TReplicationCardId replicationCardId,
    const NChaosClient::TReplicationCardFetchOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
