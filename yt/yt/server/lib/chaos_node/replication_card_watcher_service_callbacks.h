#pragma once

#include <yt/yt/ytlib/chaos_client/replication_cards_watcher.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using TCtxReplicationCardWatcherPtr = TIntrusivePtr<NYT::NRpc::TTypedServiceContext<
    NChaosClient::NProto::TReqWatchReplicationCard,
    NChaosClient::NProto::TRspWatchReplicationCard
>>;

////////////////////////////////////////////////////////////////////////////////

NChaosClient::IReplicationCardWatcherCallbacksPtr CreateReplicationCardWatcherCallbacks(
    TCtxReplicationCardWatcherPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
