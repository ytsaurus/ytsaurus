#pragma once

#include <yt/yt/ytlib/chaos_client/chaos_leases_watcher.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using TCtxChaosLeaseWatcherPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
    NChaosClient::NProto::TReqWatchChaosLease,
    NChaosClient::NProto::TRspWatchChaosLease
>>;

////////////////////////////////////////////////////////////////////////////////

NChaosClient::IChaosLeaseWatcherCallbacksPtr CreateChaosLeaseWatcherCallbacks(
    TCtxChaosLeaseWatcherPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
