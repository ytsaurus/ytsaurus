#include "chaos_node_service.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosClient {

using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;
using namespace NTransactionClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TTestChaosNodeService::TTestChaosNodeService(
    IInvokerPtr invoker,
    NLogging::TLogger logger)
    : TServiceBase(
        std::move(invoker),
        TChaosNodeServiceProxy::GetDescriptor(),
        std::move(logger))
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchChaosLease));
}

int TTestChaosNodeService::GetPendingChaosLeaseWatchCount() const
{
    auto guard = Guard(Lock_);
    return std::ssize(PendingChaosLeaseWatches_);
}

TChaosLeaseId TTestChaosNodeService::GetPendingChaosLeaseId() const
{
    auto guard = Guard(Lock_);
    YT_VERIFY(PendingChaosLeaseWatches_.size() == 1);
    return PendingChaosLeaseWatches_.front().ChaosLeaseId;
}

TTimestamp TTestChaosNodeService::GetPendingChaosLeaseTimestamp() const
{
    auto guard = Guard(Lock_);
    YT_VERIFY(PendingChaosLeaseWatches_.size() == 1);
    return PendingChaosLeaseWatches_.front().Timestamp;
}

TFuture<void> TTestChaosNodeService::GetChaosLeaseWatchReceivedFuture() const
{
    auto guard = Guard(Lock_);
    return PendingChaosLeaseWatches_.empty()
        ? ChaosLeaseWatchReceivedPromise_.ToFuture()
        : OKFuture;
}

void TTestChaosNodeService::ReplyChaosLeaseChanged(
    TTimestamp timestamp,
    TDuration timeout,
    const std::vector<TCellId>& coordinatorCellIds)
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    auto* changed = pendingWatch.Context->Response().mutable_chaos_lease_changed();
    changed->set_chaos_lease_cache_timestamp(ToProto(timestamp));
    changed->set_timeout(ToProto(timeout));
    ToProto(changed->mutable_coordinator_cell_ids(), coordinatorCellIds);
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyChaosLeaseNotChanged()
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    pendingWatch.Context->Response().mutable_chaos_lease_not_changed();
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyChaosLeaseMigrated(TCellId destinationCellId)
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    ToProto(
        pendingWatch.Context->Response().mutable_chaos_lease_migrated()->mutable_migrate_to_cell_id(),
        destinationCellId);
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyChaosLeaseInstanceIsNotLeader()
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    pendingWatch.Context->Response().mutable_instance_is_not_leader();
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyChaosLeaseDeleted()
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    pendingWatch.Context->Response().mutable_chaos_lease_deleted();
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyChaosLeaseUnknown()
{
    auto pendingWatch = TakePendingChaosLeaseWatch();
    pendingWatch.Context->Response().mutable_unknown_chaos_lease();
    pendingWatch.Context->Reply();
}

void TTestChaosNodeService::ReplyAllChaosLeaseWatchesDeleted()
{
    std::vector<TPendingChaosLeaseWatch> pendingWatches;
    {
        auto guard = Guard(Lock_);
        pendingWatches.swap(PendingChaosLeaseWatches_);
        ChaosLeaseWatchReceivedPromise_ = NewPromise<void>();
    }

    for (const auto& pendingWatch : pendingWatches) {
        pendingWatch.Context->Response().mutable_chaos_lease_deleted();
        pendingWatch.Context->Reply();
    }
}

DEFINE_RPC_SERVICE_METHOD(TTestChaosNodeService, WatchChaosLease)
{
    auto guard = Guard(Lock_);
    PendingChaosLeaseWatches_.push_back(TPendingChaosLeaseWatch{
        .Context = context,
        .ChaosLeaseId = FromProto<TChaosLeaseId>(request->chaos_lease_id()),
        .Timestamp = FromProto<TTimestamp>(request->chaos_lease_cache_timestamp()),
    });
    ChaosLeaseWatchReceivedPromise_.TrySet();
}

TTestChaosNodeService::TPendingChaosLeaseWatch TTestChaosNodeService::TakePendingChaosLeaseWatch()
{
    auto guard = Guard(Lock_);
    YT_VERIFY(PendingChaosLeaseWatches_.size() == 1);
    auto pendingWatch = std::move(PendingChaosLeaseWatches_.back());
    PendingChaosLeaseWatches_.pop_back();
    ChaosLeaseWatchReceivedPromise_ = NewPromise<void>();
    return pendingWatch;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
