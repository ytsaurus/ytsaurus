#pragma once

#include "public.h"

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TTestChaosNodeService
    : public NRpc::TServiceBase
{
public:
    TTestChaosNodeService(
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    int GetPendingChaosLeaseWatchCount() const;
    TChaosLeaseId GetPendingChaosLeaseId() const;
    NTransactionClient::TTimestamp GetPendingChaosLeaseTimestamp() const;
    TFuture<void> GetChaosLeaseWatchReceivedFuture() const;

    void ReplyChaosLeaseChanged(
        NTransactionClient::TTimestamp timestamp,
        TDuration timeout,
        const std::vector<NObjectClient::TCellId>& coordinatorCellIds);
    void ReplyChaosLeaseNotChanged();
    void ReplyChaosLeaseMigrated(NObjectClient::TCellId destinationCellId);
    void ReplyChaosLeaseInstanceIsNotLeader();
    void ReplyChaosLeaseDeleted();
    void ReplyChaosLeaseUnknown();
    void ReplyAllChaosLeaseWatchesDeleted();

private:
    using TWatchContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NProto::TReqWatchChaosLease,
        NProto::TRspWatchChaosLease>>;

    struct TPendingChaosLeaseWatch
    {
        TWatchContextPtr Context;
        TChaosLeaseId ChaosLeaseId;
        NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
    };

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::vector<TPendingChaosLeaseWatch> PendingChaosLeaseWatches_;
    TPromise<void> ChaosLeaseWatchReceivedPromise_ = NewPromise<void>();

    DECLARE_RPC_SERVICE_METHOD(NProto, WatchChaosLease);

    TPendingChaosLeaseWatch TakePendingChaosLeaseWatch();
};

DEFINE_REFCOUNTED_TYPE(TTestChaosNodeService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
