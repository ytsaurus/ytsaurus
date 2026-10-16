#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/chaos_lease.h>
#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IChaosLeaseWatcherClientCallbacks
{
    virtual ~IChaosLeaseWatcherClientCallbacks() = default;

    virtual void OnChaosLeaseUpdated(
        TChaosLeaseId chaosLeaseId,
        const TChaosLeasePtr& chaosLease,
        NTransactionClient::TTimestamp timestamp) = 0;

    virtual void OnChaosLeaseDeleted(TChaosLeaseId chaosLeaseId) = 0;
    virtual void OnUnknownChaosLease(TChaosLeaseId chaosLeaseId) = 0;
    virtual void OnChaosLeaseMigrated(TChaosLeaseId chaosLeaseId) = 0;
    virtual void OnNothingChanged(TChaosLeaseId chaosLeaseId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IChaosLeasesWatcherClient
    : public virtual TRefCounted
{
    virtual void WatchChaosLease(TChaosLeaseId chaosLeaseId) = 0;
    virtual void StopWatchingChaosLease(TChaosLeaseId chaosLeaseId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosLeasesWatcherClient)

IChaosLeasesWatcherClientPtr CreateChaosLeasesWatcherClient(
    std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
    TWeakPtr<NApi::NNative::IConnection> connection);

IChaosLeasesWatcherClientPtr CreateChaosLeasesWatcherClient(
    std::unique_ptr<IChaosLeaseWatcherClientCallbacks> callbacks,
    NRpc::IChannelPtr chaosCacheChannel,
    TWeakPtr<NApi::NNative::IConnection> connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
