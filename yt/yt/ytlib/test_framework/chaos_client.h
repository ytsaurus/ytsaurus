#pragma once

#include "public.h"

#include <yt/yt/ytlib/chaos_client/chaos_leases_watcher_client.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TTestChaosResidencyCache
    : public IChaosResidencyCache
{
public:
    TFuture<NObjectClient::TCellTag> GetChaosResidency(NObjectClient::TObjectId objectId) override;
    void ForceRefresh(
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag cellTag) override;
    void UpdateChaosObjectResidency(
        NObjectClient::TObjectId objectId,
        NObjectClient::TCellTag cellTag) override;
    void RemoveChaosObjectResidency(NObjectClient::TObjectId objectId) override;
    void PingChaosObjectResidency(NObjectClient::TObjectId objectId) override;
    void Clear() override;
    void Reconfigure(TChaosResidencyCacheConfigPtr config) override;

    std::optional<std::pair<NObjectClient::TObjectId, NObjectClient::TCellTag>> GetLastUpdatedResidency() const;
    NObjectClient::TObjectId GetLastRemovedObjectId() const;
    NObjectClient::TObjectId GetLastPingedObjectId() const;

private:
    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::optional<std::pair<NObjectClient::TObjectId, NObjectClient::TCellTag>> LastUpdatedResidency_;
    NObjectClient::TObjectId LastRemovedObjectId_;
    NObjectClient::TObjectId LastPingedObjectId_;
};

DEFINE_REFCOUNTED_TYPE(TTestChaosResidencyCache)

////////////////////////////////////////////////////////////////////////////////

class TTestChaosConnection
    : public TTestConnection
{
public:
    TTestChaosConnection(
        NRpc::IChannelFactoryPtr channelFactory,
        NRpc::IChannelPtr chaosChannel,
        IInvokerPtr invoker,
        INodeMemoryTrackerPtr memoryTracker,
        IChaosResidencyCachePtr residencyCache);

    NObjectClient::TCellId GetPrimaryMasterCellId() const override;
    NObjectClient::TCellTag GetPrimaryMasterCellTag() const override;
    NObjectClient::TCellTagList GetSecondaryMasterCellTags() const override;
    NObjectClient::TCellId GetMasterCellId(NObjectClient::TCellTag cellTag) const override;

    NRpc::IChannelPtr GetChaosChannelByObjectIdOrThrow(
        TChaosObjectId objectId,
        NHydra::EPeerKind peerKind) override;
    const IChaosResidencyCachePtr& GetChaosResidencyCache() override;

private:
    const NRpc::IChannelPtr ChaosChannel_;
    const IChaosResidencyCachePtr ResidencyCache_;
    const NObjectClient::TCellId PrimaryMasterCellId_;
};

DEFINE_REFCOUNTED_TYPE(TTestChaosConnection)

////////////////////////////////////////////////////////////////////////////////

class TTestChaosLeaseWatcherClientCallbackState
    : public TRefCounted
{
public:
    struct TUpdate
    {
        TChaosLeaseId ChaosLeaseId;
        TChaosLeasePtr ChaosLease;
        NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
    };

    void OnUpdated(
        TChaosLeaseId chaosLeaseId,
        TChaosLeasePtr chaosLease,
        NTransactionClient::TTimestamp timestamp);
    void OnDeleted(TChaosLeaseId chaosLeaseId);
    void OnUnknown(TChaosLeaseId chaosLeaseId);
    void OnMigrated(TChaosLeaseId chaosLeaseId);
    void OnNothingChanged(TChaosLeaseId chaosLeaseId);

    TFuture<void> GetUpdatedFuture() const;
    TFuture<void> GetDeletedFuture() const;
    TFuture<void> GetUnknownFuture() const;
    TFuture<void> GetMigratedFuture() const;
    TFuture<void> GetUnchangedFuture() const;

    std::optional<TUpdate> GetLastUpdate() const;
    TChaosLeaseId GetLastDeletedChaosLeaseId() const;
    TChaosLeaseId GetLastUnknownChaosLeaseId() const;
    TChaosLeaseId GetLastMigratedChaosLeaseId() const;
    TChaosLeaseId GetLastUnchangedChaosLeaseId() const;

private:
    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::optional<TUpdate> LastUpdate_;
    TChaosLeaseId LastDeletedChaosLeaseId_;
    TChaosLeaseId LastUnknownChaosLeaseId_;
    TChaosLeaseId LastMigratedChaosLeaseId_;
    TChaosLeaseId LastUnchangedChaosLeaseId_;

    TPromise<void> UpdatedPromise_ = NewPromise<void>();
    TPromise<void> DeletedPromise_ = NewPromise<void>();
    TPromise<void> UnknownPromise_ = NewPromise<void>();
    TPromise<void> MigratedPromise_ = NewPromise<void>();
    TPromise<void> UnchangedPromise_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TTestChaosLeaseWatcherClientCallbackState)

std::unique_ptr<IChaosLeaseWatcherClientCallbacks> CreateTestChaosLeaseWatcherClientCallbacks(
    TTestChaosLeaseWatcherClientCallbackStatePtr state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
