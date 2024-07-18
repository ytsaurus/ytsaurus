#pragma once

#include "store_manager.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/lsm/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreManagerBase
    : public virtual IStoreManager
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTablet*, Tablet);

public:
    TStoreManagerBase(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager,
        IInMemoryManagerPtr inMemoryManager,
        NApi::NNative::IClientPtr client);

    bool HasActiveLocks() const override;
    bool HasUnflushedStores() const override;

    void StartEpoch(ITabletSlotPtr slot) override;
    void StopEpoch() override;

    void InitializeRotation() override;
    bool IsRotationScheduled() const override;
    void ScheduleRotation(NLsm::EStoreRotationReason reason) override;
    void UnscheduleRotation() override;

    void AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint = {}) override;
    void BulkAddStores(TRange<IStorePtr> stores, bool onMount) override;

    void DiscardAllStores() override;
    void RemoveStore(IStorePtr store) override;
    void BackoffStoreRemoval(IStorePtr store) override;

    bool IsStoreFlushable(IStorePtr store) const override;
    TStoreFlushCallback BeginStoreFlush(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;
    void EndStoreFlush(IDynamicStorePtr store) override;
    void BackoffStoreFlush(IDynamicStorePtr store) override;

    void BeginStoreCompaction(IChunkStorePtr store) override;
    void EndStoreCompaction(IChunkStorePtr store) override;
    void BackoffStoreCompaction(IChunkStorePtr store) override;

    IChunkStorePtr PeekStoreForPreload() override;
    void BeginStorePreload(
        IChunkStorePtr store,
        TCallback<TFuture<void>()> callbackFuture) override;
    void EndStorePreload(IChunkStorePtr store) override;
    void BackoffStorePreload(IChunkStorePtr store) override;

    NTabletClient::EInMemoryMode GetInMemoryMode() const override;

    void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) override;
    void Remount(const TTableSettings& settings) override;

    void Rotate(bool createNewStore, NLsm::EStoreRotationReason reason, bool allowEmptyStore = false) override;

    bool IsStoreLocked(IStorePtr store) const override;
    std::vector<IStorePtr> GetLockedStores() const override;

    bool IsOverflowRotationNeeded() const override;
    TError CheckOverflow() const override;
    bool IsRotationPossible() const override;
    bool IsForcedRotationPossible() const override;
    std::optional<TInstant> GetLastPeriodicRotationTime() const override;
    void SetLastPeriodicRotationTime(TInstant value) override;

    void UpdateCommittedStoreRowCount() override;

    ISortedStoreManagerPtr AsSorted() override;
    IOrderedStoreManagerPtr AsOrdered() override;

protected:
    const TTabletManagerConfigPtr Config_;
    ITabletContext* const TabletContext_;
    const NHydra::IHydraManagerPtr HydraManager_;
    const IInMemoryManagerPtr InMemoryManager_;
    const NApi::NNative::IClientPtr Client_;

    bool RotationScheduled_ = false;
    std::optional<TInstant> LastPeriodicRotationTime_;

    THashSet<IStorePtr> LockedStores_;

    IPerTabletStructuredLoggerPtr StructuredLogger_;

    NLogging::TLogger Logger;

    virtual IDynamicStore* GetActiveStore() const = 0;
    virtual void ResetActiveStore() = 0;
    virtual void OnActiveStoreRotated() = 0;

    virtual TStoreFlushCallback MakeStoreFlushCallback(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) = 0;

    TDynamicStoreId GenerateDynamicStoreId();

    void CheckForUnlockedStore(IDynamicStore* store);

    void InvalidateCachedChunkReaders();

    void UpdateInMemoryMode();

    bool TryPreloadStoreFromInterceptedData(
        IChunkStorePtr store,
        TInMemoryChunkDataPtr chunkData);

    bool IsLeader() const;
    bool IsRecovery() const;

    TTimestamp GenerateMonotonicCommitTimestamp(TTimestamp timestampHint);

private:
    void ResetLastPeriodicRotationTime();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
