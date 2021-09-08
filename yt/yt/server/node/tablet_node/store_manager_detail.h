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
    TStoreManagerBase(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager,
        IInMemoryManagerPtr inMemoryManager,
        NApi::NNative::IClientPtr client);

    virtual bool HasActiveLocks() const override;
    virtual bool HasUnflushedStores() const override;

    virtual void StartEpoch(ITabletSlotPtr slot) override;
    virtual void StopEpoch() override;

    virtual void InitializeRotation() override;
    virtual bool IsRotationScheduled() const override;
    virtual void ScheduleRotation(NLsm::EStoreRotationReason reason) override;
    virtual void UnscheduleRotation() override;

    virtual void AddStore(IStorePtr store, bool onMount) override;
    virtual void BulkAddStores(TRange<IStorePtr> stores, bool onMount) override;

    virtual void DiscardAllStores() override;
    virtual void RemoveStore(IStorePtr store) override;
    virtual void BackoffStoreRemoval(IStorePtr store) override;

    virtual bool IsStoreFlushable(IStorePtr store) const override;
    virtual TStoreFlushCallback BeginStoreFlush(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;
    virtual void EndStoreFlush(IDynamicStorePtr store) override;
    virtual void BackoffStoreFlush(IDynamicStorePtr store) override;

    virtual void BeginStoreCompaction(IChunkStorePtr store) override;
    virtual void EndStoreCompaction(IChunkStorePtr store) override;
    virtual void BackoffStoreCompaction(IChunkStorePtr store) override;

    virtual IChunkStorePtr PeekStoreForPreload() override;
    virtual void BeginStorePreload(
        IChunkStorePtr store,
        TCallback<TFuture<void>()> callbackFuture) override;
    virtual void EndStorePreload(IChunkStorePtr store) override;
    virtual void BackoffStorePreload(IChunkStorePtr store) override;

    virtual NTabletClient::EInMemoryMode GetInMemoryMode() const override;

    virtual void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) override;
    virtual void Remount(const TTableSettings& settings) override;

    virtual void Rotate(bool createNewStore, NLsm::EStoreRotationReason reason) override;

    virtual bool IsStoreLocked(IStorePtr store) const override;
    virtual std::vector<IStorePtr> GetLockedStores() const override;

    virtual bool IsOverflowRotationNeeded() const override;
    virtual TError CheckOverflow() const override;
    virtual bool IsPeriodicRotationNeeded() const override;
    virtual bool IsRotationPossible() const override;
    virtual bool IsForcedRotationPossible() const override;
    virtual std::optional<TInstant> GetPeriodicRotationMilestone() const override;

    virtual ISortedStoreManagerPtr AsSorted() override;
    virtual IOrderedStoreManagerPtr AsOrdered() override;

    void UpdatePeriodicRotationMilestone();

protected:
    const TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;
    ITabletContext* const TabletContext_;
    const NHydra::IHydraManagerPtr HydraManager_;
    const IInMemoryManagerPtr InMemoryManager_;
    const NApi::NNative::IClientPtr Client_;

    bool RotationScheduled_ = false;
    std::optional<TInstant> PeriodicRotationMilestone_;

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

    virtual void CreateActiveStore() = 0;

    TDynamicStoreId GenerateDynamicStoreId();

    void CheckForUnlockedStore(IDynamicStore* store);

    void InvalidateCachedChunkReaders();

    void UpdateInMemoryMode();

    bool TryPreloadStoreFromInterceptedData(
        IChunkStorePtr store,
        TInMemoryChunkDataPtr chunkData);

    bool IsLeader() const;
    bool IsRecovery() const;
    bool IsMutationLoggingEnabled() const;

    TTimestamp GenerateMonotonicCommitTimestamp(TTimestamp timestampHint);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
