#pragma once

#include "store_manager_detail.h"
#include "dynamic_store_bits.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedStoreManager
    : public ISortedStoreManager
{
public:
    TReplicatedStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager = nullptr,
        IInMemoryManagerPtr inMemoryManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr);

    // IStoreManager overrides.
    bool HasActiveLocks() const override;

    bool HasUnflushedStores() const override;

    void StartEpoch(ITabletSlotPtr slot) override;
    void StopEpoch() override;

    bool ExecuteWrites(
        NTableClient::IWireProtocolReader* reader,
        TWriteContext* context) override;

    void UpdateCommittedStoreRowCount() override;

    bool IsOverflowRotationNeeded() const override;
    TError CheckOverflow() const override;
    bool IsRotationPossible() const override;
    bool IsForcedRotationPossible() const override;
    std::optional<TInstant> GetLastPeriodicRotationTime() const override;
    void SetLastPeriodicRotationTime(TInstant value) override;
    bool IsRotationScheduled() const override;
    bool IsFlushNeeded() const override;
    void InitializeRotation() override;
    void ScheduleRotation(NLsm::EStoreRotationReason reason) override;
    void UnscheduleRotation() override;
    void Rotate(bool createNewStore, NLsm::EStoreRotationReason reason, bool allowEmptyStore) override;

    void AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint) override;
    void BulkAddStores(TRange<IStorePtr> stores, bool onMount) override;
    void CreateActiveStore(TDynamicStoreId hintId = {}) override;

    void DiscardAllStores() override;
    void RemoveStore(IStorePtr store) override;
    void BackoffStoreRemoval(IStorePtr store) override;

    bool IsStoreLocked(IStorePtr store) const override;
    std::vector<IStorePtr> GetLockedStores() const override;

    IChunkStorePtr PeekStoreForPreload() override;
    void BeginStorePreload(
        IChunkStorePtr store,
        TCallback<TFuture<void>()> callbackFuture) override;
    void EndStorePreload(IChunkStorePtr store) override;
    void BackoffStorePreload(IChunkStorePtr store) override;

    NTabletClient::EInMemoryMode GetInMemoryMode() const override;

    bool IsStoreFlushable(IStorePtr store) const override;
    TStoreFlushCallback BeginStoreFlush(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;
    void EndStoreFlush(IDynamicStorePtr store) override;
    void BackoffStoreFlush(IDynamicStorePtr store) override;

    bool IsStoreCompactable(IStorePtr store) const override;
    void BeginStoreCompaction(IChunkStorePtr store) override;
    void EndStoreCompaction(IChunkStorePtr store) override;
    void BackoffStoreCompaction(IChunkStorePtr store) override;

    void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) override;
    void Remount(const TTableSettings& settings) override;

    ISortedStoreManagerPtr AsSorted() override;
    IOrderedStoreManagerPtr AsOrdered() override;

    // ISortedStoreManager overrides.
    bool SplitPartition(
        int partitionIndex,
        const std::vector<TLegacyOwningKey>& pivotKeys) override;
    void MergePartitions(
        int firstPartitionIndex,
        int lastPartitionIndex) override;
    void UpdatePartitionSampleKeys(
        TPartition* partition,
        const TSharedRange<TLegacyKey>& keys) override;

private:
    const TTabletManagerConfigPtr Config_;
    TTablet* const Tablet_;
    ITabletContext* const TabletContext_;
    const NHydra::IHydraManagerPtr HydraManager_;
    const IInMemoryManagerPtr InMemoryManager_;
    const NApi::NNative::IClientPtr Client_;

    const NLogging::TLogger Logger;
    const TOrderedStoreManagerPtr LogStoreManager_;

    NTableClient::TUnversionedRowBuilder LogRowBuilder_;
};

DEFINE_REFCOUNTED_TYPE(TReplicatedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
