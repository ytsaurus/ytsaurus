#pragma once

#include "store_manager_detail.h"
#include "dynamic_store_bits.h"
#include "sorted_dynamic_store.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManager
    : public TStoreManagerBase
    , public ISortedStoreManager
{
public:
    TSortedStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager = nullptr,
        IInMemoryManagerPtr inMemoryManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr);

    // IStoreManager overrides.
    bool ExecuteWrites(
        NTableClient::IWireProtocolReader* reader,
        TWriteContext* context) override;

    TSortedDynamicRowRef ModifyRow(
        TUnversionedRow row,
        NApi::ERowModificationType modificationType,
        TLockMask readLockMask,
        TWriteContext* context);

    TSortedDynamicRowRef ModifyRow(
        TVersionedRow row,
        TWriteContext* context);

    void StartEpoch(ITabletSlotPtr slot) override;
    void StopEpoch() override;

    void LockRow(TWriteContext* context, bool prelock, const TSortedDynamicRowRef& rowRef);
    void ConfirmRow(TWriteContext* context, const TSortedDynamicRowRef& rowRef);
    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);

    // Returns false if key obtained from wire protocol differs from key obtained from #rowRef.
    bool CommitRow(
        TTransaction* transaction,
        const NTableClient::TWireProtocolWriteCommand& command,
        const TSortedDynamicRowRef& rowRef);

    void AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);

    void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) override;
    void Remount(const TTableSettings& settings) override;

    void AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint = {}) override;
    void BulkAddStores(TRange<IStorePtr> stores, bool onMount) override;
    void DiscardAllStores() override;
    void RemoveStore(IStorePtr store) override;
    void CreateActiveStore(TDynamicStoreId hintId = {}) override;

    bool IsFlushNeeded() const override;
    bool IsStoreCompactable(IStorePtr store) const override;
    bool IsStoreFlushable(IStorePtr store) const override;

    ISortedStoreManagerPtr AsSorted() override;

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

    bool IsOverflowRotationNeeded() const override;
    TError CheckOverflow() const override;

private:
    struct TBoundaryDescriptor
    {
        TLegacyOwningKey Key;
        int Type;
        int DescriptorIndex;
        i64 DataSize;
    };

    const int KeyColumnCount_;

    TSortedDynamicStorePtr ActiveStore_;
    std::multimap<TTimestamp, ISortedStorePtr> MaxTimestampToStore_;

    // During changelog replay stores may be removed out of order.
    std::set<ui32> StoreFlushIndexQueue_;

    IDynamicStore* GetActiveStore() const override;
    void ResetActiveStore() override;
    void OnActiveStoreRotated() override;

    TStoreFlushCallback MakeStoreFlushCallback(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;


    bool CheckInactiveStoresLocks(
        TUnversionedRow row,
        TLockMask lockMask,
        TWriteContext* context);

    void SchedulePartitionSampling(TPartition* partition);
    void SchedulePartitionsSampling(int beginPartitionIndex, int endPartitionIndex);

    void TrySplitPartitionByAddedStores(
        TPartition* partition,
        std::vector<ISortedStorePtr> addedStores);
    void DoSplitPartition(
        int partitionIndex,
        const std::vector<TLegacyOwningKey>& pivotKeys);
    void DoMergePartitions(
        int firstPartitionIndex,
        int lastPartitionIndex);

    TSortedDynamicStore::TRowBlockedHandler CreateRowBlockedHandler(
        const IStorePtr& store);
    void OnRowBlocked(
        IStore* store,
        IInvokerPtr invoker,
        TSortedDynamicRow row,
        TSortedDynamicStore::TConflictInfo conflictInfo,
        TDuration timeout);
    void WaitOnBlockedRow(
        IStorePtr store,
        TSortedDynamicRow row,
        TSortedDynamicStore::TConflictInfo conflictInfo,
        TDuration timeout);

    void BuildPivotKeys(
        std::vector<TLegacyOwningKey>* pivotKeys,
        const std::vector<TBoundaryDescriptor>& chunkBoundaries);
};

DEFINE_REFCOUNTED_TYPE(TSortedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
