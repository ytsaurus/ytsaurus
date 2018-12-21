#pragma once

#include "store_manager_detail.h"
#include "dynamic_store_bits.h"
#include "sorted_dynamic_store.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

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
    virtual bool ExecuteWrites(
        NTableClient::TWireProtocolReader* reader,
        TWriteContext* context) override;

    TSortedDynamicRowRef ModifyRow(
        TUnversionedRow row,
        NApi::ERowModificationType modificationType,
        TWriteContext* context);

    TSortedDynamicRowRef ModifyRow(
        TVersionedRow row,
        TWriteContext* context);

    virtual void StartEpoch(TTabletSlotPtr slot) override;
    virtual void StopEpoch() override;

    void LockRow(TTransaction* transaction, bool prelock, const TSortedDynamicRowRef& rowRef);
    void ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);
    void AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef);

    virtual void Mount(
        const std::vector<NTabletNode::NProto::TAddStoreDescriptor>& storeDescriptors) override;
    virtual void Remount(
        TTableMountConfigPtr mountConfig,
        TTabletChunkReaderConfigPtr readerConfig,
        TTabletChunkWriterConfigPtr writerConfig,
        TTabletWriterOptionsPtr writerOptions) override;

    virtual void AddStore(IStorePtr store, bool onMount) override;
    virtual void RemoveStore(IStorePtr store) override;

    virtual bool IsFlushNeeded() const override;
    virtual bool IsStoreCompactable(IStorePtr store) const override;

    virtual ISortedStoreManagerPtr AsSorted() override;

    // ISortedStoreManager overrides.
    virtual bool SplitPartition(
        int partitionIndex,
        const std::vector<TOwningKey>& pivotKeys) override;
    virtual void MergePartitions(
        int firstPartitionIndex,
        int lastPartitionIndex) override;
    virtual void UpdatePartitionSampleKeys(
        TPartition* partition,
        const TSharedRange<TKey>& keys) override;

    virtual bool IsOverflowRotationNeeded() const override;
    virtual TError CheckOverflow() const override;

private:
    const int KeyColumnCount_;

    TSortedDynamicStorePtr ActiveStore_;
    std::multimap<TTimestamp, ISortedStorePtr> MaxTimestampToStore_;

    virtual IDynamicStore* GetActiveStore() const override;
    virtual void ResetActiveStore() override;
    virtual void OnActiveStoreRotated() override;

    virtual TStoreFlushCallback MakeStoreFlushCallback(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;

    virtual void CreateActiveStore() override;

    ui32 ComputeLockMask(TUnversionedRow row);

    bool CheckInactiveStoresLocks(
        TUnversionedRow row,
        ui32 lockMask,
        TWriteContext* context);

    void SchedulePartitionSampling(TPartition* partition);
    void SchedulePartitionsSampling(int beginPartitionIndex, int endPartitionIndex);

    void DoSplitPartition(
        int partitionIndex,
        const std::vector<TOwningKey>& pivotKeys);
    void DoMergePartitions(
        int firstPartitionIndex,
        int lastPartitionIndex);

    TSortedDynamicStore::TRowBlockedHandler CreateRowBlockedHandler(
        const IStorePtr& store);
    void OnRowBlocked(
        IStore* store,
        IInvokerPtr invoker,
        TSortedDynamicRow row,
        int lockIndex);
    void WaitOnBlockedRow(
        IStorePtr store,
        TSortedDynamicRow row,
        int lockIndex);
};

DEFINE_REFCOUNTED_TYPE(TSortedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
