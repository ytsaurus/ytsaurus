#pragma once

#include "dynamic_store_bits.h"
#include "tablet_profiling.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TStoreFlushResult
{
    std::vector<NTabletNode::NProto::TAddStoreDescriptor> StoresToAdd;
    std::vector<NTabletNode::NProto::TAddHunkChunkDescriptor> HunkChunksToAdd;
};

using TStoreFlushCallback = TCallback<TStoreFlushResult(
    const NApi::ITransactionPtr& transaction,
    const NConcurrency::IThroughputThrottlerPtr& throttler,
    TTimestamp currentTimestamp,
    const TWriterProfilerPtr& writerProfiler)>;

//! Provides a facade for modifying data within a given tablet.
/*!
 *  Each tablet has an instance of IStoreManager, which is attached to the tablet
 *  upon its construction.
 *
 *  IStoreManager instances are not bound to any specific epoch and are reused.
 */
struct IStoreManager
    : public virtual TRefCounted
{
    //! Returns |true| if there are outstanding locks to any of dynamic memory stores.
    //! Used to determine when it is safe to unmount the tablet.
    virtual bool HasActiveLocks() const = 0;

    //! Returns |true| if there are some dynamic memory stores that are not flushed yet.
    virtual bool HasUnflushedStores() const = 0;

    virtual void StartEpoch(ITabletSlotPtr slot) = 0;
    virtual void StopEpoch() = 0;

    virtual bool ExecuteWrites(
        NTableClient::IWireProtocolReader* reader,
        TWriteContext* context) = 0;

    virtual void UpdateCommittedStoreRowCount() = 0;

    virtual bool IsOverflowRotationNeeded() const = 0;
    virtual TError CheckOverflow() const = 0;
    virtual bool IsRotationPossible() const = 0;
    virtual bool IsForcedRotationPossible() const = 0;
    virtual std::optional<TInstant> GetLastPeriodicRotationTime() const = 0;
    virtual void SetLastPeriodicRotationTime(TInstant value) = 0;
    virtual bool IsRotationScheduled() const = 0;
    virtual bool IsFlushNeeded() const = 0;
    virtual void InitializeRotation() = 0;
    virtual void ScheduleRotation(NLsm::EStoreRotationReason reason) = 0;
    virtual void UnscheduleRotation() = 0;
    virtual void Rotate(bool createNewStore, NLsm::EStoreRotationReason reason, bool allowEmptyStore = false) = 0;

    virtual void AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint = {}) = 0;
    virtual void BulkAddStores(TRange<IStorePtr> stores, bool onMount) = 0;
    virtual void CreateActiveStore(TDynamicStoreId hintId = {}) = 0;

    virtual void DiscardAllStores() = 0;
    virtual void RemoveStore(IStorePtr store) = 0;
    virtual void BackoffStoreRemoval(IStorePtr store) = 0;

    virtual bool IsStoreLocked(IStorePtr store) const = 0;
    virtual std::vector<IStorePtr> GetLockedStores() const = 0;

    virtual IChunkStorePtr PeekStoreForPreload() = 0;
    virtual void BeginStorePreload(
        IChunkStorePtr store,
        TCallback<TFuture<void>()> callbackFuture) = 0;
    virtual void EndStorePreload(IChunkStorePtr store) = 0;
    virtual void BackoffStorePreload(IChunkStorePtr store) = 0;

    virtual NTabletClient::EInMemoryMode GetInMemoryMode() const = 0;

    virtual bool IsStoreFlushable(IStorePtr store) const = 0;
    virtual TStoreFlushCallback BeginStoreFlush(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) = 0;
    virtual void EndStoreFlush(IDynamicStorePtr store) = 0;
    virtual void BackoffStoreFlush(IDynamicStorePtr store) = 0;

    virtual bool IsStoreCompactable(IStorePtr store) const = 0;
    virtual void BeginStoreCompaction(IChunkStorePtr store) = 0;
    virtual void EndStoreCompaction(IChunkStorePtr store) = 0;
    virtual void BackoffStoreCompaction(IChunkStorePtr store) = 0;

    virtual void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) = 0;
    virtual void Remount(const TTableSettings& settings) = 0;

    virtual ISortedStoreManagerPtr AsSorted() = 0;
    virtual IOrderedStoreManagerPtr AsOrdered() = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreManager)

////////////////////////////////////////////////////////////////////////////////

//! A refinement of IStoreManager for sorted tablets.
struct ISortedStoreManager
    : public virtual IStoreManager
{
    virtual bool SplitPartition(
        int partitionIndex,
        const std::vector<TLegacyOwningKey>& pivotKeys) = 0;
    virtual void MergePartitions(
        int firstPartitionIndex,
        int lastPartitionIndex) = 0;
    virtual void UpdatePartitionSampleKeys(
        TPartition* partition,
        const TSharedRange<TLegacyKey>& keys) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortedStoreManager)

////////////////////////////////////////////////////////////////////////////////

//! A refinement of IStoreManager for ordered tablets.
struct IOrderedStoreManager
    : public virtual IStoreManager
{
    virtual void LockHunkStores(TWriteContext* context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrderedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
