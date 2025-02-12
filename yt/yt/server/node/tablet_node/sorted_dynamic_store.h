#pragma once

#include "private.h"
#include "dynamic_store_bits.h"
#include "sorted_dynamic_comparer.h"
#include "store_detail.h"
#include "transaction.h"
#include "store_manager.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/chunked_vector.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicStore
    : public TDynamicStoreBase
    , public TSortedStoreBase
{
public:
    TSortedDynamicStore(
        TStoreId id,
        TTablet* tablet,
        IStoreContextPtr context);
    ~TSortedDynamicStore();

    //! Returns the reader to be used during flush.
    NTableClient::IVersionedReaderPtr CreateFlushReader();

    //! Returns the reader to be used during store serialization.
    NTableClient::IVersionedReaderPtr CreateSnapshotReader();


    //! Returns the cached instance of row key comparer
    //! (obtained by calling TTablet::GetRowKeyComparer).
    const TSortedDynamicRowKeyComparer& GetRowKeyComparer() const;

    struct TConflictInfo
    {
        int LockIndex;
        TTimestamp ReadTimestamp;
    };

    using TRowBlockedHandler = TCallback<void(TSortedDynamicRow row, TConflictInfo conflictInfo, TDuration timeout)>;

    //! Sets the handler that is being invoked when read request faces a blocked row.
    void SetRowBlockedHandler(TRowBlockedHandler handler);

    //! Clears the blocked row handler.
    void ResetRowBlockedHandler();

    //! Checks if a given #row has any locks from #lockMask with prepared timestamp
    //! less than #timestamp. If so, raises |RowBlocked| signal and loops.
    void WaitOnBlockedRow(
        TSortedDynamicRow row,
        TLockMask lockMask,
        TTimestamp timestamp);

    //! Modifies (writes or deletes) a row.
    /*!
     *  If #commitTimestamp is not null then no locks are checked or taken.
     *  #transaction could be null.
     *  The row is committed immediately.
     *
     *  If #commitTimestamp is null then checks and takes the locks.
     *  #transaction cannot be null.
     *
     *  On lock failure, throws TErrorException explaining the cause.
     *
     *  If a blocked row is encountered, fills the appropriate fields in #context
     *  and returns null.
     */
    TSortedDynamicRow ModifyRow(
        NTableClient::TUnversionedRow row,
        TLockMask lockMask,
        bool isDelete,
        TWriteContext* context);

    //! Writes a versioned row into the store.
    /*!
     *  No locks are checked. Timestamps are taken directly from #row.
     */
    TSortedDynamicRow ModifyRow(
        NTableClient::TVersionedRow row,
        TWriteContext* context);

    TSortedDynamicRow MigrateSharedWriteLockedLockGroup(
        TTransaction* transaction,
        TSortedDynamicRow row,
        int lockIndex);
    TSortedDynamicRow MigrateRow(
        TTransaction* transaction,
        TSortedDynamicRow row,
        TLockMask lockMask,
        bool skipSharedWriteLocks);
    void PrepareRow(TTransaction* transaction, TSortedDynamicRow row);
    void CommitRowPartsWithNoNeedForSerialization(
        TTransaction* transaction,
        TSortedDynamicRow row,
        TLockMask lockMask,
        bool onAfterSnapshotLoaded);

    void CommitLockGroup(
        TTransaction* transaction,
        TSortedDynamicRow row,
        NTableClient::ELockType lockType,
        int lockIndex,
        bool onAfterSnapshotLoaded);

    void CommitRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask lockMask);
    void AbortRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask lockMask);
    void DeleteRow(TTransaction* transaction, TSortedDynamicRow row);
    void WriteRow(TTransaction* transaction, TSortedDynamicRow dynamicRow, TUnversionedRow row);
    void WriteLockGroup(TTransaction* transaction, int lockIndex, TSortedDynamicRow dynamicRow, TUnversionedRow row);

    // The following functions are made public for unit-testing.
    TSortedDynamicRow FindRow(NTableClient::TUnversionedRow key);
    std::vector<TSortedDynamicRow> GetAllRows();
    Y_FORCE_INLINE TTimestamp TimestampFromRevision(ui32 revision) const;
    TTimestamp GetLastWriteTimestamp(TSortedDynamicRow row, int lockIndex);

    TTimestamp GetLastExclusiveTimestamp(TSortedDynamicRow row, int lockIndex);
    TTimestamp GetLastSharedWriteTimestamp(TSortedDynamicRow row, int lockIndex);
    TTimestamp GetLastReadTimestamp(TSortedDynamicRow row, int lockIndex);

    // IStore implementation.
    EStoreType GetType() const override;
    i64 GetRowCount() const override;

    // IDynamicStore implementation.
    i64 GetTimestampCount() const override;

    // ISortedStore implementation.
    TLegacyOwningKey GetMinKey() const override;
    TLegacyOwningKey GetUpperBoundKey() const override;
    bool HasNontrivialReadRange() const override;

    i64 GetMaxDataWeight() const;
    TLegacyOwningKey GetMaxDataWeightWitnessKey() const;

    NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) override;

    NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) override;

    bool CheckRowLocks(
        TUnversionedRow row,
        TLockMask lockMask,
        TWriteContext* context) override;

    void Save(TSaveContext& context) const override;
    void Load(TLoadContext& context) override;

    TCallback<void(TSaveContext&)> AsyncSave() override;
    void AsyncLoad(TLoadContext& context) override;

    TSortedDynamicStorePtr AsSortedDynamic() override;

    void SetBackupCheckpointTimestamp(TTimestamp timestamp) override;

    // Passive dynamic stores loaded from snapshot can be flushed in arbitrary order.
    // Their flush index is null.
    DEFINE_BYVAL_RW_PROPERTY(ui32, FlushIndex, 0);

    bool IsMergeRowsOnFlushAllowed() const;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    class TReaderBase;
    class TRangeReader;
    class TLookupReader;
    class TLookupHashTable;

    const TSortedDynamicRowKeyComparer RowKeyComparer_;
    const std::unique_ptr<TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>> Rows_;
    std::unique_ptr<TLookupHashTable> LookupHashTable_;

    ui32 FlushRevision_ = InvalidRevision;

    // Generic information:
    // Column's values are sorted by timestamp and each value annotated with revision.
    // Each revision corresponds to timestamp.
    // Revisions are monotonic.
    // Timestamps corresponding to revisions are monotonic ONLY within specific column.
    //
    // Example:
    // Suppose there are 3 txs and:
    // Tx1: Write(Key1, Value1_1) with ts=1
    // Tx2: Write(Key1, Value2_1) with ts=2
    // Tx3: Write(Key2, Value1_2) with ts=3
    //
    // And they were committed to sorted dynamic store in this order: Tx1, Tx3, Tx2
    // This order determines revision to timestamp correspondence.
    //
    // Then after all three transactions are committed sorted dynamic store state will be like this:
    // Key1: [Value1_1, r=1, ts=1, Value_2_1, r=3, ts=2]
    // Key2: [Value1_2, r=2, ts=3]
    // Revisions: [ts=0, ts=1, ts=3, ts=2]
    //
    // Registration:
    // Revisions registered in different mutations are always different for consistent read wrt snapshot creation and store rotation.
    // For coarsely serialized transactions revisions registered in CommitLockGroup and used during current __transaction commit__ mutation.
    // For per-row serialized transaction revisions registered in CommitRowPartsWithNoNeedForSerialization and used as long as transaction parts are being committed.
    //
    // Persistence:
    // Revisions are store-specific and transient.
    // Concurrently with async save there could be transaction with smaller timestamp that was committed after save started.
    // As it is committed in mutation after snapshot its content should not be visible in snapshot.
    // Revisions solves it providing mutation-monotonic counter.
    // Revisions recalculated during AsyncLoad.
    // Recalculated revisions could differ from original revisions as long as they allow to generate consistent snapshot/chunk in future.
    //
    // Per-row serialization specific:
    // Transaction parts that are committed in different mutations have same revisions.
    // To keep snapshot consistent for each column there is a special last write timestamp (see CurrentLastWriteTimestampIndex_) that is fixed at the moment of each AsyncSave start.
    // Snapshot reader reads all values that have revision less that SnapshotRevision and timestamp less than per-column last write timestamp.
    // Flush reader reads all values that only have revision less that FlushRevision.
    // That means that migrated per-row serialized values will be written in two chunks.
    static const size_t RevisionsPerChunk = 1ULL << 13;
    static const size_t MaxRevisionChunks = HardRevisionsPerDynamicStoreLimit / RevisionsPerChunk + 1;
    TChunkedVector<TTimestamp, RevisionsPerChunk> RevisionToTimestamp_;

    // Unused as long as tablet serialization type is coarse.
    std::map<
        TTransaction*,
        ui32,
        std::less<TTransaction*>,
        TChunkedMemoryPoolAllocator<std::pair<TTransaction* const, ui32>>> CommitRevisionPerTransaction_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, RowBlockedLock_);
    TRowBlockedHandler RowBlockedHandler_;

    // Reused between ModifyRow calls.
    std::vector<ui32> WriteRevisions_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;

    i64 MaxDataWeight_ = 0;
    TSortedDynamicRow MaxDataWeightWitness_;

    bool MergeRowsOnFlushAllowed_ = true;

    // For each column there is two last write timestamps: active and non-active. Active should be greater or equal than non-active.
    // Active is used for writes. Non-active is used for read during snapshot save.
    // Changing CurrentLastWriteTimestampIndex_ is enough to swap them.
    // After swap active will be outdated and could stay outdated until the next snapshot.
    // To avoid reads during AsyncSave also update active timestamp to become at least non-active.
    int CurrentLastWriteTimestampIndex_ = 0;

    void OnSetPassive() override;
    void OnSetRemoved() override;

    TSortedDynamicRow AllocateRow();

    TRowBlockedHandler GetRowBlockedHandler();

    int GetBlockingLockIndex(
        TSortedDynamicRow row,
        TLockMask lockMask,
        TTimestamp timestamp);
    bool CheckRowBlocking(
        TSortedDynamicRow row,
        TLockMask lockMask,
        TWriteContext* context);

    TError CheckRowLocks(
        TSortedDynamicRow row,
        TTransaction* transaction,
        TLockMask lockMask);
    void AcquireRowLocks(
        TSortedDynamicRow row,
        TLockMask lockMask,
        bool isDelete,
        TWriteContext* context);

    void AddDeleteRevision(TSortedDynamicRow row, ui32 revision);
    void AddWriteRevision(TLockDescriptor& lock, ui32 revision);

    void AddExclusiveLockRevision(TLockDescriptor& lock, ui32 revision);
    void AddSharedWriteLockRevision(TLockDescriptor& lock, ui32 revision);
    void AddReadLockRevision(TLockDescriptor& lock, ui32 revision);

    void SetKeys(TSortedDynamicRow dstRow, const TUnversionedValue* srcKeys);
    void SetKeys(TSortedDynamicRow dstRow, TSortedDynamicRow srcRow);
    void AddValue(TSortedDynamicRow row, int index, TDynamicValue value);

    void WriteLockGroup(int lockIndex, TSortedDynamicRow dynamicRow, TUnversionedRow row, ui32 revision);
    void WriteColumn(int columnIndex, TSortedDynamicRow dynamicRow, TUnversionedRow row, ui32 revision);
    void WriteRow(TSortedDynamicRow dynamicRow, TUnversionedRow row, ui32 revision);

    void CommitLockGroup(
        TTransaction* transaction,
        TSortedDynamicRow row,
        NTableClient::ELockType lockType,
        TLockDescriptor* lock,
        bool perRowSerialized,
        bool onAfterSnapshotLoaded);

    void DrainSerializationHeap(
        TSortedDynamicRow row,
        int lockIndex,
        bool onAfterSnapshotLoaded);

    struct TLoadScratchData
    {
        TTimestampToRevisionMap TimestampToRevision;
        std::vector<std::vector<ui32>> WriteRevisions;
        TTimestamp* LastExclusiveLockTimestamps;
        TTimestamp* LastSharedWriteLockTimestamps;
        TTimestamp* LastReadLockTimestamps;
    };

    void LoadRow(TVersionedRow row, TLoadScratchData* scratchData);
    ui32 CaptureTimestamp(TTimestamp timestamp, TTimestampToRevisionMap* scratchData);
    ui32 CaptureVersionedValue(TDynamicValue* dst, const TVersionedValue& src, TTimestampToRevisionMap* scratchData);

    void CaptureUnversionedValue(TDynamicValue* dst, const TUnversionedValue& src);
    TDynamicValueData CaptureStringValue(TDynamicValueData src);
    TDynamicValueData CaptureStringValue(const TUnversionedValue& src);

    TTimestamp GetLastTimestamp(TRevisionList list) const;
    TTimestamp GetLastTimestamp(TRevisionList list, ui32 revision) const;

    ui32 GetLatestRevision() const;
    ui32 GetSnapshotRevision() const;
    ui32 RegisterRevision(TTimestamp timestamp);

    void OnDynamicMemoryUsageUpdated();

    void InsertIntoLookupHashTable(const TUnversionedValue* keyBegin, TSortedDynamicRow dynamicRow);
};

DEFINE_REFCOUNTED_TYPE(TSortedDynamicStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define SORTED_DYNAMIC_STORE_INL_H_
#include "sorted_dynamic_store-inl.h"
#undef SORTED_DYNAMIC_STORE_INL_H_

