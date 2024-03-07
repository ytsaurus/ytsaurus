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
        TTabletManagerConfigPtr config,
        TStoreId id,
        TTablet* tablet);
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
        TTimestamp CheckingTimestamp;
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

    TSortedDynamicRow MigrateRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask readLockMask);
    void PrepareRow(TTransaction* transaction, TSortedDynamicRow row);
    void CommitRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask readLockMask);
    void AbortRow(TTransaction* transaction, TSortedDynamicRow row, TLockMask readLockMask);
    void DeleteRow(TTransaction* transaction, TSortedDynamicRow row);
    void WriteRow(TTransaction* transaction, TSortedDynamicRow dynamicRow, TUnversionedRow row);

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
    class TReaderBase;
    class TRangeReader;
    class TLookupReader;
    class TLookupHashTable;

    const TSortedDynamicRowKeyComparer RowKeyComparer_;
    const std::unique_ptr<TSkipList<TSortedDynamicRow, TSortedDynamicRowKeyComparer>> Rows_;
    std::unique_ptr<TLookupHashTable> LookupHashTable_;

    ui32 FlushRevision_ = InvalidRevision;

    static const size_t RevisionsPerChunk = 1ULL << 13;
    static const size_t MaxRevisionChunks = HardRevisionsPerDynamicStoreLimit / RevisionsPerChunk + 1;
    TChunkedVector<TTimestamp, RevisionsPerChunk> RevisionToTimestamp_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, RowBlockedLock_);
    TRowBlockedHandler RowBlockedHandler_;

    // Reused between ModifyRow calls.
    std::vector<ui32> WriteRevisions_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;

    i64 MaxDataWeight_ = 0;
    TSortedDynamicRow MaxDataWeightWitness_;

    bool MergeRowsOnFlushAllowed_ = true;

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

    void WriteRow(TSortedDynamicRow dynamicRow, TUnversionedRow row, ui32 revision);

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

