#pragma once

#include "public.h"
#include "store_detail.h"
#include "dynamic_memory_store_bits.h"
#include "dynamic_memory_store_comparer.h"
#include "transaction.h"

#include <core/misc/property.h>
#include <core/misc/chunked_vector.h>

#include <core/actions/signal.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/table_client/versioned_row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TRowBlockedException
    : public std::exception
{
public:
    TRowBlockedException(
        TDynamicMemoryStorePtr store,
        TDynamicRow row,
        ui32 lockMask,
        TTimestamp timestamp)
        : Store_(std::move(store))
        , Row_(row)
        , LockMask_(lockMask)
        , Timestamp_(timestamp)
    { }

    DEFINE_BYVAL_RO_PROPERTY(TDynamicMemoryStorePtr, Store);
    DEFINE_BYVAL_RO_PROPERTY(TDynamicRow, Row);
    DEFINE_BYVAL_RO_PROPERTY(ui32, LockMask);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, Timestamp);

};

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore
    : public TStoreBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(EStoreFlushState, FlushState);

public:
    TDynamicMemoryStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet);

    ~TDynamicMemoryStore();

    //! Returns the cached instance of row key comparer
    //! (obtained by calling TTablet::GetRowKeyComparer).
    const TDynamicRowKeyComparer& GetRowKeyComparer() const;

    int GetLockCount() const;
    int Lock();
    int Unlock();

    //! Checks if a given #row has any locks from #lockMask with prepared timestamp
    //! less that #timestamp. If so, raises |RowBlocked| signal and loops.
    void WaitOnBlockedRow(
        TDynamicRow row,
        ui32 lockMask,
        TTimestamp timestamp);

    //! Writes the row taking the needed locks.
    /*!
     *  On lock failure, throws TErrorException explaining the cause.
     *  If a blocked row is encountered, throws TRowBlockedException.
     */
    TDynamicRow WriteRow(
        TTransaction* transaction,
        NTableClient::TUnversionedRow row,
        bool prelock,
        ui32 lockMask);

    //! Deletes the row taking the needed locks.
    /*!
     *  On lock failure, throws TErrorException explaining the cause.
     *  If a blocked row is encountered, throws TRowBlockedException.
     */
    TDynamicRow DeleteRow(
        TTransaction* transaction,
        TKey key,
        bool prelock);

    TDynamicRow MigrateRow(
        TTransaction* transaction,
        TDynamicRow row);

    void ConfirmRow(TTransaction* transaction, TDynamicRow row);
    void PrepareRow(TTransaction* transaction, TDynamicRow row);
    void CommitRow(TTransaction* transaction, TDynamicRow row);
    void AbortRow(TTransaction* transaction, TDynamicRow row);

    TDynamicRow FindRow(NTableClient::TUnversionedRow key);

    int GetValueCount() const;
    int GetKeyCount() const;
    
    i64 GetPoolSize() const;
    i64 GetPoolCapacity() const;

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual i64 GetUncompressedDataSize() const override;
    virtual i64 GetRowCount() const override;

    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetMaxKey() const override;

    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual void CheckRowLocks(
        TUnversionedRow row,
        TTransaction* transaction,
        ui32 lockMask) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual TCallback<void(TSaveContext&)> AsyncSave() override;
    virtual void AsyncLoad(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

    DEFINE_SIGNAL(void(TDynamicRow row, int lockIndex), RowBlocked)

private:
    class TReaderBase;
    class TRangeReader;
    class TLookupReader;

    const TTabletManagerConfigPtr Config_;

    int StoreLockCount_ = 0;
    int StoreValueCount_ = 0;

    TDynamicRowKeyComparer RowKeyComparer_;

    NTableClient::TRowBufferPtr RowBuffer_;
    std::unique_ptr<TSkipList<TDynamicRow, TDynamicRowKeyComparer>> Rows_;

    TTimestamp MinTimestamp_ = NTransactionClient::MaxTimestamp;
    TTimestamp MaxTimestamp_ = NTransactionClient::MinTimestamp;

    static const size_t RevisionsPerChunk = 1ULL << 13;
    static const size_t MaxRevisionChunks = HardRevisionsPerDynamicMemoryStoreLimit / RevisionsPerChunk + 1;
    TChunkedVector<TTimestamp, RevisionsPerChunk> RevisionToTimestamp_;


    TDynamicRow AllocateRow();

    int GetBlockingLockIndex(
        TDynamicRow row,
        ui32 lockMask,
        TTimestamp timestamp);
    void ValidateRowNotBlocked(
        TDynamicRow row,
        ui32 lockMask,
        TTimestamp timestamp);

    void CheckRowLocks(
        TDynamicRow row,
        TTransaction* transaction,
        ui32 lockMask);
    void AcquireRowLocks(
        TDynamicRow row,
        TTransaction* transaction,
        bool prelock,
        ui32 lockMask,
        bool deleteFlag);

    TValueList PrepareFixedValue(TDynamicRow row, int index);
    void AddRevision(TDynamicRow row, ui32 revision, ERevisionListKind kind);
    void SetKeys(TDynamicRow dstRow, TUnversionedValue* srcKeys);
    void SetKeys(TDynamicRow dstRow, TDynamicRow srcRow);
    void LoadRow(TVersionedRow row, yhash_map<TTimestamp, ui32>* timestampToRevision);

    void CaptureUncommittedValue(TDynamicValue* dst, const TDynamicValue& src, int index);
    ui32 CaptureTimestamp(TTimestamp timestamp, yhash_map<TTimestamp, ui32>* timestampToRevision);
    void CaptureVersionedValue(TDynamicValue* dst, const TVersionedValue& src, yhash_map<TTimestamp, ui32>* timestampToRevision);
    void CaptureUnversionedValue(TDynamicValue* dst, const TUnversionedValue& src);
    TDynamicValueData CaptureStringValue(TDynamicValueData src);
    TDynamicValueData CaptureStringValue(const TUnversionedValue& src);

    ui32 GetLatestRevision() const;
    ui32 RegisterRevision(TTimestamp timestamp);
    TTimestamp TimestampFromRevision(ui32 revision);

    void OnMemoryUsageUpdated();

};

DEFINE_REFCOUNTED_TYPE(TDynamicMemoryStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
