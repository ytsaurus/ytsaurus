#pragma once

#include "public.h"
#include "store_detail.h"
#include "dynamic_memory_store_bits.h"
#include "transaction.h"

#include <core/misc/public.h>
#include <core/misc/property.h>

#include <core/actions/signal.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

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
    TDynamicMemoryStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet);

    ~TDynamicMemoryStore();

    int GetLockCount() const;
    int Lock();
    int Unlock();

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
        NVersionedTableClient::TUnversionedRow row,
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
        const TDynamicRow row);

    void ConfirmRow(TTransaction* transaction, TDynamicRow row);
    void PrepareRow(TTransaction* transaction, TDynamicRow row);
    void CommitRow(TTransaction* transaction, TDynamicRow row);
    void AbortRow(TTransaction* transaction, TDynamicRow row);

    int GetValueCount() const;
    int GetKeyCount() const;
    
    i64 GetAlignedPoolSize() const;
    i64 GetAlignedPoolCapacity() const;

    i64 GetUnalignedPoolSize() const;
    i64 GetUnalignedPoolCapacity() const;

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual i64 GetUncompressedDataSize() const override;
    virtual i64 GetRowCount() const override;

    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetMaxKey() const override;

    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        TOwningKey lowerKey,
        TOwningKey upperKey,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual NVersionedTableClient::IVersionedLookuperPtr CreateLookuper(
        TTimestamp timestamp,
        const TColumnFilter& columnFilter) override;

    virtual void CheckRowLocks(
        TKey key,
        TTransaction* transaction,
        ui32 lockMask) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

    // Memory usage tracking.
    i64 GetMemoryUsage() const;

    DEFINE_SIGNAL(void(i64 delta), MemoryUsageUpdated)
    
    DEFINE_SIGNAL(void(TDynamicRow row, int lockIndex), RowBlocked)

private:
    class TFetcherBase;
    class TReader;
    class TLookuper;

    TTabletManagerConfigPtr Config_;

    int KeyColumnCount_;
    int SchemaColumnCount_;
    int ColumnLockCount_;

    int StoreLockCount_ = 0;
    int StoreValueCount_ = 0;

    NVersionedTableClient::TRowBuffer RowBuffer_;
    std::unique_ptr<TSkipList<TDynamicRow, TDynamicRowKeyComparer>> Rows_;

    i64 MemoryUsage_ = 0;

    TTimestamp MinTimestamp_ = NTransactionClient::MaxTimestamp;
    TTimestamp MaxTimestamp_ = NTransactionClient::MinTimestamp;


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

    TValueList AddUncommittedFixedValue(TDynamicRow row, const TVersionedValue& value);

    void AddTimestamp(TDynamicRow row, TTimestamp timestamp, ETimestampListKind kind);
    void SetKeys(TDynamicRow row, TUnversionedRow key);

    void CaptureValue(TUnversionedValue* dst, const TUnversionedValue& src);
    void CaptureValue(TVersionedValue* dst, const TVersionedValue& src);
    void CaptureValueData(TUnversionedValue* dst, const TUnversionedValue& src);
    TDynamicValueData CaptureStringValue(TDynamicValueData src);
    TDynamicValueData CaptureStringValue(const TUnversionedValue& src);

    void OnMemoryUsageUpdated();

};

DEFINE_REFCOUNTED_TYPE(TDynamicMemoryStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
