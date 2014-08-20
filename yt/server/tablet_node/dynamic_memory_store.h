#pragma once

#include "public.h"
#include "store_detail.h"
#include "dynamic_memory_store_bits.h"

#include <core/misc/public.h>

#include <core/actions/signal.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NTabletNode {

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

    TDynamicRow WriteRow(
        TTransaction* transaction,
        NVersionedTableClient::TUnversionedRow row,
        bool prelock);

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        TKey key,
        bool prelock);

    TDynamicRow MigrateRow(const TDynamicRowRef& rowRef);

    TDynamicRow FindRowAndCheckLocks(
        TKey key,
        TTransaction* transaction,
        ERowLockMode mode);

    void ConfirmRow(TDynamicRow row);
    void PrepareRow(TDynamicRow row);
    void CommitRow(TDynamicRow row);
    void AbortRow(TDynamicRow row);

    int GetValueCount() const;
    int GetKeyCount() const;
    
    i64 GetAlignedPoolSize() const;
    i64 GetAlignedPoolCapacity() const;

    i64 GetUnalignedPoolSize() const;
    i64 GetUnalignedPoolCapacity() const;

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual i64 GetDataSize() const override;

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

    virtual TTimestamp GetLatestCommitTimestamp(TKey key) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

    // Memory usage tracking.
    i64 GetMemoryUsage() const;

    DEFINE_SIGNAL(void(i64 delta), MemoryUsageUpdated)
    
    DEFINE_SIGNAL(void(TDynamicRow row), RowBlocked)

private:
    class TFetcherBase;
    class TReader;
    class TLookuper;

    TTabletManagerConfigPtr Config_;

    int KeyColumnCount_;
    int SchemaColumnCount_;

    int LockCount_ = 0;
    int ValueCount_ = 0;

    NVersionedTableClient::TRowBuffer RowBuffer_;
    std::unique_ptr<TSkipList<TDynamicRow, NVersionedTableClient::TKeyComparer>> Rows_;

    i64 MemoryUsage_ = 0;

    TTimestamp MinTimestamp_ = NTransactionClient::MaxTimestamp;
    TTimestamp MaxTimestamp_ = NTransactionClient::MinTimestamp;


    TDynamicRow AllocateRow();
    
    void CheckRowLock(
        TDynamicRow row,
        TTransaction* transaction,
        ERowLockMode mode);
    bool LockRow(
        TDynamicRow row,
        TTransaction* transaction,
        ERowLockMode mode,
        bool prelock);

    void DropUncommittedValues(TDynamicRow row);

    void AddFixedValue(
        TDynamicRow row,
        int listIndex,
        const TVersionedValue& value);
    void AddUncommittedFixedValue(
        TDynamicRow row,
        int listIndex,
        const TUnversionedValue& value);

    void AddTimestamp(TDynamicRow row, TTimestamp timestamp, ETimestampListKind kind);
    void AddUncommittedTimestamp(TDynamicRow row, ETimestampListKind kind);

    void CaptureValue(TUnversionedValue* dst, const TUnversionedValue& src);
    void CaptureValue(TVersionedValue* dst, const TVersionedValue& src);
    void CaptureValueData(TUnversionedValue* dst, const TUnversionedValue& src);

    void OnMemoryUsageUpdated();

    TOwningKey RowToKey(TDynamicRow row);

};

DEFINE_REFCOUNTED_TYPE(TDynamicMemoryStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
