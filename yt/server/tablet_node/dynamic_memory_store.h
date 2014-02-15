#pragma once

#include "public.h"
#include "store.h"
#include "dynamic_memory_store_bits.h"

#include <core/misc/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStore
    : public IStore
{
public:
    TDynamicMemoryStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet);

    ~TDynamicMemoryStore();

    TTablet* GetTablet() const;

    int GetLockCount() const;
    int Lock();
    int Unlock();

    TDynamicRow WriteRow(
        TTransaction* transaction,
        NVersionedTableClient::TUnversionedRow row,
        bool prewrite);

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool prewrite);

    TDynamicRow MigrateRow(
        TDynamicRow row,
        const TDynamicMemoryStorePtr& migrateTo);
    TDynamicRow CheckLockAndMaybeMigrateRow(
        NVersionedTableClient::TKey key,
        TTransaction* transaction,
        ERowLockMode mode,
        const TDynamicMemoryStorePtr& migrateTo);

    void ConfirmRow(TDynamicRow row);
    void PrepareRow(TDynamicRow row);
    void CommitRow(TDynamicRow row);
    void AbortRow(TDynamicRow row);

    i64 GetAllocatedStringSpace() const;
    int GetAllocatedValueCount() const;

    // IStore implementation.
    virtual TStoreId GetId() const override;

    virtual EStoreState GetState() const override;
    virtual void SetState(EStoreState state) override;

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TOwningKey lowerKey,
        NVersionedTableClient::TOwningKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYson::IYsonConsumer* consumer) override;

private:
    class TReader;

    TTabletManagerConfigPtr Config_;
    TStoreId Id_;
    TTablet* Tablet_;

    int LockCount_;

    EStoreState State_;

    int KeyColumnCount_;
    int SchemaColumnCount_;

    i64 AllocatedStringSpace_;
    int AllocatedValueCount_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

    std::unique_ptr<TSkipList<TDynamicRow, NVersionedTableClient::TKeyComparer>> Rows_;


    TDynamicRow AllocateRow();
    
    void CheckRowLock(
        TDynamicRow row,
        TTransaction* transaction,
        ERowLockMode mode);
    bool LockRow(
        TDynamicRow row,
        TTransaction* transaction,
        ERowLockMode mode,
        bool prewrite);

    void DropUncommittedValues(TDynamicRow row);

    void AddFixedValue(
        TDynamicRow row,
        int listIndex,
        const NVersionedTableClient::TVersionedValue& value);
    void AddUncommittedFixedValue(
        TDynamicRow row,
        int listIndex,
        const NVersionedTableClient::TUnversionedValue& value);

    void AddTimestamp(TDynamicRow row, TTimestamp timestamp);
    void AddUncommittedTimestamp(TDynamicRow row, TTimestamp timestamp);

    void CaptureValue(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);
    void CaptureValue(
        NVersionedTableClient::TVersionedValue* dst,
        const NVersionedTableClient::TVersionedValue& src);
    void CaptureValueData(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);

};

DEFINE_REFCOUNTED_TYPE(TDynamicMemoryStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
