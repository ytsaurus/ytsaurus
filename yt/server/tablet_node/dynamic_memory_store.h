#pragma once

#include "public.h"
#include "store.h"
#include "dynamic_memory_store_bits.h"

#include <core/misc/rcu_tree.h>

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
        TTablet* tablet);

    ~TDynamicMemoryStore();

    TTablet* GetTablet() const;

    int GetLockCount() const;
    int Lock(int delta = 1);
    int Unlock(int delta = 1);

    void MakePassive();

    TDynamicRow WriteRow(
        const NVersionedTableClient::TNameTablePtr& nameTable,
        TTransaction* transaction,
        NVersionedTableClient::TUnversionedRow row,
        bool prewrite);

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool prewrite);

    virtual NVersionedTableClient::IVersionedReaderPtr CreateReader(
        NVersionedTableClient::TKey lowerKey,
        NVersionedTableClient::TKey upperKey,
        TTimestamp timestamp,
        const NApi::TColumnFilter& columnFilter) override;
        
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
    
private:
    class TReader;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    int LockCount_;

    bool Active_;

    int KeyCount_;
    int SchemaColumnCount_;

    i64 AllocatedStringSpace_;
    int AllocatedValueCount_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

    std::unique_ptr<NVersionedTableClient::TKeyPrefixComparer> Comparer_;
    std::unique_ptr<TRcuTree<TDynamicRow, NVersionedTableClient::TKeyPrefixComparer>> Tree_;


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

    void CopyValue(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
