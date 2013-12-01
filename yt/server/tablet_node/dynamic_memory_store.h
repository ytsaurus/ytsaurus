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

    TDynamicRow WriteRow(
        const NVersionedTableClient::TNameTablePtr& nameTable,
        TTransaction* transaction,
        NVersionedTableClient::TVersionedRow row,
        bool prewrite);

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool predelete);

    virtual std::unique_ptr<IStoreScanner> CreateScanner() override;

    void ConfirmRow(TDynamicRow row);
    void PrepareRow(TDynamicRow row);
    void CommitRow(TDynamicRow row);
    void AbortRow(TDynamicRow row);
    
private:
    class TScanner;
    friend class TMemoryCompactor;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    int KeyCount_;
    int SchemaColumnCount_;

    i64 AllocatedStringSpace_;
    int AllocatedValueCount_;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

    std::unique_ptr<NVersionedTableClient::TKeyPrefixComparer> Comparer_;
    std::unique_ptr<TRcuTree<TDynamicRow, NVersionedTableClient::TKeyPrefixComparer>> Tree_;


    TDynamicRow AllocateRow();
    
    void LockRow(
        TDynamicRow row,
        TTransaction* transaction,
        bool preliminary);

    void CopyValue(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
