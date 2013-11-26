#pragma once

#include "public.h"
#include "row.h"

#include <core/misc/rcu_tree.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTable
    : public TRefCounted
{
public:
    TMemoryTable(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    ~TMemoryTable();

    TBucket WriteRow(
        const NVersionedTableClient::TNameTablePtr& nameTable,
        TTransaction* transaction,
        NVersionedTableClient::TVersionedRow row,
        bool prewrite);

    TBucket DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool predelete);

    void LookupRow(
        const NVersionedTableClient::IWriterPtr& writer,
        NVersionedTableClient::TKey key,
        NTransactionClient::TTimestamp timestamp,
        const TColumnFilter& columnFilter);


    void ConfirmBucket(TBucket bucket);
    void PrepareBucket(TBucket bucket);
    void CommitBucket(TBucket bucket);
    void AbortBucket(TBucket bucket);

private:
    class TComparer;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    int KeyCount;
    int SchemaColumnCount;

    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

    NVersionedTableClient::TNameTablePtr NameTable_;

    std::unique_ptr<TComparer> Comparer_;
    std::unique_ptr<TRcuTree<TBucket, TComparer>> Tree_;


    TBucket AllocateBucket();
    
    void LockBucket(
        TBucket bucket,
        TTransaction* transaction,
        bool preliminary);

    void InternValue(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);

    NVersionedTableClient::TTimestamp FetchTimestamp(
        TTimestampList list,
        NTransactionClient::TTimestamp timestamp);

    const NVersionedTableClient::TVersionedValue* FetchVersionedValue(
        TValueList list,
        NTransactionClient::TTimestamp minTimestamp,
        NTransactionClient::TTimestamp maxTimestamp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
