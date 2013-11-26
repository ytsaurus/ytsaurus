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
    explicit TMemoryTable(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    ~TMemoryTable();

    void WriteRows(
        TTransaction* transaction,
        NVersionedTableClient::IReaderPtr reader,
        bool prewrite,
        std::vector<TBucket>* lockedBuckets);

    void ConfirmPrewrittenBucket(TBucket bucket);
    void PrepareBucket(TBucket bucket);
    void CommitBucket(TBucket bucket);
    void AbortBucket(TBucket bucket);

    void LookupRow(
        NVersionedTableClient::TKey key,
        NTransactionClient::TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        NChunkClient::NProto::TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks);

private:
    class TComparer;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    int KeyCount;
    int SchemaColumnCount;

    TChunkedMemoryPool TreePool_;
    TChunkedMemoryPool RowPool_;
    TChunkedMemoryPool StringPool_;

    NVersionedTableClient::TNameTablePtr NameTable_;

    std::unique_ptr<TComparer> Comparer_;
    std::unique_ptr<TRcuTree<TBucket, TComparer>> Tree_;


    TBucket WriteRow(
        NVersionedTableClient::TNameTablePtr nameTable,
        TTransaction* transaction,
        NVersionedTableClient::TVersionedRow row,
        bool prewrite);

    void InternValue(
        NVersionedTableClient::TUnversionedValue* dst,
        const NVersionedTableClient::TUnversionedValue& src);

    const NVersionedTableClient::TVersionedValue* FetchVersionedValue(
        TValueList list,
        NTransactionClient::TTimestamp timestamp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
