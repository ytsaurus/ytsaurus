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

    void WriteRows(
        TTransaction* transaction,
        NVersionedTableClient::IReaderPtr reader,
        bool transient,
        std::vector<TRowGroup>* lockedGroups);

    static void ConfirmPrewrittenGroup(TRowGroup group);
    static void CommitGroup(TRowGroup group);
    static void AbortGroup(TRowGroup group);

    void LookupRows(
        NVersionedTableClient::TRow key,
        NTransactionClient::TTimestamp timestamp,
        NChunkClient::NProto::TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks);

private:
    class TComparer;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    TChunkedMemoryPool TreePool_;
    TChunkedMemoryPool RowPool_;
    TChunkedMemoryPool StringPool_;

    NVersionedTableClient::TNameTablePtr NameTable_;

    std::unique_ptr<TComparer> Comparer_;
    std::unique_ptr<TRcuTree<TRowGroup, TComparer>> Tree_;


    TRowGroup WriteRow(
        NVersionedTableClient::TNameTablePtr nameTable,
        TTransaction* transaction,
        NVersionedTableClient::TRow row,
        bool prewrite);

    void InternValue(
        NVersionedTableClient::TRowValue* dst,
        const NVersionedTableClient::TRowValue& src);

    TRowGroupItem FetchGroupItem(
        TRowGroup group,
        NTransactionClient::TTimestamp timestamp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
