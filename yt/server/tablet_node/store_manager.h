#pragma once

#include "public.h"

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TColumnFilter
{
    TColumnFilter()
        : All(true)
    { }

    TColumnFilter(const std::vector<Stroka>& columns)
        : All(false)
        , Columns(columns.begin(), columns.end())
    { }

    TColumnFilter(const TColumnFilter& other)
        : All(other.All)
        , Columns(other.Columns)
    { }

    bool All;
    TSmallVector<Stroka, NVersionedTableClient::TypicalColumnCount> Columns;
};

////////////////////////////////////////////////////////////////////////////////

class TStoreManager
    : public TRefCounted
{
public:
    TStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    void Lookup(
        NVersionedTableClient::TKey key,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        NChunkClient::NProto::TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks);
    void Write(
        TTransaction* transaction,
        NChunkClient::NProto::TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks,
        bool prewrite,
        std::vector<TDynamicRow>* lockedRows);
    void Delete(
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys,
        bool predelete,
        std::vector<TDynamicRow>* lockedRows);

    void ConfirmRow(TDynamicRow row);
    void PrepareRow(TDynamicRow row);
    void CommitRow(TDynamicRow row);
    void AbortRow(TDynamicRow row);

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    TDynamicMemoryStorePtr ActiveDynamicMemoryStore_;
    TDynamicMemoryStorePtr PassiveDynamicMemoryStore_;
    TStaticMemoryStorePtr StaticMemoryStore_;

    NVersionedTableClient::TNameTablePtr NameTable_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
