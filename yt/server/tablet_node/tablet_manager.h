#pragma once

#include "public.h"

#include <core/misc/small_vector.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/chunk_client/chunk.pb.h>

#include <server/hydra/entity_map.h>

#include <server/cell_node/public.h>

#include <server/tablet_node/tablet_manager.pb.h>

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

class TTabletManager
    : public TRefCounted
{
public:
    explicit TTabletManager(
        TTabletManagerConfigPtr config,
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    TTablet* GetTabletOrThrow(const TTabletId& id);

    void Write(
        TTablet* tablet,
        TTransaction* transaction,
        NChunkClient::NProto::TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks);

    void Delete(
        TTablet* tablet,
        TTransaction* transaction,
        const std::vector<NVersionedTableClient::TOwningKey>& keys);

    void Lookup(
        TTablet* tablet,
        NVersionedTableClient::TKey key,
        NTransactionClient::TTimestamp timestamp,
        const TColumnFilter& columnFilter,
        NChunkClient::NProto::TChunkMeta* chunkMeta,
        std::vector<TSharedRef>* blocks);

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
