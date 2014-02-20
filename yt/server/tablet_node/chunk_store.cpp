#include "stdafx.h"
#include "chunk_store.h"
#include "tablet.h"
#include "config.h"
#include "automaton.h"

#include <core/concurrency/fiber.h>

#include <core/ytree/fluent.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>
#include <ytlib/new_table_client/cached_versioned_chunk_meta.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/block_cache.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    TOwningKey minKey,
    TOwningKey maxKey,
    IBlockCachePtr blockCache,
    IChannelPtr masterChannel,
    const TNullable<TNodeDescriptor>& localDescriptor)
    : TStoreBase(
        id,
        tablet)
    , Config_(std::move(config))
    , BlockCache_(std::move(blockCache))
    , MasterChannel_(std::move(masterChannel))
    , MinKey_(std::move(minKey))
    , MaxKey_(std::move(maxKey))
{
    State_ = EStoreState::Persistent;

    YCHECK(
        TypeFromId(Id_) == EObjectType::Chunk ||
        TypeFromId(Id_) == EObjectType::ErasureChunk);
}

TChunkStore::~TChunkStore()
{ }

TOwningKey TChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TChunkStore::GetMaxKey() const
{
    return MaxKey_;
}

IVersionedReaderPtr TChunkStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    if (!ChunkReader_) {
        // TODO(babenko): provide seed replicas
        ChunkReader_ = CreateReplicationReader(
            Config_->ChunkReader,
            BlockCache_,
            MasterChannel_,
            New<TNodeDirectory>(),
            LocalDescriptor_,
            Id_);
    }

    if (!CachedMeta_) {
        auto cachedMeta = New<TCachedVersionedChunkMeta>(
            ChunkReader_,
            Tablet_->Schema(),
            Tablet_->KeyColumns());
        auto result = WaitFor(cachedMeta->Load());
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error caching meta of chunk %s",
            ~ToString(Id_));
        CachedMeta_ = cachedMeta;
    }

    TReadLimit lowerLimit;
    lowerLimit.SetKey(std::move(lowerKey));

    TReadLimit upperLimit;
    upperLimit.SetKey(std::move(upperKey));

    return CreateVersionedChunkReader(
        Config_->ChunkReader,
        ChunkReader_,
        CachedMeta_,
        lowerLimit,
        upperLimit,
        columnFilter,
        timestamp);
}

void TChunkStore::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, State_);
    Save(context, MinKey_);
    Save(context, MaxKey_);
}

void TChunkStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);
    Load(context, MinKey_);
    Load(context, MaxKey_);
}

void TChunkStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .DoIf(CachedMeta_, [&] (TFluentMap fluent) {
            const auto& miscExt = CachedMeta_->Misc();
            fluent
                .Item("compressed_data_size").Value(miscExt.compressed_data_size())
                .Item("uncompressed_data_size").Value(miscExt.uncompressed_data_size())
                .Item("key_count").Value(miscExt.row_count());
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

