#include "stdafx.h"
#include "chunk_store.h"
#include "tablet.h"
#include "config.h"
#include "automaton.h"

#include <core/concurrency/fiber.h>

#include <core/ytree/fluent.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/versioned_reader.h>
#include <ytlib/new_table_client/versioned_chunk_reader.h>
#include <ytlib/new_table_client/cached_versioned_chunk_meta.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/api/client.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/data_node/local_chunk_reader.h>
#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectClient;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NDataNode;
using namespace NCellNode;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    TTabletManagerConfigPtr config,
    const TStoreId& id,
    TTablet* tablet,
    const TChunkMeta* chunkMeta,
    TBootstrap* boostrap)
    : TStoreBase(
        id,
        tablet)
    , Config_(std::move(config))
    , Bootstrap_(boostrap)
    , DataSize_(-1)
{
    State_ = EStoreState::Persistent;

    if (chunkMeta) {
        ChunkMeta_ = *chunkMeta;
        PrecacheProperties();
    }

    YCHECK(
        TypeFromId(Id_) == EObjectType::Chunk ||
        TypeFromId(Id_) == EObjectType::ErasureChunk);
}

TChunkStore::~TChunkStore()
{ }

const TChunkMeta& TChunkStore::GetChunkMeta() const
{
    return ChunkMeta_;
}

i64 TChunkStore::GetDataSize() const
{
    return DataSize_;
}

TOwningKey TChunkStore::GetMinKey() const
{
    return MinKey_;
}

TOwningKey TChunkStore::GetMaxKey() const
{
    return MaxKey_;
}

TTimestamp TChunkStore::GetMinTimestamp() const
{
    return MinTimestamp_;
}

TTimestamp TChunkStore::GetMaxTimestamp() const
{
    return MaxTimestamp_;
}

IVersionedReaderPtr TChunkStore::CreateReader(
    TOwningKey lowerKey,
    TOwningKey upperKey,
    TTimestamp timestamp,
    const TColumnFilter& columnFilter)
{
    if (upperKey < MinKey_ || lowerKey > MaxKey_) {
        return nullptr;
    }

    auto asyncChunkReader = BIND(&CreateLocalChunkReader)
        .AsyncVia(Bootstrap_->GetControlInvoker())
        .Run(Bootstrap_, Id_);
    auto chunkReader = WaitFor(asyncChunkReader);
    if (!chunkReader) {
        // TODO(babenko): provide seed replicas
        chunkReader = CreateReplicationReader(
            Config_->ChunkReader,
            Bootstrap_->GetBlockStore()->GetBlockCache(),
            Bootstrap_->GetMasterClient()->GetMasterChannel(),
            New<TNodeDirectory>(),
            Bootstrap_->GetLocalDescriptor(),
            Id_);
    }

    if (!CachedMeta_) {
        auto cachedMetaOrError = WaitFor(TCachedVersionedChunkMeta::Load(
            chunkReader,
            Tablet_->Schema(),
            Tablet_->KeyColumns()));
        THROW_ERROR_EXCEPTION_IF_FAILED(cachedMetaOrError);
        CachedMeta_ = cachedMetaOrError.Value();
    }

    TReadLimit lowerLimit;
    lowerLimit.SetKey(std::move(lowerKey));

    TReadLimit upperLimit;
    upperLimit.SetKey(std::move(upperKey));

    return CreateVersionedChunkReader(
        Config_->ChunkReader,
        chunkReader,
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
    Save(context, ChunkMeta_);
}

void TChunkStore::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);
    Load(context, ChunkMeta_);

    PrecacheProperties();
}

void TChunkStore::BuildOrchidYson(IYsonConsumer* consumer)
{
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    BuildYsonMapFluently(consumer)
        .Item("compressed_data_size").Value(miscExt.compressed_data_size())
        .Item("uncompressed_data_size").Value(miscExt.uncompressed_data_size())
        .Item("key_count").Value(miscExt.row_count())
        .Item("min_key").Value(MinKey_)
        .Item("max_key").Value(MaxKey_)
        .Item("min_timestamp").Value(MinTimestamp_)
        .Item("max_timestamp").Value(MaxTimestamp_);
}

void TChunkStore::PrecacheProperties()
{
    // Precache frequently used values.
    auto miscExt = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    DataSize_ = miscExt.uncompressed_data_size();
    MinTimestamp_ = miscExt.min_timestamp();
    MaxTimestamp_ = miscExt.max_timestamp();

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());
    MinKey_ = FromProto<TOwningKey>(boundaryKeysExt.min());
    MaxKey_ = FromProto<TOwningKey>(boundaryKeysExt.max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

