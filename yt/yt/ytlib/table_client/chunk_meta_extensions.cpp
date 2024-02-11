#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/core/misc/object_pool.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

REGISTER_PROTO_EXTENSION(TTableSchemaExt, 50, table_schema)
REGISTER_PROTO_EXTENSION(TDataBlockMetaExt, 51, block_meta)
REGISTER_PROTO_EXTENSION(TNameTableExt, 53, name_table)
REGISTER_PROTO_EXTENSION(TBoundaryKeysExt, 55, boundary_keys)
REGISTER_PROTO_EXTENSION(TSamplesExt, 56, samples)
REGISTER_PROTO_EXTENSION(TPartitionsExt, 59, partitions)
REGISTER_PROTO_EXTENSION(TColumnMetaExt, 58, column_meta)
REGISTER_PROTO_EXTENSION(TColumnarStatisticsExt, 60, columnar_statistics)
REGISTER_PROTO_EXTENSION(THeavyColumnStatisticsExt, 61, heavy_column_statistics)
REGISTER_PROTO_EXTENSION(TKeyColumnsExt, 14, key_columns)
REGISTER_PROTO_EXTENSION(THunkChunkRefsExt, 62, hunk_chunk_refs)
REGISTER_PROTO_EXTENSION(THunkChunkMiscExt, 63, hunk_chunk_misc)
REGISTER_PROTO_EXTENSION(THunkChunkMetasExt, 64, hunk_chunk_metas)
REGISTER_PROTO_EXTENSION(TSystemBlockMetaExt, 65, system_block_meta)
REGISTER_PROTO_EXTENSION(TVersionedRowDigestExt, 66, versioned_row_digest)

////////////////////////////////////////////////////////////////////////////////

size_t TOwningBoundaryKeys::SpaceUsed() const
{
    return
        sizeof(*this) +
        MinKey.GetSpaceUsed() - sizeof(MinKey) +
        MaxKey.GetSpaceUsed() - sizeof(MaxKey);
}

void TOwningBoundaryKeys::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, MinKey);
    Persist(context, MaxKey);
}

TString ToString(const TOwningBoundaryKeys& keys)
{
    return Format("MinKey: %v, MaxKey: %v",
        keys.MinKey,
        keys.MaxKey);
}

void Serialize(const TOwningBoundaryKeys& keys, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("min_key").Value(keys.MinKey)
            .Item("max_key").Value(keys.MaxKey)
        .EndMap();
}

void Deserialize(TOwningBoundaryKeys& keys, const INodePtr& node)
{
    const auto& mapNode = node->AsMap();
    // Boundary keys for empty tables look like {} in YSON.
    if (mapNode->GetChildCount() == 0) {
        keys.MinKey = TUnversionedOwningRow();
        keys.MaxKey = TUnversionedOwningRow();
        return;
    }
    Deserialize(keys.MinKey, mapNode->GetChildOrThrow("min_key"));
    Deserialize(keys.MaxKey, mapNode->GetChildOrThrow("max_key"));
}

////////////////////////////////////////////////////////////////////////////////

bool FindBoundaryKeys(
    const TChunkMeta& chunkMeta,
    TLegacyOwningKey* minKey,
    TLegacyOwningKey* maxKey,
    std::optional<int> keyColumnCount)
{
    auto boundaryKeys = FindProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
    if (!boundaryKeys) {
        return false;
    }
    // TODO(max42): YT-14049. Padding should be done by master.
    FromProto(minKey, boundaryKeys->min(), keyColumnCount);
    FromProto(maxKey, boundaryKeys->max(), keyColumnCount);
    return true;
}

std::unique_ptr<TOwningBoundaryKeys> FindBoundaryKeys(
    const TChunkMeta& chunkMeta,
    std::optional<int> keyColumnCount)
{
    TOwningBoundaryKeys keys;
    if (!FindBoundaryKeys(chunkMeta, &keys.MinKey, &keys.MaxKey, keyColumnCount)) {
        return nullptr;
    }
    return std::make_unique<TOwningBoundaryKeys>(std::move(keys));
}

////////////////////////////////////////////////////////////////////////////////

bool FindBoundaryKeyBounds(
    const TChunkMeta& chunkMeta,
    TOwningKeyBound* lowerBound,
    TOwningKeyBound* upperBound)
{
    auto boundaryKeys = FindProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
    if (!boundaryKeys) {
        return false;
    }

    TUnversionedOwningRow minKey;
    TUnversionedOwningRow maxKey;
    FromProto(&minKey, boundaryKeys->min());
    FromProto(&maxKey, boundaryKeys->max());

    *lowerBound = TOwningKeyBound::FromRow(minKey, /*isInclusive*/true, /*isUpper*/false);
    *upperBound = TOwningKeyBound::FromRow(maxKey, /*isInclusive*/true, /*isUpper*/true);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaByPartitionTag(
    const TChunkMeta& chunkMeta,
    const TCachedBlockMetaPtr& cachedBlockMeta,
    int partitionTag)
{
    YT_VERIFY(chunkMeta.type() == static_cast<int>(EChunkType::Table));
    auto filteredChunkMeta = chunkMeta;

    std::vector<TDataBlockMeta> filteredBlocks;
    for (const auto& blockMeta : cachedBlockMeta->data_blocks()) {
        YT_VERIFY(blockMeta.partition_index() != DefaultPartitionTag);
        if (blockMeta.partition_index() == partitionTag) {
            filteredBlocks.push_back(blockMeta);
        }
    }

    auto blockMetaExt = ObjectPool<TDataBlockMetaExt>().Allocate();
    NYT::ToProto(blockMetaExt->mutable_data_blocks(), filteredBlocks);
    SetProtoExtension(filteredChunkMeta.mutable_extensions(), *blockMetaExt);

    return filteredChunkMeta;
}

////////////////////////////////////////////////////////////////////////////////

TCachedBlockMeta::TCachedBlockMeta(
    NChunkClient::TChunkId chunkId,
    TDataBlockMetaExt blockMeta)
    : TSyncCacheValueBase<TChunkId, TCachedBlockMeta>(chunkId)
    , TDataBlockMetaExt(std::move(blockMeta))
    , Weight_(SpaceUsedLong())
{ }

i64 TCachedBlockMeta::GetWeight() const
{
    return Weight_;
}

////////////////////////////////////////////////////////////////////////////////

TBlockMetaCache::TBlockMetaCache(
    TSlruCacheConfigPtr config,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler)
    : TMemoryTrackingSyncSlruCacheBase<TChunkId, TCachedBlockMeta>(
        std::move(config),
        std::move(memoryTracker),
        profiler)
{ }

i64 TBlockMetaCache::GetWeight(const TCachedBlockMetaPtr& value) const
{
    return value->GetWeight();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
