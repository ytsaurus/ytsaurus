#include "chunk_meta_extensions.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTableClient {

using namespace NProto;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::EChunkType;
using NYTree::BuildYsonFluently;
using NYson::IYsonConsumer;

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

bool TOwningBoundaryKeys::operator ==(const TOwningBoundaryKeys& other) const
{
    return MinKey == other.MinKey && MaxKey == other.MaxKey;
}

bool TOwningBoundaryKeys::operator !=(const TOwningBoundaryKeys& other) const
{
    return MinKey != other.MinKey || MaxKey != other.MaxKey;
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

////////////////////////////////////////////////////////////////////////////////

bool FindBoundaryKeys(const TChunkMeta& chunkMeta, TOwningKey* minKey, TOwningKey* maxKey)
{
    auto boundaryKeys = FindProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
    if (!boundaryKeys) {
        return false;
    }
    FromProto(minKey, boundaryKeys->min());
    FromProto(maxKey, boundaryKeys->max());
    return true;
}

std::unique_ptr<TOwningBoundaryKeys> FindBoundaryKeys(const TChunkMeta& chunkMeta)
{
    TOwningBoundaryKeys keys;
    if (!FindBoundaryKeys(chunkMeta, &keys.MinKey, &keys.MaxKey)) {
        return nullptr;
    }
    return std::make_unique<TOwningBoundaryKeys>(std::move(keys));
}

TChunkMeta FilterChunkMetaByPartitionTag(const TChunkMeta& chunkMeta, int partitionTag)
{
    YCHECK(chunkMeta.type() == static_cast<int>(EChunkType::Table));
    auto filteredChunkMeta = chunkMeta;
    auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

    std::vector<TBlockMeta> filteredBlocks;
    for (const auto& blockMeta : blockMetaExt.blocks()) {
        YCHECK(blockMeta.partition_index() != DefaultPartitionTag);
        if (blockMeta.partition_index() == partitionTag) {
            filteredBlocks.push_back(blockMeta);
        }
    }

    NYT::ToProto(blockMetaExt.mutable_blocks(), filteredBlocks);
    SetProtoExtension(filteredChunkMeta.mutable_extensions(), blockMetaExt);

    return filteredChunkMeta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
