#include "chunk_meta_extensions.h"

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

REGISTER_PROTO_EXTENSION(TMiscExt, 0, misc)
REGISTER_PROTO_EXTENSION(TBlocksExt, 1, blocks)
REGISTER_PROTO_EXTENSION(TErasurePlacementExt, 2, erasure_placement)
REGISTER_PROTO_EXTENSION(TStripedErasurePlacementExt, 3, striped_erasure_placement)

////////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaByExtensionTags(
    const TChunkMeta& chunkMeta,
    const std::optional<std::vector<int>>& extensionTags)
{
    if (!extensionTags) {
        return chunkMeta;
    }

    TChunkMeta filteredChunkMeta;
    filteredChunkMeta.set_type(chunkMeta.type());
    filteredChunkMeta.set_format(chunkMeta.format());
    filteredChunkMeta.set_features(chunkMeta.features());

    FilterProtoExtensions(
        filteredChunkMeta.mutable_extensions(),
        chunkMeta.extensions(),
        THashSet<int>(extensionTags->begin(), extensionTags->end()));

    return filteredChunkMeta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
