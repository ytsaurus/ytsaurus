#include "chunk_meta_extensions.h"

namespace NYT::NChunkClient {

using namespace NChunkClient::NProto;

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
    filteredChunkMeta.set_version(chunkMeta.version());

    FilterProtoExtensions(
        filteredChunkMeta.mutable_extensions(),
        chunkMeta.extensions(),
        THashSet<int>(extensionTags->begin(), extensionTags->end()));

    return filteredChunkMeta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
