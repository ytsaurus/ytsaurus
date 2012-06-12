#include "stdafx.h"
#include "chunk_meta_extensions.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaExtensions(const TChunkMeta& chunkMeta, const std::vector<int>& tags)
{
    TChunkMeta result;
    result.set_type(chunkMeta.type());

    std::set<int> tagsSet(tags.begin(), tags.end());

    FOREACH (const auto& extension, chunkMeta.extensions().extensions()) {
        if (tagsSet.find(extension.tag()) != tagsSet.end()) {
            *result.mutable_extensions()->add_extensions() = extension;
        }
    }

    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
