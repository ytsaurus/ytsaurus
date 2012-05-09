#include "stdafx.h"
#include "chunk_meta_extensions.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta ExtractChunkMetaExtensions(const TChunkMeta& chunkMeta, const std::vector<int>& tags)
{
    TChunkMeta result;
    result.set_type(chunkMeta.type());

    FOREACH(auto tag, tags) {
        FOREACH(const auto& extension, chunkMeta.extensions().extensions()) {
            if (tag == extension.tag()) {
                *result.mutable_extensions()->add_extensions() = extension;
            }
        }
    }

    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
