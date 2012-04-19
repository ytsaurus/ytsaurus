#include "stdafx.h"
#include "chunk_meta_extensions.h"

namespace NYT {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta ExtractExtensions(const TChunkMeta& chunkMeta, const std::vector<int>& extensionTags)
{
    TChunkMeta result;
    result.set_type(chunkMeta.type());

    FOREACH(auto tag, extensionTags) {
        FOREACH(const auto& extension, chunkMeta.extensions().extensions()) {
            if (tag == extension.tag()) {
                *result.mutable_extensions()->add_extensions() = extension;
            }
        }
    }
    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
