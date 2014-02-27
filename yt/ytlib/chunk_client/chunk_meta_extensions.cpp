#include "stdafx.h"
#include "chunk_meta_extensions.h"

#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaByExtensionTags(const TChunkMeta& chunkMeta, const std::vector<int>& tags)
{
    // ToDo: use FilterProtoExtensions.
    TChunkMeta filteredChunkMeta;
    filteredChunkMeta.set_type(chunkMeta.type());
    filteredChunkMeta.set_version(chunkMeta.version());

    yhash_set<int> tagsSet(tags.begin(), tags.end());

    for (const auto& extension : chunkMeta.extensions().extensions()) {
        if (tagsSet.find(extension.tag()) != tagsSet.end()) {
            *filteredChunkMeta.mutable_extensions()->add_extensions() = extension;
        }
    }

    return filteredChunkMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
