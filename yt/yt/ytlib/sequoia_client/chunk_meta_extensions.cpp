#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NSequoiaClient {

using namespace NTableClient;
using namespace NQueryClient;

using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::THunkChunkRefsExt;
using NTableClient::NProto::THunkChunkMiscExt;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::NProto::THeavyColumnStatisticsExt;

////////////////////////////////////////////////////////////////////////////////

NRecords::TChunkMetaExtensionsKey GetChunkMetaExtensionsKey(NChunkClient::TChunkId chunkId)
{
    return {
        .IdHash = chunkId.Parts32[0],
        .Id = ToString(chunkId)
    };
}

#define ITERATE_CHUNK_META_EXTENSIONS(XX) \
    XX(MiscExt) \
    XX(HunkChunkRefsExt) \
    XX(HunkChunkMiscExt) \
    XX(BoundaryKeysExt) \
    XX(HeavyColumnStatisticsExt)

namespace {

int GetChunkMetaExtensionColumnId(int extensionTag)
{
    const auto& idMapping = NRecords::TChunkMetaExtensionsDescriptor::Get()->GetIdMapping();
    switch (extensionTag) {
        #define XX(name) \
            case TProtoExtensionTag<T##name>::Value: \
                return *idMapping.name;
        ITERATE_CHUNK_META_EXTENSIONS(XX)
        #undef XX
        default:
            YT_ABORT();
    }
}

} // namespace

NTableClient::TColumnFilter GetChunkMetaExtensionsColumnFilter(const THashSet<int>& extensionTags)
{
    std::vector<int> columnIds;
    columnIds.reserve(extensionTags.size());
    for (auto extensionTag : extensionTags) {
        columnIds.push_back(GetChunkMetaExtensionColumnId(extensionTag));
    }
    return NTableClient::TColumnFilter(std::move(columnIds));
}

namespace NRecords {

void ToProto(
    NYT::NProto::TExtensionSet* protoExtensions,
    const TChunkMetaExtensions& extensions)
{
    #define XX(name) \
        if (extensions.name) { \
            auto* protoExtension = protoExtensions->add_extensions(); \
            protoExtension->set_tag(TProtoExtensionTag<T##name>::Value); \
            protoExtension->set_data(extensions.name); \
        }
    ITERATE_CHUNK_META_EXTENSIONS(XX)
    #undef XX
}

void FromProto(
    TChunkMetaExtensions* extensions,
    const NYT::NProto::TExtensionSet& protoExtensions)
{
    for (const auto& protoExtension : protoExtensions.extensions()) {
        switch (protoExtension.tag()) {
            #define XX(name) \
                case TProtoExtensionTag<T##name>::Value: \
                    extensions->name = protoExtension.data(); \
                    break;
            ITERATE_CHUNK_META_EXTENSIONS(XX)
            #undef XX
        }
    }
}

} // namespace NRecords

#undef ITERATE_CHUNK_META_EXTENSIONS

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
