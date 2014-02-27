#include "stdafx.h"
#include "chunk_meta_extensions.h"

namespace NYT {
namespace NTableClient {

using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaByPartitionTag(const TChunkMeta& chunkMeta, int partitionTag)
{
    auto filteredChunkMeta = chunkMeta;

    auto channelsExt = GetProtoExtension<TChannelsExt>(chunkMeta.extensions());
    // Partition chunks must have only one channel.
    YCHECK(channelsExt.items_size() == 1);

    std::vector<NTableClient::NProto::TBlockInfo> filteredBlocks;
    for (const auto& blockInfo : channelsExt.items(0).blocks()) {
        YCHECK(blockInfo.partition_tag() != DefaultPartitionTag);
        if (blockInfo.partition_tag() == partitionTag) {
            filteredBlocks.push_back(blockInfo);
        }
    }

    ToProto(channelsExt.mutable_items(0)->mutable_blocks(), filteredBlocks);
    SetProtoExtension(filteredChunkMeta.mutable_extensions(), channelsExt);

    return filteredChunkMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
