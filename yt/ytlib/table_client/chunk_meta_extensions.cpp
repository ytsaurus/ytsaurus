#include "stdafx.h"
#include "chunk_meta_extensions.h"

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;

using NChunkClient::EChunkType;

///////////////////////////////////////////////////////////////////////////////

TChunkMeta FilterChunkMetaByPartitionTag(const TChunkMeta& chunkMeta, int partitionTag)
{
    YCHECK(chunkMeta.type() == EChunkType::Table);
    auto filteredChunkMeta = chunkMeta;

    if (chunkMeta.version() == ETableChunkFormat::Old) {
        auto channelsExt = GetProtoExtension<TChannelsExt>(chunkMeta.extensions());
        // Partition chunks must have only one channel.
        YCHECK(channelsExt.items_size() == 1);

        std::vector<TBlockInfo> filteredBlocks;
        for (const auto& blockInfo : channelsExt.items(0).blocks()) {
            YCHECK(blockInfo.partition_tag() != DefaultPartitionTag);
            if (blockInfo.partition_tag() == partitionTag) {
                filteredBlocks.push_back(blockInfo);
            }
        }

        ToProto(channelsExt.mutable_items(0)->mutable_blocks(), filteredBlocks);
        SetProtoExtension(filteredChunkMeta.mutable_extensions(), channelsExt);
    } else {
        // New chunks.
        auto blockMetaExt = GetProtoExtension<TBlockMetaExt>(chunkMeta.extensions());

        std::vector<TBlockMeta> filteredBlocks;
        for (const auto& blockMeta : blockMetaExt.entries()) {
            YCHECK(blockMeta.partition_index() != DefaultPartitionTag);
            if (blockMeta.partition_index() == partitionTag) {
                filteredBlocks.push_back(blockMeta);
            }
        }

        ToProto(blockMetaExt.mutable_entries(), filteredBlocks);
        SetProtoExtension(filteredChunkMeta.mutable_extensions(), blockMetaExt);
    }

    return filteredChunkMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
