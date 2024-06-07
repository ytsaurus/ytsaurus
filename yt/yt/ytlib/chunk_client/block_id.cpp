#include "block_id.h"

#include <yt/yt/core/misc/guid.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TBlockId::TBlockId(
    TChunkId chunkId,
    int blockIndex)
    : ChunkId(chunkId)
    , BlockIndex(blockIndex)
{ }

TBlockId::TBlockId()
    : ChunkId(NullChunkId)
    , BlockIndex(-1)
{ }

void FormatValue(TStringBuilderBase* builder, const TBlockId& id, TStringBuf spec)
{
    FormatValue(builder, Format("%v:%v", id.ChunkId, id.BlockIndex), spec);
}

void ToProto(NProto::TBlockId* protoBlockId, const TBlockId& blockId)
{
    using NYT::ToProto;

    ToProto(protoBlockId->mutable_chunk_id(), blockId.ChunkId);
    protoBlockId->set_block_index(blockId.BlockIndex);
}

void FromProto(TBlockId* blockId, const NProto::TBlockId& protoBlockId)
{
    using NYT::FromProto;

    FromProto(&blockId->ChunkId, protoBlockId.chunk_id());
    blockId->BlockIndex = protoBlockId.block_index();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

