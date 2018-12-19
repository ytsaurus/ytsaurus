#include "block_id.h"

#include <yt/core/misc/format.h>

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

TString ToString(const TBlockId& id)
{
    return Format("%v:%v", id.ChunkId, id.BlockIndex);
}

bool operator == (const TBlockId& lhs, const TBlockId& rhs)
{
    return lhs.ChunkId == rhs.ChunkId &&
           lhs.BlockIndex == rhs.BlockIndex;
}

bool operator != (const TBlockId& lhs, const TBlockId& rhs)
{
    return !(lhs == rhs);
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

