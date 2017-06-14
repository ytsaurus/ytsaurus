#include "block_id.h"

#include <yt/core/misc/format.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TBlockId::TBlockId(
    const TChunkId& chunkId,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

