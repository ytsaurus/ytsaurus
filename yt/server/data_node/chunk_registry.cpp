#include "stdafx.h"
#include "chunk_registry.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TChunkRegistry::TChunkRegistry(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

IChunkPtr TChunkRegistry::FindChunk(const TChunkId& chunkId)
{
    // There are two possible places where we can look for a chunk: ChunkStore and ChunkCache.
    auto storedChunk = Bootstrap_->GetChunkStore()->FindChunk(chunkId);
    if (storedChunk) {
        return storedChunk;
    }

    auto cachedChunk = Bootstrap_->GetChunkCache()->FindChunk(chunkId);
    if (cachedChunk) {
        return cachedChunk;
    }

    return nullptr;
}

IChunkPtr TChunkRegistry::GetChunk(const TChunkId& chunkId)
{
    auto chunk = FindChunk(chunkId);
    if (!chunk) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v",
            chunkId);
    }
    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
