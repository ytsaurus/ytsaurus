#include "chunk_registry.h"
#include "chunk.h"
#include "chunk_cache.h"
#include "chunk_store.h"
#include "location.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/core/concurrency/thread_affinity.h>

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
    VERIFY_THREAD_AFFINITY_ANY();

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

IChunkPtr TChunkRegistry::GetChunkOrThrow(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

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
