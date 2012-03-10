#include "stdafx.h"
#include "chunk_registry.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "bootstrap.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TChunkRegistry::TChunkRegistry(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{ }

TChunkPtr TChunkRegistry::FindChunk(const TChunkId& chunkId) const
{
    // There are two possible places where we can look for a chunk: ChunkStore and ChunkCache.
    auto storedChunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);
    if (storedChunk) {
        return storedChunk;
    }

    auto cachedChunk = Bootstrap->GetChunkCache()->FindChunk(chunkId);
    if (cachedChunk) {
        return cachedChunk;
    }

    return NULL;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
