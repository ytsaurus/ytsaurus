#include "stdafx.h"
#include "chunk_registry.h"
#include "chunk.h"
#include "location.h"
#include "chunk_store.h"
#include "chunk_cache.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TChunkPtr TChunkRegistry::FindChunk(const TChunkId& chunkId) const
{
    // There are two possible places where we can look for a chunk: ChunkStore and ChunkCache.
    if (ChunkStore) {
        auto storedChunk = ChunkStore->FindChunk(chunkId);
        if (storedChunk) {
            return storedChunk;
        }
    }

    if (ChunkCache) {
        auto cachedChunk = ChunkCache->FindChunk(chunkId);
        if (cachedChunk) {
            return cachedChunk;
        }
    }

    return NULL;
}

void TChunkRegistry::SetChunkStore(TChunkStore* chunkStore)
{
    ChunkStore = chunkStore;
}

void TChunkRegistry::SetChunkCache(TChunkCache* chunkCache)
{
    ChunkCache = chunkCache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
