#include "stdafx.h"
#include "chunk_store.h"

#include "../misc/foreach.h"
#include "../misc/assert.h"

#include <utility>
#include <limits>
#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(
    TChunkHolderConfig* config,
    TReaderCache* readerCache)
    : Config(config)
    , ReaderCache(readerCache)
{
    LOG_INFO("Chunk storage scan started");

    try {
        FOREACH (auto& storageLocation, Config->ChunkStorageLocations) {
            auto location = New<TLocation>(ELocationType::Store, ~storageLocation, ~ReaderCache);
            Locations_.push_back(location);

            FOREACH (const auto& descriptor, location->Scan()) {
                auto chunk = New<TStoredChunk>(~location, descriptor);
                RegisterChunk(~chunk);
            }
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ~CurrentExceptionMessage());
    }

    LOG_INFO("Chunk storage scan completed, %d chunk(s) total", ChunkMap.ysize());
}

void TChunkStore::RegisterChunk(TStoredChunk* chunk)
{
    YASSERT(ChunkMap.insert(MakePair(chunk->GetId(), chunk)).second);
    chunk->GetLocation()->RegisterChunk(chunk);

    LOG_DEBUG("Chunk registered (ChunkId: %s, Size: %" PRId64 ")",
        ~chunk->GetId().ToString(),
        chunk->GetSize());

    ChunkAdded_.Fire(chunk);
}

TStoredChunk::TPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->Second();
}

void TChunkStore::RemoveChunk(TStoredChunk* chunk)
{
    // Hold the chunk during removal.
    TStoredChunk::TPtr chunk_ = chunk;
    auto chunkId = chunk->GetId();

    YVERIFY(ChunkMap.erase(chunkId) == 1);
    
    auto location = chunk->GetLocation();
    location->UnregisterChunk(chunk);
    location->RemoveChunk(chunk);

    LOG_INFO("Chunk removed (ChunkId: %s)", ~chunkId.ToString());

    ChunkRemoved_.Fire(chunk);
}

TLocation::TPtr TChunkStore::GetNewChunkLocation()
{
    YASSERT(!Locations_.empty());

    yvector<TLocation*> candidates;
    candidates.reserve(Locations_.size());

    int minCount = Max<int>();
    FOREACH (const auto& location, Locations_) {
        int count = location->GetSessionCount();
        if (count < minCount) {
            candidates.clear();
            minCount = count;
        }
        if (count == minCount) {
            candidates.push_back(~location);
        }
    }

    return candidates[RandomNumber(candidates.size())];
}

TChunkStore::TChunks TChunkStore::GetChunks() const
{
    TChunks result;
    result.reserve(ChunkMap.ysize());
    FOREACH(const auto& pair, ChunkMap) {
        result.push_back(pair.Second());
    }
    return result;
}

int TChunkStore::GetChunkCount() const
{
    return ChunkMap.ysize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
