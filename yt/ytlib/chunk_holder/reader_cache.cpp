#include "stdafx.h"
#include "reader_cache.h"

//#include "../misc/foreach.h"
//#include "../misc/assert.h"
//
//#include <utility>
//#include <limits>
//#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkStore::TCachedReader
    : public TCacheValueBase<TChunkId, TCachedReader>
    , public TFileReader
{
public:
    typedef TIntrusivePtr<TCachedReader> TPtr;

    TCachedReader(const TChunkId& chunkId, Stroka fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TFileReader(fileName)
    { }

};

////////////////////////////////////////////////////////////////////////////////

class TReaderCache::TImpl
    : public TCapacityLimitedCache<TChunkId, TCachedReader>
{
public:
    typedef TIntrusivePtr<TReaderCache> TPtr;

    TReaderCache(
        const TChunkHolderConfig& config,
        TChunkStore::TPtr chunkStore)
        : TCapacityLimitedCache<TChunkId, TCachedReader>(config.MaxCachedFiles)
        , ChunkStore(chunkStore)
    { }

    TCachedReader::TPtr Get(TChunk* chunk)
    {
        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            try {
                auto reader = New<TCachedReader>(chunk->GetId(), chunk->GetFileName());
                reader->Open();
                cookie.EndInsert(reader);
            } catch (...) {
                LOG_FATAL("Error opening chunk (ChunkId: %s)\n%s",
                    ~chunkId.ToString(),
                    ~CurrentExceptionMessage());
            }
        }
        return cookie.GetAsyncResult()->Get();
    }

    bool Remove(TChunk::TPtr chunk)
    {
        return TCacheBase::Remove(chunk->GetId());
    }

private:
    TChunkStore::TPtr ChunkStore;

};

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(const TChunkHolderConfig& config)
    : Config(config)
    , ReaderCache(New<TReaderCache>(Config, this))
{
    InitLocations();
    ScanLocations(); 
}


void TChunkStore::ScanLocations()
{
    LOG_INFO("Storage scan started");

    try {
        FOREACH(auto location, Locations_) {
            FOREACH (const auto& descriptor, location->Scan()) {
                auto chunk = New<TStoredChunk>(~location, descriptor);
                RegisterChunk(~chunk);
            }
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ~CurrentExceptionMessage());
    }

    LOG_INFO("Storage scan completed, %s chunks total", ChunkMap.ysize());
}

void TChunkStore::InitLocations()
{
    FOREACH (const auto& config, Config.Locations) {
        Locations_.push_back(New<TLocation>(config));
    }
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

    ReaderCache->Remove(chunk);
    chunk->GetLocation()->UnregisterChunk(chunk);
        
    Stroka fileName = chunk->GetFileName();
    if (!NFS::Remove(fileName + ChunkMetaSuffix)) {
        LOG_FATAL("Error removing chunk meta file (ChunkId: %s)", ~chunkId.ToString());
    }
    if (!NFS::Remove(fileName)) {
        LOG_FATAL("Error removing chunk file (ChunkId: %s)", ~chunkId.ToString());
    }

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

TFileReader::TPtr TChunkStore::GetChunkReader(TStoredChunk* chunk)
{
    return ReaderCache->Get(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
