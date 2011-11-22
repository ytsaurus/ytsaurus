#include "stdafx.h"
#include "chunk_store.h"

#include "../misc/foreach.h"
#include "../misc/assert.h"

#include <util/folder/filelist.h>
#include <util/random/random.h>
#include <utility>
#include <limits>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TLocation::TLocation(Stroka path)
    : Path(path)
    , AvailableSpace(0)
    , UsedSpace(0)
    , ActionQueue(New<TActionQueue>())
    , SessionCount(0)
{ }

void TLocation::RegisterChunk(TIntrusivePtr<TChunk> chunk)
{
    i64 size = chunk->GetSize();
    UsedSpace += size;
    AvailableSpace -= size;
}

void TLocation::UnregisterChunk(TIntrusivePtr<TChunk> chunk)
{
    i64 size = chunk->GetSize();
    UsedSpace -= size;
    AvailableSpace += size;
}

i64 TLocation::GetAvailableSpace()
{
    try {
        AvailableSpace = NFS::GetAvailableSpace(Path);
    } catch (...) {
        LOG_FATAL("Failed to compute available space at %s\n%s",
            ~Path.Quote(),
            ~CurrentExceptionMessage());
    }
    return AvailableSpace;
}

IInvoker::TPtr TLocation::GetInvoker() const
{
    return ActionQueue->GetInvoker();
}

i64 TLocation::GetUsedSpace() const
{
    return UsedSpace;
}

Stroka TLocation::GetPath() const
{
    return Path;
}

double TLocation::GetLoadFactor() const
{
    return (double) UsedSpace / (UsedSpace + AvailableSpace);
}

void TLocation::IncrementSessionCount()
{
    ++SessionCount;
    LOG_DEBUG("Location %s has %d sessions", ~Path, SessionCount);
}

void TLocation::DecrementSessionCount()
{
    --SessionCount;
    LOG_DEBUG("Location %s has %d sessions", ~Path, SessionCount);
}
    
int TLocation::GetSessionCount() const
{
    return SessionCount;
}

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

class TChunkStore::TReaderCache
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
        TInsertCookie cookie(chunk->GetId());
        if (BeginInsert(&cookie)) {
            try {
                auto file = New<TCachedReader>(
                    chunk->GetId(),
                    ChunkStore->GetChunkFileName(chunk));
                cookie.EndInsert(file);
            } catch (...) {
                LOG_FATAL("Error opening chunk (ChunkId: %s)\n%s",
                    ~chunk->GetId().ToString(),
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
    ScanChunks(); 
}

void TChunkStore::ScanChunks()
{
    try {
        FOREACH(const auto& location, Locations_) {
            auto path = location->GetPath();

            NFS::ForcePath(path);
            NFS::CleanTempFiles(path);

            LOG_INFO("Scanning location %s", ~path);

            TFileList fileList;
            fileList.Fill(path);
            const char* fileName;
            while ((fileName = fileList.Next()) != NULL) {
                auto chunkId = TChunkId::FromString(fileName);
                if (!chunkId.IsEmpty()) {
                    RegisterChunk(chunkId, ~location);
                }
            }
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ~CurrentExceptionMessage());
    }

    LOG_INFO("%d chunks found", ChunkMap.ysize());
}

void TChunkStore::InitLocations()
{
    FOREACH (const auto& config, Config.Locations) {
        Locations_.push_back(New<TLocation>(config));
    }
}

TChunk::TPtr TChunkStore::RegisterChunk(
    const TChunkId& chunkId,
    TLocation* location)
{
    auto fileName = GetChunkFileName(chunkId, location);
    auto reader = New<TFileReader>(fileName);

    auto chunk = New<TChunk>(chunkId, ~reader, location);
    ChunkMap.insert(MakePair(chunkId, chunk));

    location->RegisterChunk(chunk);

    LOG_DEBUG("Chunk registered (Id: %s)", ~chunkId.ToString());

    ChunkAdded_.Fire(~chunk);

    return chunk;
}

TChunk::TPtr TChunkStore::FindChunk(const TChunkId& chunkId)
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->Second();
}

void TChunkStore::RemoveChunk(TChunk* chunk)
{
    // Hold the chunk during removal.
    TChunk::TPtr chunk_ = chunk;

    YVERIFY(ChunkMap.erase(chunk->GetId()) == 1);

    ReaderCache->Remove(chunk);
    chunk->GetLocation()->UnregisterChunk(chunk);
        
    auto fileName = GetChunkFileName(chunk);
    if (!NFS::Remove(fileName)) {
        LOG_FATAL("Error removing chunk file (ChunkId: %s)",
            ~chunk->GetId().ToString());
    }

    LOG_INFO("Chunk removed (Id: %s)",
        ~chunk->GetId().ToString());

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

Stroka TChunkStore::GetChunkFileName(const TChunkId& chunkId, TLocation* location)
{
    return location->GetPath() + "/" + chunkId.ToString();
}

Stroka TChunkStore::GetChunkFileName(TChunk* chunk)
{
    return GetChunkFileName(chunk->GetId(), ~chunk->GetLocation());
}

TChunkStore::TChunks TChunkStore::GetChunks()
{
    TChunks result;
    result.reserve(ChunkMap.ysize());
    FOREACH(const auto& pair, ChunkMap) {
        result.push_back(pair.Second());
    }
    return result;
}

int TChunkStore::GetChunkCount()
{
    return ChunkMap.ysize();
}

TFileReader::TPtr TChunkStore::GetChunkReader(TChunk* chunk)
{
    return ReaderCache->Get(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
