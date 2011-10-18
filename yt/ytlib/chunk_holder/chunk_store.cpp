#include "chunk_store.h"

#include "../misc/foreach.h"
#include "../misc/assert.h"

#include <util/folder/filelist.h>
#include <util/random/random.h>
#include <utility>
#include <limits>

namespace NYT {
namespace NChunkHolder {

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
        LOG_FATAL("Failed to compute available space at %s: %s",
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

void TLocation::IncSessionCount()
{
    ++SessionCount;
    LOG_DEBUG("Location %s has %i sessions", ~Path, SessionCount);
}

void TLocation::DecSessionCount()
{
    --SessionCount;
    LOG_DEBUG("Location %s has %i sessions", ~Path, SessionCount);
}
    
int TLocation::GetSessionCount() const
{
    return SessionCount;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkStore::TCachedReader
    : public TCacheValueBase<TChunkId, TCachedReader>
    , public TFileChunkReader
{
public:
    typedef TIntrusivePtr<TCachedReader> TPtr;

    TCachedReader(const TChunkId& chunkId, Stroka fileName)
        : TCacheValueBase<TChunkId, TCachedReader>(chunkId)
        , TFileChunkReader(fileName)
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

    TCachedReader::TPtr Get(TChunk::TPtr chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        if (BeginInsert(&cookie)) {
            try {
                auto file = New<TCachedReader>(
                    chunk->GetId(),
                    ChunkStore->GetChunkFileName(chunk));
                cookie.EndInsert(file);
            } catch (...) {
                LOG_FATAL("Error opening chunk (ChunkId: %s, What: %s)",
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
        FOREACH(const auto& location, Locations) {
            auto path = location->GetPath();

            NFS::ForcePath(~path);

            NFS::CleanTempFiles(path);

            LOG_INFO("Scanning location %s", ~path);

            TFileList fileList;
            fileList.Fill(path);
            const char* fileName;
            while ((fileName = fileList.Next()) != NULL) {
                auto chunkId = TChunkId::FromString(fileName);
                if (!chunkId.IsEmpty()) {
                    auto fullName = path + "/" + fileName;
                    i64 size = NFS::GetFileSize(fullName);
                    RegisterChunk(chunkId, size, location);
                }
            }
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations: %s", ~CurrentExceptionMessage());
    }

    LOG_INFO("%d chunks found", ChunkMap.ysize());
}

void TChunkStore::InitLocations()
{
    for (int i = 0; i < Config.Locations.ysize(); ++i) {
        Locations.push_back(New<TLocation>(Config.Locations[i]));
    }
}

TChunk::TPtr TChunkStore::RegisterChunk(
    const TChunkId& chunkId,
    i64 size,
    TLocation::TPtr location)
{
    auto chunk = New<TChunk>(chunkId, size, location);
    ChunkMap.insert(MakePair(chunkId, chunk));

    location->RegisterChunk(chunk);

    LOG_DEBUG("Chunk registered (Id: %s, Size: %" PRId64 ")",
        ~chunkId.ToString(),
        size);

    ChunkAdded_.Fire(chunk);

    return chunk;
}

TChunk::TPtr TChunkStore::FindChunk(const TChunkId& chunkId)
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->Second();
}

void TChunkStore::RemoveChunk(TChunk::TPtr chunk)
{
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
    using namespace std;

    YASSERT(!Locations.empty());

    // Return location with minimum sessions.

    vector<TLocations::const_iterator> tmp;
    tmp.reserve(Locations.size());

    int minSessionCount = numeric_limits<int>::max();
    int c;

    for (TLocations::const_iterator it = Locations.begin(); it != Locations.end(); ++it) {
        c = (*it)->GetSessionCount();
        if (c > minSessionCount) {
            continue;
        }
        if (c < minSessionCount) {
            tmp.clear();
            minSessionCount = c;
        }
        tmp.push_back(it);
    }

    // If we have several locations with the same number of opened sessions,
    // select session randomly.

    return *tmp[RandomNumber(tmp.size())];
}

Stroka TChunkStore::GetChunkFileName(const TChunkId& chunkId, TLocation::TPtr location)
{
    return location->GetPath() + "/" + chunkId.ToString();
}

Stroka TChunkStore::GetChunkFileName(TChunk::TPtr chunk)
{
    return GetChunkFileName(chunk->GetId(), chunk->GetLocation());
}

const yvector<TLocation::TPtr> TChunkStore::GetLocations() const
{
    return Locations;
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

TFuture<TChunkMeta::TPtr>::TPtr TChunkStore::GetChunkMeta(TChunk::TPtr chunk)
{
    auto meta = chunk->Meta;
    if (~meta != NULL) {
        return New< TFuture<TChunkMeta::TPtr> >(meta);
    }

    auto invoker = chunk->GetLocation()->GetInvoker();
    return
        FromMethod(
            &TChunkStore::DoGetChunkMeta,
            TPtr(this),
            chunk)
        ->AsyncVia(invoker)
        ->Do();
}

TChunkMeta::TPtr TChunkStore::DoGetChunkMeta(TChunk::TPtr chunk)
{
    auto reader = GetChunkReader(chunk);
    if (~reader == NULL)
        return NULL;

    auto meta = New<TChunkMeta>(reader);
    chunk->Meta = meta;
    return meta;
}

TFileChunkReader::TPtr TChunkStore::GetChunkReader(TChunk::TPtr chunk)
{
    return ~ReaderCache->Get(chunk);
}

TParamSignal<TChunk::TPtr>& TChunkStore::ChunkAdded()
{
    return ChunkAdded_;
}

TParamSignal<TChunk::TPtr>& TChunkStore::ChunkRemoved()
{
    return ChunkRemoved_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
