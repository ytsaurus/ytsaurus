#include "chunk_store.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

// TODO: drop once NFS provides GetFileSize
#include <util/system/oldfile.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

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
            TCachedReader::TPtr file;
            try {
                file = New<TCachedReader>(
                    chunk->GetId(),
                    ChunkStore->GetChunkFileName(chunk));
            } catch (...) {
                LOG_FATAL("Error opening chunk (ChunkId: %s, What: %s)",
                    ~chunk->GetId().ToString(),
                    ~CurrentExceptionMessage());
            }
            EndInsert(file, &cookie);
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
    ScanChunks(); 
    InitIOQueues();
}

void TChunkStore::ScanChunks()
{
    for (int location = 0; location < Config.Locations.ysize(); ++location) {
        auto path = Config.Locations[location];

        // TODO: make a function in NYT::NFS
        MakePathIfNotExist(~path);

        NFS::CleanTempFiles(path);
        
        LOG_INFO("Scanning location %s", ~path);

        TFileList fileList;
        fileList.Fill(Config.Locations[location]);
        const char* fileName;
        while ((fileName = fileList.Next()) != NULL) {
            auto chunkId = TChunkId::FromString(fileName);
            if (!chunkId.IsEmpty()) {
                auto fullName = path + "/" + fileName;
                // TODO: make a function in NYT::NFS
                i64 size = TOldOsFile::Length(~fullName);
                RegisterChunk(chunkId, size, location);
            }
        }
    }

    LOG_INFO("%d chunks found", ChunkMap.ysize());
}

void TChunkStore::InitIOQueues()
{
    for (int location = 0; location < Config.Locations.ysize(); ++location) {
        IOInvokers.push_back(~New<TActionQueue>());
    }
}

TChunk::TPtr TChunkStore::RegisterChunk(
    const TChunkId& chunkId,
    i64 size,
    int location)
{
    auto chunk = New<TChunk>(chunkId, size, location);
    ChunkMap.insert(MakePair(chunkId, chunk));

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
        
    auto fileName = GetChunkFileName(chunk);
    if (!NFS::Remove(fileName)) {
        LOG_FATAL("Error removing chunk file (ChunkId: %s)",
            ~chunk->GetId().ToString());
    }

    ChunkRemoved_.Fire(chunk);
}

IInvoker::TPtr TChunkStore::GetIOInvoker(int location)
{
    return IOInvokers[location];
}

int TChunkStore::GetNewChunkLocation(const TChunkId& chunkId)
{
    // TODO: code here
    return chunkId.Parts[0] % Config.Locations.ysize();
}

Stroka TChunkStore::GetChunkFileName(const TChunkId& chunkId, int location)
{
    return Config.Locations[location] + "/" + chunkId.ToString();
}

Stroka TChunkStore::GetChunkFileName(TChunk::TPtr chunk)
{
    return GetChunkFileName(chunk->GetId(), chunk->GetLocation());
}

THolderStatistics TChunkStore::GetStatistics() const
{
    // TODO: do something meaningful
    THolderStatistics result;
    result.AvailableSpace = 100;
    result.UsedSpace = 100;
    result.ChunkCount = ChunkMap.ysize();
    return result;
}

NYT::NChunkHolder::TChunkStore::TChunks TChunkStore::GetChunks()
{
    TChunks result;
    result.reserve(ChunkMap.ysize());
    for (TChunkMap::iterator it = ChunkMap.begin();
        it != ChunkMap.end();
        ++it)
    {
        result.push_back(it->Second());
    }
    return result;
}

TAsyncResult<TChunkMeta::TPtr>::TPtr TChunkStore::GetChunkMeta(TChunk::TPtr chunk)
{
    auto meta = chunk->Meta;
    if (~meta != NULL) {
        return New< TAsyncResult<TChunkMeta::TPtr> >(meta);
    }

    auto invoker = GetIOInvoker(chunk->GetLocation());
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
