#include "chunk_store.h"

#include "../misc/foreach.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/random/random.h>

// TODO: drop once NFS provides GetFileSize
#include <util/system/oldfile.h>

#if defined(_linux_)
#include <sys/vfs.h>
#elif defined(_freebsd_) || defined(_darwin_)
#error We do not support freebsd right now, do we?
#include <sys/param.h>
#include <sys/mount.h>
#elif defined (_win_)
#include <windows.h>
#endif

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TLocation::TLocation(const Stroka& path)
    : Path(path)
    , AvailableSpace(0)
    , UsedSpace(0)
    , Invoker(~New<TActionQueue>())
{ }

void TLocation::RegisterChunk(i64 size)
{
    UsedSpace += size;
    AvailableSpace -= size;
}

i64 TLocation::GetAvailableSpace()
{
    // ToDo: may be smarter update
#if !defined( _win_)
    struct statfs fsData;
    int res = statfs(~Path, &fsData);
    LOG_FATAL_IF(res != 0, "statfs failed on location %s", ~Path);
    AvailableSpace = fsData.f_bavail * fsData.f_bsize;
#else
    ui64 freeBytes;
    int res = GetDiskFreeSpaceExA(
        ~Path, 
        (PULARGE_INTEGER)&freeBytes,
        (PULARGE_INTEGER)NULL,
        (PULARGE_INTEGER)NULL);
    LOG_FATAL_IF(res == 0, "GetDiskFreeSpaceExA failed on location %s", ~Path);
    AvailableSpace = freeBytes;
#endif
    return AvailableSpace;
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
    InitLocations();
    ScanChunks(); 
}

void TChunkStore::ScanChunks()
{
    FOREACH(auto location, Locations) {
        auto path = location->GetPath();

        // TODO: make a function in NYT::NFS
        MakePathIfNotExist(~path);

        NFS::CleanTempFiles(path);
        
        LOG_INFO("Scanning location %s", ~path);

        TFileList fileList;
        fileList.Fill(path);
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

    location->RegisterChunk(size);

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

TLocation::TPtr TChunkStore::GetNewChunkLocation()
{
    // Choose location with probability in proportion to its load 
    float sumLoad = 0;
    FOREACH(auto location, Locations) {
        sumLoad += location->GetLoad();
    }

    float rnd = RandomNumber<float>() * sumLoad;
    FOREACH(auto location, Locations) {
        rnd -= location->GetLoad();
        if (rnd < 0) {
            return location;
        }
    }

    YASSERT(false);
}

Stroka TChunkStore::GetChunkFileName(const TChunkId& chunkId, TLocation::TPtr location)
{
    return location->GetPath() + "/" + chunkId.ToString();
}

Stroka TChunkStore::GetChunkFileName(TChunk::TPtr chunk)
{
    return GetChunkFileName(chunk->GetId(), chunk->GetLocation());
}

THolderStatistics TChunkStore::GetStatistics() const
{
    THolderStatistics result;

    FOREACH(auto location, Locations) {
        result.AvailableSpace += location->GetAvailableSpace();
        result.UsedSpace += location->GetUsedSpace();
    }

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
