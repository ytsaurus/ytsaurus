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
    i64 size = chunk->Info().size();
    UsedSpace += size;
    AvailableSpace -= size;
}

void TLocation::UnregisterChunk(TIntrusivePtr<TChunk> chunk)
{
    i64 size = chunk->Info().size();
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

Stroka TLocation::GetChunkFileName(const TChunkId& chunkId) const
{
    ui8 firstByte = static_cast<ui8>(chunkId.Parts[0] >> 24);
    return NFS::CombinePaths(
        Path,
        Sprintf("%x/%s", firstByte, ~chunkId.ToString()));
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
        auto chunkId = chunk->GetId();
        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            try {
                auto reader = New<TCachedReader>(
                    chunk->GetId(),
                    chunk->GetFileName());
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
    ScanChunks(); 
}

namespace {

void DeleteFile(const Stroka& fileName)
{
    if (!NFS::Remove(fileName)) {
        LOG_ERROR("Error deleting file %s", ~fileName.Quote());
    }
}

} // namespace <anonymous>

void TChunkStore::ScanChunks()
{
    LOG_INFO("Storage scan started");

    Stroka metaSuffix = ChunkMetaSuffix;

    try {
        FOREACH(const auto& location, Locations_) {
            Stroka path = location->GetPath();
            LOG_INFO("Scanning storage location %s", ~path);

            NFS::ForcePath(path);
            NFS::CleanTempFiles(path);

            TFileList fileList;
            fileList.Fill(path, TStringBuf(), TStringBuf(), 2);
            i32 size = fileList.Size();
            
            yhash_set<Stroka> fileNames;
            yhash_set<TChunkId> chunks;
            for (i32 i = 0; i < size; ++i) {
                Stroka fileName = fileList.Next();
                fileNames.insert(NFS::CombinePaths(path, fileName));

                TChunkId chunkId;
                if (TChunkId::FromString(
                        NFS::GetFileNameWithoutExtension(fileName), &chunkId))
                {
                    chunks.insert(chunkId);
                } else {
                    LOG_ERROR("Invalid chunk filename (FileName: %s)", ~fileName.Quote());
                }
            }

            FOREACH (const auto& chunkId, chunks) {
                auto chunkFileName = location->GetChunkFileName(chunkId);
                auto chunkMetaFileName = chunkFileName + metaSuffix;
                bool hasMeta = fileNames.find(chunkMetaFileName) != fileNames.end();
                bool hasData = fileNames.find(chunkFileName) != fileNames.end();
                if (hasMeta && hasData) {
                    RegisterChunk(chunkId, ~location);
                } else if (!hasMeta) {
                    LOG_WARNING("Missing meta file for %s", ~chunkFileName.Quote());
                    DeleteFile(chunkMetaFileName);
                } else if (!hasData) {
                    LOG_WARNING("Missing data file for %s", ~chunkMetaFileName.Quote());
                    DeleteFile(chunkFileName);
                }
            }
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ~CurrentExceptionMessage());
    }

    LOG_INFO("Storage scan completed (ChunkCount: %d)", ChunkMap.ysize());
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
    auto fileName = location->GetChunkFileName(chunkId);
    
    auto reader = New<TFileReader>(fileName);
    reader->Open();

    auto chunk = New<TChunk>(reader->GetChunkInfo(), location);
    ChunkMap.insert(MakePair(chunkId, chunk));

    location->RegisterChunk(chunk);

    LOG_DEBUG("Chunk registered (ChunkId: %s)", ~chunkId.ToString());

    ChunkAdded_.Fire(~chunk);

    return chunk;
}

TChunk::TPtr TChunkStore::FindChunk(const TChunkId& chunkId) const
{
    auto it = ChunkMap.find(chunkId);
    return it == ChunkMap.end() ? NULL : it->Second();
}

void TChunkStore::RemoveChunk(TChunk* chunk)
{
    // Hold the chunk during removal.
    TChunk::TPtr chunk_ = chunk;
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

TFileReader::TPtr TChunkStore::GetChunkReader(TChunk* chunk)
{
    return ReaderCache->Get(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
