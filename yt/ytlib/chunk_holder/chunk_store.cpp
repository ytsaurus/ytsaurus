#include "chunk_store.h"

#include "../misc/string.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

// TODO: drop once NFS provides GetFileSize
#include <util/system/oldfile.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkStore::TChunkStore(const TChunkHolderConfig& config)
    : Config(config)
{
    ScanChunks(); 
    InitIOQueues();
}

void TChunkStore::ScanChunks()
{
    for (int location = 0; location < Config.Locations.ysize(); ++location) {
        Stroka path = Config.Locations[location];

        NFS::CleanTempFiles(path);

        // TODO: make a function in NYT::NFS
        MakeDirIfNotExist(~Config.Locations[location]);

        LOG_INFO("Scanning location %s", ~path);

        TFileList fileList;
        fileList.Fill(Config.Locations[location]);
        const char* fileName;
        while ((fileName = fileList.Next()) != NULL) {
            // TODO: use our own guid class
            TChunkId id = GetGuid(fileName);
            if (!id.IsEmpty()) {
                Stroka fullName = path + "/" + fileName;
                // TODO: make a function in NYT::NFS
                i64 size = TOldOsFile::Length(~fullName);
                RegisterChunk(id, size, location);
            }
        }
    }

    LOG_INFO("%d chunks found", ChunkMap.ysize());
}

void TChunkStore::InitIOQueues()
{
    for (int location = 0; location < Config.Locations.ysize(); ++location) {
        IOInvokers.push_back(new TActionQueue());
    }
}

TChunk::TPtr TChunkStore::RegisterChunk(
    const TChunkId& chunkId,
    i64 size,
    int location)
{
    TChunk::TPtr chunk = new TChunk(chunkId, size, location);
    ChunkMap.insert(MakePair(chunkId, chunk));

    LOG_DEBUG("Chunk registered (Id: %s, Size: %" PRId64 ")",
        ~StringFromGuid(chunkId),
        size);

    return chunk;
}

TChunk::TPtr TChunkStore::FindChunk(const TChunkId& chunkId)
{
    TChunkMap::iterator it = ChunkMap.find(chunkId);
    if (it == ChunkMap.end())
        return NULL;
    else
        return it->Second();
}

IInvoker::TPtr TChunkStore::GetIOInvoker(int location)
{
    return IOInvokers[location];
}

int TChunkStore::GetNewChunkLocation(const TChunkId& chunkId)
{
    // TODO: code here
    return chunkId.dw[0] % Config.Locations.ysize();
}

Stroka TChunkStore::GetChunkFileName(const TChunkId& chunkId, int location)
{
    return Config.Locations[location] + "/" + StringFromGuid(chunkId);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
