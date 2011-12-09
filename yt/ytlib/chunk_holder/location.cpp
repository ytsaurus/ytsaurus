#include "stdafx.h"
#include "location.h"
#include "chunk.h"

#include "../misc/fs.h"
#include "../chunk_client/format.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TLocation::TLocation(const Stroka& path)
    : Path(path)
    , AvailableSpace(0)
    , UsedSpace(0)
    , ActionQueue(New<TActionQueue>("ChunkLocation"))
    , SessionCount(0)
{ }

void TLocation::RegisterChunk(TChunk* chunk)
{
    i64 size = chunk->GetSize();
    UsedSpace += size;
    AvailableSpace -= size;
}

void TLocation::UnregisterChunk(TChunk* chunk)
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

Stroka TLocation::GetChunkFileName(const TChunkId& chunkId) const
{
    ui8 firstByte = static_cast<ui8>(chunkId.Parts[0] >> 24);
    return NFS::CombinePaths(
        Path,
        Sprintf("%x/%s", firstByte, ~chunkId.ToString()));
}

namespace {

void RemoveFile(const Stroka& fileName)
{
    if (!NFS::Remove(fileName)) {
        LOG_ERROR("Error deleting file %s", ~fileName.Quote());
    }
}

} // namespace <anonymous>

yvector<TChunkDescriptor> TLocation::Scan()
{
    LOG_INFO("Scanning storage location %s", ~Path);

    NFS::ForcePath(Path);
    NFS::CleanTempFiles(Path);

    yhash_set<Stroka> fileNames;
    yhash_set<TChunkId> chunkIds;

    TFileList fileList;
    // NB: 2 is the recursion depth
    fileList.Fill(Path, TStringBuf(), TStringBuf(), 2);
    i32 size = fileList.Size();
    for (i32 i = 0; i < size; ++i) {
        Stroka fileName = fileList.Next();
        fileNames.insert(NFS::CombinePaths(Path, fileName));

        TChunkId chunkId;
        if (TChunkId::FromString(
            NFS::GetFileNameWithoutExtension(fileName), &chunkId))
        {
            chunkIds.insert(chunkId);
        } else {
            LOG_ERROR("Invalid chunk filename (FileName: %s)", ~fileName.Quote());
        }
    }

    yvector<TChunkDescriptor> result;
    result.reserve(chunkIds.size());

    FOREACH (const auto& chunkId, chunkIds) {
        auto chunkFileName = GetChunkFileName(chunkId);
        auto chunkMetaFileName = chunkFileName + ChunkMetaSuffix;
        bool hasMeta = fileNames.find(chunkMetaFileName) != fileNames.end();
        bool hasData = fileNames.find(chunkFileName) != fileNames.end();
        if (hasMeta && hasData) {
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.Size = NFS::GetFileSize(chunkFileName);
            result.push_back(descriptor);
        } else if (!hasMeta) {
            LOG_WARNING("Missing meta file for %s", ~chunkFileName.Quote());
            RemoveFile(chunkMetaFileName);
        } else if (!hasData) {
            LOG_WARNING("Missing data file for %s", ~chunkMetaFileName.Quote());
            RemoveFile(chunkFileName);
        }
    }

    LOG_INFO("Done, found %d chunks", result.ysize());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
