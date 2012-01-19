#include "stdafx.h"
#include "file_writer.h"

#include <ytlib/misc/fs.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

TChunkFileWriter::TChunkFileWriter(const TChunkId& id, const Stroka& fileName)
    : Id(id)
    , FileName(fileName)
    , Result(ToFuture(TError()))
    , Open(false)
    , Closed(false)
    , DataSize(0)
{
    ChunkMeta.set_id(id.ToProto());
}

bool TChunkFileWriter::EnsureOpen()
{
    if (Open) {
        return true;
    }

    try {
        DataFile.Reset(new TFile(
            FileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq));
    } catch (...) {
        Result = ToFuture(TError(Sprintf("Error opening chunk file %s\n%s",
            ~FileName.Quote(),
            ~CurrentExceptionMessage())));
        return false;
    }

    Open = true;
    return true;
}

TAsyncError::TPtr TChunkFileWriter::AsyncWriteBlock(const TSharedRef& data)
{
    YASSERT(!Closed);

    if (!EnsureOpen()) {
        return Result;
    }

    auto* blockInfo = ChunkMeta.add_blocks();
    blockInfo->set_offset(DataFile->GetPosition());
    blockInfo->set_size(static_cast<int>(data.Size()));
    blockInfo->set_checksum(GetChecksum(data));

    try {
        DataFile->Write(data.Begin(), data.Size());
    } catch (...) {
        Result = ToFuture(TError(Sprintf("Error writing chunk file %s\n%s",
            ~FileName.Quote(),
            ~CurrentExceptionMessage())));
        return Result;
    }

    DataSize += data.Size();

    return Result;
}

TAsyncError::TPtr TChunkFileWriter::AsyncClose(const TChunkAttributes& attributes)
{
    if (Closed) {
        return Result;
    }

    if (!EnsureOpen()) {
        return Result;
    }

    try {
        DataFile->Close();
        DataFile.Destroy();
    } catch (...) {
        Result = ToFuture(TError(Sprintf("Error closing chunk file %s\n%s",
            ~FileName.Quote(),
            ~CurrentExceptionMessage())));
        return Result;
    }

    // Write meta.
    *ChunkMeta.mutable_attributes() = attributes;
    
    TBlob metaBlob(ChunkMeta.ByteSize());
    if (!ChunkMeta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        LOG_FATAL("Failed to serialize chunk meta (FileName: %s)",
            ~FileName);
    }

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaBlob);

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(
            chunkMetaFileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq);
        Write(chunkMetaFile, header);
        chunkMetaFile.Write(metaBlob.begin(), metaBlob.ysize());
        chunkMetaFile.Close();
    } catch (...) {
        Result = ToFuture(TError(Sprintf("Error writing chunk file meta %s\n%s",
            ~FileName.Quote(),
            ~CurrentExceptionMessage())));
        return Result;
    }

    if (!NFS::Rename(chunkMetaFileName + NFS::TempFileSuffix, chunkMetaFileName)) {
        Result = ToFuture(TError(Sprintf("Error renaming temp chunk meta file %s",
            ~chunkMetaFileName.Quote())));
        return Result;
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        Result = ToFuture(TError(Sprintf("Error renaming temp chunk file %s",
            ~FileName.Quote())));
        return Result;
    }

    ChunkInfo.set_id(Id.ToProto());
    ChunkInfo.set_meta_checksum(header.Checksum);
    ChunkInfo.set_size(DataSize + metaBlob.size() + sizeof (TChunkMetaHeader));
    ChunkInfo.mutable_blocks()->MergeFrom(ChunkMeta.blocks());
    ChunkInfo.mutable_attributes()->CopyFrom(ChunkMeta.attributes());

    Closed = true;
    return Result;
}

void TChunkFileWriter::Cancel(const TError& error)
{
    if (!Open || Closed) {
        return;
    }

    try {
        DataFile->Close();
    } catch (...) {
        LOG_WARNING("Error closing temp chunk file %s", ~FileName.Quote());
    }

    DataFile.Destroy();
    
    if (!NFS::Remove(FileName + NFS::TempFileSuffix)) {
        LOG_WARNING("Error deleting temp chunk file %s", ~FileName.Quote());
    }

    Closed = true;
}

TChunkId TChunkFileWriter::GetChunkId() const
{
    return Id;
}

TChunkInfo TChunkFileWriter::GetChunkInfo() const
{
    YASSERT(Closed);
    return ChunkInfo;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

