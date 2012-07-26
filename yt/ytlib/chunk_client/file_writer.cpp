#include "stdafx.h"
#include "file_writer.h"

#include <ytlib/chunk_holder/chunk_meta_extensions.h>

#include <ytlib/misc/fs.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkWriterLogger;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(const Stroka& fileName, bool directMode)
    : FileName(fileName)
    , IsOpen(false)
    , IsClosed(false)
    , DataSize(0)
    , Result(MakeFuture(TError()))
    , DirectMode(directMode)
{ }

void TFileWriter::Open()
{
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    ui32 oMode = CreateAlways | WrOnly | Seq;
    if (DirectMode) {
        oMode |= Direct;
    }

    DataFile.Reset(new TFile(FileName + NFS::TempFileSuffix, oMode));

    IsOpen = true;
}

bool TFileWriter::TryWriteBlock(const TSharedRef& block)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    try {
        auto* blockInfo = BlocksExt.add_blocks();
        blockInfo->set_offset(DataFile->GetPosition());
        blockInfo->set_size(static_cast<int>(block.Size()));
        blockInfo->set_checksum(GetChecksum(block));

        DataFile->Write(block.Begin(), block.Size());

        DataSize += block.Size();
        return true;
    } catch (yexception& e) {
        Result = MakeFuture(TError(
            "Failed to write block to file: %s",
            e.what()));
        return false;
    }
}

TAsyncError TFileWriter::GetReadyEvent()
{
    return Result;
}

TAsyncError TFileWriter::AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen || !Result.Get().IsOK()) {
        return Result;
    }

    IsOpen = false;
    IsClosed = true;

    try {
        DataFile->Close();
        DataFile.Destroy();
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Failed to close chunk data file %s\n%s",
            ~FileName,
            ex.what()));
    }

    // Write meta.
    ChunkMeta.CopyFrom(chunkMeta);
    UpdateProtoExtension(ChunkMeta.mutable_extensions(), BlocksExt);
    
    TBlob metaBlob;
    YCHECK(SerializeToProto(&ChunkMeta, &metaBlob));

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(TRef::FromBlob(metaBlob));

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(
            chunkMetaFileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq);
        WritePod(chunkMetaFile, header);
        chunkMetaFile.Write(&*metaBlob.begin(), metaBlob.size());
        chunkMetaFile.Close();
    } catch (const std::exception& ex) {
        return MakeFuture(TError(
            "Failed to write chunk meta to %s\n%s",
            ~chunkMetaFileName.Quote(),
            ex.what()));
    }

    if (!NFS::Rename(chunkMetaFileName + NFS::TempFileSuffix, chunkMetaFileName)) {
        return MakeFuture(TError(
            "Error renaming temp chunk meta file %s",
            ~chunkMetaFileName.Quote()));
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        return MakeFuture(TError(
            "Error renaming temp chunk file %s",
            ~FileName.Quote()));
    }

    ChunkInfo.set_meta_checksum(header.Checksum);
    ChunkInfo.set_size(DataSize + metaBlob.size() + sizeof (TChunkMetaHeader));

    return MakeFuture(TError());
}


namespace {

void RemoveFile(const Stroka& fileName)
{
    if (!NFS::Remove(fileName)) {
        LOG_FATAL("Error deleting file %s", ~fileName.Quote());
    }
}

} // namespace

void TFileWriter::Abort()
{
    if (!IsOpen) {
        return;
    }
    IsClosed = true;
    IsOpen = false;

    DataFile.Destroy();
    RemoveFile(FileName + NFS::TempFileSuffix);
}

const TChunkInfo& TFileWriter::GetChunkInfo() const
{
    YASSERT(IsClosed);
    return ChunkInfo;
}

const TChunkMeta& TFileWriter::GetChunkMeta() const
{
    YASSERT(IsClosed);
    return ChunkMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

