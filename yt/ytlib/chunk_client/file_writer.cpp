#include "stdafx.h"
#include "file_writer.h"

#include <ytlib/chunk_holder/extensions.h>

#include <ytlib/misc/fs.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkHolder::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

TChunkFileWriter::TChunkFileWriter(const Stroka& fileName)
    : FileName(fileName)
    , IsOpen(false)
    , IsClosed(false)
    , DataSize(0)
{ }

void TChunkFileWriter::Open()
{
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    DataFile.Reset(new TFile(
        FileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq));

    IsOpen = true;
}

TAsyncError TChunkFileWriter::AsyncWriteBlocks(const std::vector<TSharedRef>& blocks)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    try {
        FOREACH (auto& data, blocks) {
            auto* blockInfo = Blocks.add_blocks();
            blockInfo->set_offset(DataFile->GetPosition());
            blockInfo->set_size(static_cast<int>(data.Size()));
            blockInfo->set_checksum(GetChecksum(data));

            DataFile->Write(data.Begin(), data.Size());

            DataSize += data.Size();
        }
    } catch (yexception& e) {
        return MakeFuture(TError(
            "Failed to write block to file: %s",
            e.what()));
    }

    return MakeFuture(TError());
}

TAsyncError TChunkFileWriter::AsyncClose(
    const std::vector<TSharedRef>& blocks,
    const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen)
        return MakeFuture(TError());

    {
        auto res = AsyncWriteBlocks(MoveRV(blocks));
        if (!res->Get().IsOK())
            return res;
    }

    IsOpen = false;
    IsClosed = true;

    try {
        DataFile->Close();
        DataFile.Destroy();
    } catch (yexception& e) {
        return MakeFuture(TError(
            "Failed to close file: %s",
            e.what()));
    }

    // Write meta.
    ChunkMeta.CopyFrom(chunkMeta);
    SetProtoExtension(ChunkMeta.mutable_extensions(), Blocks);
    
    TBlob metaBlob(ChunkMeta.ByteSize());
    if (!ChunkMeta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        LOG_FATAL("Failed to serialize chunk meta (FileName: %s)",
            ~FileName);
    }

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(TRef::FromBlob(metaBlob));

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;

    try {
        TFile chunkMetaFile(
            chunkMetaFileName + NFS::TempFileSuffix,
            CreateAlways | WrOnly | Seq);
        Write(chunkMetaFile, header);
        chunkMetaFile.Write(metaBlob.begin(), metaBlob.ysize());
        chunkMetaFile.Close();
    } catch (yexception& e) {
        return MakeFuture(TError(
            "Failed to write chunk meta to file: %s",
            e.what()));
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

const TChunkInfo& TChunkFileWriter::GetChunkInfo() const
{
    YASSERT(IsClosed);
    return ChunkInfo;
}

const TChunkMeta& TChunkFileWriter::GetChunkMeta() const
{
    YASSERT(IsClosed);
    return ChunkMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

