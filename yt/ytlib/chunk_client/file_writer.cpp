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

static NLog::TLogger& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(const Stroka& fileName)
    : FileName(fileName)
    , IsOpen(false)
    , IsClosed(false)
    , DataSize(0)
{ }

void TFileWriter::Open()
{
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    DataFile.Reset(new TFile(
        FileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq));

    IsOpen = true;
}

TAsyncError TFileWriter::AsyncWriteBlocks(const std::vector<TSharedRef>& blocks)
{
    YASSERT(IsOpen);
    YASSERT(!IsClosed);

    try {
        FOREACH (auto& data, blocks) {
            auto* blockInfo = BlocksExt.add_blocks();
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

TAsyncError TFileWriter::AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta)
{
    if (!IsOpen) {
        return MakeFuture(TError());
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
    YVERIFY(SerializeToProto(&ChunkMeta, &metaBlob));

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

