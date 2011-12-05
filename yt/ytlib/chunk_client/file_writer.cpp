#include "stdafx.h"
#include "file_writer.h"

#include "../misc/fs.h"
#include "../misc/serialize.h"
#include "../logging/log.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkServer::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;
    
///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(const TChunkId& id, const Stroka& fileName)
    : Id(id)
    , FileName(fileName)
    , Result(New<TAsyncStreamState::TAsyncResult>())
{
    ChunkMeta.set_id(id.ToProto());
    DataFile.Reset(new TFile(
        FileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq));
    Result->Set(TAsyncStreamState::TResult());
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncWriteBlock(const TSharedRef& data)
{
    auto* blockInfo = ChunkMeta.add_blocks();
    blockInfo->set_offset(DataFile->GetPosition());
    blockInfo->set_size(static_cast<int>(data.Size()));
    blockInfo->set_checksum(GetChecksum(data));

    DataFile->Write(data.Begin(), data.Size());
    return Result;
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncClose(const TChunkAttributes& attributes)
{
    DataFile->Close();
    DataFile.Destroy();

    // Write metainfo.
    *ChunkMeta.mutable_attributes() = attributes;
    
    TBlob metaBlob(ChunkMeta.ByteSize());
    if (!ChunkMeta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        LOG_FATAL("Failed to serialize chunk info (FileName: %s)",
            ~FileName);
    }

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaBlob);

    Stroka chunkInfoFileName = FileName + ChunkMetaSuffix;
    TFile chunkInfoFile(
        chunkInfoFileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq);
    Write(chunkInfoFile, header);
    chunkInfoFile.Write(metaBlob.begin(), metaBlob.ysize());
    chunkInfoFile.Close();

    if (!NFS::Rename(chunkInfoFileName + NFS::TempFileSuffix, chunkInfoFileName)) {
        LOG_FATAL("Error renaming temp chunk info file (FileName: %s)", ~chunkInfoFileName);
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        LOG_FATAL("Error renaming temp chunk file (FileName: %s)", ~FileName);
    }

    return Result;
}

void TFileWriter::Cancel(const Stroka& /*errorMessage*/)
{
    DataFile->Close();
    DataFile.Destroy();
    NFS::Remove(FileName + NFS::TempFileSuffix);
}

TChunkId TFileWriter::GetChunkId() const
{
    return Id;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

