#include "stdafx.h"
#include "file_writer.h"

#include "../misc/fs.h"
#include "../misc/serialize.h"
#include "../logging/log.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkClient"); // TODO: move to common.h
    
///////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(const TChunkId& id, const Stroka& fileName)
    : Id(id)
    , FileName(fileName)
    , Result(New<TAsyncStreamState::TAsyncResult>())
{
    ChunkInfo.SetId(id.ToProto());
    DataFile.Reset(
        new TFile(FileName + NFS::TempFileSuffix, CreateAlways | WrOnly | Seq));
    Result->Set(TAsyncStreamState::TResult());
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncWriteBlock(const TSharedRef& data)
{
    TBlockInfo* blockInfo = ChunkInfo.AddBlocks();
    blockInfo->SetOffset(DataFile->GetPosition());
    blockInfo->SetSize(static_cast<int>(data.Size()));
    blockInfo->SetChecksum(GetChecksum(data));

    DataFile->Write(data.Begin(), data.Size());
    return Result;
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncClose(const NProto::TChunkAttributes& chunkAttributes)
{
    DataFile->Close();
    DataFile.Reset(NULL);

    *ChunkInfo.MutableAttributes() = chunkAttributes;
    
    TBlob infoBlob(ChunkInfo.ByteSize());
    if (!ChunkInfo.SerializeToArray(infoBlob.begin(), infoBlob.ysize())) {
        LOG_FATAL("Failed to serialize chunk info in %s",
            ~FileName.Quote());
    }

    TChunkInfoHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(infoBlob);

    // Writing metainfo
    Stroka chunkInfoFileName = FileName + ChunkInfoSuffix;
    TFile chunkInfoFile(
        chunkInfoFileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq);
    Write(chunkInfoFile, header);
    chunkInfoFile.Write(infoBlob.begin(), infoBlob.ysize());
    chunkInfoFile.Close();

    if (!NFS::Rename(chunkInfoFileName + NFS::TempFileSuffix, chunkInfoFileName)) {
        return New<TAsyncStreamState::TAsyncResult>(TAsyncStreamState::TResult(
            false, 
            Sprintf("Error renaming temp chunk info file (FileName: %s)",
                ~chunkInfoFileName)));
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        return New<TAsyncStreamState::TAsyncResult>(TAsyncStreamState::TResult(
            false, 
            Sprintf("Error renaming temp chunk info file (FileName: %s)",
                ~FileName)));
    }

    return Result;
}

void TFileWriter::Cancel(const Stroka& /*errorMessage*/)
{
    // TODO: Delete files
    DataFile.Destroy();
}

TChunkId TFileWriter::GetChunkId() const
{
    return Id;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

