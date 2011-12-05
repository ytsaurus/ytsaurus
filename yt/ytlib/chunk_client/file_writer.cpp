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
    ChunkMeta.SetId(id.ToProto());
    DataFile.Reset(new TFile(
        FileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq));
    Result->Set(TAsyncStreamState::TResult());
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncWriteBlock(const TSharedRef& data)
{
    auto* blockInfo = ChunkMeta.AddBlocks();
    blockInfo->SetOffset(DataFile->GetPosition());
    blockInfo->SetSize(static_cast<int>(data.Size()));
    blockInfo->SetChecksum(GetChecksum(data));

    DataFile->Write(data.Begin(), data.Size());
    return Result;
}

TAsyncStreamState::TAsyncResult::TPtr 
TFileWriter::AsyncClose(const TChunkAttributes& attributes)
{
    DataFile->Close();
    DataFile.Destroy();

    // Write meta.
    *ChunkMeta.MutableAttributes() = attributes;
    
    TBlob metaBlob(ChunkMeta.ByteSize());
    if (!ChunkMeta.SerializeToArray(metaBlob.begin(), metaBlob.ysize())) {
        LOG_FATAL("Failed to serialize chunk meta (FileName: %s)",
            ~FileName);
    }

    TChunkMetaHeader header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaBlob);

    Stroka chunkMetaFileName = FileName + ChunkMetaSuffix;
    TFile chunkMetaFile(
        chunkMetaFileName + NFS::TempFileSuffix,
        CreateAlways | WrOnly | Seq);
    Write(chunkMetaFile, header);
    chunkMetaFile.Write(metaBlob.begin(), metaBlob.ysize());
    chunkMetaFile.Close();

    if (!NFS::Rename(chunkMetaFileName + NFS::TempFileSuffix, chunkMetaFileName)) {
        LOG_FATAL("Error renaming temp chunk meta file (FileName: %s)", ~chunkMetaFileName);
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

