#include "stdafx.h"
#include "file_reader.h"

#include "../misc/serialize.h"
#include "../misc/fs.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

using namespace NChunkServer::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(const Stroka& fileName)
    : FileName(fileName)
    , InfoSize(-1)
    , DataSize(-1)
{ }

void TFileReader::Open()
{
    Stroka chunkInfoFileName = FileName + ChunkInfoSuffix;
    TFile chunkInfoFile(
        chunkInfoFileName + NFS::TempFileSuffix, OpenExisting | RdOnly | Seq);
    InfoSize = chunkInfoFile.GetLength();
    TBufferedFileInput chunkInfoInput(chunkInfoFile);
        
    TChunkInfoHeader header;
    Read(chunkInfoInput, &header);
    if (header.Signature != TChunkInfoHeader::ExpectedSignature) {
        ythrow yexception()
            << Sprintf("Incorrect signature in chunk info header (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
                ~FileName,
                TChunkInfoHeader::ExpectedSignature,
                header.Signature);
    }

    Stroka metaBlob = chunkInfoInput.ReadAll();
    TRef metaRef(metaBlob.begin(), metaBlob.size());
    TChecksum checksum = GetChecksum(metaRef);
    if (checksum != header.Checksum) {
        ythrow yexception()
            << Sprintf("Incorrect checksum in chunk info file (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
                ~FileName,
                header.Checksum,
                checksum);
    }

    TChunkMeta meta;
    if (!DeserializeProtobuf(&meta, metaRef)) {
        ythrow yexception() << Sprintf("Failed to parse chunk info (FileName: %s)",
            ~FileName);
    }

    ChunkInfo.SetId(meta.GetId());
    ChunkInfo.SetMetaChecksum(checksum);
    ChunkInfo.MutableBlocks()->MergeFrom(meta.GetBlocks()); // is it a proper way of using MergeFrom?
    ChunkInfo.MutableAttributes()->CopyFrom(meta.GetAttributes());

    DataFile.Reset(new TFile(FileName, OpenExisting | RdOnly));
    DataSize = DataFile->GetLength();

    ChunkInfo.SetSize(DataSize + InfoSize);
}

TFuture<IAsyncReader::TReadResult>::TPtr
TFileReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    TReadResult result;
    result.Blocks.reserve(blockIndexes.ysize());

    for (int index = 0; index < blockIndexes.ysize(); ++index) {
        i32 blockIndex = blockIndexes[index];
        result.Blocks.push_back(ReadBlock(blockIndex));
    }

    return New< TFuture<TReadResult> >(result);
}

TSharedRef TFileReader::ReadBlock(int blockIndex)
{
    i32 blockCount = ChunkInfo.BlocksSize();

    if (blockIndex > blockCount || blockIndex < -blockCount) {
        return TSharedRef();
    }

    while (blockIndex < 0) {
        blockIndex += blockCount;
    }

    if (blockIndex >= blockCount) {
        return TSharedRef();
    }

    const TBlockInfo& blockInfo = ChunkInfo.GetBlocks(blockIndex);
    TBlob data(blockInfo.GetSize());
    i64 offset = blockInfo.GetOffset();
    DataFile->Pread(data.begin(), data.size(), offset); 

    TSharedRef result(MoveRV(data));

    TChecksum checksum = GetChecksum(result);
    if (checksum != blockInfo.GetChecksum()) {
        ythrow yexception()
            << Sprintf("Incorrect checksum in chunk block (FileName: %s, BlockIndex: %d, Expected: %" PRIx64 ", Found: %" PRIx64 ")",
                ~FileName,
                blockIndex,
                blockInfo.GetChecksum(),
                checksum);
    }

    return result;
}

i64 TFileReader::GetInfoSize() const
{
    return InfoSize;
}

i64 TFileReader::GetDataSize() const
{
    return DataSize;
}

i64 TFileReader::GetFullSize() const
{
    return InfoSize + DataSize;
}

i32 TFileReader::GetBlockCount() const
{
    return ChunkInfo.BlocksSize();
}

const NChunkServer::NProto::TChunkInfo& TFileReader::GetChunkInfo() const
{
    return ChunkInfo;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

