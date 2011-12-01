#include "stdafx.h"
#include "file_reader.h"

#include "../misc/serialize.h"
#include "../misc/fs.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(const Stroka& fileName)
    : FileName(fileName)
{ }

void TFileReader::Open()
{
    Stroka chunkInfoFileName = FileName + ChunkInfoSuffix;
    TBufferedFileInput chunkInfoFile(chunkInfoFileName + NFS::TempFileSuffix);
    
    TChunkInfoHeader header;
    Read(chunkInfoFile, &header);
    if (header.Signature != TChunkInfoHeader::ExpectedSignature) {
        ythrow yexception()
            << Sprintf("Incorrect signature in chunk info header (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
                ~FileName,
                TChunkInfoHeader::ExpectedSignature,
                header.Signature);
    }

    Stroka infoBlob = chunkInfoFile.ReadAll();
    TRef infoRef(infoBlob.begin(), infoBlob.size());
    TChecksum checksum = GetChecksum(infoRef);
    if (checksum != header.Checksum) {
        ythrow yexception()
            << Sprintf("Incorrect checksum in chunk info file (FileName: %s, Expected: %" PRIx64 ", Found: %" PRIx64")",
                ~FileName,
                header.Checksum,
                checksum);
    }

    if (!DeserializeProtobuf(&ChunkInfo, infoRef)) {
        ythrow yexception() << Sprintf("Failed to parse chunk info (FileName: %s)",
            ~FileName);
    }

    DataFile.Reset(new TFile(FileName, OpenExisting | RdOnly));
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

