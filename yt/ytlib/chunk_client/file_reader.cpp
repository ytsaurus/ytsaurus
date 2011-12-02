#include "stdafx.h"
#include "file_reader.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(const Stroka& fileName)
    : FileName(fileName)
{
    File.Reset(new TFile(fileName, OpenExisting|RdOnly));
    Size = File->GetLength();

    TChunkFooter footer;
    File->Seek(File->GetLength() - sizeof (footer), sSet);
    File->Read(&footer, sizeof (footer));

    if (footer.Signature != TChunkFooter::ExpectedSignature) {
        ythrow yexception() << Sprintf("Chunk footer signature mismatch (FileName: %s)",
            ~fileName.Quote());
    }

    YASSERT(footer.MetaSize >= 0);
    YASSERT(footer.MetaOffset >= 0);

    TBlob metaBlob(footer.MetaSize);
    File->Pread(metaBlob.begin(), footer.MetaSize, footer.MetaOffset);

    if (!DeserializeProtobuf(&Meta, metaBlob)) {
        ythrow yexception() << Sprintf("Failed to parse chunk meta (FileName: %s)",
            ~FileName.Quote());
    }

    MasterMeta = TSharedRef(TBlob(
        Meta.mastermeta().begin(),
        Meta.mastermeta().end()));

    TChunkOffset currentOffset = 0;
    BlockOffsets.reserve(GetBlockCount());
    for (int blockIndex = 0; blockIndex < GetBlockCount(); ++blockIndex) {
        BlockOffsets.push_back(currentOffset);
        currentOffset += Meta.blocks(blockIndex).size();
    }
}

i64 TFileReader::GetSize() const
{
    return Size;
}

i32 TFileReader::GetBlockCount() const
{
    return Meta.blocks_size();
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
    i32 blockCount = GetBlockCount();

    if (blockIndex > blockCount || blockIndex < -blockCount) {
        return TSharedRef();
    }

    if (blockIndex < 0) {
        blockIndex += blockCount;
    }

    if (blockIndex >= blockCount) {
        return TSharedRef();
    }

    const TBlockInfo& blockInfo = Meta.blocks(blockIndex);

    TBlob data(blockInfo.size());
    File->Pread(data.begin(), data.size(), BlockOffsets[blockIndex]); 

    TSharedRef result(MoveRV(data));

    if (blockInfo.checksum() != GetChecksum(result)) {
        ythrow yexception() << Sprintf("Chunk footer signature mismatch (FileName: %s, BlockIndex: %d)",
            ~FileName.Quote(),
            blockIndex);
    }

    return result;
}

TSharedRef TFileReader::GetMasterMeta() const
{
    return MasterMeta;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

