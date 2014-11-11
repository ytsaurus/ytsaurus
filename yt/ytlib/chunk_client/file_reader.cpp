#include "stdafx.h"
#include "file_reader.h"
#include "chunk_meta_extensions.h"
#include "format.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/fs.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(const Stroka& fileName)
    : FileName_(fileName)
    , Opened_(false)
    , MetaSize_(-1)
    , DataSize_(-1)
{ }

void TFileReader::Open()
{
    YCHECK(!Opened_);

    Stroka metaFileName = FileName_ + ChunkMetaSuffix;
    TFile metaFile(
        metaFileName,
        OpenExisting | RdOnly | Seq | CloseOnExec);

    MetaSize_ = metaFile.GetLength();
    TBufferedFileInput metaInput(metaFile);

    TChunkMetaHeader metaHeader;
    ReadPod(metaInput, metaHeader);
    if (metaHeader.Signature != TChunkMetaHeader::ExpectedSignature) {
        THROW_ERROR_EXCEPTION("Incorrect header signature in chunk meta file %Qv: expected %v, actual %v",
            FileName_,
            TChunkMetaHeader::ExpectedSignature,
            metaHeader.Signature);
    }

    auto metaBlob = metaInput.ReadAll();
    auto metaBlobRef = TRef::FromString(metaBlob);

    auto checksum = GetChecksum(metaBlobRef);
    if (checksum != metaHeader.Checksum) {
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk meta file %Qv: expected %v, actual %v",
            FileName_,
            metaHeader.Checksum,
            checksum);
    }

    if (!DeserializeFromProtoWithEnvelope(&Meta_, metaBlobRef)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %Qv",
            FileName_);
    }

    BlocksExt_ = GetProtoExtension<TBlocksExt>(Meta_.extensions());
    BlockCount_ = BlocksExt_.blocks_size();

    DataFile_.reset(new TFile(FileName_, OpenExisting | RdOnly | CloseOnExec));
    Opened_ = true;
}

IChunkReader::TAsyncReadBlocksResult TFileReader::ReadBlocks(const std::vector<int>& blockIndexes)
{
    YCHECK(Opened_);

    try {
        std::vector<TSharedRef> blocks;
        blocks.reserve(blockIndexes.size());

        for (int index = 0; index < blockIndexes.size(); ++index) {
            int blockIndex = blockIndexes[index];
            YCHECK(blockIndex >= 0);
            if (blockIndex < BlockCount_) {
                blocks.push_back(ReadBlock(blockIndex));
            } else {
                blocks.push_back(TSharedRef());
            }
        }

        return MakeFuture(TReadBlocksResult(std::move(blocks)));
    } catch (const std::exception& ex) {
        return MakeFuture<TReadBlocksResult>(ex);
    }
}

IChunkReader::TAsyncReadBlocksResult TFileReader::ReadBlocks(
    int firstBlockIndex,
    int blockCount)
{
    YCHECK(Opened_);
    YCHECK(firstBlockIndex >= 0);

    try {
        std::vector<TSharedRef> blocks;
        blocks.reserve(blockCount);

        for (int blockIndex = firstBlockIndex;
             blockIndex < BlockCount_ && blockIndex < firstBlockIndex + blockCount;
             ++blockIndex)
        {
            blocks.push_back(ReadBlock(blockIndex));
        }

        return MakeFuture(TReadBlocksResult(std::move(blocks)));
    } catch (const std::exception& ex) {
        return MakeFuture<TReadBlocksResult>(ex);
    }
}

TSharedRef TFileReader::ReadBlock(int blockIndex)
{
    YCHECK(Opened_);
    YCHECK(blockIndex >= 0 && blockIndex < BlockCount_);

    const auto& blockInfo = BlocksExt_.blocks(blockIndex);
    struct TFileChunkBlockTag { };
    auto data = TSharedRef::Allocate<TFileChunkBlockTag>(blockInfo.size(), false);
    i64 offset = blockInfo.offset();
    DataFile_->Pread(data.Begin(), data.Size(), offset);

    auto checksum = GetChecksum(data);
    if (checksum != blockInfo.checksum()) {
        THROW_ERROR_EXCEPTION("Incorrect checksum of block %v in chunk data file %Qv: expected %v, actual %v",
            blockIndex,
            FileName_,
            blockInfo.checksum(),
            checksum);
    }

    return data;
}

TChunkMeta TFileReader::GetMeta(const std::vector<int>* extensionTags /*= nullptr*/)
{
    return extensionTags
        ? FilterChunkMetaByExtensionTags(Meta_, *extensionTags)
        : Meta_;
}

i64 TFileReader::GetMetaSize() const
{
    YCHECK(Opened_);
    return MetaSize_;
}

i64 TFileReader::GetDataSize() const
{
    YCHECK(Opened_);
    return DataSize_;
}

i64 TFileReader::GetFullSize() const
{
    YCHECK(Opened_);
    return MetaSize_ + DataSize_;
}

IChunkReader::TAsyncGetMetaResult TFileReader::GetMeta(
    const TNullable<int>& partitionTag,
    const std::vector<int>* extensionTags)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YCHECK(!partitionTag);
    YCHECK(Opened_);
    return MakeFuture(TGetMetaResult(GetMeta(extensionTags)));
}

TChunkId TFileReader::GetChunkId() const 
{
    YUNREACHABLE();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
