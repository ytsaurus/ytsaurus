#include "stdafx.h"
#include "file_reader.h"
#include "chunk_meta_extensions.h"
#include "format.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const Stroka& fileName,
    bool validateBlocksChecksums)
    : FileName_(fileName)
    , ValidateBlockChecksums_(validateBlocksChecksums)
{ }

void TFileReader::Open()
{
    YCHECK(!Opened_);

    auto metaFileName = FileName_ + ChunkMetaSuffix;
    TFile metaFile(
        metaFileName,
        OpenExisting | RdOnly | Seq | CloseOnExec);

    MetaSize_ = metaFile.GetLength();
    TBufferedFileInput metaInput(metaFile);

    TChunkMetaHeader metaHeader;
    ReadPod(metaInput, metaHeader);
    if (metaHeader.Signature != TChunkMetaHeader::ExpectedSignature) {
        THROW_ERROR_EXCEPTION("Incorrect header signature in chunk meta file %v: expected %v, actual %v",
            FileName_,
            TChunkMetaHeader::ExpectedSignature,
            metaHeader.Signature);
    }

    auto metaBlob = metaInput.ReadAll();
    auto metaBlobRef = TRef::FromString(metaBlob);

    auto checksum = GetChecksum(metaBlobRef);
    if (checksum != metaHeader.Checksum) {
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk meta file %v: expected %v, actual %v",
            FileName_,
            metaHeader.Checksum,
            checksum);
    }

    if (!DeserializeFromProtoWithEnvelope(&Meta_, metaBlobRef)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            FileName_);
    }

    BlocksExt_ = GetProtoExtension<TBlocksExt>(Meta_.extensions());
    BlockCount_ = BlocksExt_.blocks_size();

    DataFile_.reset(new TFile(FileName_, OpenExisting | RdOnly | CloseOnExec));
    Opened_ = true;
}

TFuture<std::vector<TSharedRef>> TFileReader::ReadBlocks(const std::vector<int>& blockIndexes)
{
    YCHECK(Opened_);

    try {
        std::vector<TSharedRef> blocks;
        blocks.reserve(blockIndexes.size());

        // Extract maximum contiguous ranges of blocks.
        int localIndex = 0;
        while (localIndex < blockIndexes.size()) {
            int startLocalIndex = localIndex;
            int startBlockIndex = blockIndexes[startLocalIndex];
            int endLocalIndex = startLocalIndex;
            while (endLocalIndex < blockIndexes.size() && blockIndexes[endLocalIndex] == startBlockIndex + (endLocalIndex - startLocalIndex)) {
                ++endLocalIndex;
            }

            int blockCount = endLocalIndex - startLocalIndex;
            auto subblocks = DoReadBlocks(startBlockIndex, blockCount);
            blocks.insert(blocks.end(), subblocks.begin(), subblocks.end());

            localIndex = endLocalIndex;
        }

        return MakeFuture(blocks);
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TSharedRef>>(ex);
    }
}

TFuture<std::vector<TSharedRef>> TFileReader::ReadBlocks(
    int firstBlockIndex,
    int blockCount)
{
    YCHECK(Opened_);
    YCHECK(firstBlockIndex >= 0);

    try {
        return MakeFuture(DoReadBlocks(firstBlockIndex, blockCount));
    } catch (const std::exception& ex) {
        return MakeFuture<std::vector<TSharedRef>>(ex);
    }
}

std::vector<TSharedRef> TFileReader::DoReadBlocks(
    int firstBlockIndex,
    int blockCount)
{
    if (firstBlockIndex + blockCount > BlockCount_) {
        THROW_ERROR_EXCEPTION("Requested to read blocks [%v,%v] from chunk %v while only %v blocks exist",
            firstBlockIndex,
            firstBlockIndex + blockCount - 1,
            FileName_,
            BlockCount_);
    }

    // Read all blocks within a single request.
    int lastBlockIndex = firstBlockIndex + blockCount - 1;
    const auto& firstBlockInfo = BlocksExt_.blocks(firstBlockIndex);
    const auto& lastBlockInfo = BlocksExt_.blocks(lastBlockIndex);
    i64 totalSize = lastBlockInfo.offset() + lastBlockInfo.size() - firstBlockInfo.offset();

    struct TFileChunkBlockTag { };
    auto data = TSharedMutableRef::Allocate<TFileChunkBlockTag>(totalSize, false);
    DataFile_->Pread(data.Begin(), data.Size(), firstBlockInfo.offset());

    // Slice the result; validate checksums.
    std::vector<TSharedRef> blocks;
    blocks.reserve(blockCount);
    for (int localIndex = 0; localIndex < blockCount; ++localIndex) {
        int blockIndex = firstBlockIndex + localIndex;
        const auto& blockInfo = BlocksExt_.blocks(blockIndex);
        auto block = data.Slice(
            blockInfo.offset() - firstBlockInfo.offset(),
            blockInfo.offset() - firstBlockInfo.offset() + blockInfo.size());
        if (ValidateBlockChecksums_) {
            auto checksum = GetChecksum(block);
            if (checksum != blockInfo.checksum()) {
                THROW_ERROR_EXCEPTION("Incorrect checksum of block %v in chunk data file %v: expected %v, actual %v",
                      blockIndex,
                      FileName_,
                      blockInfo.checksum(),
                      checksum);
            }
        }
        blocks.push_back(block);
    }

    return blocks;
}

TChunkMeta TFileReader::GetMeta(const TNullable<std::vector<int>>& extensionTags)
{
    return FilterChunkMetaByExtensionTags(Meta_, extensionTags);
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

TFuture<NProto::TChunkMeta> TFileReader::GetMeta(
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    // Partition tag filtering not implemented here
    // because there is no practical need.
    // Implement when necessary.
    YCHECK(!partitionTag);
    YCHECK(Opened_);
    return MakeFuture(GetMeta(extensionTags));
}

TChunkId TFileReader::GetChunkId() const
{
    YUNREACHABLE();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
