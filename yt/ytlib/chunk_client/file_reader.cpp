#include "stdafx.h"
#include "file_reader.h"
#include "chunk_meta_extensions.h"
#include "format.h"

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ReadHeader(
    const TRef& metaFileBlobRef,
    const Stroka& fileName,
    TChunkMetaHeader_2* metaHeader,
    TRef* metaBlobRef)
{
    if (metaFileBlobRef.Size() < sizeof(T)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            fileName,
            sizeof(T));
    }
    *static_cast<T*>(metaHeader) = *reinterpret_cast<const T*>(metaFileBlobRef.Begin());
    *metaBlobRef = metaFileBlobRef.Slice(sizeof(T), metaFileBlobRef.Size());
}

} // namespace

TFileReader::TFileReader(
    const TChunkId& chunkId,
    const Stroka& fileName,
    bool validateBlocksChecksums)
    : ChunkId_(chunkId)
    , FileName_(fileName)
    , ValidateBlockChecksums_(validateBlocksChecksums)
{ }

void TFileReader::Open()
{
    YCHECK(!Opened_);

    auto metaFileName = FileName_ + ChunkMetaSuffix;
    TFile metaFile(
        metaFileName,
        OpenExisting | RdOnly | Seq | CloseOnExec);

    TBufferedFileInput metaFileInput(metaFile);
    auto metaFileBlob = metaFileInput.ReadAll();
    auto metaFileBlobRef = TRef::FromString(metaFileBlob);

    TChunkMetaHeader_2 metaHeader;
    TRef metaBlobRef;
    const auto* metaHeaderBase = reinterpret_cast<const TChunkMetaHeaderBase*>(metaFileBlobRef.Begin());
    switch (metaHeaderBase->Signature) {
        case TChunkMetaHeader_1::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_1>(metaFileBlobRef, metaFileName, &metaHeader, &metaBlobRef);
            metaHeader.ChunkId = ChunkId_;
            break;

        case TChunkMetaHeader_2::ExpectedSignature:
            ReadHeader<TChunkMetaHeader_2>(metaFileBlobRef, metaFileName, &metaHeader, &metaBlobRef);
            break;

        default:
            THROW_ERROR_EXCEPTION("Incorrect header signature %x in chunk meta file %v",
                metaHeaderBase->Signature,
                FileName_);
    }

    auto checksum = GetChecksum(metaBlobRef);
    if (checksum != metaHeader.Checksum) {
        THROW_ERROR_EXCEPTION("Incorrect checksum in chunk meta file %v: expected %v, actual %v",
            metaFileName,
            metaHeader.Checksum,
            checksum);
    }

    if (ChunkId_ != NullChunkId && metaHeader.ChunkId != ChunkId_) {
        THROW_ERROR_EXCEPTION("Invalid chunk id in meta file %v: expected %v, actual %v",
            metaFileName,
            ChunkId_,
            metaHeader.ChunkId);
    }

    if (!TryDeserializeFromProtoWithEnvelope(&Meta_, metaBlobRef)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            metaFileName);
    }

    MetaSize_ = metaBlobRef.Size();

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
    return ChunkId_;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
