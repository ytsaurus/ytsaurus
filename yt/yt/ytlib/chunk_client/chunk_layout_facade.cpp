#include "chunk_layout_facade.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NChunkClient {

//////////////////////////////////////////////////////////////////////////////

namespace {

//////////////////////////////////////////////////////////////////////////////

template <class T>
void ReadMetaHeader(
    const TSharedRef& metaFileBlob,
    const TString& fileName,
    TChunkMetaHeader_2* metaHeader,
    TRef* metaBlob)
{
    if (metaFileBlob.Size() < sizeof(T)) {
        THROW_ERROR_EXCEPTION("Chunk meta file %v is too short: at least %v bytes expected",
            fileName,
            sizeof(T));
    }
    *static_cast<T*>(metaHeader) = *reinterpret_cast<const T*>(metaFileBlob.Begin());
    *metaBlob = metaFileBlob.Slice(sizeof(T), metaFileBlob.Size());
}

//////////////////////////////////////////////////////////////////////////////

} // namespace

//////////////////////////////////////////////////////////////////////////////

TBlocksExt::TBlocksExt(const NChunkClient::NProto::TBlocksExt& message)
{
    Blocks.reserve(message.blocks_size());
    for (const auto& blockInfo : message.blocks()) {
        Blocks.push_back(TMyBlockInfo{
            .Offset = blockInfo.offset(),
            .Size = blockInfo.size(),
            .Checksum = blockInfo.checksum(),
        });
    }

    SyncOnClose = message.sync_on_close();
}

//////////////////////////////////////////////////////////////////////////////

TChunkLayoutReader::TChunkLayoutReader(NChunkClient::TChunkId chunkId, TString chunkFileName, TString chunkMetaFileName, const TOptions& options)
    : ChunkId_(chunkId)
    , ChunkFileName_(std::move(chunkFileName))
    , ChunkMetaFileName_(std::move(chunkMetaFileName))
    , Options_(options)
    , Logger(ChunkClientLogger().WithTag("ChunkId: %v", ChunkId_))
{ }

std::vector<TChunkLayoutReader::TBlockRange> TChunkLayoutReader::GetBlockRanges(const std::vector<int>& blockIndexes)
{
    std::vector<TBlockRange> blockRanges;

    int localIndex = 0;
    while (localIndex < std::ssize(blockIndexes)) {
        auto endLocalIndex = localIndex;

        while (endLocalIndex < std::ssize(blockIndexes) && blockIndexes[endLocalIndex] == blockIndexes[localIndex] + (endLocalIndex - localIndex)) {
            ++endLocalIndex;
        }

        blockRanges.push_back({blockIndexes[localIndex], blockIndexes[localIndex] + (endLocalIndex - localIndex)});
        localIndex = endLocalIndex;
    }

    return blockRanges;
}

std::vector<TChunkLayoutReader::TBlockRange> TChunkLayoutReader::GetBlockRanges(int startBlockIndex, int blockCount)
{
    return {{startBlockIndex, startBlockIndex + blockCount}};
}

TChunkLayoutReader::TReadRequest TChunkLayoutReader::GetReadRequest(TBlockRange blockRange, const TBlocksExtPtr& blocksExt)
{
    YT_VERIFY(blockRange.StartBlockIndex >= 0);
    YT_VERIFY(blockRange.EndBlockIndex > blockRange.StartBlockIndex);

    int chunkBlockCount = std::ssize(blocksExt->Blocks);
    if (blockRange.EndBlockIndex > chunkBlockCount) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MalformedReadRequest,
            "Requested to read blocks [%v, %v) from chunk %v while only %v blocks exist",
            blockRange.StartBlockIndex,
            blockRange.EndBlockIndex,
            ChunkFileName_,
            chunkBlockCount);
    }

    const auto& firstBlockInfo = blocksExt->Blocks[blockRange.StartBlockIndex];
    const auto& lastBlockInfo = blocksExt->Blocks[blockRange.EndBlockIndex - 1];

    return {
        .Offset = firstBlockInfo.Offset,
        .Size = lastBlockInfo.Offset + lastBlockInfo.Size - firstBlockInfo.Offset,
    };
}

TChunkLayoutReader::TChunkMetaWithChunkId TChunkLayoutReader::DeserializeMeta(TSharedRef metaFileBlob)
{
    TChunkLayoutReader::TChunkMetaWithChunkId result;

    if (metaFileBlob.Size() < sizeof(TChunkMetaHeaderBase)) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Chunk meta file %v is too short: at least %v bytes expected",
            ChunkMetaFileName_,
            sizeof(TChunkMetaHeaderBase));
    }

    TChunkMetaHeader_2 metaHeader;
    TRef metaBlob;
    const auto* metaHeaderBase = reinterpret_cast<const TChunkMetaHeaderBase*>(metaFileBlob.Begin());

    switch (metaHeaderBase->Signature) {
        case TChunkMetaHeader_1::ExpectedSignature:
            ReadMetaHeader<TChunkMetaHeader_1>(metaFileBlob, ChunkMetaFileName_, &metaHeader, &metaBlob);
            metaHeader.ChunkId = ChunkId_;
            break;

        case TChunkMetaHeader_2::ExpectedSignature:
            ReadMetaHeader<TChunkMetaHeader_2>(metaFileBlob, ChunkMetaFileName_, &metaHeader, &metaBlob);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::BrokenChunkFileMeta,
                "Incorrect header signature %x in chunk meta file %v",
                metaHeaderBase->Signature,
                ChunkMetaFileName_);
    }

    auto checksum = GetChecksum(metaBlob);
    if (checksum != metaHeader.Checksum) {
        // TODO(achulkov2): [PForReview] Provide callback option.
        // DumpBrokenMeta(metaBlob);
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::BrokenChunkFileMeta,
            "Incorrect checksum in chunk meta file %v: expected %x, actual %x",
            ChunkMetaFileName_,
            metaHeader.Checksum,
            checksum)
            << TErrorAttribute("meta_file_length", metaFileBlob.Size());
    }

    if (ChunkId_ != NullChunkId && metaHeader.ChunkId != ChunkId_) {
        THROW_ERROR_EXCEPTION("Invalid chunk id in meta file %v: expected %v, actual %v",
            ChunkMetaFileName_,
            ChunkId_,
            metaHeader.ChunkId);
    }
    result.ChunkId = metaHeader.ChunkId;

    NProto::TChunkMeta meta;
    if (!TryDeserializeProtoWithEnvelope(&meta, metaBlob)) {
        THROW_ERROR_EXCEPTION("Failed to parse chunk meta file %v",
            ChunkMetaFileName_);
    }
    result.ChunkMeta = New<TRefCountedChunkMeta>(std::move(meta));

    return result;
}

std::vector<TBlock> TChunkLayoutReader::DeserializeBlocks(TSharedRef blocksBlob, TBlockRange blockRange, const TBlocksExtPtr& blocksExt)
{
    std::vector<TBlock> blocks;
    blocks.reserve(blockRange.EndBlockIndex - blockRange.StartBlockIndex);

    // TODO(achulkov2): [PForReview] Add validation that blob size is equal to the sum of block sizes.

    const auto& firstBlockInfo = blocksExt->Blocks[blockRange.StartBlockIndex];

    for (int blockIndex = blockRange.StartBlockIndex; blockIndex < blockRange.EndBlockIndex; ++blockIndex) {
        const auto& blockInfo = blocksExt->Blocks[blockIndex];
        auto blockData = blocksBlob.Slice(
            blockInfo.Offset - firstBlockInfo.Offset,
            blockInfo.Offset - firstBlockInfo.Offset + blockInfo.Size);
        if (Options_.ValidateBlockChecksums) {
            auto checksum = GetChecksum(blockData);
            if (checksum != blockInfo.Checksum) {
                // TODO(achulkov2): [PForReview] Provide callback option.
                // DumpBrokenBlock(blockIndex, blockInfo, blockData);
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                    "Incorrect checksum of block %v in chunk data file %v: expected %v, actual %v",
                    blockIndex,
                    ChunkFileName_,
                    blockInfo.Checksum,
                    checksum)
                    << TErrorAttribute("first_block_index", blockRange.StartBlockIndex)
                    << TErrorAttribute("block_count", blockRange.EndBlockIndex - blockRange.StartBlockIndex);
            }
        }
        blocks.emplace_back(blockData, blockInfo.Checksum);
    }

    return blocks;
}

//////////////////////////////////////////////////////////////////////////////

TChunkLayoutFacade::TChunkLayoutFacade(NChunkClient::TChunkId chunkId)
    : ChunkId_(chunkId)
    , Logger(ChunkClientLogger().WithTag("ChunkId: %v", ChunkId_))
{ }

TChunkLayoutFacade::TWriteRequest TChunkLayoutFacade::AddBlocks(const std::vector<TBlock>& blocks)
{
    TWriteRequest request;

    request.StartOffset = DataSize_;
    request.EndOffset = request.StartOffset;

    request.Buffers.reserve(blocks.size());

    for (const auto& block : blocks) {
        auto error = block.ValidateChecksum();
        YT_LOG_FATAL_UNLESS(error.IsOK(), error, "Block checksum mismatch during file writing");

        auto* blockInfo = BlocksExt_.add_blocks();
        blockInfo->set_offset(request.EndOffset);
        blockInfo->set_size(ToProto<i64>(block.Size()));
        blockInfo->set_checksum(block.GetOrComputeChecksum());

        request.EndOffset += block.Size();
        request.Buffers.push_back(block.Data);
    }

    DataSize_ = request.EndOffset;

    return request;
}

TSharedMutableRef TChunkLayoutFacade::PrepareChunkMetaBlob()
{
    auto metaData = SerializeProtoToRefWithEnvelope(*ChunkMeta_);

    TChunkMetaHeader_2 header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);
    header.ChunkId = ChunkId_;

    MetaDataSize_ = metaData.Size() + sizeof(header);

    struct TMetaBufferTag
    { };

    auto buffer = TSharedMutableRef::Allocate<TMetaBufferTag>(MetaDataSize_, {.InitializeStorage = false});
    ::memcpy(buffer.Begin(), &header, sizeof(header));
    ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

    return buffer;
}

TSharedMutableRef TChunkLayoutFacade::Close(TDeferredChunkMetaPtr chunkMeta)
{
    if (!chunkMeta->IsFinalized()) {
        auto& mapping = chunkMeta->BlockIndexMapping();
        mapping = std::vector<int>(BlocksExt_.blocks().size());
        std::iota(mapping->begin(), mapping->end(), 0);
        chunkMeta->Finalize();
    }

    ChunkMeta_->CopyFrom(*chunkMeta);
    SetProtoExtension(ChunkMeta_->mutable_extensions(), BlocksExt_);

    auto chunkMetaBlob = PrepareChunkMetaBlob();

    ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
    return chunkMetaBlob;
}

i64 TChunkLayoutFacade::GetDataSize() const
{
    return DataSize_;
}

i64 TChunkLayoutFacade::GetMetaDataSize() const
{
    return MetaDataSize_;
}

const NChunkClient::TRefCountedChunkMetaPtr& TChunkLayoutFacade::GetChunkMeta() const
{
    return ChunkMeta_;
}

const NChunkClient::NProto::TChunkInfo& TChunkLayoutFacade::GetChunkInfo() const
{
    return ChunkInfo_;
}


//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient