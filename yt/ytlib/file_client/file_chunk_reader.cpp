#include "stdafx.h"

#include "file_chunk_reader.h"
#include "private.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/config.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <core/rpc/channel.h>

#include <core/compression/codec.h>

namespace NYT {
namespace NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

class TFileChunkReader
    : public IFileChunkReader
{
public:
    TFileChunkReader(
        TSequentialReaderConfigPtr sequentialConfig,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr uncompressedBlockCache,
        NCompression::ECodec codecId,
        i64 startOffset,
        i64 endOffset);

    virtual TFuture<void> Open() override;
    virtual TFuture<void> GetReadyEvent() override;

    virtual bool ReadBlock(TSharedRef* block) override;

    virtual TDataStatistics GetDataStatistics() const override;

    virtual TFuture<void> GetFetchingCompletedEvent() override;

private:

    TSequentialReaderConfigPtr SequentialConfig_;
    IChunkReaderPtr ChunkReader_;
    IBlockCachePtr UncompressedBlockCache_;
    NCompression::ECodec CodecId_;
    i64 StartOffset_;
    i64 EndOffset_;

    TSequentialReaderPtr SequentialReader_;
    TFuture<void> ReadyEvent_ = VoidFuture;
    bool BlockFetched_ = true;

    NLog::TLogger Logger;

    void DoOpen();

    TSharedRef GetBlock() const;

};

DECLARE_REFCOUNTED_CLASS(TFileChunkReader)
DEFINE_REFCOUNTED_TYPE(TFileChunkReader)

////////////////////////////////////////////////////////////////////////////////

TFileChunkReader::TFileChunkReader(
    TSequentialReaderConfigPtr sequentialConfig,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    NCompression::ECodec codecId,
    i64 startOffset,
    i64 endOffset)
    : SequentialConfig_(std::move(sequentialConfig))
    , ChunkReader_(std::move(chunkReader))
    , UncompressedBlockCache_(std::move(uncompressedBlockCache))
    , CodecId_(codecId)
    , StartOffset_(startOffset)
    , EndOffset_(endOffset)
    //, ReadyEvent_(VoidFuture)
    , Logger(FileClientLogger)
{
    Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
}

TFuture<void> TFileChunkReader::Open()
{
    return BIND(&TFileChunkReader::DoOpen, MakeWeak(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

void TFileChunkReader::DoOpen()
{
    LOG_INFO("Requesting chunk meta");

    auto metaOrError = WaitFor(ChunkReader_->GetMeta());
    THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Failed to get file chunk meta");

    LOG_INFO("Chunk meta received");
    const auto& meta = metaOrError.Value();
    
    auto type = EChunkType(meta.type());
    if (type != EChunkType::File) {
        THROW_ERROR_EXCEPTION("Invalid chunk type: expected %Qlv, actual %Qlv",
            EChunkType::File,
            type);
    }

    if (meta.version() != FormatVersion) {
        THROW_ERROR_EXCEPTION("Invalid file chunk format version: expected %v, actual %v",
            FormatVersion,
            meta.version());
    }

    std::vector<TSequentialReader::TBlockInfo> blockSequence;

    // COMPAT(psushin): new file chunk meta!
    auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(meta.extensions());

    i64 selectedSize = 0;
    int blockIndex = 0;
    auto addBlock = [&] (int index, i64 size) -> bool {
        if (StartOffset_ >= size) {
            StartOffset_ -= size;
            EndOffset_ -= size;
            ++blockIndex;
            return true;
        } else if (selectedSize < EndOffset_) {
            selectedSize += size;
            blockSequence.push_back(TSequentialReader::TBlockInfo(index, size));
            return true;
        }
        return false;
    };

    int blockCount = 0;
    if (fileBlocksExt) {
        // New chunk.
        blockCount = fileBlocksExt->blocks_size();
        blockSequence.reserve(blockCount);

        for (int index = 0; index < blockCount; ++index) {
            if (!addBlock(index, fileBlocksExt->blocks(index).size())) {
                break;
            }
        }
    } else {
        // Old chunk.
        auto blocksExt = GetProtoExtension<TBlocksExt>(meta.extensions());
        blockCount = blocksExt.blocks_size();

        blockSequence.reserve(blockCount);
        for (int index = 0; index < blockCount; ++index) {
            if (!addBlock(index, blocksExt.blocks(index).size())) {
                break;
            }
        }
    }
    YCHECK(blockCount >= 0);

    LOG_INFO("Reading %v blocks out of %v starting from %v (SelectedSize: %v)",
        blockSequence.size(),
        blockCount,
        blockIndex,
        selectedSize);

    SequentialReader_ = New<TSequentialReader>(
        SequentialConfig_,
        std::move(blockSequence),
        ChunkReader_,
        UncompressedBlockCache_,
        CodecId_);

    LOG_INFO("File reader opened");
}

bool TFileChunkReader::ReadBlock(TSharedRef* block)
{
    block->Reset();
    if (BlockFetched_ && !SequentialReader_->HasMoreBlocks()) {
        return false;
    }

    if (BlockFetched_) {
        BlockFetched_ = false;
        ReadyEvent_ = SequentialReader_->FetchNextBlock();
        if (!ReadyEvent_.IsSet()) {
            return true;
        }
    }

    if (ReadyEvent_.Get().IsOK()) {
        *block = GetBlock();
        StartOffset_ = std::max(StartOffset_ - static_cast<i64>(block->Size()), (i64)0);
        EndOffset_ = std::max(EndOffset_ - static_cast<i64>(block->Size()), (i64)0);
        BlockFetched_ = true;
    }

    return true;
}

TFuture<void> TFileChunkReader::GetReadyEvent()
{
    return ReadyEvent_;
}

TSharedRef TFileChunkReader::GetBlock() const
{
    auto block = SequentialReader_->GetCurrentBlock();

    auto* begin = block.Begin();
    auto* end = block.End();

    YCHECK(EndOffset_ > 0);

    if (EndOffset_ < block.Size()) {
        end = block.Begin() + EndOffset_;
    }

    if (StartOffset_ > 0) {
        begin = block.Begin() + StartOffset_;
    }

    return block.Slice(TRef(begin, end));
}

TFuture<void> TFileChunkReader::GetFetchingCompletedEvent()
{
    YCHECK(SequentialReader_);
    return SequentialReader_->GetFetchingCompletedEvent();
}

TDataStatistics TFileChunkReader::GetDataStatistics() const
{
    YCHECK(SequentialReader_);
    auto dataStatistics = ZeroDataStatistics();
    dataStatistics.set_uncompressed_data_size(SequentialReader_->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(SequentialReader_->GetCompressedDataSize());
}

////////////////////////////////////////////////////////////////////////////////

IFileChunkReaderPtr CreateFileChunkReader(
    TSequentialReaderConfigPtr sequentialConfig,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    NCompression::ECodec codecId,
    i64 startOffset,
    i64 endOffset)
{
    return New<TFileChunkReader>(
        sequentialConfig, 
        chunkReader, 
        uncompressedBlockCache, 
        codecId, 
        startOffset, 
        endOffset);
}

////////////////////////////////////////////////////////////////////////////////

class TFileMultiChunkReader
    : public IFileMultiChunkReader
    , public TSequentialMultiChunkReaderBase
{
public:
    TFileMultiChunkReader(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        IChannelPtr masterChannel,
        IBlockCachePtr compressedBlockCache,
        IBlockCachePtr uncompressedBlockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs);

    virtual bool ReadBlock(TSharedRef* block) override;


private:
    TMultiChunkReaderConfigPtr Config_;
    IBlockCachePtr UncompressedBlockCache_;

    IFileChunkReaderPtr CurrentReader_ = nullptr;

    virtual IChunkReaderBasePtr CreateTemplateReader(const TChunkSpec& chunkSpec, IChunkReaderPtr asyncReader) override;
    virtual void OnReaderSwitched() override;
};

////////////////////////////////////////////////////////////////////////////////

TFileMultiChunkReader::TFileMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr compressedBlockCache,
    IBlockCachePtr uncompressedBlockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs)
    : TSequentialMultiChunkReaderBase(
        config, 
        options, 
        masterChannel, 
        compressedBlockCache, 
        nodeDirectory, 
        chunkSpecs)
    , Config_(config)
    , UncompressedBlockCache_(uncompressedBlockCache) 
{ }

bool TFileMultiChunkReader::ReadBlock(TSharedRef* block)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

    block->Reset();

    // Nothing to read.
    if (!CurrentReader_)
        return false;

    bool readerFinished = !CurrentReader_->ReadBlock(block);
    if (!block->Empty()) {
        return true;
    }

    return OnEmptyRead(readerFinished);
}

IChunkReaderBasePtr TFileMultiChunkReader::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IChunkReaderPtr chunkReader)
{
    auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());

    i64 startOffset = 0;
    if (chunkSpec.has_lower_limit() && chunkSpec.lower_limit().has_offset()) {
        startOffset = chunkSpec.lower_limit().offset();
    }

    i64 endOffset = std::numeric_limits<i64>::max();
    if (chunkSpec.has_upper_limit() && chunkSpec.upper_limit().has_offset()) {
        endOffset = chunkSpec.upper_limit().offset();
    }

    LOG_INFO(
        "Creating file chunk reader (StartOffset: %v, EndOffset: %v)",
        startOffset,
        endOffset);

    return CreateFileChunkReader(
        Config_,
        std::move(chunkReader),
        UncompressedBlockCache_,
        NCompression::ECodec(miscExt.compression_codec()),
        startOffset,
        endOffset);
}

void TFileMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<IFileChunkReader*>(CurrentSession_.ChunkReader.Get());
    YCHECK(CurrentReader_);
}

////////////////////////////////////////////////////////////////////////////////

IFileMultiChunkReaderPtr CreateFileMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr compressedBlockCache,
    IBlockCachePtr uncompressedBlockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs)
{
    return New<TFileMultiChunkReader>(
        config, 
        options, 
        masterChannel, 
        compressedBlockCache, 
        uncompressedBlockCache, 
        nodeDirectory, 
        chunkSpecs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
