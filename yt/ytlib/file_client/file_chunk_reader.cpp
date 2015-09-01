#include "stdafx.h"

#include "file_chunk_reader.h"
#include "private.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/api/client.h>

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
        TSequentialReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        i64 startOffset,
        i64 endOffset)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , ChunkReader_(std::move(chunkReader))
        , BlockCache_(std::move(blockCache))
        , CodecId_(codecId)
        , StartOffset_(startOffset)
        , EndOffset_(endOffset)
    {
        Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TFileChunkReader::DoOpen, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    virtual bool ReadBlock(TSharedRef* block) override
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

        YCHECK(ReadyEvent_.IsSet());
        if (ReadyEvent_.Get().IsOK()) {
            *block = GetBlock();
            YCHECK(!block->Empty());
            BlockFetched_ = true;
        }

        return true;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        YCHECK(SequentialReader_);
        auto dataStatistics = ZeroDataStatistics();
        dataStatistics.set_uncompressed_data_size(SequentialReader_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(SequentialReader_->GetCompressedDataSize());
        return dataStatistics;
    }

    virtual TFuture<void> GetFetchingCompletedEvent() override
    {
        YCHECK(SequentialReader_);
        return SequentialReader_->GetFetchingCompletedEvent();
    }

private:
    const TSequentialReaderConfigPtr Config_;
    const TMultiChunkReaderOptionsPtr Options_;
    const IChunkReaderPtr ChunkReader_;
    const IBlockCachePtr BlockCache_;
    const NCompression::ECodec CodecId_;

    i64 StartOffset_;
    i64 EndOffset_;

    TSequentialReaderPtr SequentialReader_;
    TFuture<void> ReadyEvent_ = VoidFuture;
    bool BlockFetched_ = true;

    NLogging::TLogger Logger = FileClientLogger;

    void DoOpen()
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
            Config_,
            std::move(blockSequence),
            ChunkReader_,
            BlockCache_,
            CodecId_);

        LOG_INFO("File reader opened");
    }

    TSharedRef GetBlock()
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

        StartOffset_ = std::max(StartOffset_ - static_cast<i64>(block.Size()), (i64)0);
        EndOffset_ = std::max(EndOffset_ - static_cast<i64>(block.Size()), (i64)0);

        return block.Slice(begin, end);
    }

};

IFileChunkReaderPtr CreateFileChunkReader(
    TSequentialReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    i64 startOffset,
    i64 endOffset)
{
    return New<TFileChunkReader>(
        config,
        options,
        chunkReader,
        blockCache,
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
        NApi::IClientPtr client,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        NConcurrency::IThroughputThrottlerPtr throttler)
        : TSequentialMultiChunkReaderBase(
            config,
            options,
            client,
            blockCache,
            nodeDirectory,
            chunkSpecs,
            throttler)
        , Config_(config)
    { }

    virtual bool ReadBlock(TSharedRef* block) override
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

        if (OnEmptyRead(readerFinished)) {
            return true;
        } else {
            CurrentReader_.Reset();
            return false;
        }
    }

private:
    const TMultiChunkReaderConfigPtr Config_;

    IFileChunkReaderPtr CurrentReader_;


    virtual IChunkReaderBasePtr CreateTemplateReader(
        const TChunkSpec& chunkSpec,
        IChunkReaderPtr chunkReader) override
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

        LOG_INFO("Creating file chunk reader (StartOffset: %v, EndOffset: %v)",
            startOffset,
            endOffset);

        return CreateFileChunkReader(
            Config_,
            Options_,
            std::move(chunkReader),
            BlockCache_,
            NCompression::ECodec(miscExt.compression_codec()),
            startOffset,
            endOffset);
    }

    virtual void OnReaderSwitched() override
    {
        CurrentReader_ = dynamic_cast<IFileChunkReader*>(CurrentSession_.ChunkReader.Get());
        YCHECK(CurrentReader_);
    }
};

IFileMultiChunkReaderPtr CreateFileMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    NApi::IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    IThroughputThrottlerPtr throttler)
{
    return New<TFileMultiChunkReader>(
        config, 
        options, 
        client,
        blockCache,
        nodeDirectory, 
        chunkSpecs,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
