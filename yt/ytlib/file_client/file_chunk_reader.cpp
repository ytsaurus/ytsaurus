#include "file_chunk_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_memory_manager.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/replication_reader.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/multi_reader_base.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/reader_factory.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/channel.h>

namespace NYT::NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

class TFileChunkReader
    : public IFileReader
{
public:
    TFileChunkReader(
        TBlockFetcherConfigPtr config,
        IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        const TClientBlockReadOptions& blockReadOptions,
        i64 startOffset,
        i64 endOffset,
        TChunkReaderMemoryManagerPtr chunkReaderMemoryManager)
        : Config_(std::move(config))
        , ChunkReader_(std::move(chunkReader))
        , BlockCache_(std::move(blockCache))
        , CodecId_(codecId)
        , BlockReadOptions_(blockReadOptions)
        , StartOffset_(startOffset)
        , EndOffset_(endOffset)
    {
        if (chunkReaderMemoryManager) {
            MemoryManager_ = chunkReaderMemoryManager;
        } else {
            MemoryManager_ = New<TChunkReaderMemoryManager>(TChunkReaderMemoryManagerOptions(Config_->WindowSize));
        }

        Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
        if (BlockReadOptions_.ReadSessionId) {
            Logger.AddTag("ReadSessionId: %v", BlockReadOptions_.ReadSessionId);
        }

        YT_LOG_INFO("Creating file chunk reader (StartOffset: %v, EndOffset: %v)",
            startOffset,
            endOffset);

        ReadyEvent_ = BIND(&TFileChunkReader::DoOpen, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    virtual bool ReadBlock(TBlock* block) override
    {
        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (BlockFetched_ && !SequentialBlockFetcher_->HasMoreBlocks()) {
            return false;
        }

        *block = TBlock();
        if (BlockFetched_) {
            BlockFetched_ = false;
            MemoryManager_->SetRequiredMemorySize(SequentialBlockFetcher_->GetNextBlockSize());
            CurrentBlock_ = SequentialBlockFetcher_->FetchNextBlock();
            ReadyEvent_ = CurrentBlock_.As<void>();
            if (!ReadyEvent_.IsSet()) {
                return true;
            }
        }

        YT_VERIFY(ReadyEvent_.IsSet());
        if (ReadyEvent_.Get().IsOK()) {
            *block = GetBlock();
            YT_VERIFY(!block->Data.Empty());
            BlockFetched_ = true;
        }

        return true;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        TDataStatistics dataStatistics;
        dataStatistics.set_uncompressed_data_size(SequentialBlockFetcher_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(SequentialBlockFetcher_->GetCompressedDataSize());
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        return TCodecStatistics().Append(SequentialBlockFetcher_->GetDecompressionTime());
    }

    virtual bool IsFetchingCompleted() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        return SequentialBlockFetcher_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        if (ReadyEvent_.IsSet() && !ReadyEvent_.Get().IsOK()) {
            return std::vector<TChunkId>(1, ChunkReader_->GetChunkId());
        } else {
            return std::vector<TChunkId>();
        }
    }


private:
    const TBlockFetcherConfigPtr Config_;
    const IChunkReaderPtr ChunkReader_;
    const IBlockCachePtr BlockCache_;
    const NCompression::ECodec CodecId_;
    const TClientBlockReadOptions BlockReadOptions_;

    i64 StartOffset_;
    i64 EndOffset_;

    TChunkReaderMemoryManagerPtr MemoryManager_;

    TSequentialBlockFetcherPtr SequentialBlockFetcher_;
    TFuture<void> ReadyEvent_;
    bool BlockFetched_ = true;

    NLogging::TLogger Logger = FileClientLogger;

    TFuture<TBlock> CurrentBlock_;

    void DoOpen()
    {
        YT_LOG_INFO("Requesting chunk meta");

        auto metaOrError = WaitFor(ChunkReader_->GetMeta(BlockReadOptions_));
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Failed to get file chunk meta");

        YT_LOG_INFO("Chunk meta received");
        const auto& meta = metaOrError.Value();

        auto type = EChunkType(meta->type());
        if (type != EChunkType::File) {
            THROW_ERROR_EXCEPTION("Invalid chunk type: expected %Qlv, actual %Qlv",
                EChunkType::File,
                type);
        }

        if (meta->version() != FormatVersion) {
            THROW_ERROR_EXCEPTION("Invalid file chunk format version: expected %v, actual %v",
                FormatVersion,
                meta->version());
        }

        std::vector<TBlockFetcher::TBlockInfo> blockSequence;

        // COMPAT(psushin): new file chunk meta!
        auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(meta->extensions());

        i64 selectedSize = 0;
        int blockIndex = 0;
        auto addBlock = [&] (int index, i64 size) -> bool {
            if (selectedSize == 0 && StartOffset_ >= size) {
                StartOffset_ -= size;
                EndOffset_ -= size;
                ++blockIndex;
                return true;
            } else if (selectedSize < EndOffset_) {
                selectedSize += size;
                blockSequence.push_back(TBlockFetcher::TBlockInfo{index, size, index /* priority */});
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
            auto blocksExt = GetProtoExtension<TBlocksExt>(meta->extensions());
            blockCount = blocksExt.blocks_size();

            blockSequence.reserve(blockCount);
            for (int index = 0; index < blockCount; ++index) {
                if (!addBlock(index, blocksExt.blocks(index).size())) {
                    break;
                }
            }
        }
        YT_VERIFY(blockCount >= 0);

        YT_LOG_INFO("Reading %v blocks out of %v starting from %v (SelectedSize: %v)",
            blockSequence.size(),
            blockCount,
            blockIndex,
            selectedSize);

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());

        SequentialBlockFetcher_ = New<TSequentialBlockFetcher>(
            Config_,
            std::move(blockSequence),
            MemoryManager_,
            ChunkReader_,
            BlockCache_,
            CodecId_,
            static_cast<double>(miscExt.compressed_data_size()) / miscExt.uncompressed_data_size(),
            BlockReadOptions_);

        YT_LOG_INFO("File reader opened");
    }

    TBlock GetBlock()
    {
        const auto& block = CurrentBlock_.Get().ValueOrThrow();

        auto* begin = block.Data.Begin();
        auto* end = block.Data.End();

        YT_VERIFY(EndOffset_ > 0);

        if (EndOffset_ < block.Size()) {
            end = block.Data.Begin() + EndOffset_;
        }

        if (StartOffset_ > 0) {
            begin = block.Data.Begin() + StartOffset_;
        }

        StartOffset_ = std::max(StartOffset_ - static_cast<i64>(block.Size()), (i64)0);
        EndOffset_ = std::max(EndOffset_ - static_cast<i64>(block.Size()), (i64)0);

        return TBlock(block.Data.Slice(begin, end));
    }

};

IFileReaderPtr CreateFileChunkReader(
    TBlockFetcherConfigPtr config,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    NCompression::ECodec codecId,
    const TClientBlockReadOptions& blockReadOptions,
    i64 startOffset,
    i64 endOffset,
    TChunkReaderMemoryManagerPtr chunkReaderMemoryManager)
{
    return New<TFileChunkReader>(
        std::move(config),
        std::move(chunkReader),
        std::move(blockCache),
        codecId,
        blockReadOptions,
        startOffset,
        endOffset,
        std::move(chunkReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

class TFileMultiChunkReader
    : public IFileReader
    , public TSequentialMultiReaderBase
{
public:
    using TSequentialMultiReaderBase::TSequentialMultiReaderBase;

    virtual bool ReadBlock(TBlock* block) override
    {
        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        *block = TBlock();

        // Nothing to read.
        if (!CurrentReader_) {
            return false;
        }

        bool readerFinished = !CurrentReader_->ReadBlock(block);
        if (!block->Data.Empty()) {
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
    IFileReaderPtr CurrentReader_;

    virtual void OnReaderSwitched() override
    {
        CurrentReader_ = dynamic_cast<IFileReader*>(CurrentSession_.Reader.Get());
        YT_VERIFY(CurrentReader_);
    }
};

IFileReaderPtr CreateFileMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TClientBlockReadOptions& blockReadOptions,
    const std::vector<TChunkSpec>& chunkSpecs,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    std::vector<IReaderFactoryPtr> factories;
    for (const auto& chunkSpec : chunkSpecs) {
        auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);

        auto createReader = [=] () {
            auto remoteReader = CreateRemoteReader(
                chunkSpec,
                config,
                options,
                client,
                nodeDirectory,
                localDescriptor,
                localNodeId,
                blockCache,
                trafficMeter,
                bandwidthThrottler,
                rpsThrottler);

            auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());

            i64 startOffset = 0;
            if (chunkSpec.has_lower_limit() && chunkSpec.lower_limit().has_offset()) {
                startOffset = chunkSpec.lower_limit().offset();
            }

            i64 endOffset = std::numeric_limits<i64>::max();
            if (chunkSpec.has_upper_limit() && chunkSpec.upper_limit().has_offset()) {
                endOffset = chunkSpec.upper_limit().offset();
            }

            return CreateFileChunkReader(
                config,
                std::move(remoteReader),
                blockCache,
                CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec()),
                blockReadOptions,
                startOffset,
                endOffset,
                multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate));
        };

        auto canCreateReader = [=] {
            return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
        };

        factories.emplace_back(CreateReaderFactory(
            createReader,
            canCreateReader,
            TDataSliceDescriptor(chunkSpec)));
    }

    auto reader = New<TFileMultiChunkReader>(
        config,
        options,
        factories,
        multiReaderMemoryManager);
    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient

