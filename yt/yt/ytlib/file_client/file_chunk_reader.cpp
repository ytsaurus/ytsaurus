#include "file_chunk_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/reader_factory.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NFileClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTracing;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TFileReaderAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TFileReaderAdapter(IFileReaderPtr underlying)
        : Underlying_(underlying)
    { }

    virtual TFuture<TSharedRef> Read()
    {
        TBlock block;
        if (!Underlying_->ReadBlock(&block)) {
            return MakeFuture(TSharedRef{});
        }

        if (!block.Data.Empty()) {
            return MakeFuture(block.Data);
        }

        return Underlying_->GetReadyEvent()
            .Apply(BIND(&TFileReaderAdapter::Read, MakeStrong(this)));
    }

private:
    IFileReaderPtr Underlying_;
};

IAsyncZeroCopyInputStreamPtr CreateFileReaderAdapter(IFileReaderPtr underlying)
{
    return New<TFileReaderAdapter>(underlying);
}

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
        const TClientChunkReadOptions& chunkReadOptions,
        i64 startOffset,
        i64 endOffset,
        const NChunkClient::TDataSource& dataSource,
        TChunkReaderMemoryManagerHolderPtr chunkReaderMemoryManagerHolder)
        : Config_(std::move(config))
        , ChunkReader_(std::move(chunkReader))
        , BlockCache_(std::move(blockCache))
        , CodecId_(codecId)
        , ChunkReadOptions_(chunkReadOptions)
        , StartOffset_(startOffset)
        , EndOffset_(endOffset)
        , TraceContext_(CreateTraceContextFromCurrent("FileChunkReader"))
        , FinishGuard_(TraceContext_)
    {
        // NB(gepardo): Real extraChunkTags will be packed later, in DoOpen().
        PackBaggageForChunkReader(TraceContext_, dataSource, TExtraChunkTags{});

        TCurrentTraceContextGuard guard(TraceContext_);

        if (chunkReaderMemoryManagerHolder) {
            MemoryManagerHolder_ = chunkReaderMemoryManagerHolder;
        } else {
            MemoryManagerHolder_ = TChunkReaderMemoryManager::CreateHolder(TChunkReaderMemoryManagerOptions(Config_->WindowSize));
        }

        Logger.AddTag("ChunkId: %v", ChunkReader_->GetChunkId());
        if (ChunkReadOptions_.ReadSessionId) {
            Logger.AddTag("ReadSessionId: %v", ChunkReadOptions_.ReadSessionId);
        }

        YT_LOG_DEBUG("Creating file chunk reader (StartOffset: %v, EndOffset: %v)",
            startOffset,
            endOffset);

        ReadyEvent_ = BIND(&TFileChunkReader::DoOpen, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    bool ReadBlock(TBlock* block) override
    {
        TCurrentTraceContextGuard guard(TraceContext_);

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (BlockFetched_ && !SequentialBlockFetcher_->HasMoreBlocks()) {
            return false;
        }

        *block = TBlock();
        if (BlockFetched_) {
            BlockFetched_ = false;
            MemoryManagerHolder_->Get()->SetRequiredMemorySize(SequentialBlockFetcher_->GetNextBlockSize());
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

    TDataStatistics GetDataStatistics() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        TDataStatistics dataStatistics;
        dataStatistics.set_uncompressed_data_size(SequentialBlockFetcher_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(SequentialBlockFetcher_->GetCompressedDataSize());
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        return TCodecStatistics().Append(SequentialBlockFetcher_->GetDecompressionTime());
    }

    bool IsFetchingCompleted() const override
    {
        YT_VERIFY(SequentialBlockFetcher_);
        return SequentialBlockFetcher_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
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
    const TClientChunkReadOptions ChunkReadOptions_;

    i64 StartOffset_;
    i64 EndOffset_;

    TChunkReaderMemoryManagerHolderPtr MemoryManagerHolder_;

    TSequentialBlockFetcherPtr SequentialBlockFetcher_;
    TFuture<void> ReadyEvent_;
    bool BlockFetched_ = true;

    NLogging::TLogger Logger = FileClientLogger;

    TFuture<TBlock> CurrentBlock_;

    TTraceContextPtr TraceContext_;
    TTraceContextFinishGuard FinishGuard_;

    void DoOpen()
    {
        YT_LOG_DEBUG("Requesting chunk meta");

        auto metaOrError = WaitFor(ChunkReader_->GetMeta(ChunkReadOptions_));
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Failed to get file chunk meta");

        YT_LOG_DEBUG("Chunk meta received");
        const auto& meta = metaOrError.Value();

        auto type = EChunkType(meta->type());
        if (type != EChunkType::File) {
            THROW_ERROR_EXCEPTION("Invalid chunk type: expected %Qlv, actual %Qlv",
                EChunkType::File,
                type);
        }

        auto format = CheckedEnumCast<EChunkFormat>(meta->format());
        if (format != EChunkFormat::FileDefault) {
            THROW_ERROR_EXCEPTION("Unsupported file chunk format %Qlv",
                format);
        }

        std::vector<TBlockFetcher::TBlockInfo> blockSequence;

        // COMPAT(psushin): new file chunk meta!
        auto fileBlocksExt = FindProtoExtension<NFileClient::NProto::TBlocksExt>(meta->extensions());

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions());

        if (miscExt.system_block_count() > 0) {
            // NB: Hence in block infos we set block type to UncompressedData.
            THROW_ERROR_EXCEPTION("System blocks are not supported for files");
        }

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
                blockSequence.push_back({
                    .ReaderIndex = 0,
                    .BlockIndex = index,
                    .Priority = index,
                    .UncompressedDataSize = size,
                    .BlockType = EBlockType::UncompressedData,
                });
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
            auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(meta->extensions());
            blockCount = blocksExt.blocks_size();

            blockSequence.reserve(blockCount);
            for (int index = 0; index < blockCount; ++index) {
                if (!addBlock(index, blocksExt.blocks(index).size())) {
                    break;
                }
            }
        }
        YT_VERIFY(blockCount >= 0);

        PackBaggageFromExtraChunkTags(TraceContext_, MakeExtraChunkTags(miscExt));

        SequentialBlockFetcher_ = New<TSequentialBlockFetcher>(
            Config_,
            std::move(blockSequence),
            MemoryManagerHolder_,
            std::vector{ChunkReader_},
            BlockCache_,
            CodecId_,
            static_cast<double>(miscExt.compressed_data_size()) / miscExt.uncompressed_data_size(),
            ChunkReadOptions_);
        SequentialBlockFetcher_->Start();

        YT_LOG_DEBUG("File chunk reader opened (BlockIndexes: %v-%v, SelectedSize: %v)",
            blockIndex,
            blockIndex + blockCount - 1,
            selectedSize);
    }

    TBlock GetBlock()
    {
        const auto& block = CurrentBlock_.Get().ValueOrThrow();

        const auto* begin = block.Data.Begin();
        const auto* end = block.Data.End();

        YT_VERIFY(EndOffset_ > 0);

        if (EndOffset_ < std::ssize(block.Data)) {
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
    const TClientChunkReadOptions& chunkReadOptions,
    i64 startOffset,
    i64 endOffset,
    const NChunkClient::TDataSource& dataSource,
    TChunkReaderMemoryManagerHolderPtr chunkReaderMemoryManagerHolder)
{
    return New<TFileChunkReader>(
        std::move(config),
        std::move(chunkReader),
        std::move(blockCache),
        codecId,
        chunkReadOptions,
        startOffset,
        endOffset,
        dataSource,
        std::move(chunkReaderMemoryManagerHolder));
}

////////////////////////////////////////////////////////////////////////////////

class TFileMultiChunkReader
    : public IFileReader
{
public:
    TFileMultiChunkReader(IMultiReaderManagerPtr multiReaderManager)
        : MultiReaderManager_(std::move(multiReaderManager))
    {
        MultiReaderManager_->SubscribeReaderSwitched(BIND(
            &TFileMultiChunkReader::OnReaderSwitched,
            MakeWeak(this)));
    }

    bool ReadBlock(TBlock* block) override
    {
        if (!MultiReaderManager_->GetReadyEvent().IsSet() || !MultiReaderManager_->GetReadyEvent().Get().IsOK()) {
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

        if (MultiReaderManager_->OnEmptyRead(readerFinished)) {
            return true;
        } else {
            CurrentReader_.Reset();
            return false;
        }
    }

    void Open()
    {
        MultiReaderManager_->Open();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }

private:
    IMultiReaderManagerPtr MultiReaderManager_;
    IFileReaderPtr CurrentReader_;

    void OnReaderSwitched()
    {
        CurrentReader_ = dynamic_cast<IFileReader*>(MultiReaderManager_->GetCurrentSession().Reader.Get());
        YT_VERIFY(CurrentReader_);
    }
};

IFileReaderPtr CreateFileMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    const TClientChunkReadOptions& chunkReadOptions,
    const std::vector<TChunkSpec>& chunkSpecs,
    const NChunkClient::TDataSource& dataSource,
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

        auto createReader = BIND([=] () -> IReaderBasePtr {
            auto remoteReader = CreateRemoteReader(
                chunkSpec,
                config,
                options,
                chunkReaderHost);

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
                chunkReaderHost->BlockCache,
                CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec()),
                chunkReadOptions,
                startOffset,
                endOffset,
                dataSource,
                multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate));
        });

        auto canCreateReader = BIND([=] {
            return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
        });

        factories.push_back(CreateReaderFactory(
            createReader,
            canCreateReader,
            TDataSliceDescriptor(chunkSpec)));
    }

    auto reader = New<TFileMultiChunkReader>(CreateSequentialMultiReaderManager(
        std::move(config),
        std::move(options),
        factories,
        std::move(multiReaderMemoryManager)));

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient

