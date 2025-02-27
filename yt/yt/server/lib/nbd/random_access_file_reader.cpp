#include "random_access_file_reader.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

namespace NYT::NNbd {

using namespace NApi;
using namespace NApi::NNative;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NFileClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

struct TRandomAccessFileReaderTag { };

////////////////////////////////////////////////////////////////////////////////

class TRandomAccessFileReader
    : public IRandomAccessFileReader
{
public:
    TRandomAccessFileReader(
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
        TYPath path,
        TChunkReaderHostPtr readerHost,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        IInvokerPtr invoker,
        TLogger logger)
        : ChunkSpecs_(std::move(chunkSpecs))
        , Path_(std::move(path))
        , InThrottler_(std::move(inThrottler))
        , OutRpsThrottler_(std::move(outRpsThrottler))
        , ChunkReaderHost_(std::move(readerHost))
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger.WithTag("Path: %v", Path_)))
    { }

    void Initialize() override
    {
        InitializeChunkStructs();
    }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        ReadBytes_ += length;

        YT_LOG_DEBUG("Start read from file (Offset: %v, Length: %v)",
            offset,
            length);

        if (length == 0) {
            YT_LOG_DEBUG("Finish read from file (Offset: %v, Length: %v)",
                offset,
                length);
            return MakeFuture<TSharedRef>({});
        }

        auto readFuture = ReadFromChunks(
            Chunks_,
            offset,
            length);
        return readFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<std::vector<TSharedRef>>& chunkReadResults) {
            std::vector<TSharedRef> refs;
            for (const auto& blockReadResults : chunkReadResults) {
                refs.insert(refs.end(), blockReadResults.begin(), blockReadResults.end());
            }

            // Merge refs into single ref.
            auto mergedRefs = MergeRefsToRef<TRandomAccessFileReaderTag>(refs);
            YT_LOG_DEBUG("Finish read from file (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
                offset,
                length,
                mergedRefs.Size());
            return mergedRefs;
        }).AsyncVia(Invoker_));
    }

    i64 GetSize() const override
    {
        return Size_;
    }

    TReadersStatistics GetStatistics() const override
    {
        return TReadersStatistics{
            .ReadBytes = ReadBytes_.load(),
            .DataBytesReadFromCache = ReadBlockBytesFromCache_.load(),
            .DataBytesReadFromDisk = ReadBlockBytesFromDisk_.load(),
            .MetaBytesReadFromDisk = ReadBlockMetaBytesFromDisk_.load()
        };
    }

    TYPath GetPath() const override
    {
        return Path_;
    }

private:
    struct TBlock
    {
        i64 Size = 0;
        i64 Offset = 0;
    };

    struct TChunk
    {
        i64 Size = 0;
        i64 Index = 0;
        i64 Offset = 0;
        IChunkReaderPtr Reader;
        IChunkReader::TReadBlocksOptions ReadBlocksOptions;
        NChunkClient::NProto::TChunkSpec Spec;

        mutable TFuture<std::vector<TBlock>> BlocksExtFuture;
    };

    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs_;
    const TYPath Path_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;
    std::vector<TChunk> Chunks_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ChunkLock);

    i64 Size_ = 0;

    std::atomic<i64> ReadBytes_;
    std::atomic<i64> ReadBlockBytesFromCache_;
    std::atomic<i64> ReadBlockBytesFromDisk_;
    std::atomic<i64> ReadBlockMetaBytesFromDisk_;

    TFuture<std::vector<std::vector<TSharedRef>>> ReadFromChunks(
        const std::vector<TChunk>& chunks,
        i64 offset,
        i64 length)
    {
        if (offset + length > Size_) {
            THROW_ERROR_EXCEPTION(
                "Invalid read offset %v with length %v",
                offset,
                length);
        }

        std::vector<TFuture<std::vector<TSharedRef>>> readFutures;
        for (const auto& chunk : chunks) {
            YT_VERIFY(chunk.Size > 0);

            if (length <= 0) {
                break;
            }

            auto chunkBegin = chunk.Offset;
            auto chunkEnd = chunkBegin + chunk.Size;

            if (offset >= chunkEnd || offset + length <= chunkBegin) {
                continue;
            }

            i64 beginWithinChunk = std::max(offset - chunk.Offset, 0l);
            i64 endWithinChunk = std::min(beginWithinChunk + length, chunk.Size);
            i64 sizeWithinChunk = endWithinChunk - beginWithinChunk;

            YT_VERIFY(0 <= beginWithinChunk);
            YT_VERIFY(beginWithinChunk < endWithinChunk);
            YT_VERIFY(endWithinChunk <= chunk.Size);
            YT_VERIFY(sizeWithinChunk <= chunk.Size);
            YT_VERIFY(sizeWithinChunk <= length);

            auto readFuture = ReadFromChunk(
                chunk,
                beginWithinChunk,
                sizeWithinChunk);
            readFutures.push_back(std::move(readFuture));

            length -= sizeWithinChunk;
            offset += sizeWithinChunk;
        }

        return AllSucceeded(readFutures);
    }

    TFuture<std::vector<TBlock>> GetBlockExt(const TChunk& chunk)
    {
        auto guard = Guard(ChunkLock);

        if (chunk.BlocksExtFuture) {
            // Blocks ext is already being requested.
            return chunk.BlocksExtFuture;
        }

        YT_LOG_INFO("Start fetching chunk meta blocks extension (Chunk: %v)",
            chunk.Index);

        std::vector<int> extensionTags = {TProtoExtensionTag<NFileClient::NProto::TBlocksExt>::Value};
        auto index = chunk.Index;
        auto offset = chunk.Offset;

        auto future = chunk.Reader->GetMeta(
            /*options*/ {},
            /*partitionTag*/ std::nullopt,
            extensionTags)
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                auto blockOffset = offset;
                auto blocksExt = GetProtoExtension<NFileClient::NProto::TBlocksExt>(meta->extensions());

                std::vector<TBlock> blocks;
                blocks.reserve(blocksExt.blocks_size());

                for (const auto& blockInfo : blocksExt.blocks()) {
                    blocks.push_back({blockInfo.size(), blockOffset});
                    blockOffset += blockInfo.size();
                }

                YT_LOG_INFO("Finish fetching chunk meta blocks extension (Chunk: %v, BlockInfoCount: %v)",
                    index,
                    blocksExt.blocks_size());
                return blocks;
            }));

        chunk.BlocksExtFuture = future;
        return future;
    }

    TFuture<std::vector<TSharedRef>> ReadFromChunk(
        const TChunk& chunk,
        i64 offset,
        i64 length)
    {
        YT_LOG_DEBUG("Read from chunk (Chunk: %v, ChunkSize: %v, Offset: %v, Length: %v)",
            chunk.Index,
            chunk.Size,
            offset,
            length);

        if (offset + length > chunk.Size) {
            THROW_ERROR_EXCEPTION(
                "Invalid read offset %v with length %v",
                offset,
                length);
        }

        struct TBlocksFetchResult
        {
            std::vector<NChunkClient::TBlock> Blocks;
            std::vector<int> Indexes;
            std::vector<TBlock> BlocksExt;
        };

        auto blocksFuture = GetBlockExt(chunk);
        auto readFuture = blocksFuture.Apply(BIND([
            offset,
            length,
            reader = chunk.Reader,
            options = chunk.ReadBlocksOptions
        ] (const std::vector<TBlock>& blocksExt) {
            std::vector<int> blockIndexes;
            i64 blockOffsetWithinChunk = 0;

            for (int blockIndex = 0; blockIndex < std::ssize(blocksExt); ++blockIndex) {
                auto blockSize = blocksExt[blockIndex].Size;

                i64 blockBegin = blockOffsetWithinChunk;
                i64 blockEnd = blockBegin + blockSize;
                blockOffsetWithinChunk += blockSize;

                if (offset >= blockEnd || offset + length <= blockBegin) {
                    continue;
                }

                blockIndexes.push_back(blockIndex);
            }

            return reader->ReadBlocks(options, blockIndexes)
                .ApplyUnique(BIND([
                    indexes = std::move(blockIndexes),
                    blocksExt = blocksExt
                ] (std::vector<NChunkClient::TBlock>&& blocks) mutable {
                    return TBlocksFetchResult{
                        .Blocks = std::move(blocks),
                        .Indexes = std::move(indexes),
                        .BlocksExt = std::move(blocksExt),
                    };
                }));
        }));

        return readFuture.ApplyUnique(BIND([
            index = chunk.Index,
            chunkOffset = chunk.Offset,
            chunkSize = chunk.Size,
            offset,
            length,
            statistics = chunk.ReadBlocksOptions.ClientOptions.ChunkReaderStatistics,
            this,
            this_ = MakeStrong(this)
        ] (TBlocksFetchResult&& blocksFetchResult) mutable {
            const auto& blocks = blocksFetchResult.Blocks;
            const auto& blockIndexes = blocksFetchResult.Indexes;
            auto readOffset = offset;

            YT_VERIFY(blocks.size() == blockIndexes.size());

            // Update read block counters.
            i64 readBlockBytesFromCache = statistics->DataBytesReadFromCache.exchange(0);
            ReadBlockBytesFromCache_ += readBlockBytesFromCache;

            i64 readBlockBytesFromDisk = statistics->DataBytesReadFromDisk.exchange(0);
            ReadBlockBytesFromDisk_ += readBlockBytesFromDisk;

            i64 readBlockMetaBytesFromDisk = statistics->MetaBytesReadFromDisk.exchange(0);
            ReadBlockMetaBytesFromDisk_ += readBlockMetaBytesFromDisk;

            std::vector<TSharedRef> refs;
            for (int i = 0; i < std::ssize(blockIndexes); ++i) {
                auto blockIndex = blockIndexes[i];
                const auto& block = blocksFetchResult.BlocksExt[blockIndex];

                YT_VERIFY(std::ssize(blocks[i].Data) == block.Size);
                YT_VERIFY(chunkOffset <= block.Offset);
                YT_VERIFY(block.Offset + block.Size <= chunkOffset + chunkSize);

                i64 blockOffset = block.Offset - chunkOffset;
                YT_VERIFY(0 <= blockOffset);

                i64 blockSize = block.Size;
                YT_VERIFY(0 < blockSize);

                i64 beginWithinBlock = std::max(readOffset - blockOffset, 0l);
                i64 endWithinBlock = std::min(beginWithinBlock + length, blockSize);
                i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

                YT_VERIFY(0 <= beginWithinBlock);
                YT_VERIFY(beginWithinBlock < endWithinBlock);
                YT_VERIFY(endWithinBlock <= std::ssize(blocks[i].Data));
                YT_VERIFY(sizeWithinBlock <= blockSize);
                YT_VERIFY(sizeWithinBlock <= length);

                YT_LOG_DEBUG("Read from block (Chunk: %v, Block: %v, Begin: %v, End: %v, Size %v)",
                    index,
                    blockIndex,
                    beginWithinBlock,
                    endWithinBlock,
                    sizeWithinBlock);

                auto ref = blocks[i].Data.Slice(
                    beginWithinBlock,
                    endWithinBlock);
                refs.push_back(std::move(ref));

                length -= sizeWithinBlock;
                readOffset += sizeWithinBlock;
            }

            return refs;
        }));
    }

    void InitializeChunkStructs()
    {
        YT_LOG_INFO("Initializing chunk structs (ChunkSpecCount: %v)",
            ChunkSpecs_.size());

        i64 offset = 0;
        for (auto& chunkSpec : ChunkSpecs_) {
            Chunks_.push_back({});
            auto& chunk = Chunks_.back();

            chunk.Spec = chunkSpec;
            chunk.Offset = offset;
            chunk.Index = Chunks_.size() - 1;

            auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());

            if (FromProto<NCompression::ECodec>(miscExt.compression_codec()) != NCompression::ECodec::None) {
                THROW_ERROR_EXCEPTION(
                    "Compression codec %Qlv for filesystem image %v is not supported",
                    FromProto<NCompression::ECodec>(miscExt.compression_codec()),
                    Path_);
            }

            chunk.Size = miscExt.uncompressed_data_size();

            YT_LOG_INFO("Start creating chunk reader (Chunk: %v)",
                chunk.Index);

            auto readerConfig = New<TReplicationReaderConfig>();
            readerConfig->UseBlockCache = true;
            readerConfig->UseAsyncBlockCache = true;

            auto reader = CreateReplicationReader(
                std::move(readerConfig),
                New<TRemoteReaderOptions>(),
                ChunkReaderHost_,
                FromProto<TChunkId>(chunkSpec.chunk_id()),
                /*seedReplicas*/ {});

            chunk.Reader = CreateReplicationReaderThrottlingAdapter(
                std::move(reader),
                InThrottler_,
                OutRpsThrottler_,
                /*mediumThrottler*/ GetUnlimitedThrottler());

            chunk.ReadBlocksOptions.ClientOptions.WorkloadDescriptor.Category = NYT::EWorkloadCategory::UserInteractive;

            YT_LOG_INFO("Finish creating chunk reader (Chunk: %v)",
                chunk.Index);

            offset += miscExt.uncompressed_data_size();
            Size_ += miscExt.uncompressed_data_size();
        }

        YT_LOG_INFO("Initialized chunk structs (ChunkSpecCount: %v)",
            ChunkSpecs_.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs,
    TYPath path,
    TChunkReaderHostPtr readerHost,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TRandomAccessFileReader>(
        std::move(chunkSpecs),
        std::move(path),
        std::move(readerHost),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
