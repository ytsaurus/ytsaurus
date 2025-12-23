#include "s3_reader.h"

#include "chunk_reader.h"
#include "chunk_layout_facade.h"
#include "chunk_meta_generator.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/api/table_client.h>

#include <yt/yt/client/table_client/name_table.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/result.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NThreading;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////

class TS3Reader
: public IChunkReader
{
public:
    TS3Reader(
        TS3MediumDescriptorPtr mediumDescriptor,
        TS3ReaderConfigPtr config,
        TChunkId chunkId,
        EChunkFormat chunkFormat,
        TChunkReplicaWithMedium replicaWithMedium)
        : MediumDescriptor_(std::move(mediumDescriptor))
        , Client_(MediumDescriptor_->GetClient())
        , Config_(std::move(config))
        , ChunkId_(std::move(chunkId))
        , ChunkFormat_(chunkFormat)
        , ReplicaWithMedium_(std::move(replicaWithMedium))
        , ChunkPlacement_(MediumDescriptor_->GetChunkPlacement(ChunkId_, std::string(ReplicaWithMedium_.GetSourceUri())))
        , ChunkLayoutReader_(New<TChunkLayoutReader>(
            ChunkId_,
            ToString(ChunkPlacement_),
            ReplicaWithMedium_.GetMetaPersistence() == EChunkMetaPersistence::S3 ? ToString(GetChunkMetaPlacement()) : "",
            TChunkLayoutReader::TOptions{
                // TODO(achulkov2): Drop this for offshore chunks with stored generated meta.
                .ValidateBlockChecksums = ReplicaWithMedium_.GetSourceUri().empty(),
            }
        ))
    { }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return ReadBlockRanges(options, ChunkLayoutReader_->GetBlockRanges(blockIndexes));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        return ReadBlockRanges(options, ChunkLayoutReader_->GetBlockRanges(firstBlockIndex, blockCount));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TClientChunkReadOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) override
    {
        // TODO(achulkov2): [PDuringReview] Support partition tag and extension tags.
        Y_UNUSED(partitionTag);
        Y_UNUSED(extensionTags);

        // TODO(achulkov2): [PForReview] Can we just ignore partition tag and extension tag parameters? Or should we throw an exception instead.

        return DoGetMeta(options);
    }

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    //! This is needed for erasure repair, so no implementation necessary.
    TInstant GetLastFailureTime() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TS3MediumDescriptorPtr MediumDescriptor_;
    const NS3::IClientPtr Client_;
    const TS3ReaderConfigPtr Config_;
    const TChunkId ChunkId_;
    const EChunkFormat ChunkFormat_;
    const TChunkReplicaWithMedium ReplicaWithMedium_;
    const NS3::TObjectDescriptor ChunkPlacement_;
    const TChunkLayoutReaderPtr ChunkLayoutReader_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, MetaLock_);
    // TODO(achulkov2): [PLater] Think about caching metas in the native client. More likely this is a job for S3 proxies.
    TRefCountedChunkMetaPtr ChunkMeta_;
    TBlocksExtPtr BlocksExt_;

    NS3::TObjectDescriptor GetChunkMetaPlacement() const
    {
        YT_VERIFY(ReplicaWithMedium_.GetMetaPersistence() == EChunkMetaPersistence::S3);
        return MediumDescriptor_->GetChunkMetaPlacement(ChunkId_, /*externallyAttached*/ !ReplicaWithMedium_.GetSourceUri().empty());
    }

    IInvokerPtr GetSessionInvoker(const TReadBlocksOptions& options) const
    {
        return options.SessionInvoker ? options.SessionInvoker : GetCompressionInvoker(options.ClientOptions.WorkloadDescriptor);
    }

    TFuture<std::vector<TBlock>> ReadBlockRanges(
        const TReadBlocksOptions& options,
        const std::vector<TChunkLayoutReader::TBlockRange>& blockRanges,
        const TBlocksExtPtr& blocksExt = nullptr)
    {
        if (!blocksExt) {
            return GetBlocksExt(options.ClientOptions, GetSessionInvoker(options))
                .Apply(BIND(&TS3Reader::ReadBlockRanges, MakeStrong(this), options, blockRanges).AsyncVia(GetSessionInvoker(options)));
        }

        std::vector<TFuture<std::vector<TBlock>>> futures;
        futures.reserve(blockRanges.size());

        for (const auto& blockRange : blockRanges) {
            futures.push_back(ReadBlockRange(options, blockRange, blocksExt));
        }

        return AllSet(std::move(futures))
            .Apply(BIND([] (const std::vector<TErrorOr<std::vector<TBlock>>>& results) {
                std::vector<TBlock> blocks;
                for (const auto& result : results) {
                    const auto& rangeBlocks = result.ValueOrThrow();
                    blocks.insert(blocks.end(), rangeBlocks.begin(), rangeBlocks.end());
                }
                return blocks;
            }));
    }

    TFuture<std::vector<TBlock>> ReadBlockRange(
        const TReadBlocksOptions& options,
        const TChunkLayoutReader::TBlockRange& blockRange,
        const TBlocksExtPtr& blocksExt)
    {
        auto readRequest = ChunkLayoutReader_->GetReadRequest(blockRange, blocksExt);
        YT_VERIFY(readRequest.Offset >= 0);
        YT_VERIFY(readRequest.Size >= 1);

        NS3::TGetObjectRequest request;
        request.Bucket = ChunkPlacement_.Bucket();
        request.Key = ChunkPlacement_.Key();
        request.Range = Format("bytes=%v-%v", readRequest.Offset, readRequest.Offset + readRequest.Size - 1);

        return Client_->GetObject(request)
            .Apply(BIND([
                this, this_ = MakeStrong(this),
                blockRange, blocksExt,
                statistics = options.ClientOptions.ChunkReaderStatistics
            ] (const NS3::TGetObjectResponse& response) {
                statistics->DataBytesReadFromDisk.fetch_add(response.Data.Size(), std::memory_order::relaxed);
                return ChunkLayoutReader_->DeserializeBlocks(response.Data, blockRange, blocksExt);
            })
            .AsyncVia(GetSessionInvoker(options)));
    }

    TFuture<TRefCountedChunkMetaPtr> GenerateMetaFromChunkFile(const TClientChunkReadOptions& options, EChunkFormat format)
    {
        class TChunkFile
            : public TS3ArrowRandomAccessFile
        {
        public:
            TChunkFile(TString bucket, TString key, NS3::IClientPtr client, TChunkReaderStatisticsPtr chunkReaderStatistics)
                : TS3ArrowRandomAccessFile(bucket, key, client)
                , ChunkReaderStatistics_(std::move(chunkReaderStatistics))
            { }

            arrow20::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override
            {
                auto result = TS3ArrowRandomAccessFile::ReadAt(position, nbytes, out);
                if (result.ok()) {
                    ChunkReaderStatistics_->MetaBytesReadFromDisk.fetch_add(result.ValueOrDie(), std::memory_order::relaxed);
                }
                return result;
            }

        private:
            TChunkReaderStatisticsPtr ChunkReaderStatistics_;
        };

        // TODO(achulkov2): Path to read chunk file from needs to end up here.
        auto chunkFile = std::make_shared<TChunkFile>(ChunkPlacement_.Bucket(), ChunkPlacement_.Key(), Client_, options.ChunkReaderStatistics);
        auto chunkMetaGenerator = CreateArrowTableChunkMetaGenerator(
            format,
            std::move(chunkFile),
            TArrowTableChunkMetaGeneratorOptions{
                // The GetHash() invocation should return the same hash as the one in
                // attach_table.cpp file. This guarantees consistency of the generated samples.
                .SampleRandomSeed = ChunkPlacement_.GetHash(),
                .SampleStrategy = NTableClient::EChunkMetaSampleGenerationStrategy::Fast});
        chunkMetaGenerator->Generate();
        return MakeFuture(chunkMetaGenerator->GetChunkMeta());
    }

    TFuture<TRefCountedChunkMetaPtr> FetchMetaFromMetaFile(const TClientChunkReadOptions& options)
    {
        NS3::TGetObjectRequest request;
        auto chunkMetaPlacement = GetChunkMetaPlacement();
        request.Bucket = chunkMetaPlacement.Bucket();
        request.Key = chunkMetaPlacement.Key();

        return Client_->GetObject(request)
            .Apply(BIND([
                this, this_ = MakeStrong(this),
                statistics = options.ChunkReaderStatistics
            ] (const NS3::TGetObjectResponse& response) {
                statistics->MetaBytesReadFromDisk.fetch_add(response.Data.Size(), std::memory_order::relaxed);
                auto metaWithChunkId = ChunkLayoutReader_->DeserializeMeta(response.Data);
                return metaWithChunkId.ChunkMeta;
            }));
    }

    TFuture<TRefCountedChunkMetaPtr> FetchOrGenerateMeta(const TClientChunkReadOptions& options)
    {
        return ReplicaWithMedium_.GetMetaPersistence() == EChunkMetaPersistence::S3
            ? FetchMetaFromMetaFile(options)
            : GenerateMetaFromChunkFile(options, ChunkFormat_);
    }

    TRefCountedChunkMetaPtr CacheChunkMeta(const TRefCountedChunkMetaPtr& chunkMeta)
    {
        auto guard = WriterGuard(MetaLock_);

        if (!ChunkMeta_) {
            ChunkMeta_ = chunkMeta;
            BlocksExt_ = New<TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(ChunkMeta_->extensions()));
        }

        return ChunkMeta_;
    }

    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(const TClientChunkReadOptions& options, IInvokerPtr invoker = nullptr)
    {
        {
            auto guard = ReaderGuard(MetaLock_);

            if (ChunkMeta_) {
                return MakeFuture<TRefCountedChunkMetaPtr>(ChunkMeta_);
            }
        }

        return FetchOrGenerateMeta(options)
            .Apply(
                BIND(&TS3Reader::CacheChunkMeta, MakeStrong(this))
                .AsyncVia(invoker ? invoker : GetCurrentInvoker()));
    }


    TFuture<TBlocksExtPtr> GetBlocksExt(const TClientChunkReadOptions& options, IInvokerPtr invoker = nullptr)
    {
        return DoGetMeta(options, invoker)
            .AsVoid()
            .Apply(BIND([this, this_ = MakeStrong(this)] () {
                auto guard = ReaderGuard(MetaLock_);
                return BlocksExt_;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateS3Reader(
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3ReaderConfigPtr config,
    TChunkId chunkId,
    EChunkFormat chunkFormat,
    TChunkReplicaWithMedium replicaWithMedium)
{
    YT_VERIFY(IsRegularChunkId(chunkId));

    // TODO(achulkov2): [PForReview] Fix me.
    if (!config) {
        config = New<TS3ReaderConfig>();
    }

    return New<TS3Reader>(
        std::move(mediumDescriptor),
        std::move(config),
        std::move(chunkId),
        chunkFormat,
        std::move(replicaWithMedium));
}

////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr TryCreateS3ReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    IThroughputThrottlerPtr /*bandwidthThrottler*/,
    IThroughputThrottlerPtr /*rpsThrottler*/,
    IThroughputThrottlerPtr /*mediumThrottler*/)
{
    auto* underlyingReplicationReader = dynamic_cast<TS3Reader*>(underlyingReader.Get());
    if (!underlyingReplicationReader) {
        return nullptr;
    }

    return underlyingReader;
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient