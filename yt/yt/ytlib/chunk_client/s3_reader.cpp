#include "s3_reader.h"

#include "chunk_reader.h"
#include "chunk_layout_facade.h"
#include "chunk_meta_generator.h"
#include "config.h"

#include <arrow/table.h>
#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <parquet/api/reader.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>

#include <yt/yt/library/arrow_parquet_adapter/arrow.h>

#include <arrow/json/reader.h>

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
        std::string_view objectKey)
        : MediumDescriptor_(std::move(mediumDescriptor))
        , Client_(MediumDescriptor_->GetClient())
        , Config_(std::move(config))
        , ChunkId_(std::move(chunkId))
        , ChunkFormat_(chunkFormat)
        , ChunkPlacement_(MediumDescriptor_->GetChunkPlacement(ChunkId_, objectKey))
        , ChunkMetaPlacement_(MediumDescriptor_->GetChunkMetaPlacement(ChunkId_, objectKey))
        // TODO(achulkov2): [PDuringReview] Format S3 paths in such a way that they can be passed to S3 clients (e.g. with bucket).
        , ChunkLayoutReader_(New<TChunkLayoutReader>(ChunkId_, ChunkPlacement_.Key, ChunkMetaPlacement_.Key, TChunkLayoutReader::TOptions()))
    {
        Cerr << "Creating S3 reader for chunk" << Endl;
    }

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
        const TGetMetaOptions& options,
        std::optional<int> partitionTag = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) override
    {
        // TODO(achulkov2): [PDuringReview] Fill statistics in options.
        Y_UNUSED(options);

        // TODO(achulkov2): [PDuringReview] Support partition tag and extension tags.
        Y_UNUSED(partitionTag);
        Y_UNUSED(extensionTags);

        // TODO(achulkov2): [PForReview] Can we just ignore partition tag and extension tag parameters? Or should we throw an exception instead.

        return DoGetMeta();
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
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkPlacement_;
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkMetaPlacement_;
    const TChunkLayoutReaderPtr ChunkLayoutReader_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, MetaLock_);
    // TODO(achulkov2): [PLater] Think about caching metas in the native client. More likely this is a job for S3 proxies.
    TRefCountedChunkMetaPtr ChunkMeta_;
    TBlocksExtPtr BlocksExt_;

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
            return GetBlocksExt(GetSessionInvoker(options))
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
        request.Bucket = ChunkPlacement_.Bucket;
        request.Key = ChunkPlacement_.Key;
        request.Range = Format("bytes=%v-%v", readRequest.Offset, readRequest.Offset + readRequest.Size - 1);

        return Client_->GetObject(request)
            .Apply(BIND([this, this_ = MakeStrong(this), blockRange, blocksExt] (const NS3::TGetObjectResponse& response) {
                // TODO(achulkov2): [PDuringReview] Increment counters in statistics. Maybe even do it inside chunk layout reader.
                return ChunkLayoutReader_->DeserializeBlocks(response.Data, blockRange, blocksExt);
            })
            .AsyncVia(GetSessionInvoker(options)));
    }

    TFuture<TRefCountedChunkMetaPtr> GenerateMetaFromChunkFile(EChunkFormat format)
    {
        // TODO(achulkov2): Path to read chunk file from needs to end up here.
        auto chunkFile = std::make_shared<TS3ArrowRandomAccessFile>(ChunkPlacement_.Bucket, ChunkPlacement_.Key, Client_);
        auto chunkMetaGenerator = CreateArrowChunkMetaGenerator(format, std::move(chunkFile));
        chunkMetaGenerator->Generate();
        return MakeFuture(chunkMetaGenerator->GetChunkMeta());
    }

    TFuture<TRefCountedChunkMetaPtr> FetchMetaFromMetaFile()
    {
        NS3::TGetObjectRequest request;
        request.Bucket = ChunkMetaPlacement_.Bucket;
        request.Key = ChunkMetaPlacement_.Key;

        return Client_->GetObject(request)
            .Apply(BIND([this, this_ = MakeStrong(this)] (const NS3::TGetObjectResponse& response) {
                auto metaWithChunkId = ChunkLayoutReader_->DeserializeMeta(response.Data);
                return metaWithChunkId.ChunkMeta;
            }));
    }

    TFuture<TRefCountedChunkMetaPtr> FetchOrGenerateMeta()
    {
        // TODO(achulkov2): Here we somehow need to know whether to read meta from meta file or generate it from chunk file.

        return ChunkFormat_ == EChunkFormat::TableUnversionedSchemalessHorizontal
            ? FetchMetaFromMetaFile()
            : GenerateMetaFromChunkFile(ChunkFormat_);
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

    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(IInvokerPtr invoker = nullptr)
    {
        {
            auto guard = ReaderGuard(MetaLock_);

            if (ChunkMeta_) {
                return MakeFuture<TRefCountedChunkMetaPtr>(ChunkMeta_);
            }
        }

        return FetchOrGenerateMeta()
            .Apply(
                BIND(&TS3Reader::CacheChunkMeta, MakeStrong(this))
                .AsyncVia(invoker ? invoker : GetCurrentInvoker()));
    }


    TFuture<TBlocksExtPtr> GetBlocksExt(IInvokerPtr invoker = nullptr)
    {
        return DoGetMeta(invoker)
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
    std::string_view objectKey)
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
        objectKey);
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