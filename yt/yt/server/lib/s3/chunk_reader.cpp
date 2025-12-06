#include "chunk_reader.h"

#include "config.h"

#include <yt/yt/server/lib/io/chunk_file_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/library/s3/client.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<NIO::TPhysicalChunkLayoutReader::TBlockRange> GetBlockRanges(const std::vector<int>& blockIndexes)
{
    std::vector<NIO::TPhysicalChunkLayoutReader::TBlockRange> blockRanges;

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

std::vector<NIO::TPhysicalChunkLayoutReader::TBlockRange> GetBlockRanges(int startBlockIndex, int blockCount)
{
    return {{startBlockIndex, startBlockIndex + blockCount}};
}

NIO::TReadRequest MakeSimpleReadRequest(const TString& chunkFileName, NIO::TPhysicalChunkLayoutReader::TBlockRange blockRange, const NIO::TBlocksExtPtr& blocksExt)
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
            chunkFileName,
            chunkBlockCount);
    }

    const auto& firstBlockInfo = blocksExt->Blocks[blockRange.StartBlockIndex];
    const auto& lastBlockInfo = blocksExt->Blocks[blockRange.EndBlockIndex - 1];

    return {
        .Offset = firstBlockInfo.Offset,
        .Size = lastBlockInfo.Offset + lastBlockInfo.Size - firstBlockInfo.Offset,
    };
}

}

////////////////////////////////////////////////////////////////////////////

class TS3Reader
    : public IChunkReader
{
public:
    TS3Reader(
        IClientPtr client,
        const TS3MediumDescriptorPtr& mediumDescriptor,
        TS3ReaderConfigPtr config,
        TChunkId chunkId)
        : Client_(std::move(client))
        , ChunkId_(std::move(chunkId))
        , ChunkPlacement_(mediumDescriptor->GetChunkPlacement(ChunkId_))
        , ChunkMetaPlacement_(mediumDescriptor->GetChunkMetaPlacement(ChunkId_))
        , PhysicalChunkLayoutReader_(New<NIO::TPhysicalChunkLayoutReader>(
            ChunkId_,
            ChunkPlacement_.Key,
            ChunkMetaPlacement_.Key,
            NIO::TPhysicalChunkLayoutReader::TOptions{
                .ValidateBlockChecksums = config->ValidateBlockChecksums,
            },
            ChunkClientLogger(),
            BIND(&TS3Reader::DumpBrokenBlock, MakeWeak(this)),
            BIND(&TS3Reader::DumpBrokenMeta, MakeWeak(this))))
        , Logger(ChunkClientLogger())
    { }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        return ReadBlockRanges(options, GetBlockRanges(blockIndexes));
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        return ReadBlockRanges(options, GetBlockRanges(firstBlockIndex, blockCount));
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TGetMetaOptions& options,
        const std::optional<TPartitionTags>& partitionTags = std::nullopt,
        const std::optional<std::vector<int>>& extensionTags = {}) override
    {
        if (partitionTags) {
            // YT_LOG_ALERT("Get meta request for S3 media was formed with partition tags (PartitionTags: %v)", partitionTag);
            // THROW_ERROR_EXCEPTION("Partiton tags are not supported in get meta request")
            //     << TErrorAttribute("partition_tags", partitionTags);
        }
        if (extensionTags) {
            YT_LOG_ALERT("Get meta request for S3 media was formed with extension tags (ExtensionTags: %v)", extensionTags);
            THROW_ERROR_EXCEPTION("Extension tags are not supported in get meta request")
                << TErrorAttribute("extension_tags", extensionTags);
        }

        return DoGetMeta(options.ClientOptions);
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
    const IClientPtr Client_;
    const TChunkId ChunkId_;
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkPlacement_;
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkMetaPlacement_;
    const NIO::TPhysicalChunkLayoutReaderPtr PhysicalChunkLayoutReader_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, MetaLock_);
    TRefCountedChunkMetaPtr ChunkMeta_;
    NIO::TBlocksExtPtr BlocksExt_;

    IInvokerPtr GetSessionInvoker(const TReadBlocksOptions& options) const
    {
        return options.SessionInvoker ? options.SessionInvoker : GetCompressionInvoker(options.ClientOptions.WorkloadDescriptor);
    }

    TFuture<std::vector<TBlock>> ReadBlockRanges(
        const TReadBlocksOptions& options,
        const std::vector<NIO::TPhysicalChunkLayoutReader::TBlockRange>& blockRanges,
        const NIO::TBlocksExtPtr& blocksExt = nullptr)
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
        const NIO::TPhysicalChunkLayoutReader::TBlockRange& blockRange,
        const NIO::TBlocksExtPtr& blocksExt)
    {
        auto readRequest = MakeSimpleReadRequest(PhysicalChunkLayoutReader_->GetChunkFileName(), blockRange, blocksExt);
        YT_VERIFY(readRequest.Offset >= 0);
        YT_VERIFY(readRequest.Size >= 1);

        TGetObjectRequest request;
        request.Bucket = ChunkPlacement_.Bucket;
        request.Key = ChunkPlacement_.Key;
        request.Range = Format("bytes=%v-%v", readRequest.Offset, readRequest.Offset + readRequest.Size - 1);

        return Client_->GetObject(request)
            .Apply(BIND([this, this_ = MakeStrong(this), blockRange, blocksExt, options] (const TGetObjectResponse& response) {
                return PhysicalChunkLayoutReader_->DeserializeBlocks(response.Data, blockRange, blocksExt, options.ClientOptions.ChunkReaderStatistics);
            })
            .AsyncVia(GetSessionInvoker(options)));
    }

    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(
        const TClientChunkReadOptions& options,
        IInvokerPtr invoker = nullptr)
    {
        {
            auto guard = ReaderGuard(MetaLock_);

            if (ChunkMeta_) {
                return MakeFuture<TRefCountedChunkMetaPtr>(ChunkMeta_);
            }
        }

        TGetObjectRequest request;
        request.Bucket = ChunkMetaPlacement_.Bucket;
        request.Key = ChunkMetaPlacement_.Key;
        return Client_->GetObject(request)
            .Apply(BIND([options, this, this_ = MakeStrong(this)] (const TGetObjectResponse& response) {
                auto metaWithChunkId = PhysicalChunkLayoutReader_->DeserializeMeta(response.Data, options.ChunkReaderStatistics);

                auto guard = WriterGuard(MetaLock_);

                if (!ChunkMeta_) {
                    ChunkMeta_ = metaWithChunkId.ChunkMeta;
                    BlocksExt_ = New<NIO::TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(ChunkMeta_->extensions()));
                }

                return ChunkMeta_;
            }).AsyncVia(invoker ? invoker : GetCurrentInvoker()));
    }


    TFuture<NIO::TBlocksExtPtr> GetBlocksExt(
        const TClientChunkReadOptions& options,
        IInvokerPtr invoker = nullptr)
    {
        return DoGetMeta(options, invoker)
            .AsVoid()
            .Apply(BIND([this, this_ = MakeStrong(this)] () {
                auto guard = ReaderGuard(MetaLock_);
                return BlocksExt_;
            }));
    }


    void DumpBrokenMeta(TRef /*block*/) const
    {
    }

    void DumpBrokenBlock(
        int /*blockIndex*/,
        const NIO::TBlockInfo& /*blockInfo*/,
        TRef /*block*/) const
    {
    }
};

////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateS3RegularChunkReader(
    IClientPtr client,
    const TS3MediumDescriptorPtr& mediumDescriptor,
    TS3ReaderConfigPtr config,
    TChunkId chunkId)
{
    YT_VERIFY(IsRegularChunkId(chunkId));

    return New<TS3Reader>(
        std::move(client),
        std::move(mediumDescriptor),
        std::move(config),
        std::move(chunkId));
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
