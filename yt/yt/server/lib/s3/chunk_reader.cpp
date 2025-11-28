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
using namespace NIO;

////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TBlockRange> GetBlockRanges(const std::vector<int>& blockIndexes)
{
    std::vector<TBlockRange> blockRanges;

    int localIndex = 0;
    while (localIndex < std::ssize(blockIndexes)) {
        auto endLocalIndex = localIndex;

        while (endLocalIndex < std::ssize(blockIndexes) && blockIndexes[endLocalIndex] == blockIndexes[localIndex] + (endLocalIndex - localIndex)) {
            ++endLocalIndex;
        }

        blockRanges.emplace_back(blockIndexes[localIndex], blockIndexes[localIndex] + (endLocalIndex - localIndex));
        localIndex = endLocalIndex;
    }

    return blockRanges;
}

std::vector<TBlockRange> GetBlockRanges(int startBlockIndex, int blockCount)
{
    return {{startBlockIndex, startBlockIndex + blockCount}};
}

TReadRequest MakeSimpleReadRequest(const std::string& chunkFileName, TBlockRange blockRange, const TBlocksExtPtr& blocksExt)
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
        , Config_(std::move(config))
        , ChunkId_(std::move(chunkId))
        , ChunkPlacement_(mediumDescriptor->GetChunkPlacement(ChunkId_))
        , ChunkMetaPlacement_(mediumDescriptor->GetChunkMetaPlacement(ChunkId_))
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
        const TGetMetaOptions& /*options*/,
        const std::optional<TPartitionTags>& /*partitionTags*/,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        if (extensionTags) {
            YT_LOG_ALERT("Get meta request for S3 media was formed with extension tags (ExtensionTags: %v)", extensionTags);
            THROW_ERROR_EXCEPTION("Extension tags are not supported in get meta request")
                << TErrorAttribute("extension_tags", extensionTags);
        }

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
    const IClientPtr Client_;
    const TS3ReaderConfigPtr Config_;
    const TChunkId ChunkId_;
    const TString ChunkFileName_;
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkPlacement_;
    const TS3MediumDescriptor::TS3ObjectPlacement ChunkMetaPlacement_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, MetaLock_);
    TRefCountedChunkMetaPtr ChunkMeta_;

    IInvokerPtr GetSessionInvoker(const TReadBlocksOptions& options) const
    {
        return options.SessionInvoker ? options.SessionInvoker : GetCompressionInvoker(options.ClientOptions.WorkloadDescriptor);
    }

    TFuture<std::vector<TBlock>> ReadBlockRanges(
        const TReadBlocksOptions& options,
        const std::vector<TBlockRange>& blockRanges,
        const TBlocksExtPtr& blocksExt = nullptr)
    {
        if (!blocksExt) {
            return GetBlocksExt(GetSessionInvoker(options))
                .Apply(BIND(&TS3Reader::ReadBlockRanges, MakeStrong(this), options, blockRanges).AsyncVia(GetSessionInvoker(options)));
        }

        std::vector<TFuture<std::vector<TBlock>>> futures;
        futures.reserve(blockRanges.size());

        for (const auto& blockRange : blockRanges) {
            futures.emplace_back(ReadBlockRange(options, blockRange, blocksExt));
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
        const TBlockRange& blockRange,
        const TBlocksExtPtr& blocksExt)
    {
        auto readRequest = MakeSimpleReadRequest(ChunkPlacement_.Key, blockRange, blocksExt);
        YT_VERIFY(readRequest.Offset >= 0);
        YT_VERIFY(readRequest.Size >= 1);

        TGetObjectRequest request;
        request.Bucket = ChunkPlacement_.Bucket;
        request.Key = ChunkPlacement_.Key;
        request.Range = Format("bytes=%v-%v", readRequest.Offset, readRequest.Offset + readRequest.Size - 1);

        return Client_->GetObject(request)
            .Apply(BIND([this, this_ = MakeStrong(this), blockRange, blocksExt] (const TGetObjectResponse& response) {
                return DeserializeBlocks(
                    std::move(response.Data),
                    blockRange,
                    Config_->ValidateBlockChecksums,
                    ChunkPlacement_.Key,
                    blocksExt,
                    /*dumpBrokenBlocks*/ {});
            })
            .AsyncVia(GetSessionInvoker(options)));
    }

    TFuture<TRefCountedChunkMetaPtr> DoGetMeta(
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
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TGetObjectResponse& response) {
                auto meta = DeserializeChunkMeta(
                    std::move(response.Data),
                    ChunkMetaPlacement_.Key,
                    ChunkId_,
                    /*dumpBrokenMeta*/ {});

                auto guard = WriterGuard(MetaLock_);

                if (!ChunkMeta_) {
                    ChunkMeta_ = std::move(meta);
                }

                return ChunkMeta_;
            }).AsyncVia(invoker ? invoker : GetCurrentInvoker()));
    }

    TFuture<TBlocksExtPtr> GetBlocksExt(
        IInvokerPtr invoker = nullptr)
    {
        return DoGetMeta(invoker)
            .AsVoid()
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                auto guard = ReaderGuard(MetaLock_);
                return New<TBlocksExt>(GetProtoExtension<NChunkClient::NProto::TBlocksExt>(ChunkMeta_->extensions()));
            }));
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
