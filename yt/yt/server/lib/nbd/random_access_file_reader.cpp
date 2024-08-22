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

struct TRandomAccessFileReaderTag {};

////////////////////////////////////////////////////////////////////////////////

class TRandomAccessFileReader
    : public IRandomAccessFileReader
{
public:
    TRandomAccessFileReader(
        TString path,
        NNative::IClientPtr client,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        const TLogger& logger);

    TRandomAccessFileReader(
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        TString path,
        NNative::IClientPtr client,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        const TLogger& logger);

    TRandomAccessFileReader(
        TUserObject userObject,
        TString path,
        NNative::IClientPtr client,
        IThroughputThrottlerPtr inThrottler,
        IThroughputThrottlerPtr outRpsThrottler,
        const TLogger& logger);

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override;

    i64 GetSize() const override;

    void Open();

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
        std::vector<TBlock> Blocks;
        IChunkReaderPtr Reader;
        IChunkReader::TReadBlocksOptions ReadBlocksOptions;
        NChunkClient::NProto::TChunkSpec Spec;
        TRefCountedChunkMetaPtr Meta;
    };

    const std::optional<::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>> ChunkSpecs_;
    const TString Path_;
    const IThroughputThrottlerPtr InThrottler_;
    const IThroughputThrottlerPtr OutRpsThrottler_;
    const NNative::IClientPtr Client_;
    const TLogger Logger;
    TChunkReaderHostPtr ChunkReaderHost_;
    std::vector<TChunk> Chunks_;
    TUserObject UserObject_;
    i64 Size_ = 0;

    std::atomic<i64> ReadBytes_;
    std::atomic<i64> ReadBlockBytesFromCache_;
    std::atomic<i64> ReadBlockBytesFromDisk_;
    std::atomic<i64> ReadBlockMetaBytesFromDisk_;

    TFuture<std::vector<std::vector<TSharedRef>>> ReadFromChunks(
        const std::vector<TChunk>& chunks,
        i64 offset,
        i64 length);

    TFuture<std::vector<TSharedRef>> ReadFromChunk(
        const TChunk& chunk,
        i64 offset,
        i64 length);

    template <typename T>
    void InitializeChunkStructs(
        const TUserObject& userObject,
        const T& chunkSpecs);

    std::vector<NChunkClient::NProto::TChunkSpec> GetChunkSpecs(
        const TUserObject& userObject,
        const NApi::NNative::IClientPtr& client,
        const NLogging::TLogger& logger);

    TRandomAccessFileReaderStatistics GetStatistics() const override;
};

////////////////////////////////////////////////////////////////////////////////

TRandomAccessFileReader::TRandomAccessFileReader(
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    const TLogger& logger)
    : Path_(std::move(path))
    , InThrottler_(std::move(inThrottler))
    , OutRpsThrottler_(std::move(outRpsThrottler))
    , Client_(std::move(client))
    , Logger(logger)
    , UserObject_(path)
{ }

TRandomAccessFileReader::TRandomAccessFileReader(
    const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    const TLogger& logger)
    : ChunkSpecs_(chunkSpecs)
    , Path_(std::move(path))
    , InThrottler_(std::move(inThrottler))
    , OutRpsThrottler_(std::move(outRpsThrottler))
    , Client_(std::move(client))
    , Logger(logger)
    , UserObject_(path)
{ }

TRandomAccessFileReader::TRandomAccessFileReader(
    TUserObject userObject,
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    const TLogger& logger)
    : Path_(std::move(path))
    , InThrottler_(std::move(inThrottler))
    , OutRpsThrottler_(std::move(outRpsThrottler))
    , Client_(std::move(client))
    , Logger(logger)
    , UserObject_(std::move(userObject))
{ }

TFuture<TSharedRef> TRandomAccessFileReader::Read(
    i64 offset,
    i64 length)
{
    ReadBytes_ += length;

    YT_LOG_DEBUG("Start read (Offset: %v, Length: %v)",
        offset,
        length);

    if (length == 0) {
        YT_LOG_DEBUG("Finish read (Offset: %v, Length: %v)",
            offset,
            length);
        return MakeFuture<TSharedRef>({});
    }

    auto readFuture = ReadFromChunks(
        Chunks_,
        offset,
        length);
    return readFuture.Apply(BIND([=, Logger = Logger] (const std::vector<std::vector<TSharedRef>>& chunkReadResults) {
        std::vector<TSharedRef> refs;
        for (const auto& blockReadResults : chunkReadResults) {
            refs.insert(refs.end(), blockReadResults.begin(), blockReadResults.end());
        }

        // Merge refs into single ref.
        auto mergedRefs = MergeRefsToRef<TRandomAccessFileReaderTag>(refs);
        YT_LOG_DEBUG("Finish read (Offset: %v, ExpectedLength: %v, ResultLength: %v)",
            offset,
            length,
            mergedRefs.Size());
        return mergedRefs;
    }));
}

i64 TRandomAccessFileReader::GetSize() const
{
    return Size_;
}

void TRandomAccessFileReader::Open()
{
    YT_LOG_INFO("Initializing random access file reader (Path: %v)", Path_);

    TRichYPath richPath{Path_};

    if (ChunkSpecs_) {
        UserObject_ = TUserObject(richPath);

        YT_LOG_INFO("Creating chunk reader host (File: %v)", UserObject_.GetPath());
        ChunkReaderHost_ = TChunkReaderHost::FromClient(Client_);
        YT_LOG_INFO("Created chunk reader host (File: %v)", UserObject_.GetPath());

        InitializeChunkStructs(
            UserObject_,
            *ChunkSpecs_);
    } else {
        if (!UserObject_.IsPrepared()) {
            UserObject_ = GetUserObject(
                richPath,
                Client_,
                Logger);
        }

        if (UserObject_.Type != NCypressClient::EObjectType::File) {
            THROW_ERROR_EXCEPTION("Invalid type of file %Qlv, expected %Qlv, but got %Qlv",
                UserObject_.GetPath(),
                NCypressClient::EObjectType::File,
                UserObject_.Type);
        }

        auto chunkSpecs = GetChunkSpecs(
            UserObject_,
            Client_,
            Logger);

        YT_LOG_INFO("Creating chunk reader host (File: %v)", UserObject_.GetPath());
        ChunkReaderHost_ = TChunkReaderHost::FromClient(Client_);
        YT_LOG_INFO("Created chunk reader host (File: %v)", UserObject_.GetPath());

        InitializeChunkStructs(
            UserObject_,
            chunkSpecs);
    }

    YT_LOG_INFO("Initialized random access file reader (Path: %v)", Path_);
}

TFuture<std::vector<std::vector<TSharedRef>>> TRandomAccessFileReader::ReadFromChunks(
    const std::vector<TChunk>& chunks,
    i64 offset,
    i64 length)
{
    if (offset + length > Size_) {
        THROW_ERROR_EXCEPTION("Invalid read offset %v with length %v", offset, length);
    }

    std::vector<TFuture<std::vector<TSharedRef>>> readFutures;
    for (const auto& chunk : chunks) {
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

TFuture<std::vector<TSharedRef>> TRandomAccessFileReader::ReadFromChunk(
    const TChunk& chunk,
    i64 offset,
    i64 length)
{
    YT_LOG_DEBUG("Read (Chunk: %v, ChunkSize: %v, Offset: %v, Length: %v)",
        chunk.Index,
        chunk.Size,
        offset,
        length);

    if (offset + length > chunk.Size) {
        THROW_ERROR_EXCEPTION("Invalid read offset %v with length %v", offset, length);
    }

    std::vector<int> blockIndexes;
    i64 blockOffsetWithinChunk = 0;
    for (int blockIndex = 0; blockIndex < std::ssize(chunk.Blocks); ++blockIndex) {
        auto blockSize = chunk.Blocks[blockIndex].Size;

        i64 blockBegin = blockOffsetWithinChunk;
        i64 blockEnd = blockBegin + blockSize;
        blockOffsetWithinChunk += blockSize;

        if (offset >= blockEnd || offset + length <= blockBegin) {
            continue;
        }

        blockIndexes.push_back(blockIndex);
    }

    auto readFuture = chunk.Reader->ReadBlocks(
        chunk.ReadBlocksOptions,
        blockIndexes);
    return readFuture.Apply(BIND([=, this, this_ = MakeStrong(this), Logger = Logger] (const std::vector<NChunkClient::TBlock>& blocks) mutable {
        YT_VERIFY(blocks.size() == blockIndexes.size());

        // Update read block counters.
        auto& chunkReaderStatistics = chunk.ReadBlocksOptions.ClientOptions.ChunkReaderStatistics;

        i64 readBlockBytesFromCache = chunkReaderStatistics->DataBytesReadFromCache.exchange(0);
        ReadBlockBytesFromCache_ += readBlockBytesFromCache;

        i64 readBlockBytesFromDisk = chunkReaderStatistics->DataBytesReadFromDisk.exchange(0);
        ReadBlockBytesFromDisk_ += readBlockBytesFromDisk;

        i64 readBlockMetaBytesFromDisk = chunkReaderStatistics->MetaBytesReadFromDisk.exchange(0);
        ReadBlockMetaBytesFromDisk_ += readBlockMetaBytesFromDisk;

        std::vector<TSharedRef> refs;
        for (int i = 0; i < std::ssize(blockIndexes); ++i) {
            auto blockIndex = blockIndexes[i];
            const auto& block = chunk.Blocks[blockIndex];

            YT_VERIFY(std::ssize(blocks[i].Data) == block.Size);
            YT_VERIFY(chunk.Offset <= block.Offset);
            YT_VERIFY(block.Offset + block.Size <= chunk.Offset + chunk.Size);

            i64 blockOffset = block.Offset - chunk.Offset;
            YT_VERIFY(0 <= blockOffset);

            i64 blockSize = block.Size;
            YT_VERIFY(0 < blockSize);

            i64 beginWithinBlock = std::max(offset - blockOffset, 0l);
            i64 endWithinBlock = std::min(beginWithinBlock + length, blockSize);
            i64 sizeWithinBlock = endWithinBlock - beginWithinBlock;

            YT_VERIFY(0 <= beginWithinBlock);
            YT_VERIFY(beginWithinBlock < endWithinBlock);
            YT_VERIFY(endWithinBlock <= std::ssize(blocks[i].Data));
            YT_VERIFY(sizeWithinBlock <= blockSize);
            YT_VERIFY(sizeWithinBlock <= length);

            YT_LOG_DEBUG("Read (Chunk: %v, Block: %v, Begin: %v, End: %v, Size %v)",
                chunk.Index,
                blockIndex,
                beginWithinBlock,
                endWithinBlock,
                sizeWithinBlock);

            auto ref = blocks[i].Data.Slice(
                beginWithinBlock,
                endWithinBlock);
            refs.push_back(std::move(ref));

            length -= sizeWithinBlock;
            offset += sizeWithinBlock;
        }

        return refs;
    }));
}

template <typename T>
void TRandomAccessFileReader::InitializeChunkStructs(
    const TUserObject& userObject,
    const T& chunkSpecs)
{
    YT_LOG_INFO("Initializing chunk structs (File: %v, ChunkSpecs: %v)",
        userObject.GetPath(),
        chunkSpecs.size());

    i64 offset = 0;
    for (auto& chunkSpec : chunkSpecs) {
        Chunks_.push_back({});
        auto& chunk = Chunks_.back();

        chunk.Spec = chunkSpec;
        chunk.Offset = offset;
        chunk.Index = Chunks_.size() - 1;

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());

        if (CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec()) != NCompression::ECodec::None) {
            THROW_ERROR_EXCEPTION("Compression codec %Qlv for filesystem image %v is not supported",
                CheckedEnumCast<NCompression::ECodec>(miscExt.compression_codec()),
                userObject.GetPath());
        }

        chunk.Size = miscExt.uncompressed_data_size();

        YT_LOG_INFO("Start creating chunk reader (File: %v, Chunk: %v)",
            userObject.GetPath(),
            chunk.Index);

        auto readerConfig = New<TReplicationReaderConfig>();
        readerConfig->UseBlockCache = true;

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

        YT_LOG_INFO("Finish creating chunk reader (File: %v, Chunk: %v)",
            userObject.GetPath(),
            chunk.Index);

        YT_LOG_INFO("Start fetching chunk meta blocks extension (File: %v, Chunk: %v)",
            userObject.GetPath(),
            chunk.Index);

        std::vector<int> extensionTags = {TProtoExtensionTag<NFileClient::NProto::TBlocksExt>::Value};
        chunk.Meta = WaitFor(chunk.Reader->GetMeta(
            /*options*/ {},
            /*partitionTag*/ std::nullopt,
            extensionTags))
            .ValueOrThrow();

        auto blocksExt = GetProtoExtension<NFileClient::NProto::TBlocksExt>(chunk.Meta->extensions());

        YT_LOG_INFO("Finish fetching chunk meta blocks extension (File: %v, Chunk: %v, BlockInfos: %v)",
            userObject.GetPath(),
            chunk.Index,
            blocksExt.blocks_size());

        for (const auto& blockInfo : blocksExt.blocks()) {
            chunk.Blocks.push_back({blockInfo.size(), offset});
            offset += blockInfo.size();
        }

        Size_ += miscExt.uncompressed_data_size();
    }

    YT_LOG_INFO("Initialized chunk structs (File: %v, ChunkSpecs: %v)",
        userObject.GetPath(),
        chunkSpecs.size());
}

//! Fetch object's chunk specs from Cypress.
std::vector<NChunkClient::NProto::TChunkSpec> TRandomAccessFileReader::GetChunkSpecs(
    const TUserObject& userObject,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& Logger)
{
    YT_LOG_INFO("Fetching file chunk specs (File: %v)", userObject.GetPath());

    auto proxy = CreateObjectServiceReadProxy(
        client,
        NApi::EMasterChannelKind::Follower,
        userObject.ExternalCellTag);

    auto batchReq = proxy.ExecuteBatchWithRetries(client->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    auto req = TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
    AddCellTagToSyncWith(req, userObject.ObjectId);

    TLegacyReadLimit lowerLimit, upperLimit;
    ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>({TLegacyReadRange(lowerLimit, upperLimit)}));

    SetTransactionId(
        req,
        NullTransactionId);
    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

    batchReq->AddRequest(req);
    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error fetching chunks for file %Qlv",
        userObject.GetPath());

    const auto& batchRsp = batchRspOrError.Value();
    const auto& rspOrError = batchRsp->GetResponse<NFileClient::TFileYPathProxy::TRspFetch>(0);
    const auto& rsp = rspOrError.Value();

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    ProcessFetchResponse(
        client,
        rsp,
        userObject.ExternalCellTag,
        client->GetNativeConnection()->GetNodeDirectory(),
        /*maxChunksPerLocateRequest*/ 10,
        /*rangeIndex*/ std::nullopt,
        Logger,
        &chunkSpecs);

    YT_LOG_INFO("Fetched file chunk specs (File: %v, ChunkSpecs: %v)",
        userObject.GetPath(),
        chunkSpecs.size());

    return chunkSpecs;
}

TRandomAccessFileReaderStatistics TRandomAccessFileReader::GetStatistics() const
{
    return TRandomAccessFileReaderStatistics{
        .ReadBytes = ReadBytes_.load(),
        .ReadBlockBytesFromCache = ReadBlockBytesFromCache_.load(),
        .ReadBlockBytesFromDisk = ReadBlockBytesFromDisk_.load(),
        .ReadBlockMetaBytesFromDisk = ReadBlockMetaBytesFromDisk_.load()
    };
}

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
{
    auto fileReader = New<TRandomAccessFileReader>(
        std::move(path),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(logger.WithTag("Path: %v, TransactionId: %v, ReadSessionId: %v", path)));

    fileReader->Open();
    return fileReader;
}

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
{
    auto fileReader = New<TRandomAccessFileReader>(
        chunkSpecs,
        std::move(path),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(logger));

    fileReader->Open();
    return fileReader;
}

IRandomAccessFileReaderPtr CreateRandomAccessFileReader(
    NChunkClient::TUserObject userObject,
    TString path,
    NNative::IClientPtr client,
    IThroughputThrottlerPtr inThrottler,
    IThroughputThrottlerPtr outRpsThrottler,
    TLogger logger)
{
    auto fileReader = New<TRandomAccessFileReader>(
        std::move(userObject),
        std::move(path),
        std::move(client),
        std::move(inThrottler),
        std::move(outRpsThrottler),
        std::move(logger));

    fileReader->Open();
    return fileReader;
}

////////////////////////////////////////////////////////////////////////////////

//! Fetch basic object attributes from Cypress.
TUserObject GetUserObject(
    const TRichYPath& richPath,
    NNative::IClientPtr client,
    const TLogger& Logger)
{
    YT_LOG_INFO("Fetching file basic attributes (File: %v)", richPath);

    TUserObject userObject(richPath);

    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        NullTransactionId,
        Logger,
        NYTree::EPermission::Read);

    YT_LOG_INFO("Fetched file basic attributes (File: %v)", richPath);

    return userObject;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
