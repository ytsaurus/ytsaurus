#include "cypress_file_block_device.h"
#include "block_device.h"

#include <yt/yt/server/lib/nbd/private.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/file_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NNbd {

using namespace NYTree;
using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static TUserObject GetUserObject(
    const NYPath::TRichYPath& richPath,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger);
static TString GetFilesystem(
    const TUserObject& userObject,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger);
static std::vector<NChunkClient::NProto::TChunkSpec> GetChunkSpecs(
    const TUserObject& userObject,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

struct TCypressFileBlockDeviceTag { };

class TCypressFileBlockDevice
    : public IBlockDevice
{
public:
    TCypressFileBlockDevice(
        const TString& exportId,
        TCypressFileBlockDeviceConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger)
        : ExportId_(exportId)
        , Config_(std::move(config))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
    {}

    TCypressFileBlockDevice(
        const TString& exportId,
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        TCypressFileBlockDeviceConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger)
        : ExportId_(exportId)
        , ChunkSpecs_(chunkSpecs)
        , Config_(std::move(config))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Logger(logger.WithTag("ExportId: %v", ExportId_))
    {}

    virtual i64 GetTotalSize() const override
    {
        return FileSize_;
    }

    virtual bool IsReadOnly() const override
    {
        return true;
    }

    virtual TString DebugString() const override
    {
        return Format("{Cypress Path, %v}", Config_->Path);
    }

    virtual TFuture<TSharedRef> Read(
        i64 offset,
        i64 length) override
    {
        auto readId = TGuid::Create();

        YT_LOG_DEBUG("Start read (Offset: %v, Length: %v, ReadId: %v)",
            offset,
            length,
            readId);

        if (length == 0) {
            YT_LOG_DEBUG("Finish read (Offset: %v, Length: %v, ReadId: %v)",
                offset,
                length,
                readId);
            return MakeFuture<TSharedRef>({});
        }

        auto readFuture = ReadFromChunks(Chunks_, offset, length, readId);
        return readFuture.Apply(BIND([=, Logger=Logger](const std::vector<std::vector<TSharedRef>>& chunkReadResults) {
            YT_LOG_DEBUG("Finish read (Offset: %v, Length: %v, ReadId: %v)",
                offset,
                length,
                readId);

            std::vector<TSharedRef> refs;
            for (const auto& blockReadResults : chunkReadResults) {
                refs.insert(refs.end(), blockReadResults.begin(), blockReadResults.end());
            }
            // Merge refs into single ref.
            return MergeRefsToRef<TCypressFileBlockDeviceTag>(refs);
        }));
    }

    virtual TFuture<void> Write(
        i64 /*offset*/,
        const TSharedRef& /*data*/,
        const TWriteOptions& /*options*/) override
    {
        THROW_ERROR_EXCEPTION("Writes are not supported");
    }

    virtual TFuture<void> Flush() override
    {
        return VoidFuture;
    }

    virtual TFuture<void> Initialize() override
    {
        return BIND(&TCypressFileBlockDevice::DoInitialize, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
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
        std::vector<TBlock> Blocks;
        IChunkReaderPtr Reader;
        NChunkClient::NProto::TChunkSpec Spec;
        TRefCountedChunkMetaPtr Meta;
    };

    void DoInitialize()
    {
        YT_LOG_INFO("Initializing Cypress file block divice (Path: %v)", Config_->Path);

        NYPath::TRichYPath richPath{Config_->Path};

        if (ChunkSpecs_) {
            TUserObject userObject{richPath};

            YT_LOG_INFO("Creating chunk reader host (File: %v)", userObject.GetPath());
            ChunkReaderHost_ = TChunkReaderHost::FromClient(Client_);
            YT_LOG_INFO("Created chunk reader host (File: %v)", userObject.GetPath());

            InitializeChunkStructs(userObject, *ChunkSpecs_);
        } else {
            auto userObject = GetUserObject(richPath, Client_, Logger);
            if (userObject.Type != NCypressClient::EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of file %Qlv, expected %Qlv, but got %Qlv",
                    userObject.GetPath(),
                    NCypressClient::EObjectType::File,
                    userObject.Type);
            }

            auto filesystem = GetFilesystem(userObject, Client_, Logger);
            if (filesystem != "ext4" && filesystem != "squashfs") {
                THROW_ERROR_EXCEPTION("Invalid filesystem attribute %Qv of file %v",
                    filesystem,
                    userObject.GetPath());
            }

            auto chunkSpecs = GetChunkSpecs(userObject, Client_, Logger);

            YT_LOG_INFO("Creating chunk reader host (File: %v)", userObject.GetPath());
            ChunkReaderHost_ = TChunkReaderHost::FromClient(Client_);
            YT_LOG_INFO("Created chunk reader host (File: %v)", userObject.GetPath());

            InitializeChunkStructs(userObject, chunkSpecs);
        }

        YT_LOG_INFO("Initialized Cypress file block device (Path: %v)", Config_->Path);
    }

    TFuture<std::vector<std::vector<TSharedRef>>> ReadFromChunks(const std::vector<TChunk>& chunks, i64 offset, i64 length, const TGuid& readId)
    {
        if (offset + length > FileSize_) {
            THROW_ERROR_EXCEPTION("Invalid read offset %Qlv with length %Qlv", offset, length);
        }

        std::vector<TFuture<std::vector<TSharedRef>>> readFutures;
        // TODO(yuryalekseev): use lower bound
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

            auto readFuture = ReadFromChunk(chunk, beginWithinChunk, sizeWithinChunk, readId);
            readFutures.push_back(std::move(readFuture));

            length -= sizeWithinChunk;
            offset += sizeWithinChunk;
        }

        return AllSucceeded(readFutures);
    }

    TFuture<std::vector<TSharedRef>> ReadFromChunk(const TChunk& chunk, i64 offset, i64 length, const TGuid& readId)
    {
        YT_LOG_DEBUG("Read (Chunk: %v, ChunkSize: %v, Offset: %v, Length: %v, ReadId: %v)",
            chunk.Index, chunk.Size, offset, length, readId);

        if (offset + length > chunk.Size) {
            THROW_ERROR_EXCEPTION("Invalid read offset %Qlv with length %Qlv", offset, length);
        }

        std::vector<int> blockIndexes;
        i64 blockOffsetWithinChunk = 0;
        // TODO(yuryalekseev): use lower bound
        for (auto b = 0u; b < chunk.Blocks.size(); ++b) {
            auto blockSize = chunk.Blocks[b].Size;

            i64 blockBegin = blockOffsetWithinChunk;
            i64 blockEnd = blockBegin + blockSize;
            blockOffsetWithinChunk += blockSize;

            if (offset >= blockEnd || offset + length <= blockBegin) {
                continue;
            }

            blockIndexes.push_back(b);
        }

        IChunkReader::TReadBlocksOptions readBlocksOptions;
        readBlocksOptions.EstimatedSize = length;
        readBlocksOptions.ClientOptions.WorkloadDescriptor.Category = EWorkloadCategory::UserInteractive;
        readBlocksOptions.ClientOptions.ReadSessionId = readId;

        auto readFuture = chunk.Reader->ReadBlocks(std::move(readBlocksOptions), blockIndexes);
        return readFuture.Apply(BIND([=, Logger=Logger](const std::vector<NChunkClient::TBlock>& blocks) mutable {
            YT_VERIFY(blocks.size() == blockIndexes.size());

            std::vector<TSharedRef> refs;
            for (auto i = 0u; i < blockIndexes.size(); ++i) {
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
                YT_VERIFY(endWithinBlock <= (i64)blocks[i].Data.size());
                YT_VERIFY(sizeWithinBlock <= blockSize);
                YT_VERIFY(sizeWithinBlock <= length);

                YT_LOG_DEBUG("Read (Chunk: %v, Block: %v, Begin: %v, End: %v, Size %v, ReadId: %v)",
                    chunk.Index, blockIndex, beginWithinBlock, endWithinBlock, sizeWithinBlock, readId);

                auto ref = blocks[i].Data.Slice(beginWithinBlock, endWithinBlock);
                refs.push_back(std::move(ref));

                length -= sizeWithinBlock;
                offset += sizeWithinBlock;
            }

            return refs;
        }));
    }

    template <typename T>
    void InitializeChunkStructs(const TUserObject& userObject, const T& chunkSpecs)
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

            YT_LOG_INFO("Creating chunk reader (File: %v, Chunk: %v)",
                userObject.GetPath(),
                chunk.Index);

            auto readerConfig = New<TReplicationReaderConfig>();
            readerConfig->UseBlockCache = true;

            chunk.Reader = CreateReplicationReader(
                std::move(readerConfig),
                New<TRemoteReaderOptions>(),
                ChunkReaderHost_,
                FromProto<TChunkId>(chunkSpec.chunk_id()),
                {} /* seedReplicas */);

            YT_LOG_INFO("Created chunk reader (File: %v, Chunk: %v)",
                userObject.GetPath(),
                chunk.Index);

            YT_LOG_INFO("Fetching chunk meta blocks extension (File: %v, Chunk: %v)",
                userObject.GetPath(),
                chunk.Index);

            std::vector<int> extensionTags = {TProtoExtensionTag<NFileClient::NProto::TBlocksExt>::Value};
            chunk.Meta = WaitFor(chunk.Reader->GetMeta({}, std::nullopt, extensionTags))
                .ValueOrThrow();

            auto blocksExt = GetProtoExtension<NFileClient::NProto::TBlocksExt>(chunk.Meta->extensions());

            YT_LOG_INFO("Fetched chunk meta blocks extension (File: %v, Chunk: %v, BlockInfos: %v)",
                userObject.GetPath(),
                chunk.Index,
                blocksExt.blocks_size());

            for (const auto& blockInfo : blocksExt.blocks()) {
                chunk.Blocks.push_back({blockInfo.size(), offset});
                offset += blockInfo.size();
            }

            FileSize_ += miscExt.uncompressed_data_size();
        }

        YT_LOG_INFO("Initialized chunk structs (File: %v, ChunkSpecs: %v)",
            userObject.GetPath(),
            chunkSpecs.size());
    }

private:
    const TString ExportId_;
    const std::optional<::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>> ChunkSpecs_;
    const TCypressFileBlockDeviceConfigPtr Config_;
    const NApi::NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    TChunkReaderHostPtr ChunkReaderHost_;
    std::vector<TChunk> Chunks_;
    i64 FileSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Fetch basic object attributes from Cypress.
static TUserObject GetUserObject(
    const NYPath::TRichYPath& richPath,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& Logger)
{
    YT_LOG_INFO("Fetching file basic attributes (File: %v)", richPath);

    TUserObject userObject(richPath);

    GetUserObjectBasicAttributes(
        client,
        {&userObject},
        NCypressClient::NullTransactionId,
        Logger,
        EPermission::Read);

    YT_LOG_INFO("Fetched file basic attributes (File: %v)", richPath);

    return userObject;
}

////////////////////////////////////////////////////////////////////////////////

//! Fetch object's filesystem attribute from Cypress.
static TString GetFilesystem(const TUserObject& userObject, NApi::NNative::IClientPtr client, const NLogging::TLogger& Logger)
{
    YT_LOG_INFO("Fetching file filesystem attribute (File: %v)", userObject.GetPath());

    auto proxy = NObjectClient::CreateObjectServiceReadProxy(
        client,
        NApi::EMasterChannelKind::Follower,
        userObject.ExternalCellTag);

    auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
    ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
        "filesystem",
    });

    NObjectClient::AddCellTagToSyncWith(req, userObject.ObjectId);
    NCypressClient::SetTransactionId(req, NCypressClient::NullTransactionId);

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting extended attributes for file %Qlv", userObject.GetPath());

    const auto& rsp = rspOrError.Value();
    auto attributes = ConvertToAttributes(NYson::TYsonString(rsp->value()));
    auto filesystem = attributes->Get<TString>("filesystem", "");

    YT_LOG_INFO("Fetched file filesystem attribute (File: %v, Filesystem: %v)", userObject.GetPath(), filesystem);

    return filesystem;
}

////////////////////////////////////////////////////////////////////////////////

//! Fetch object's chunk specs from Cypress.
static std::vector<NChunkClient::NProto::TChunkSpec> GetChunkSpecs(
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

    auto req = NFileClient::TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
    NObjectClient::AddCellTagToSyncWith(req, userObject.ObjectId);

    TLegacyReadLimit lowerLimit, upperLimit;
    ToProto(req->mutable_ranges(), std::vector<TLegacyReadRange>({TLegacyReadRange(lowerLimit, upperLimit)}));

    NCypressClient::SetTransactionId(req, NCypressClient::NullTransactionId);
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
        10,
        std::nullopt,
        Logger,
        &chunkSpecs);

    YT_LOG_INFO("Fetched file chunk specs (File: %v, ChunkSpecs: %v)", userObject.GetPath(), chunkSpecs.size());

    return chunkSpecs;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<NChunkClient::NProto::TChunkSpec> GetChunkSpecs(
    const TString& path,
    const NApi::NNative::IClientPtr& client,
    IInvokerPtr invoker,
    const NLogging::TLogger& Logger)
{
    NYPath::TRichYPath richPath{path};

    auto userObject = GetUserObject(richPath, client, Logger);
    if (userObject.Type != NCypressClient::EObjectType::File) {
        THROW_ERROR_EXCEPTION("Invalid type of file %Qlv: expected %Qlv, but got %Qlv",
            userObject.GetPath(),
            NCypressClient::EObjectType::File,
            userObject.Type);
    }

    auto filesystem = GetFilesystem(userObject, client, Logger);
    if (filesystem != "ext4" && filesystem != "squashfs") {
        THROW_ERROR_EXCEPTION("Invalid filesystem attribute %Qlv of file %Qlv",
            filesystem,
            userObject.GetPath());
    }

    YT_LOG_INFO("Fetching file chunk specs (File: %v)", userObject.GetPath());

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        client,
        NYT::NApi::TMasterReadOptions{},
        client->GetNativeConnection()->GetNodeDirectory(),
        invoker,
        client->GetNativeConnection()->GetConfig()->MaxChunksPerFetch,
        client->GetNativeConnection()->GetConfig()->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /* tableIndex */) {
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            req->set_fetch_parity_replicas(true);
        },
        Logger
    );

    chunkSpecFetcher->Add(
        userObject.ObjectId,
        userObject.ExternalCellTag,
        userObject.ChunkCount,
        0 /* tableIndex */);

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("Fetched file chunk specs (File: %v, ChunkSpecs: %v)",
        userObject.GetPath(),
        chunkSpecFetcher->ChunkSpecs().size());

    return chunkSpecFetcher->ChunkSpecs();
}

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    const TString& exportId,
    TCypressFileBlockDeviceConfigPtr exportConfig,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
{
    return New<TCypressFileBlockDevice>(exportId, std::move(exportConfig), std::move(client), std::move(invoker), logger);
}

////////////////////////////////////////////////////////////////////////////////

IBlockDevicePtr CreateCypressFileBlockDevice(
    const TString& exportId,
    const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TCypressFileBlockDeviceConfigPtr exportConfig,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger)
{
    return New<TCypressFileBlockDevice>(exportId, chunkSpecs, std::move(exportConfig), std::move(client), std::move(invoker), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
