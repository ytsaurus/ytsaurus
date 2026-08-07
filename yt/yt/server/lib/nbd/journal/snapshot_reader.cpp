#include "snapshot_reader.h"

#include <yt/yt/server/lib/nbd/journal/records/snapshot_block.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 CompressedDataSizeToBlockCount(i64 compressedDataSize, const TBlockDeviceGeometry& geometry)
{
    // A block is stored as [THunkPayloadHeader][payload].
    i64 blockWithHeaderSize = sizeof(THunkPayloadHeader) + geometry.BlockSize;
    return compressedDataSize / blockWithHeaderSize;
}

TChunkBlockCounts FetchChunkBlockCountsNative(
    const NNative::IClientPtr& client,
    const TUserObject& userObject,
    const TBlockDeviceGeometry& geometry,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
{
    const auto& connectionConfig = client->GetNativeConnection()->GetConfig();
    auto fetcher = New<TMasterChunkSpecFetcher>(
        client,
        client->GetNativeConnection()->GetNodeDirectory(),
        invoker,
        TMasterChunkSpecFetcherOptions{
            .MaxChunksPerFetch = connectionConfig->MaxChunksPerFetch,
            .MaxChunksPerLocateRequest = connectionConfig->MaxChunksPerLocateRequest,
            .FetchRequestInitializer = [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                NCypressClient::SetTransactionId(req, userObject.ExternalTransactionId);
            },
            .ChunkListContentType = EChunkListContentType::Hunk,
        },
        logger);

    fetcher->Add(
        userObject.ObjectId,
        userObject.ExternalCellTag,
        TUserObject::UndefinedChunkCount);

    WaitFor(fetcher->Fetch())
        .ThrowOnError();

    TChunkBlockCounts result;
    for (const auto& chunkSpec : fetcher->ChunkSpecs()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto compressedDataSize = GetChunkCompressedDataSize(chunkSpec);
        EmplaceOrCrash(result, chunkId, CompressedDataSizeToBlockCount(compressedDataSize, geometry));
    }
    return result;
}

TChunkBlockCounts FetchChunkBlockCountsFallback(
    const NNative::IClientPtr& client,
    const std::vector<TChunkId>& chunkIds,
    const TBlockDeviceGeometry& geometry)
{
    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
    auto batchReq = proxy.ExecuteBatch();
    for (auto chunkId : chunkIds) {
        batchReq->AddRequest(TYPathProxy::Get(FromObjectId(chunkId) + "/@compressed_data_size"));
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGet>();
    YT_VERIFY(std::ssize(rspsOrError) == std::ssize(chunkIds));

    TChunkBlockCounts chunkBlockCounts;
    for (int index = 0; index < std::ssize(chunkIds); ++index) {
        const auto& rsp = rspsOrError[index]
            .ValueOrThrow();
        auto compressedDataSize = ConvertTo<i64>(TYsonString(rsp->value()));
        auto blockCount = CompressedDataSizeToBlockCount(compressedDataSize, geometry);
        EmplaceOrCrash(chunkBlockCounts, chunkIds[index], blockCount);
    }
    return chunkBlockCounts;
}

TChunkBlockCounts FetchChunkBlockCounts(
    const NNative::IClientPtr& client,
    const TUserObject& userObject,
    const std::vector<TChunkId>& referencedChunkIds,
    const TBlockDeviceGeometry& geometry,
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    try {
        YT_LOG_INFO("Fetching journal chunk block counts");
        return FetchChunkBlockCountsNative(client, userObject, geometry, invoker, logger);
    } catch (const TErrorException& ex) {
        // COMPAT(babenko)
        auto refusedJournalChunks = ex.Error().FindMatching([] (const TError& innerError) {
            return innerError.GetMessage().find("while fetching hunk chunks") != std::string::npos;
        });
        if (!refusedJournalChunks) {
            throw;
        }
        YT_LOG_INFO("Master cannot fetch journal chunks through a hunk chunk list; falling back to per-chunk reads");
        return FetchChunkBlockCountsFallback(client, referencedChunkIds, geometry);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSnapshotReader
    : public ISnapshotReader
{
public:
    TSnapshotReader(
        NNative::IClientPtr client,
        TUserObject userObject,
        TSnapshotLoadSpec loadSpec,
        TBlockDeviceGeometry geometry,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , UserObject_(std::move(userObject))
        , LoadSpec_(std::move(loadSpec))
        , Geometry_(geometry)
        , Invoker_(std::move(invoker))
        , Logger(std::move(logger))
    { }

    TFuture<void> Open() final
    {
        return BIND(&TSnapshotReader::DoOpen, MakeStrong(this))
            .AsyncVia(Client_->GetConnection()->GetInvoker())
            .Run();
    }

    TFuture<std::vector<TSnapshotBlock>> ReadBlocks() final
    {
        return BIND(&TSnapshotReader::DoReadBlocks, MakeStrong(this))
            .AsyncVia(Client_->GetConnection()->GetInvoker())
            .Run();
    }

    std::vector<TChunkId> GetReferencedChunkIds() const final
    {
        return {ReferencedChunkIds_.begin(), ReferencedChunkIds_.end()};
    }

    TFuture<TChunkBlockCounts> GetChunkBlockCounts() final
    {
        return BIND(&TSnapshotReader::DoGetChunkBlockCounts, MakeStrong(this))
            .AsyncVia(Client_->GetConnection()->GetInvoker())
            .Run();
    }

private:
    const NNative::IClientPtr Client_;
    const TUserObject UserObject_;
    const TSnapshotLoadSpec LoadSpec_;
    const TBlockDeviceGeometry Geometry_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    ISchemafulUnversionedReaderPtr Reader_;
    THashSet<TChunkId> ReferencedChunkIds_;
    i64 BlockCount_ = 0;

    void DoOpen()
    {
        YT_LOG_INFO("Reading snapshot block map");

        const auto& schema = NRecords::TSnapshotBlockDescriptor::Get()->GetSchema();

        // Read the payload column as a raw global hunk reference instead of fetching the referenced
        // block: DecodeHunks disables hunk decoding, so hunk values pass through with EValueFlags::Hunk.
        auto readerOptions = New<NTableClient::TTableReaderOptions>();
        readerOptions->DecodeHunks = false;

        TClientChunkReadOptions chunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserBatch),
        };

        Reader_ = CreateSchemafulReaderAdapter(
            [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                return CreateAppropriateSchemalessMultiChunkReader(
                    readerOptions,
                    New<TTableReaderConfig>(),
                    New<TChunkReaderHost>(Client_),
                    LoadSpec_,
                    chunkReadOptions,
                    /*unordered*/ false,
                    nameTable,
                    columnFilter);
            },
            schema);
    }

    std::vector<TSnapshotBlock> DoReadBlocks()
    {
        YT_VERIFY(Reader_);

        auto batch = ReadRowBatch(Reader_);
        if (!batch) {
            YT_LOG_INFO("Snapshot block map read (BlockCount: %v)",
                BlockCount_);
            return {};
        }

        auto rows = batch->MaterializeRows();
        std::vector<TSnapshotBlock> blocks;
        blocks.reserve(rows.size());
        for (auto row : rows) {
            auto [blockIndex, payload] = FromUnversionedRow<int, TStringBuf>(row);

            auto hunkValue = ReadHunkValue(TRef(payload.data(), payload.size()));
            const auto* globalRef = std::get_if<TGlobalRefHunkValue>(&hunkValue);
            if (!globalRef) {
                THROW_ERROR_EXCEPTION("Journal snapshot payload is not a global hunk reference");
            }

            blocks.push_back({
                .Index = blockIndex,
                .Ref = {
                    .ChunkId = globalRef->ChunkId,
                    .RecordIndex = globalRef->BlockIndex,
                    .RecordOffset = globalRef->BlockOffset,
                    .PayloadLength = globalRef->Length,
                },
            });
            ReferencedChunkIds_.insert(globalRef->ChunkId);
        }

        BlockCount_ += std::ssize(blocks);
        return blocks;
    }

    TChunkBlockCounts DoGetChunkBlockCounts()
    {
        return FetchChunkBlockCounts(
            Client_,
            UserObject_,
            GetReferencedChunkIds(),
            Geometry_,
            Invoker_,
            Logger);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateSnapshotReader(
    NNative::IClientPtr client,
    TUserObject userObject,
    TSnapshotLoadSpec loadSpec,
    TBlockDeviceGeometry geometry,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TSnapshotReader>(
        std::move(client),
        std::move(userObject),
        std::move(loadSpec),
        geometry,
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
