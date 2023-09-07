#include "queue_static_table_exporter.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NLogging;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TQueueExportTask
    : public TRefCounted
{
public:
    TQueueExportTask(
        NNative::IConnectionPtr connection,
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        TRichYPath queuePath,
        TRichYPath destinationPath,
        TQueueExportOptions options,
        const TLogger& logger)
        : Connection_(std::move(connection))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , QueuePath_(std::move(queuePath))
        , DestinationPath_(std::move(destinationPath))
        , Options_(std::move(options))
        , Logger(logger.WithTag(
            "Destination: %v",
            DestinationPath_.GetPath()))
    { }

    TFuture<void> Run()
    {
        return BIND(&TQueueExportTask::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const NNative::IConnectionPtr Connection_;
    const NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;

    TRichYPath QueuePath_;
    TRichYPath DestinationPath_;
    TQueueExportOptions Options_;

    TLogger Logger;

    ui64 QueueTabletCount_ = 0;

    TTransactionPtr UploadTransaction_;

    void DoRun()
    {
        YT_LOG_DEBUG(
            "Starting export (LowerExportTimestamp: %v, UpperExportTimestamp: %v)",
            Options_.LowerExportTimestamp,
            Options_.UpperExportTimestamp);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        auto transactionId = transaction->GetId();
        WaitFor(transaction->LockNode(QueuePath_.GetPath(), ELockMode::Snapshot))
            .ThrowOnError();
        WaitFor(transaction->LockNode(DestinationPath_.GetPath(), ELockMode::Exclusive))
            .ThrowOnError();

        Options_.TransactionId = transactionId;

        YT_LOG_DEBUG(
            "Started transaction and locked nodes (TransactionId: %v, LockedQueueNode: %v, LockedDestinationNode %v)",
            transactionId,
            QueuePath_.GetPath(),
            DestinationPath_.GetPath());

        auto queueObject = TUserObject(QueuePath_, transactionId);
        auto destinationObject = TUserObject(DestinationPath_, transactionId);

        PrepareForExport(queueObject, destinationObject);

        auto fetchedChunkSpecs = FetchAndFilterChunkSpecs(queueObject);

        BeginUpload(fetchedChunkSpecs, queueObject, destinationObject);
        TeleportChunkMeta(fetchedChunkSpecs, destinationObject);
        auto dataStatistics = AttachChunks(fetchedChunkSpecs, destinationObject);
        EndUpload(queueObject, destinationObject, dataStatistics);

        auto commitResultOrError = WaitFor(transaction->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(commitResultOrError, "Error committing main export task transaction");

        YT_LOG_DEBUG("Finished export");
    }

    void GetAndFillBasicAttributes(
        TUserObject& queueObject,
        TUserObject& destinationObject) const
    {
        YT_LOG_DEBUG("Starting collecting basic attributes");

        auto proxy = CreateObjectServiceReadProxy(Client_, TMasterReadOptions().ReadFrom);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TObjectYPathProxy::GetBasicAttributes(queueObject.GetPath());
            req->set_populate_security_tags(true);
            SetTransactionId(req, *queueObject.TransactionId);
            batchReq->AddRequest(req, "get_queue_attributes");
        }

        {
            auto req = TObjectYPathProxy::GetBasicAttributes(destinationObject.GetPath());
            SetTransactionId(req, *destinationObject.TransactionId);
            batchReq->AddRequest(req, "get_dst_attributes");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error getting basic attributes of inputs and outputs");
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>(
                "get_queue_attributes");
            YT_VERIFY(rspsOrError.size() == 1);
            const auto& rsp = rspsOrError[0].Value();

            queueObject.ObjectId = FromProto<TObjectId>(rsp->object_id());
            queueObject.ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
            queueObject.ExternalTransactionId = rsp->has_external_transaction_id()
                ? FromProto<TTransactionId>(rsp->external_transaction_id())
                : *queueObject.TransactionId;
            queueObject.SecurityTags =
                FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
        }
        {
            auto rspsOrError =
                batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_dst_attributes");
            YT_VERIFY(rspsOrError.size() == 1);
            const auto& rsp = rspsOrError[0].Value();

            destinationObject.ObjectId = FromProto<TObjectId>(rsp->object_id());
            destinationObject.ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
        }

        YT_LOG_DEBUG("Finished collecting basic attributes");
    }

    void ValidateType(
        TUserObject& queueObject,
        TUserObject& destinationObject) const
    {
        destinationObject.Type = TypeFromId(destinationObject.ObjectId);
        queueObject.Type = TypeFromId(queueObject.ObjectId);

        auto checkType = [&](const TUserObject& object) {
            if (object.Type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION(
                    "Invalid type of %v: expected %Qlv, %Qlv found",
                    object.GetPath(),
                    EObjectType::Table,
                    object.Type);
            }
        };

        checkType(queueObject);
        checkType(destinationObject);
    }

    IAttributeDictionaryPtr FetchNodeAttributes(
        const TUserObject& queueObject,
        const std::vector<TString>& attributeKeys) const
    {
        YT_LOG_DEBUG("Start fetching attributes for queue (RequestedAttributes: %v)", attributeKeys);
        auto proxy = CreateObjectServiceReadProxy(Client_, TMasterReadOptions().ReadFrom);

        auto req = TYPathProxy::Get(queueObject.GetPath() + "/@");

        AddCellTagToSyncWith(req, queueObject.ObjectId);
        SetTransactionId(req, *queueObject.TransactionId);
        for (const auto& attributeKey: attributeKeys) {
            req->mutable_attributes()->add_keys(attributeKey);
        }
        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error fetching attributes %v for queue %Qv", queueObject.GetPath());

        const auto& rsp = rspOrError.Value();

        YT_LOG_DEBUG("Finished fetching attributes for queue (RequestedAttributes: %v)", attributeKeys);

        return ConvertToAttributes(TYsonString(rsp->value()));
    }

    void PrepareForExport(
        TUserObject& queueObject,
        TUserObject& destinationObject)
    {
        GetAndFillBasicAttributes(queueObject, destinationObject);

        ValidateType(queueObject, destinationObject);

        auto attributes = FetchNodeAttributes(queueObject, {"chunk_count", "dynamic", "schema", "tablet_count"});

        if (!attributes->Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Queue %Qv should be dynamic", queueObject.GetPath());
        }

        if (attributes->Get<TTableSchemaPtr>("schema")->IsSorted()) {
            THROW_ERROR_EXCEPTION("Queue %Qv should be ordered", queueObject.GetPath());
        }

        QueueTabletCount_ = attributes->Get<i64>("tablet_count");
        queueObject.ChunkCount = attributes->Get<i64>("chunk_count");
    }

    // For now this function performs temporary behavior of filtering chunks by timestamps.
    bool InTimePeriod(const TTimestamp& timestamp) const
    {
        return Options_.LowerExportTimestamp <= timestamp &&
            timestamp <= Options_.UpperExportTimestamp;
    }

    std::vector<TChunkSpec> FilterChunksAndOrderByTablets(TIntrusivePtr<TMasterChunkSpecFetcher>& chunkSpecFetcher)
    {
        ui64 totalFetchedChunkCount = 0;
        auto allFetchedChunkSpecs = chunkSpecFetcher->GetChunkSpecsOrderedNaturally();

        std::vector<std::vector<TChunkSpec>> chunkSpecsPerTablet(QueueTabletCount_);
        for (const auto& chunkSpec : allFetchedChunkSpecs) {
            auto chunk = New<TInputChunk>(chunkSpec);
            YT_VERIFY(!chunk->IsDynamicStore());

            const auto format = FromProto<EChunkFormat>(chunkSpec.chunk_meta().format());
            ValidateTableChunkFormatVersioned(format, /*versioned*/ false);

            auto miscExt =
                GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());
            auto maxWrittenRowTimestamp = FromProto<TTimestamp>(miscExt.max_timestamp());
            if (InTimePeriod(maxWrittenRowTimestamp)) {
                auto tabletIndex = chunkSpec.tablet_index();
                if (tabletIndex >= std::ssize(chunkSpecsPerTablet)) {
                    chunkSpecsPerTablet.resize(tabletIndex + 1);
                }
                chunkSpecsPerTablet[tabletIndex].push_back(chunkSpec);
                ++totalFetchedChunkCount;
            }
        }

        std::vector<TChunkSpec> resultingChunkSpecs;
        resultingChunkSpecs.reserve(totalFetchedChunkCount);
        for (const auto& tablet : chunkSpecsPerTablet) {
            resultingChunkSpecs.insert(resultingChunkSpecs.end(), tablet.begin(), tablet.end());
        }

        return resultingChunkSpecs;
    }

    std::vector<TChunkSpec> FetchAndFilterChunkSpecs(const TUserObject& queueObject)
    {
        YT_LOG_DEBUG("Starting fetching chunk specs");

        auto prepareFetchRequest = [&](
            const TChunkOwnerYPathProxy::TReqFetchPtr& request,
            int /*index*/) {
            request->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            request->set_omit_dynamic_stores(true);
            SetTransactionId(request, *queueObject.TransactionId);
        };

        auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
            Client_,
            TMasterReadOptions{},
            Connection_->GetNodeDirectory(),
            Connection_->GetInvoker(),
            Connection_->GetConfig()->MaxChunksPerFetch,
            Connection_->GetConfig()->MaxChunksPerLocateRequest,
            prepareFetchRequest,
            Logger);

        chunkSpecFetcher->Add(
            queueObject.ObjectId,
            queueObject.ExternalCellTag,
            queueObject.ChunkCount);

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        auto resultingChunkSpecs = FilterChunksAndOrderByTablets(chunkSpecFetcher);

        YT_LOG_DEBUG(
            "Finished fetching chunk specs (FetchedChunkCount: %v)", resultingChunkSpecs.size());

        return resultingChunkSpecs;
    }

    TCellTagList GetAffectedCellTags(
        const std::vector<TChunkSpec>& chunkSpecs,
        const TUserObject& destinationObject)
    {
        THashSet<TCellTag> cellTags;

        for (const auto& chunkSpec : chunkSpecs) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto cellTag = CellTagFromId(chunkId);
            cellTags.insert(cellTag);
        }

        cellTags.insert(destinationObject.ExternalCellTag);

        return {cellTags.begin(), cellTags.end()};
    }

    void BeginUpload(
        const std::vector<TChunkSpec>& chunkSpecs,
        const TUserObject& queueObject,
        const TUserObject& destinationObject)
    {
        auto destinationObjectCellTag = CellTagFromId(destinationObject.ObjectId);
        auto proxy = CreateObjectServiceWriteProxy(Client_, destinationObjectCellTag);

        auto req = TChunkOwnerYPathProxy::BeginUpload(destinationObject.GetObjectIdPath());
        req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
        req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));

        req->set_upload_transaction_title(Format(
            "Exporting queue %v to static table %v",
            queueObject.GetPath(),
            destinationObject.GetPath()));

        auto cellTags = GetAffectedCellTags(chunkSpecs, destinationObject);
        cellTags.erase(
            std::remove(cellTags.begin(), cellTags.end(), destinationObjectCellTag), cellTags.end());
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(
            ToProto<i64>(Connection_->GetConfig()->UploadTransactionTimeout));
        GenerateMutationId(req);

        SetTransactionId(req, Options_.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error starting upload to %Qv", queueObject.GetPath());

        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

        UploadTransaction_ = Client_->GetTransactionManager()->Attach(
            uploadTransactionId,
            TTransactionAttachOptions{
                .AutoAbort = true,
                .Ping = true,
                .PingAncestors = true,
            });

        YT_LOG_DEBUG(
            "Started upload of queue data to static table (UploadTransaction: %v)",
            UploadTransaction_->GetId());
    }

    void TeleportChunkMeta(
        const std::vector<TChunkSpec>& chunkSpecs,
        const TUserObject& destinationObject)
    {
        YT_VERIFY(UploadTransaction_);

        auto teleporter = New<TChunkTeleporter>(
            Client_->GetNativeConnection()->GetConfig(),
            Client_,
            Client_->GetNativeConnection()->GetInvoker(),
            UploadTransaction_->GetId(),
            Logger);

        for (const auto& chunkSpec : chunkSpecs) {
            teleporter->RegisterChunk(
                FromProto<TChunkId>(chunkSpec.chunk_id()), destinationObject.ExternalCellTag);
        }
        WaitFor(teleporter->Run())
            .ThrowOnError();
    }

   TChunkListId GetChunkListId(const TUserObject& destinationObject)
    {
        YT_VERIFY(UploadTransaction_);

        auto proxy = CreateObjectServiceWriteProxy(Client_, destinationObject.ExternalCellTag);
        auto req = TChunkOwnerYPathProxy::GetUploadParams(destinationObject.GetObjectIdPath());
        SetTransactionId(req, UploadTransaction_->GetId());

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error requesting upload parameters for %Qv", destinationObject.GetPath());

        const auto& rsp = rspOrError.Value();
        return FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    TDataStatistics AttachChunks(
        const std::vector<TChunkSpec>& chunkSpecs,
        const TUserObject& destinationObject)
    {
        YT_VERIFY(UploadTransaction_);

        YT_LOG_DEBUG("Started upload of chunks");

        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader, destinationObject.ExternalCellTag));

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetTransactionId(batchReq, UploadTransaction_->GetId());
        SetSuppressUpstreamSync(&batchReq->Header(), true);

        auto chunkListId = GetChunkListId(destinationObject);

        auto req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), chunkListId);

        for (const auto& chunkSpec : chunkSpecs) {
            *req->add_child_ids() = chunkSpec.chunk_id();
        }
        req->set_request_statistics(true);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error attaching chunks to %Qv",
            destinationObject.GetPath());

        const auto& batchRsp = batchRspOrError.Value();

        const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);

        YT_LOG_DEBUG("Finished upload of chunks");

        return rsp.statistics();
    }

    void EndUpload(
        const TUserObject& queueObject,
        const TUserObject& destinationObject,
        const TDataStatistics& statistics)
    {
        YT_VERIFY(UploadTransaction_);

        auto proxy = CreateObjectServiceWriteProxy(Client_, CellTagFromId(destinationObject.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(destinationObject.GetObjectIdPath());
        *req->mutable_statistics() = statistics;

        std::vector<TSecurityTag> inferredSecurityTags;
        inferredSecurityTags.insert(
            inferredSecurityTags.end(),
            queueObject.SecurityTags.begin(),
            queueObject.SecurityTags.end());
        SortUnique(inferredSecurityTags);

        std::vector<TSecurityTag> securityTags;
        if (auto explicitSecurityTags = destinationObject.Path.GetSecurityTags()) {
            securityTags = *explicitSecurityTags;
        } else {
            securityTags = inferredSecurityTags;
        }

        ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
        SetTransactionId(req, UploadTransaction_->GetId());
        GenerateMutationId(req);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error ending upload to %Qv", destinationObject.GetPath());

        UploadTransaction_->Detach();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueExportTask);

////////////////////////////////////////////////////////////////////////////////

TQueueExporter::TQueueExporter(
    NNative::IConnectionPtr connection,
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    TLogger logger)
    : Connection_(std::move(connection))
    , Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , Logger(std::move(logger))
{ }

TFuture<void> TQueueExporter::ExportToStaticTable(
    const TRichYPath& queuePath,
    const TRichYPath& destinationPath,
    TQueueExportOptions options) const
{
    auto exportTask = New<TQueueExportTask>(
        Connection_,
        Client_,
        Invoker_,
        queuePath,
        destinationPath,
        options,
        Logger);
    return exportTask->Run();
}

////////////////////////////////////////////////////////////////////////////////

TString GenerateStaticTableName(const TRichYPath& queuePath)
{
    return queuePath.GetPath() + "-export-result";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
