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

#include <yt/yt/client/queue_client/config.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NQueueClient;
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

DECLARE_REFCOUNTED_CLASS(TTabletExportProgress)

class TTabletExportProgress
    : public NYTree::TYsonStruct
{
public:
    TChunkId LastChunk;
    TTimestamp MaxTimestamp;
    i64 RowCount;
    // TODO(achulkov2): Chunk count?

    REGISTER_YSON_STRUCT(TTabletExportProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("last_chunk", &TThis::LastChunk)
            .Default(NullChunkId);
        registrar.Parameter("max_timestamp", &TThis::MaxTimestamp)
            .Default(NullTimestamp);
        registrar.Parameter("row_count", &TThis::RowCount)
            .Default(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletExportProgress)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TExportProgress)

class TExportProgress
    : public TYsonStruct
{
public:
    TInstant LastExportIterationInstant;
    ui64 LastExportedFragmentUnixTs;
    THashMap<i64, TTabletExportProgressPtr> Tablets;

    void Update(i64 tabletIndex, TChunkId chunkId, TTimestamp maxTimestamp, i64 rowCount)
    {
        auto tabletProgressIt = Tablets.find(tabletIndex);
        if (tabletProgressIt == Tablets.end()) {
            tabletProgressIt = Tablets.emplace(tabletIndex, New<TTabletExportProgress>()).first;
        }

        tabletProgressIt->second->LastChunk = chunkId;
        tabletProgressIt->second->MaxTimestamp = std::max(tabletProgressIt->second->MaxTimestamp, maxTimestamp);
        tabletProgressIt->second->RowCount = rowCount;
    }

    REGISTER_YSON_STRUCT(TExportProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("last_export_iteration_instant", &TThis::LastExportIterationInstant)
            .Default(TInstant::Zero());
        registrar.Parameter("last_exported_fragment_unix_ts", &TThis::LastExportedFragmentUnixTs)
            .Default(0);
        registrar.Parameter("tablets", &TThis::Tablets)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TExportProgress)

////////////////////////////////////////////////////////////////////////////////

//! Wrapper-class for performing a single export iteration.
class TQueueExportTask
    : public TRefCounted
{
public:
    TQueueExportTask(
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        TYPath queue,
        TYPath exportDirectory,
        TDuration exportPeriod,
        const TLogger& logger)
        : Client_(std::move(client))
        , Connection_(Client_->GetNativeConnection())
        , Invoker_(std::move(invoker))
        , Queue_(std::move(queue))
        , ExportDirectory_(std::move(exportDirectory))
        , ExportPeriod_(exportPeriod)
        , Logger(logger.WithTag(
            "ExportDirectory: %v, ExportPeriod: %v",
            ExportDirectory_,
            ExportPeriod_))
    { }

    TFuture<void> Run()
    {
        return BIND(&TQueueExportTask::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const NNative::IClientPtr Client_;
    const NNative::IConnectionPtr Connection_;
    const IInvokerPtr Invoker_;

    const TYPath Queue_;
    const TYPath ExportDirectory_;
    const TDuration ExportPeriod_;

    const TLogger Logger;

    //! Options used for Cypress requests.
    NApi::TTransactionalOptions Options_;

    //! Instant of current export iteration.
    TInstant ExportInstant_;
    //! The output table unix ts corresponding to the current export iteration.
    //! NB: We use the instant above to compute this value, i.e. it corresponds to the host's physical time.
    //! This is fine, see the comment for DoRun below.
    ui64 ExportFragmentUnixTs_;
    //! Corresponds to the queue being exported.
    TUserObject QueueObject_;
    //! Original chunk specs fetched from master.
    std::vector<TChunkSpec> ChunkSpecs_;
    //! Pointers to chunk specs for chunks that are going to be exported within this iteration.
    std::vector<const TChunkSpec*> ChunkSpecsToExport_;
    //! Corresponds to the actual output table created.
    TUserObject DestinationObject_;
    TTransactionPtr UploadTransaction_;
    //! Data statistics collected from attaching chunks.
    TDataStatistics DataStatistics_;

    static constexpr TStringBuf ExportProgressAttributeName_ = "queue_static_export_progress";
    static constexpr TStringBuf ExportDestinationAttributeName_ = "queue_static_export_destination";

    //! Performs the following steps:
    //!   1) Starts transaction, obtains locks on input queue (snapshot) and export directory (exclusive).
    //!   2) Determines the export fragment timestamp as the largest fragment timestamp which is not greater than the
    //!      current physical time.
    //!      A fragment timestamp is always divisible by the export period in seconds.
    //!   3) Fetches chunk specs, skipping dynamic stores, already exported chunks and chunks with timestamp larger than
    //!      the export fragment timestamp.
    //!   4) Teleports and uploads these chunks to an appropriately named output table to the export directory.
    //!
    //! NB: We use the host's physical time to compute the unix ts of the next table to export.
    //! We rely on this time being mostly monotonous and not too different from the cluster time obtained via timestamp generation.
    //! In any case, we will only produce an output table if its unix ts is strictly greater than the one of the last exported table.
    //! If the physical time diverges from the cluster time, the only effect is that some chunks might end up being
    //! exported into later tables, which is perfectly fine.
    void DoRun()
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));

        YT_LOG_INFO("Started queue static export iteration");

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        auto transactionId = transaction->GetId();
        WaitFor(transaction->LockNode(Queue_, ELockMode::Snapshot))
            .ThrowOnError();
        WaitFor(transaction->LockNode(ExportDirectory_, ELockMode::Exclusive))
            .ThrowOnError();

        Options_.TransactionId = transactionId;

        YT_LOG_INFO("Started export transaction and locked nodes (TransactionId: %v)", transactionId);

        ExportInstant_ = TInstant::Now();
        ComputeExportFragmentUnixTs();

        QueueObject_ = TUserObject(Queue_, transactionId);

        PrepareQueueForExport();
        auto currentExportProgress = ValidateDestinationAndFetchProgress();

        FetchChunkSpecs();
        auto newExportProgress = SelectChunkSpecsToExport(currentExportProgress);

        if (ChunkSpecsToExport_.empty()) {
            YT_LOG_DEBUG("No chunks to export, aborting export transaction (TransactionId: %v)", transactionId);
            transaction->Abort();
            return;
        }

        CreateOutputTable();
        BeginUpload();
        TeleportChunkMeta();
        AttachChunks();
        EndUpload();

        UpdateCypressExportProgress(currentExportProgress, newExportProgress);

        auto commitResultOrError = WaitFor(transaction->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            commitResultOrError,
            "Error committing main export task transaction for queue %v",
            Queue_);

        YT_LOG_INFO("Finished queue static export iteration");
    }

    ui64 GetMinFragmentUnixTs(TTimestamp timestamp)
    {
        auto period = ExportPeriod_.Seconds();
        YT_VERIFY(period > 0);

        auto unixTs = UnixTimeFromTimestamp(timestamp);
        // NB: The timestamp is in range [unixTs, unixTs + 1). Since our granularity is in seconds, we can compute the
        // next fragment unix ts as the strict next tick for the lower bound.
        return (unixTs / period + 1) * period;
    }

    void ComputeExportFragmentUnixTs()
    {
        auto period = ExportPeriod_.Seconds();
        YT_VERIFY(period > 0);

        auto exportUnixTs = ExportInstant_.Seconds();
        // NB: The unix ts of the closest tick to the left.
        ExportFragmentUnixTs_ = (exportUnixTs / period) * period;
    }

    void GetAndFillBasicAttributes(
        TUserObject& object,
        bool populateSecurityTags) const
    {
        YT_LOG_DEBUG("Started collecting basic attributes");

        auto proxy = CreateObjectServiceReadProxy(Client_, TMasterReadOptions().ReadFrom);
        auto req = TObjectYPathProxy::GetBasicAttributes(object.GetPath());
        req->set_populate_security_tags(populateSecurityTags);
        SetTransactionId(req, *object.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error getting basic attributes of queue %v", object.GetPath());

        const auto& rsp = rspOrError.Value();

        object.ObjectId = FromProto<TObjectId>(rsp->object_id());
        object.Type = TypeFromId(object.ObjectId);
        object.ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
        object.ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *object.TransactionId;
        if (populateSecurityTags) {
            object.SecurityTags =
                FromProto<std::vector<TSecurityTag>>(rsp->security_tags().items());
        }

        YT_LOG_DEBUG("Finished collecting basic attributes");
    }

    IAttributeDictionaryPtr FetchNodeAttributes(const TYPath& path, const std::vector<TStringBuf>& attributeKeys) const
    {
        YT_LOG_DEBUG(
            "Started fetching attributes (Path: %v, PathRequestedAttributes: %v)",
            path,
            attributeKeys);

        // TODO(achulkov2): Change to simple Client_->GetNode with attributes.
        auto proxy = CreateObjectServiceReadProxy(Client_, TMasterReadOptions().ReadFrom);
        auto req = TYPathProxy::Get(path + "/@");
        SetTransactionId(req, Options_.TransactionId);
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error fetching attributes %v for path %v",
            path);

        YT_LOG_DEBUG(
            "Finished fetching attributes (Path: %v, PathRequestedAttributes: %v)",
            path,
            attributeKeys);
        return ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
    }

    void PrepareQueueForExport()
    {
        GetAndFillBasicAttributes(QueueObject_, /*populateSecurityTags*/ true);

        if (QueueObject_.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION(
                "Invalid type of %v: expected %Qlv, %Qlv found",
                QueueObject_.GetPath(),
                EObjectType::Table,
                QueueObject_.Type);
        }

        auto attributes = FetchNodeAttributes(
            QueueObject_.GetPath(), {"chunk_count", "dynamic", "schema"});

        if (!attributes->Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Queue %v should be a dynamic table", QueueObject_.GetPath());
        }

        if (attributes->Get<TTableSchemaPtr>("schema")->IsSorted()) {
            THROW_ERROR_EXCEPTION("Queue %v should be an ordered dynamic table", QueueObject_.GetPath());
        }

        QueueObject_.ChunkCount = attributes->Get<i64>("chunk_count");
    }

    TExportProgressPtr ValidateDestinationAndFetchProgress()
    {
        auto exportDirectoryAttributes = FetchNodeAttributes(
            ExportDirectory_,
            {ExportDestinationAttributeName_, ExportProgressAttributeName_});

        auto destinationConfig = exportDirectoryAttributes->Get<TQueueStaticExportDestinationConfig>(ExportDestinationAttributeName_);
        if (destinationConfig.OriginatingQueueId != QueueObject_.ObjectId) {
            THROW_ERROR_EXCEPTION(
                "Destination config is not configured to accept exports from queue %v, configured id %v does not match queue id %v",
                destinationConfig.OriginatingQueueId,
                QueueObject_.ObjectId);
        }

        auto currentExportProgress = exportDirectoryAttributes->Find<TExportProgressPtr>(ExportProgressAttributeName_);
        if (currentExportProgress && currentExportProgress->LastExportedFragmentUnixTs >= ExportFragmentUnixTs_) {
            THROW_ERROR_EXCEPTION(
                "Fragment with unix ts %v is already exported, last exported fragment unix ts is %v",
                ExportFragmentUnixTs_,
                currentExportProgress->LastExportedFragmentUnixTs);
        }


        return currentExportProgress ? currentExportProgress : New<TExportProgress>();
    }

    TExportProgressPtr SelectChunkSpecsToExport(const TExportProgressPtr& currentExportProgress)
    {
        auto newExportProgress = CloneYsonStruct(currentExportProgress);

        std::map<i64, std::vector<const TChunkSpec*>> tabletToChunkSpecs;
        for (const auto& chunkSpec : ChunkSpecs_) {
            tabletToChunkSpecs[chunkSpec.tablet_index()].push_back(&chunkSpec);
        }

        for (const auto& [tabletIndex, tabletSpecs] : tabletToChunkSpecs) {
            auto lastExportedSpecIt = std::find_if(tabletSpecs.begin(), tabletSpecs.end(), [&, tabletIndex = tabletIndex] (auto tabletSpec) {
                auto tabletProgressIt = currentExportProgress->Tablets.find(tabletIndex);
                return tabletProgressIt != currentExportProgress->Tablets.end() && tabletProgressIt->second->LastChunk == FromProto<TChunkId>(tabletSpec->chunk_id());
            });

            auto specToExportIt = (lastExportedSpecIt == tabletSpecs.end() ? tabletSpecs.begin() : (lastExportedSpecIt + 1));
            for (; specToExportIt != tabletSpecs.end(); ++specToExportIt) {
                auto* chunkSpec = *specToExportIt;
                // TODO(achulkov2): Get rid of this allocation?
                TInputChunkPtr chunk = New<TInputChunk>(*chunkSpec);
                // NB: This is guaranteed by setting omit_dynamic_stores(true) while fetching.
                YT_VERIFY(!chunk->IsDynamicStore());

                auto chunkFormat = FromProto<EChunkFormat>(chunkSpec->chunk_meta().format());
                ValidateTableChunkFormatVersioned(chunkFormat, /*versioned*/ false);

                auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec->chunk_meta().extensions());
                auto maxTimestamp = FromProto<TTimestamp>(miscExt.max_timestamp());
                // We only export chunks which are compatible with the current export fragment ts.
                if (GetMinFragmentUnixTs(maxTimestamp) > ExportFragmentUnixTs_) {
                    // NB: Latter chunks might have smaller max timestamps in case of weak commit ordering, but we do
                    // not export any of them to maintain intra-tablet chunk order within the exported tables.
                    break;
                }

                ChunkSpecsToExport_.push_back(chunkSpec);
                newExportProgress->Update(
                    tabletIndex,
                    chunk->GetChunkId(),
                    maxTimestamp,
                    chunkSpec->table_row_index() + miscExt.row_count());
            }
        }

        newExportProgress->LastExportedFragmentUnixTs = ExportFragmentUnixTs_;
        newExportProgress->LastExportIterationInstant = ExportInstant_;
        return newExportProgress;
    }

    void FetchChunkSpecs()
    {
        YT_LOG_DEBUG("Started fetching chunk specs (Count: %v)", QueueObject_.ChunkCount);

        auto prepareFetchRequest = [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& request, int /*index*/) {
            request->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            request->set_omit_dynamic_stores(true);
            SetTransactionId(request, *QueueObject_.TransactionId);
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
            QueueObject_.ObjectId,
            QueueObject_.ExternalCellTag,
            QueueObject_.ChunkCount);

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = chunkSpecFetcher->GetChunkSpecsOrderedNaturally();
        YT_LOG_DEBUG("Finished fetching chunk specs (Count: %v)", ChunkSpecs_.size());
    }

    void CreateOutputTable()
    {
        auto destinationPath = Format(
            "%s/%v-%v",
            ExportDirectory_,
            ExportFragmentUnixTs_,
            ExportPeriod_.Seconds());
        DestinationObject_ = TUserObject(destinationPath, Options_.TransactionId);

        TCreateNodeOptions createOptions;
        createOptions.TransactionId = Options_.TransactionId;
        createOptions.Attributes = CreateEphemeralAttributes();
        WaitFor(Client_->CreateNode(DestinationObject_.GetPath(), EObjectType::Table, createOptions))
            .ThrowOnError();

        GetAndFillBasicAttributes(DestinationObject_, /*populateSecurityTags*/ false);
    }

    static TCellTagList GetAffectedCellTags(
        const std::vector<const TChunkSpec*>& chunkSpecs,
        const TUserObject& destinationObject,
        const std::optional<TCellTag> cellTagToExclude)
    {
        THashSet<TCellTag> cellTags;

        for (const auto& chunkSpec : chunkSpecs) {
            auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
            auto cellTag = CellTagFromId(chunkId);
            cellTags.insert(cellTag);
        }

        cellTags.insert(destinationObject.ExternalCellTag);

        if (cellTagToExclude) {
            cellTags.erase(*cellTagToExclude);
        }

        return {cellTags.begin(), cellTags.end()};
    }

    void BeginUpload()
    {
        auto destinationObjectCellTag = CellTagFromId(DestinationObject_.ObjectId);
        auto proxy = CreateObjectServiceWriteProxy(Client_, destinationObjectCellTag);

        auto req = TChunkOwnerYPathProxy::BeginUpload(DestinationObject_.GetObjectIdPath());
        req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
        req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));

        req->set_upload_transaction_title(Format(
            "Exporting queue %v to static table %v",
            QueueObject_.GetPath(),
            DestinationObject_.GetPath()));

        auto cellTags = GetAffectedCellTags(
            ChunkSpecsToExport_,
            DestinationObject_,
            /*cellTagToExclude*/ destinationObjectCellTag);
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(
            ToProto<i64>(Connection_->GetConfig()->UploadTransactionTimeout));
        GenerateMutationId(req);

        SetTransactionId(req, Options_.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error starting upload to %v", QueueObject_.GetPath());

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
            "Started upload transaction for queue export (Destination: %v, UploadTransactionId: %v)",
            DestinationObject_.GetPath(),
            UploadTransaction_->GetId());
    }

    void TeleportChunkMeta()
    {
        YT_VERIFY(UploadTransaction_);

        auto teleporter = New<TChunkTeleporter>(
            Client_->GetNativeConnection()->GetConfig(),
            Client_,
            Client_->GetNativeConnection()->GetInvoker(),
            UploadTransaction_->GetId(),
            Logger);

        for (const auto* chunkSpec : ChunkSpecsToExport_) {
            teleporter->RegisterChunk(FromProto<TChunkId>(chunkSpec->chunk_id()), DestinationObject_.ExternalCellTag);
        }
        WaitFor(teleporter->Run())
            .ThrowOnError();
    }

   TChunkListId GetChunkListId()
    {
        YT_VERIFY(UploadTransaction_);

        auto proxy = CreateObjectServiceWriteProxy(Client_, DestinationObject_.ExternalCellTag);
        auto req = TChunkOwnerYPathProxy::GetUploadParams(DestinationObject_.GetObjectIdPath());
        SetTransactionId(req, UploadTransaction_->GetId());

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error requesting upload parameters for %v", DestinationObject_.GetPath());

        const auto& rsp = rspOrError.Value();
        return FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    void AttachChunks()
    {
        YT_VERIFY(UploadTransaction_);

        YT_LOG_DEBUG(
            "Started chunk upload (Destination: %v, UploadTransactionId: %v, ChunkCount: %v)",
            DestinationObject_.GetPath(),
            UploadTransaction_->GetId(),
            ChunkSpecsToExport_.size());

        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader, DestinationObject_.ExternalCellTag));

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetTransactionId(batchReq, UploadTransaction_->GetId());
        SetSuppressUpstreamSync(&batchReq->Header(), true);

        auto chunkListId = GetChunkListId();

        auto req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), chunkListId);

        for (const auto* chunkSpec : ChunkSpecsToExport_) {
            *req->add_child_ids() = chunkSpec->chunk_id();
        }
        req->set_request_statistics(true);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error attaching chunks to %v",
            DestinationObject_.GetPath());

        const auto& batchRsp = batchRspOrError.Value();

        const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);

        DataStatistics_ = rsp.statistics();

        YT_LOG_DEBUG(
            "Finished chunk upload (Destination: %v, UploadTransactionId: %v, ChunkCount: %v)",
            DestinationObject_.GetPath(),
            UploadTransaction_->GetId(),
            ChunkSpecsToExport_.size());
    }

    void EndUpload()
    {
        YT_VERIFY(UploadTransaction_);

        auto proxy = CreateObjectServiceWriteProxy(Client_, CellTagFromId(DestinationObject_.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(DestinationObject_.GetObjectIdPath());
        *req->mutable_statistics() = DataStatistics_;

        std::vector<TSecurityTag> inferredSecurityTags;
        inferredSecurityTags.insert(
            inferredSecurityTags.end(),
            QueueObject_.SecurityTags.begin(),
            QueueObject_.SecurityTags.end());
        SortUnique(inferredSecurityTags);

        auto securityTags = DestinationObject_.Path.GetSecurityTags().value_or(inferredSecurityTags);

        ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
        SetTransactionId(req, UploadTransaction_->GetId());
        GenerateMutationId(req);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error ending upload to %v", DestinationObject_.GetPath());

        UploadTransaction_->Detach();
    }

    void UpdateCypressExportProgress(
        const TExportProgressPtr& /*currentExportProgress*/,
        const TExportProgressPtr& newExportProgress)
    {
        TSetNodeOptions options;
        options.TransactionId = Options_.TransactionId;
        WaitFor(Client_->SetNode(
            Format("%v/@%v", ExportDirectory_, ExportProgressAttributeName_),
            ConvertToYsonString(newExportProgress),
            options))
            .ThrowOnError();

        // TODO(achulkov2): Log summary of progress difference.
        YT_LOG_DEBUG("Updated export progress");
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueExportTask)

////////////////////////////////////////////////////////////////////////////////

TQueueExporter::TQueueExporter(
    NNative::IClientPtr client,
    IInvokerPtr invoker,
    const TLogger& logger)
    : Client_(std::move(client))
    , Invoker_(std::move(invoker))
    , Logger(logger)
{ }

TFuture<void> TQueueExporter::RunExportIteration(
    TYPath queue,
    TYPath exportDirectory,
    TDuration exportPeriod)
{
    auto exportTask = New<TQueueExportTask>(
        Client_,
        Invoker_,
        std::move(queue),
        std::move(exportDirectory),
        exportPeriod,
        Logger);
    return exportTask->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
