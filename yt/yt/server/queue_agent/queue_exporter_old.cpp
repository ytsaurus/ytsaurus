#include "queue_exporter_old.h"

#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/core/concurrency/scheduled_executor.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NAlertManager;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NProfiling;
using namespace NObjectClient;
using namespace NQueueClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NLogging;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

void TQueueTabletExportProgressOld::Register(TRegistrar registrar)
{
    registrar.Parameter("last_chunk", &TThis::LastChunk)
        .Default(NullChunkId);
    registrar.Parameter("max_timestamp", &TThis::MaxTimestamp)
        .Default(NullTimestamp);
    registrar.Parameter("row_count", &TThis::RowCount)
        .Default(0);
    registrar.Parameter("chunk_count", &TThis::ChunkCount)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TQueueExportProgressOld::Update(i64 tabletIndex, TChunkId chunkId, TTimestamp maxTimestamp, i64 rowCount)
{
    auto tabletProgressIt = Tablets.find(tabletIndex);
    if (tabletProgressIt == Tablets.end()) {
        tabletProgressIt = Tablets.emplace(tabletIndex, New<TQueueTabletExportProgressOld>()).first;
    }

    tabletProgressIt->second->LastChunk = chunkId;
    ++tabletProgressIt->second->ChunkCount;
    tabletProgressIt->second->MaxTimestamp = std::max(tabletProgressIt->second->MaxTimestamp, maxTimestamp);
    tabletProgressIt->second->RowCount = rowCount;
}

void TQueueExportProgressOld::Register(TRegistrar registrar)
{
    registrar.Parameter("last_successful_export_iteration_instant", &TThis::LastSuccessfulExportIterationInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_exported_fragment_iteration_instant", &TThis::LastExportedFramgentIterationInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_exported_fragment_unix_ts", &TThis::LastExportedFragmentUnixTs)
        .Default(0);
    registrar.Parameter("tablets", &TThis::Tablets)
        .Default();
    registrar.Parameter("queue_object_id", &TThis::QueueObjectId)
        .Default(NullObjectId);
}

////////////////////////////////////////////////////////////////////////////////

struct TProgressDiffOld
{
    i64 RowCount = 0;
    i64 ChunkCount = 0;

    TProgressDiffOld(const TQueueExportProgressOldPtr& currentProgress, const TQueueExportProgressOldPtr& newProgress)
    {
        for (const auto& [tabletIndex, newTabletProgress] : newProgress->Tablets) {
            auto currentTabletProgress = currentProgress->Tablets.Value(tabletIndex, New<TQueueTabletExportProgressOld>());
            RowCount += newTabletProgress->RowCount - currentTabletProgress->RowCount;
            ChunkCount += newTabletProgress->ChunkCount - currentTabletProgress->ChunkCount;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Wrapper-class for performing a single export iteration.
class TQueueExportTaskOld
    : public TRefCounted
{
public:
    TQueueExportTaskOld(
        NNative::IClientPtr client,
        TQueueExportProfilingCountersOldPtr profilingCounters,
        TYPath queue,
        TQueueStaticExportConfigPtr exportConfig,
        const TLogger& logger)
        : Client_(std::move(client))
        , Connection_(Client_->GetNativeConnection())
        , ProfilingCounters_(std::move(profilingCounters))
        , Queue_(std::move(queue))
        , ExportConfig_(std::move(exportConfig))
        , Logger(logger.WithTag(
            "ExportDirectory: %v, ExportPeriod: %v",
            ExportConfig_->ExportDirectory,
            ExportConfig_->ExportPeriod))
    { }

    //! Performs the following steps:
    //!   1) Starts transaction, obtains locks on input queue (snapshot) and <export_directory>/@queue_static_exporter attribute (shared).
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
    TQueueExportProgressOldPtr Run()
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));

        YT_LOG_INFO("Started queue static export iteration");
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Finished queue static export iteration");
        });

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        auto transactionId = transaction->GetId();
        auto queueObjectId = WaitFor(transaction->LockNode(Queue_, ELockMode::Snapshot))
            .ValueOrThrow()
            .NodeId;

        // Take it for guarantee that only one queue agent instance do this export.
        WaitFor(transaction->LockNode(
            ExportConfig_->ExportDirectory,
            ELockMode::Shared,
            TLockNodeOptions{
                .AttributeKey = TString(ExporterAttributeName_),
            }))
            .ThrowOnError();

        Options_.TransactionId = transactionId;

        YT_LOG_INFO("Started export transaction and locked nodes (TransactionId: %v)",
            transactionId);

        ExportInstant_ = TInstant::Now();
        ComputeExportFragmentUnixTs();

        QueueObject_ = TUserObject(FromObjectId(queueObjectId), transactionId);

        PrepareQueueForExport();
        auto currentExportProgress = ValidateDestinationAndFetchProgress();

        FetchChunkSpecs();
        auto newExportProgress = SelectChunkSpecsToExport(currentExportProgress);
        YT_VERIFY(newExportProgress->QueueObjectId == QueueObject_.ObjectId);

        if (ChunkSpecsToExport_.empty()) {
            // NB(apachee): New export progress is taking into account if there are chunks
            // to export, meaning in this case only last successful export iteration instant would
            // be changed.
            YT_LOG_DEBUG("No chunks to export, committing export transaction prematurely (TransactionId: %v)",
                transactionId);
        } else {
            CreateOutputTable();
            BeginUpload();
            TeleportChunkMeta();
            AttachChunks();
            EndUpload();
        }

        auto diff = UpdateCypressExportProgress(currentExportProgress, newExportProgress);

        auto commitResultOrError = WaitFor(transaction->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            commitResultOrError,
            "Error committing main export task transaction for queue %v",
            Queue_);

        ProfilingCounters_->ExportedRows.Increment(diff.RowCount);
        ProfilingCounters_->ExportedChunks.Increment(diff.ChunkCount);
        ProfilingCounters_->ExportedTables.Increment();

        return newExportProgress;
    }

private:
    const NNative::IClientPtr Client_;
    const NNative::IConnectionPtr Connection_;
    // const IInvokerPtr Invoker_;
    const TQueueExportProfilingCountersOldPtr ProfilingCounters_;

    const TYPath Queue_;
    const TQueueStaticExportConfigPtr ExportConfig_;

    const TLogger Logger;

    //! Options used for Cypress requests.
    NApi::TTransactionalOptions Options_;

    //! Instant of current export iteration.
    TInstant ExportInstant_;
    //! The output table unix ts corresponding to the current export iteration.
    //! NB: We use the instant above to compute this value, i.e. it corresponds to the host's physical time.
    //! This is fine, see the comment for Run above.
    ui64 ExportFragmentUnixTs_;
    //! Corresponds to the queue being exported.
    TUserObject QueueObject_;
    TTableSchemaPtr QueueSchema_;
    TMasterTableSchemaId QueueSchemaId_;
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
    static constexpr TStringBuf ExporterAttributeName_ = "queue_static_exporter";

    ui64 GetMinFragmentUnixTs(TTimestamp timestamp)
    {
        auto period = ExportConfig_->ExportPeriod.Seconds();
        YT_VERIFY(period > 0);

        auto unixTs = UnixTimeFromTimestamp(timestamp);
        // NB: The timestamp is in range [unixTs, unixTs + 1). Since our granularity is in seconds, we can compute the
        // next fragment unix ts as the strict next tick for the lower bound.
        return (unixTs / period + 1) * period;
    }

    void ComputeExportFragmentUnixTs()
    {
        auto period = ExportConfig_->ExportPeriod.Seconds();
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
            attributeKeys,
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
        YT_VERIFY(QueueObject_.GetObjectIdPath() == QueueObject_.GetPath());

        if (QueueObject_.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION(
                "Invalid type of %v: expected %Qlv, %Qlv found",
                Queue_,
                EObjectType::Table,
                QueueObject_.Type);
        }

        auto attributes = FetchNodeAttributes(
            QueueObject_.GetPath(), {"chunk_count", "dynamic", "schema", "schema_id"});

        if (!attributes->Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Queue %v should be a dynamic table", Queue_);
        }

        QueueSchema_ = attributes->Get<TTableSchemaPtr>("schema");
        QueueSchemaId_ = attributes->Get<TMasterTableSchemaId>("schema_id");
        if (QueueSchema_->IsSorted()) {
            THROW_ERROR_EXCEPTION("Queue %v should be an ordered dynamic table", Queue_);
        }

        QueueObject_.ChunkCount = attributes->Get<i64>("chunk_count");
    }

    TQueueExportProgressOldPtr ValidateDestinationAndFetchProgress()
    {
        auto exportDirectoryAttributes = FetchNodeAttributes(
            ExportConfig_->ExportDirectory,
            {ExportDestinationAttributeName_, ExportProgressAttributeName_});

        auto destinationConfig = exportDirectoryAttributes->Get<TQueueStaticExportDestinationConfig>(ExportDestinationAttributeName_);
        if (destinationConfig.OriginatingQueueId != QueueObject_.ObjectId) {
            THROW_ERROR_EXCEPTION(
                "Destination config is not configured to accept exports from queue %v, configured id %v does not match queue id %v",
                Queue_,
                destinationConfig.OriginatingQueueId,
                QueueObject_.ObjectId);
        }

        auto currentExportProgress = exportDirectoryAttributes->Find<TQueueExportProgressOldPtr>(ExportProgressAttributeName_);
        if (currentExportProgress && currentExportProgress->LastExportedFragmentUnixTs >= ExportFragmentUnixTs_) {
            THROW_ERROR_EXCEPTION(
                "Fragment with unix ts %v is already exported, last exported fragment unix ts is %v",
                ExportFragmentUnixTs_,
                currentExportProgress->LastExportedFragmentUnixTs);
        }

        // COMPAT(apachee): There are exports without "queue_object_id" field set. We do not want to ignore export progress in this case.
        if (currentExportProgress && currentExportProgress->QueueObjectId == NullObjectId) {
            currentExportProgress->QueueObjectId = QueueObject_.ObjectId;
        }

        // NB(apachee): If export progress corresponds to different queue, then assume it's the first export in this directory.
        if (currentExportProgress && currentExportProgress->QueueObjectId == QueueObject_.ObjectId) {
            return currentExportProgress;
        }

        return New<TQueueExportProgressOld>();
    }

    TQueueExportProgressOldPtr SelectChunkSpecsToExport(const TQueueExportProgressOldPtr& currentExportProgress)
    {
        auto newExportProgress = CloneYsonStruct(currentExportProgress);

        std::map<i64, std::vector<const TChunkSpec*>> tabletToChunkSpecs;
        for (const auto& chunkSpec : ChunkSpecs_) {
            tabletToChunkSpecs[chunkSpec.tablet_index()].push_back(&chunkSpec);
        }

        for (const auto& [tabletIndex, chunkSpecs] : tabletToChunkSpecs) {
            auto lastExportedSpecIt = std::find_if(chunkSpecs.begin(), chunkSpecs.end(), [&, tabletIndex = tabletIndex] (auto* chunkSpec) {
                auto tabletProgressIt = currentExportProgress->Tablets.find(tabletIndex);
                return tabletProgressIt != currentExportProgress->Tablets.end() && tabletProgressIt->second->LastChunk == FromProto<TChunkId>(chunkSpec->chunk_id());
            });

            auto specToExportIt = (lastExportedSpecIt == chunkSpecs.end() ? chunkSpecs.begin() : (lastExportedSpecIt + 1));
            for (; specToExportIt != chunkSpecs.end(); ++specToExportIt) {
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

        newExportProgress->LastSuccessfulExportIterationInstant = ExportInstant_;
        if (!ChunkSpecsToExport_.empty()) {
            newExportProgress->LastExportedFramgentIterationInstant = ExportInstant_;
            newExportProgress->LastExportedFragmentUnixTs = ExportFragmentUnixTs_;
        }

        newExportProgress->QueueObjectId = QueueObject_.ObjectId;

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
            // XXX(achulkov2, gritukan): YT-11825
            /*chunkCount*/ -1);

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = chunkSpecFetcher->GetChunkSpecsOrderedNaturally();
        YT_LOG_DEBUG("Finished fetching chunk specs (Count: %v)", ChunkSpecs_.size());
    }

    TString GetOutputTableName(ui64 unixTs)
    {
        auto periodInSeconds = ExportConfig_->ExportPeriod.Seconds();

        if (!ExportConfig_->UseUpperBoundForTableNames) {
            unixTs -= periodInSeconds;
        }

        auto instant = TInstant::Seconds(unixTs);

        auto outputTableName = ExportConfig_->OutputTableNamePattern;

        std::vector<std::pair<TString, TString>> variables = {
            {"%UNIX_TS", ToString(unixTs)},
            {"%PERIOD", ToString(periodInSeconds)},
            {"%ISO", instant.ToStringUpToSeconds()},
        };

        // Replace all occurrences of variables with their values.
        for (const auto& [variable, value] : variables) {
            for (size_t position = 0; (position = outputTableName.find(variable, position)) != TString::npos; ) {
                outputTableName.replace(position, variable.length(), value);
            }
        }

        return instant.FormatGmTime(outputTableName.c_str());
    }

    void CreateOutputTable()
    {
        auto destinationPath = Format(
            "%s/%v",
            ExportConfig_->ExportDirectory,
            GetOutputTableName(ExportFragmentUnixTs_));
        DestinationObject_ = TUserObject(destinationPath, Options_.TransactionId);

        TCreateNodeOptions createOptions;
        createOptions.TransactionId = Options_.TransactionId;
        createOptions.Attributes = CreateEphemeralAttributes();
        if (ExportConfig_->ExportTtl) {
            createOptions.Attributes->Set("expiration_time", ExportInstant_ + ExportConfig_->ExportTtl);
        }
        WaitFor(Client_->CreateNode(DestinationObject_.GetPath(), EObjectType::Table, createOptions))
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Created output node for export (DestinationPath: %v, OutputTableNamePattern: %v, UseUpperBoundForTableNames: %v, ExportTtl: %v, ExportFragmentUnixTs: %v)",
            DestinationObject_.GetPath(),
            ExportConfig_->OutputTableNamePattern,
            ExportConfig_->UseUpperBoundForTableNames,
            ExportConfig_->ExportTtl,
            ExportFragmentUnixTs_);

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
        req->set_update_mode(ToProto(EUpdateMode::Overwrite));
        req->set_lock_mode(ToProto(ELockMode::Exclusive));
        if (CanUseSchemaId()) {
            ToProto(req->mutable_table_schema_id(), QueueSchemaId_);
        } else {
            ToProto(req->mutable_table_schema(), QueueSchema_);
        }
        req->set_schema_mode(ToProto(ETableSchemaMode::Strong));

        req->set_upload_transaction_title(Format(
            "Exporting queue %v to static table %v",
            Queue_,
            DestinationObject_.GetPath()));

        auto cellTags = GetAffectedCellTags(
            ChunkSpecsToExport_,
            DestinationObject_,
            /*cellTagToExclude*/ destinationObjectCellTag);
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(
            ToProto(Connection_->GetConfig()->UploadTransactionTimeout));
        GenerateMutationId(req);

        SetTransactionId(req, Options_.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error starting upload to %v", Queue_);

        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

        auto attachOptions = TTransactionAttachOptions{};
        attachOptions.AutoAbort = true;
        attachOptions.PingPeriod = Connection_->GetConfig()->UploadTransactionPingPeriod;
        attachOptions.Ping = true;
        attachOptions.PingAncestors = true;
        UploadTransaction_ = Client_->GetTransactionManager()->Attach(
            uploadTransactionId,
            attachOptions);

        YT_LOG_DEBUG(
            "Started upload transaction for queue export (Destination: %v, UploadTransactionId: %v, OutputTableSchemaId: %v)",
            DestinationObject_.GetPath(),
            UploadTransaction_->GetId(),
            QueueSchemaId_);
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
            EMasterChannelKind::Leader,
            DestinationObject_.ExternalCellTag));

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
        // COMPAT(h0pless): remove this when all masters are 24.2.
        req->set_schema_mode(ToProto(ETableSchemaMode::Strong));
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

    TProgressDiffOld UpdateCypressExportProgress(
        const TQueueExportProgressOldPtr& currentExportProgress,
        const TQueueExportProgressOldPtr& newExportProgress)
    {
        TSetNodeOptions options;
        options.TransactionId = Options_.TransactionId;
        WaitFor(Client_->SetNode(
            Format("%v/@%v", ExportConfig_->ExportDirectory, ExportProgressAttributeName_),
            ConvertToYsonString(newExportProgress),
            options))
            .ThrowOnError();


        TProgressDiffOld diff{currentExportProgress, newExportProgress};
        YT_LOG_DEBUG("Updated export progress (ExportedRows: %v, ExportedChunks: %v)", diff.RowCount, diff.ChunkCount);
        return diff;
    }

    bool CanUseSchemaId() const
    {
        return CellTagFromId(QueueObject_.ObjectId) == CellTagFromId(DestinationObject_.ObjectId);
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueExportTaskOld)

////////////////////////////////////////////////////////////////////////////////

TQueueExportProfilingCountersOld::TQueueExportProfilingCountersOld(const TProfiler& profiler)
    : ExportedRows(profiler.Counter("/exported_rows"))
    , ExportedChunks(profiler.Counter("/exported_chunks"))
    , ExportedTables(profiler.Counter("/exported_tables"))
{ }

////////////////////////////////////////////////////////////////////////////////

TQueueExporterOld::TQueueExporterOld(
    TString exportName,
    TCrossClusterReference queue,
    const TQueueStaticExportConfigPtr& exportConfig,
    const TQueueExporterDynamicConfig& dynamicConfig,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker,
    IAlertCollectorPtr alertCollector,
    const TProfiler& queueProfiler,
    const TLogger& logger)
    : ExportName_(std::move(exportName))
    , Queue_(std::move(queue))
    , ExportConfig_(exportConfig)
    , DynamicConfig_(dynamicConfig)
    , ExportProgress_(New<TQueueExportProgressOld>())
    , ClientDirectory_(std::move(clientDirectory))
    , Invoker_(std::move(invoker))
    , AlertCollector_(std::move(alertCollector))
    , ProfilingCounters_(New<TQueueExportProfilingCountersOld>(queueProfiler.WithPrefix("/static_export").WithTag("export_name", ExportName_)))
    , Executor_(New<TScheduledExecutor>(
            Invoker_,
            BIND_NO_PROPAGATE(&TQueueExporterOld::Export, MakeWeak(this)),
            /*interval*/ std::nullopt
        ))
    , Logger(QueueStaticTableExporterLogger().WithTag("%v, ExportName: %v",
        logger.GetTag(),
        ExportName_))
{
    Executor_->Start();
    Executor_->SetInterval(DynamicConfig_.Enable
        ? std::optional(ExportConfig_->ExportPeriod)
        : std::nullopt);
}

TQueueExportProgressOldPtr TQueueExporterOld::GetExportProgressOld() const
{
    auto guard = Guard(Lock_);
    return ExportProgress_;
}

NQueueClient::TQueueStaticExportConfigPtr TQueueExporterOld::GetConfig()
{
    auto guard = Guard(Lock_);
    return ExportConfig_;
}

void TQueueExporterOld::Export()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    // XXX(apachee): Rename this and TQueueExporterOld to QueueStaticTableExporter and TQueueStaticTableExporter respectively?
    auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueExporterIteration"));

    try {
        GuardedExport();
    } catch (const std::exception& ex) {
        AlertCollector_->StageAlert(CreateAlert(
            NAlerts::EErrorCode::QueueAgentQueueControllerStaticExportFailed,
            "Failed to perform static export for queue",
            /*tags*/ {{"export_name", ExportName_}},
            ex));
    }

    AlertCollector_->PublishAlerts();
}

void TQueueExporterOld::GuardedExport()
{
    auto config = GetConfig();

    auto exportTask = New<TQueueExportTaskOld>(
        ClientDirectory_->GetClientOrThrow(Queue_.Cluster),
        ProfilingCounters_,
        Queue_.Path,
        config,
        Logger);

    auto nextExportProgress = exportTask->Run();

    auto guard = Guard(Lock_);

    if (config->ExportDirectory == ExportConfig_->ExportDirectory) {
        ExportProgress_ = std::move(nextExportProgress);
    }
}

TQueueExportProgressPtr TQueueExporterOld::GetExportProgress() const
{
    auto exportProgressOld = GetExportProgressOld();
    return ConvertTo<TQueueExportProgressPtr>(exportProgressOld);
}

void TQueueExporterOld::OnExportConfigChanged(const NQueueClient::TQueueStaticExportConfigPtr& newExportConfig)
{
    auto guard = Guard(Lock_);

    if (ExportConfig_->ExportPeriod != newExportConfig->ExportPeriod) {
        Executor_->SetInterval(DynamicConfig_.Enable
            ? std::optional(newExportConfig->ExportPeriod)
            : std::nullopt);
    }

    if (ExportConfig_->ExportDirectory != newExportConfig->ExportDirectory) {
        ExportProgress_ = New<TQueueExportProgressOld>();
    }

    ExportConfig_ = newExportConfig;
}

void TQueueExporterOld::OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig)
{
    auto guard = Guard(Lock_);

    if (DynamicConfig_.Enable != newDynamicConfig.Enable) {
        Executor_->SetInterval(newDynamicConfig.Enable
            ? std::optional(ExportConfig_->ExportPeriod)
            : std::nullopt);
    }

    DynamicConfig_ = newDynamicConfig;
}

void TQueueExporterOld::Stop()
{
    AlertCollector_->Stop();
    YT_UNUSED_FUTURE(Executor_->Stop());
}

void TQueueExporterOld::BuildOrchidYson(NYTree::TFluentAny fluent) const
{
    fluent
        .BeginAttributes()
            .Item("queue_exporter_implementation_type").Value(GetImplementationType())
        .EndAttributes()
        .Entity();
}

EQueueExporterImplementation TQueueExporterOld::GetImplementationType() const
{
    return EQueueExporterImplementation::Old;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
