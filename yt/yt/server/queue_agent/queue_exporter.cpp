#include "queue_exporter.h"

#include "private.h"
#include "queue_export_manager.h"

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

#include <util/generic/map.h>

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

namespace {

////////////////////////////////////////////////////////////////////////////////

// The following terminology is used for queue static table exports.
//
// _Exporter_ launches _export tasks_, which export queue data into _exported tables_, and each _exported table_ corresponds to distinct _export (unix) timestamp_.
// The term _export_ itself refers to the described process.
//
// The following convention is used for export unix timestamps.
//
// Queue rows are divided in ranges. Each range
// is described by unix timestamp range [period * k; period * (k + 1)).
// For a fixed period, right bound is used to describe this unix timestamp range and is called export unix timestamp, e.g. export unix timestamp T corresponds
// to range [T - period; T).
//
// Row belongs to the range with export unix timestamp T, iff unix timestamp from its chunk max timestamp
// lies in [T - period; T) or, in case there already is an exported table with such export unix timestamp T, first vacant export unix timestamp.
// The latter can happen if row was flushed too late to be a part of exported table with export unix timestamp T.
//
// The following convention is used for handling time.
//
// Both TInstant and Unix Time measure time since epoch (timezone is UTC).
// TInstant has granularity in milliseconds and we convert it to Unix Time by discarding milliseconds.
// Unix timestamp T corresponds to TInstant::Seconds(T).
// Most of calculations are done with unix time, since we chose our granularity to be in seconds.

//! Find export unix ts range containing #unixTs for a given #exportPeriod.
//!
//! Returns [#beginTs; endTs), where endTs is also export unix ts describing the range.
//! Alternatively, beginTs is export unix ts that comes before endTs.
std::pair<ui64, ui64> GetExportUnixTsRange(ui64 unixTs, TDuration exportPeriod)
{
    auto period = exportPeriod.Seconds();
    YT_VERIFY(period > 0);

    std::pair<ui64, ui64> result;
    result.first = (unixTs / period) * period;
    result.second = result.first + period;
    return result;
}

//! The greatest export unix ts lower than #now according to #exportPeriod.
ui64 GetExportUnixTsUpperBound(TInstant now, TDuration exportPeriod)
{
    return GetExportUnixTsRange(now.Seconds(), exportPeriod).first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TQueueTabletExportProgress::Register(TRegistrar registrar)
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

void TQueueExportProgress::Update(i64 tabletIndex, TChunkId chunkId, TTimestamp maxTimestamp, i64 rowCount)
{
    auto tabletProgressIt = Tablets.find(tabletIndex);
    if (tabletProgressIt == Tablets.end()) {
        tabletProgressIt = Tablets.emplace(tabletIndex, New<TQueueTabletExportProgress>()).first;
    }

    tabletProgressIt->second->LastChunk = chunkId;
    ++tabletProgressIt->second->ChunkCount;
    tabletProgressIt->second->MaxTimestamp = std::max(tabletProgressIt->second->MaxTimestamp, maxTimestamp);
    tabletProgressIt->second->RowCount = rowCount;
}

void TQueueExportProgress::Register(TRegistrar registrar)
{
    registrar.Parameter("last_successful_export_task_instant", &TThis::LastSuccessfulExportTaskInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_export_task_instant", &TThis::LastExportTaskInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_successful_export_iteration_instant", &TThis::LastSuccessfulExportIterationInstant)
        .Default(TInstant::Zero());
    registrar.Parameter("last_export_unix_ts", &TThis::LastExportUnixTs)
        .Default(0)
        .Alias("last_exported_fragment_unix_ts");
    registrar.Parameter("tablets", &TThis::Tablets)
        .Default();
    registrar.Parameter("queue_object_id", &TThis::QueueObjectId)
        .Default(NullObjectId);
}

////////////////////////////////////////////////////////////////////////////////

struct TProgressDiff
{
    i64 RowCount = 0;
    i64 ChunkCount = 0;

    TProgressDiff(const TQueueExportProgressPtr& currentProgress, const TQueueExportProgressPtr& newProgress)
    {
        for (const auto& [tabletIndex, newTabletProgress] : newProgress->Tablets) {
            auto currentTabletProgress = currentProgress->Tablets.Value(tabletIndex, New<TQueueTabletExportProgress>());
            RowCount += newTabletProgress->RowCount - currentTabletProgress->RowCount;
            ChunkCount += newTabletProgress->ChunkCount - currentTabletProgress->ChunkCount;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueueExportTask
    : public TRefCounted
{
public:
    TQueueExportTask(
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        TYPath queue,
        TQueueStaticExportConfigPtr exportConfig,
        TQueueExporterDynamicConfig dynamicConfig,
        TQueueExportProfilingCountersPtr profilingCounters,
        const TLogger& logger)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Connection_(std::move(Client_->GetNativeConnection()))
        , ProfilingCounters_(std::move(profilingCounters))
        , Queue_(std::move(queue))
        , ExportConfig_(std::move(exportConfig))
        , DynamicConfig_(std::move(dynamicConfig))
        , Logger(logger.WithTag(
            "ExportDirectory: %v, ExportPeriod: %v",
            ExportConfig_->ExportDirectory,
            ExportConfig_->ExportPeriod))
    { }

    TFuture<void> Run()
    {
        return BIND(&TQueueExportTask::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TError GetExportError() const
    {
        return ExportError_;
    }

    TQueueExportProgressPtr GetExportProgress() const
    {
        return QueueExportProgress_;
    }

private:
    const NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const NNative::IConnectionPtr Connection_;
    const TQueueExportProfilingCountersPtr ProfilingCounters_;

    const TYPath Queue_;
    const TQueueStaticExportConfigPtr ExportConfig_;
    const TQueueExporterDynamicConfig DynamicConfig_;

    const TLogger Logger;

    //! Options used for Cypress requests.
    TTransactionalOptions Options_;

    //! Instant of current export task.
    TInstant TaskInstant_;

    //! Upper bound on export unix ts of exported tables created in this export task.
    //! NB: We use the instant above to compute this value, i.e. it corresponds to the host's physical time.
    //! This is fine, see the comment for Run above.
    ui64 ExportUnixTsUpperBound_;
    //! Corresponds to the queue being exported.
    TUserObject QueueObject_;
    TTableSchemaPtr QueueSchema_;
    TMasterTableSchemaId QueueSchemaId_;
    //! Original chunk specs fetched from master.
    std::vector<TChunkSpec> ChunkSpecs_;
    //! Grouping of chunk specs by their corresponding export unix ts.
    TMap<ui64, std::vector<const TChunkSpec*>> ChunkSpecsToExportByUnixTs_;
    // Export task part error.
    TError ExportError_;
    //! Most recent export progress.
    // Corresponds to export progress currently stored in export directory attribute.
    TQueueExportProgressPtr QueueExportProgress_;

    static constexpr TStringBuf ExportProgressAttributeName_ = "queue_static_export_progress";
    static constexpr TStringBuf ExportDestinationAttributeName_ = "queue_static_export_destination";
    static constexpr TStringBuf ExporterAttributeName_ = "queue_static_exporter";

    struct TTaskPart final
    {
        //! Export unix ts of exported table to be created.
        const ui64 ExportUnixTs;

        //! Chunk specs corresponding to exported table to be created.
        const std::vector<const TChunkSpec*>& ChunkSpecsToExport;

        //! Options used for Cypress requests.
        TTransactionalOptions TransactionOptions;
        //! Corresponds to the actual output table created.
        TUserObject DestinationObject;
        TTransactionPtr UploadTransaction;

        //! Data statistics collected from attaching chunks.
        TDataStatistics DataStatistics;

    };

    std::vector<TTaskPart> TaskParts_;

    //! Performs the following steps:
    //!   1) Starts transaction, obtain locks on input queue (snapshot) and <export_directory>/@queue_static_exporter attribute (shared).
    //!   2) Determines export unix ts upper bound as the most recent exported table that can be created according to the current physical time.
    //!   3) Fetches chunk specs, skipping dynamic stores, already exported chunks and chunks with timestamp larger than
    //!      the export unix timestamp upper bound.
    //!   4) Groups together chunk specs by export unix ts corresponding to their export unix ts.
    //!   5) Creates exported table for each chunk spec group in nested transaction, stopping at the first failed, creating at most MaxExportedTableCountPerTask tables.
    //!
    //! NB: We use the host's physical time to compute the unix ts of the next table to export.
    //! We rely on this time being mostly monotonous and not too different from the cluster time obtained via timestamp generation.
    //! In any case, we will only produce an output table if its unix ts is strictly greater than the one of the last exported table.
    //! If the physical time diverges from the cluster time, the only effect is that some chunks might end up being
    //! exported into later tables, which is perfectly fine.
    void DoRun()
    {
        YT_LOG_INFO("Started export task");
        auto logFinally = Finally([&] {
            YT_LOG_INFO("Finished export task");
        });

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        auto transactionId = transaction->GetId();
        auto queueObjectId = WaitFor(transaction->LockNode(Queue_, ELockMode::Snapshot))
            .ValueOrThrow()
            .NodeId;

        // We take a shared lock with some fixed fictional attribute key (#ExporterAttributeName_),
        // which acts as exclusive lock for this export directory across all queue agent instances.
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

        TaskInstant_ = TInstant::Now();
        ExportUnixTsUpperBound_ = GetExportUnixTsUpperBound(TaskInstant_, ExportConfig_->ExportPeriod);

        QueueObject_ = TUserObject(FromObjectId(queueObjectId), transactionId);

        PrepareQueueForExport();
        auto currentExportProgress = ValidateDestinationAndFetchProgress();
        QueueExportProgress_ = currentExportProgress;

        FetchChunkSpecs();
        SelectChunkSpecsToExport(currentExportProgress);
        PrepareTaskParts();

        int exportedTableCount = 0;
        auto newExportProgress = CloneYsonStruct(currentExportProgress);
        newExportProgress->QueueObjectId = QueueObject_.ObjectId;

        if (TaskParts_.empty()) {
            // NB(apachee): New export progress is taking into account if there are chunks
            // to export, meaning in this case only last successful export task instant would
            // be changed.
            YT_LOG_DEBUG("No chunks to export, committing export transaction prematurely (TransactionId: %v)",
                transactionId);
        } else {
            for (auto& taskPart : TaskParts_) {
                auto taskPartError = RunTaskPart(taskPart, transaction);
                if (!taskPartError.IsOK()) {
                    YT_LOG_ERROR(taskPartError);
                    ExportError_ = taskPartError;
                    break;
                }

                AdvanceExportProgress(taskPart, newExportProgress);

                ++exportedTableCount;
            }
        }

        int skippedTableCount = std::ssize(TaskParts_) - exportedTableCount;

        newExportProgress->LastExportTaskInstant = TaskInstant_;
        if (skippedTableCount == 0) {
            newExportProgress->LastSuccessfulExportTaskInstant = TaskInstant_;
            newExportProgress->LastSuccessfulExportIterationInstant = TaskInstant_;
        } else {
            YT_VERIFY(!ExportError_.IsOK());
        }

        auto diff = UpdateCypressExportProgress(currentExportProgress, newExportProgress);

        auto commitResultOrError = WaitFor(transaction->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            commitResultOrError,
            "Error committing main export task transaction for queue %v",
            Queue_);

        QueueExportProgress_ = newExportProgress;

        YT_LOG_DEBUG("Finished creating exported tables (PreparedTableCount: %v, ExportedTableCount: %v, SkippedTableCount: %v)",
            TaskParts_.size(),
            exportedTableCount,
            skippedTableCount);

        ProfilingCounters_->ExportedRows.Increment(diff.RowCount);
        ProfilingCounters_->ExportedChunks.Increment(diff.ChunkCount);
        ProfilingCounters_->ExportedTables.Increment(exportedTableCount);
        ProfilingCounters_->SkippedTables.Increment(skippedTableCount);
    }

    void PrepareTaskParts()
    {
        TaskParts_.reserve(ChunkSpecsToExportByUnixTs_.size());

        int preparedTableCount = 0;

        for (const auto& [exportUnixTs, chunkSpecs] : ChunkSpecsToExportByUnixTs_) {
            if (preparedTableCount >= DynamicConfig_.MaxExportedTableCountPerTask) {
                break;
            }

            TaskParts_.push_back(TTaskPart{
                .ExportUnixTs = exportUnixTs,
                .ChunkSpecsToExport = chunkSpecs,
            });

            ++preparedTableCount;
        }

        YT_LOG_DEBUG("Prepared for creating exported tables (TableCount: %v, AvailableTableCount: %v, MaxExportedTableCountPerTask: %v)",
            preparedTableCount,
            ChunkSpecsToExportByUnixTs_.size(),
            DynamicConfig_.MaxExportedTableCountPerTask);
    }

    TError RunTaskPart(TTaskPart& taskPart, const ITransactionPtr& parentTransaction)
    {
        if (taskPart.ChunkSpecsToExport.empty()) {
            return TError();
        }

        try {
            GuardedRunTaskPart(taskPart, parentTransaction);
        } catch (const std::exception& ex) {
            return TError("Queue export task part failed")
                << ex
                << TErrorAttribute("export_unix_ts", taskPart.ExportUnixTs);
        }
        return TError();
    }

    void GuardedRunTaskPart(TTaskPart& taskPart, const ITransactionPtr& parentTransaction)
    {
        auto taskPartTransaction = WaitFor(parentTransaction->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        taskPart.TransactionOptions.TransactionId = taskPartTransaction->GetId();

        CreateOutputTable(taskPart);
        BeginUpload(taskPart);
        TeleportChunkMeta(taskPart);
        AttachChunks(taskPart);
        EndUpload(taskPart);

        WaitFor(taskPartTransaction->Commit())
            .ThrowOnError();
    }

    //! Advances #progress in-place by taking into account chunks from task part.
    void AdvanceExportProgress(const TTaskPart& taskPart, TQueueExportProgressPtr& progress) const
    {
        for (const auto chunkSpec : taskPart.ChunkSpecsToExport) {
            TInputChunkPtr chunk = New<TInputChunk>(*chunkSpec);
            // NB: This is guaranteed by setting omit_dynamic_stores(true) while fetching.
            YT_VERIFY(!chunk->IsDynamicStore());

            auto chunkFormat = FromProto<EChunkFormat>(chunkSpec->chunk_meta().format());
            ValidateTableChunkFormatVersioned(chunkFormat, /*versioned*/ false);

            auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec->chunk_meta().extensions());
            auto maxTimestamp = FromProto<TTimestamp>(miscExt.max_timestamp());

            progress->Update(
                chunkSpec->tablet_index(),
                chunk->GetChunkId(),
                maxTimestamp,
                chunkSpec->table_row_index() + miscExt.row_count());
        }

        progress->LastExportUnixTs = taskPart.ExportUnixTs;
    }

    ui64 GetMinExportUnixTs(TTimestamp timestamp)
    {
        // NB: The timestamp is in range [unixTs, unixTs + 1). Since our granularity is in seconds, we can compute the
        // next export unix ts as the strict next tick for the lower bound.
        return GetExportUnixTsRange(UnixTimeFromTimestamp(timestamp), ExportConfig_->ExportPeriod).second;
    }

    static void GetAndFillBasicAttributes(
        const TLogger& logger,
        const NNative::IClientPtr& client,
        TUserObject& object,
        bool populateSecurityTags)
    {
        const auto& Logger = logger;

        YT_LOG_DEBUG("Started collecting basic attributes");

        auto proxy = CreateObjectServiceReadProxy(client, TMasterReadOptions().ReadFrom);
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
        GetAndFillBasicAttributes(Logger, Client_, QueueObject_, /*populateSecurityTags*/ true);
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

    TQueueExportProgressPtr ValidateDestinationAndFetchProgress()
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

        auto currentExportProgress = exportDirectoryAttributes->Find<TQueueExportProgressPtr>(ExportProgressAttributeName_);
        if (currentExportProgress && currentExportProgress->LastExportUnixTs >= ExportUnixTsUpperBound_) {
            THROW_ERROR_EXCEPTION(
                "Rows corresponding to export unix ts %v have already been exported as last export unix ts is %v",
                ExportUnixTsUpperBound_,
                currentExportProgress->LastExportUnixTs);
        }

        // COMPAT(apachee): There are exports without "queue_object_id" field set. We do not want to ignore export progress in this case.
        if (currentExportProgress && currentExportProgress->QueueObjectId == NullObjectId) {
            currentExportProgress->QueueObjectId = QueueObject_.ObjectId;
        }

        // NB(apachee): If export progress corresponds to different queue, then assume it's the first export in this directory.
        if (currentExportProgress && currentExportProgress->QueueObjectId == QueueObject_.ObjectId) {
            return currentExportProgress;
        }

        return New<TQueueExportProgress>();
    }

    void SelectChunkSpecsToExport(const TQueueExportProgressPtr& currentExportProgress)
    {
        std::map<i64, std::vector<const TChunkSpec*>> tabletToChunkSpecs;
        for (const auto& chunkSpec : ChunkSpecs_) {
            tabletToChunkSpecs[chunkSpec.tablet_index()].push_back(&chunkSpec);
        }

        // NB(apachee): It is possible that rows corresponding to the last exported table were flushed late, in which case we export those rows to the next exported table.
        auto initialAccumulatedMinExportUnixTs = GetExportUnixTsRange(currentExportProgress->LastExportUnixTs, ExportConfig_->ExportPeriod).second;

        for (const auto& [tabletIndex, chunkSpecs] : tabletToChunkSpecs) {
            auto lastExportedSpecIt = std::find_if(chunkSpecs.begin(), chunkSpecs.end(), [&, tabletIndex = tabletIndex] (auto* chunkSpec) {
                auto tabletProgressIt = currentExportProgress->Tablets.find(tabletIndex);
                return tabletProgressIt != currentExportProgress->Tablets.end() && tabletProgressIt->second->LastChunk == FromProto<TChunkId>(chunkSpec->chunk_id());
            });

            // NB(apachee): Monotonically increasing export unix ts to provide intra-tablet chunk order.
            // It is required in case of weak commit ordering, as in this case latter chunks might have smaller max timestamps.
            ui64 accumulatedMinExportUnixTs = initialAccumulatedMinExportUnixTs;

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
                accumulatedMinExportUnixTs = std::max(accumulatedMinExportUnixTs, GetMinExportUnixTs(maxTimestamp));
                // We only export chunks which are compatible with the export unix ts upper bound.
                if (accumulatedMinExportUnixTs > ExportUnixTsUpperBound_) {
                    // NB: Latter chunks might have smaller max timestamps in case of weak commit ordering, but we do
                    // not export any of them to maintain intra-tablet chunk order within the exported tables.
                    break;
                }

                ChunkSpecsToExportByUnixTs_[accumulatedMinExportUnixTs].push_back(chunkSpec);
            }
        }
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

    void CreateOutputTable(TTaskPart& taskPart)
    {
        auto destinationPath = Format(
            "%s/%v",
            ExportConfig_->ExportDirectory,
            GetOutputTableName(taskPart.ExportUnixTs));
        taskPart.DestinationObject = TUserObject(destinationPath, taskPart.TransactionOptions.TransactionId);

        TCreateNodeOptions createOptions;
        createOptions.TransactionId = taskPart.TransactionOptions.TransactionId;
        createOptions.Attributes = CreateEphemeralAttributes();
        if (ExportConfig_->ExportTtl) {
            // NB(apachee): We use task part export ts to calculate expiration time for easier #ExportTtl logic. This may potentially
            // lead to exported table being deleted right away leading to some overhead for queue agent and master, but this should
            // rarely be the case, so we ignore it at this moment.
            // NB(apachee): For TInstant <-> Unix Time conversion clarification see the comment at the top.
            // TODO(apachee): Implement exported table skipping mechanics to further reduce queue agent and master load.
            createOptions.Attributes->Set("expiration_time", TInstant::Seconds(taskPart.ExportUnixTs) + ExportConfig_->ExportTtl);
        }
        WaitFor(Client_->CreateNode(taskPart.DestinationObject.GetPath(), EObjectType::Table, createOptions))
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Created output node for export (DestinationPath: %v, OutputTableNamePattern: %v, UseUpperBoundForTableNames: %v, ExportTtl: %v, ExportUnixTs: %v)",
            taskPart.DestinationObject.GetPath(),
            ExportConfig_->OutputTableNamePattern,
            ExportConfig_->UseUpperBoundForTableNames,
            ExportConfig_->ExportTtl,
            taskPart.ExportUnixTs);

        GetAndFillBasicAttributes(Logger, Client_, taskPart.DestinationObject, /*populateSecurityTags*/ false);
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

    void BeginUpload(TTaskPart& taskPart)
    {
        auto destinationObjectCellTag = CellTagFromId(taskPart.DestinationObject.ObjectId);
        auto proxy = CreateObjectServiceWriteProxy(Client_, destinationObjectCellTag);

        auto req = TChunkOwnerYPathProxy::BeginUpload(taskPart.DestinationObject.GetObjectIdPath());
        req->set_update_mode(ToProto<int>(EUpdateMode::Overwrite));
        req->set_lock_mode(ToProto<int>(ELockMode::Exclusive));
        if (CanUseSchemaId(taskPart)) {
            ToProto(req->mutable_table_schema_id(), QueueSchemaId_);
        } else {
            ToProto(req->mutable_table_schema(), QueueSchema_);
        }
        req->set_schema_mode(ToProto<int>(ETableSchemaMode::Strong));

        req->set_upload_transaction_title(Format(
            "Exporting queue %v to static table %v",
            Queue_,
            taskPart.DestinationObject.GetPath()));

        auto cellTags = GetAffectedCellTags(
            taskPart.ChunkSpecsToExport,
            taskPart.DestinationObject,
            /*cellTagToExclude*/ destinationObjectCellTag);
        ToProto(req->mutable_upload_transaction_secondary_cell_tags(), cellTags);
        req->set_upload_transaction_timeout(
            ToProto<i64>(Connection_->GetConfig()->UploadTransactionTimeout));
        GenerateMutationId(req);

        SetTransactionId(req, taskPart.TransactionOptions.TransactionId);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error starting upload to %v", Queue_);

        const auto& rsp = rspOrError.Value();

        auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

        taskPart.UploadTransaction = Client_->GetTransactionManager()->Attach(
            uploadTransactionId,
            TTransactionAttachOptions{
                .AutoAbort = true,
                .Ping = true,
                .PingAncestors = true,
            });

        YT_LOG_DEBUG(
            "Started upload transaction for queue export (Destination: %v, UploadTransactionId: %v, OutputTableSchemaId: %v)",
            taskPart.DestinationObject.GetPath(),
            taskPart.UploadTransaction->GetId(),
            QueueSchemaId_);
    }

    void TeleportChunkMeta(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto teleporter = New<TChunkTeleporter>(
            Client_->GetNativeConnection()->GetConfig(),
            Client_,
            Client_->GetNativeConnection()->GetInvoker(),
            taskPart.UploadTransaction->GetId(),
            Logger);

        for (const auto* chunkSpec : taskPart.ChunkSpecsToExport) {
            teleporter->RegisterChunk(FromProto<TChunkId>(chunkSpec->chunk_id()), taskPart.DestinationObject.ExternalCellTag);
        }
        WaitFor(teleporter->Run())
            .ThrowOnError();
    }

    TChunkListId GetChunkListId(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto proxy = CreateObjectServiceWriteProxy(Client_, taskPart.DestinationObject.ExternalCellTag);
        auto req = TChunkOwnerYPathProxy::GetUploadParams(taskPart.DestinationObject.GetObjectIdPath());
        SetTransactionId(req, taskPart.UploadTransaction->GetId());

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error requesting upload parameters for %v", taskPart.DestinationObject.GetPath());

        const auto& rsp = rspOrError.Value();
        return FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    void AttachChunks(TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        YT_LOG_DEBUG(
            "Started chunk upload (Destination: %v, UploadTransactionId: %v, ChunkCount: %v)",
            taskPart.DestinationObject.GetPath(),
            taskPart.UploadTransaction->GetId(),
            taskPart.ChunkSpecsToExport.size());

        TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            taskPart.DestinationObject.ExternalCellTag));

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        SetTransactionId(batchReq, taskPart.UploadTransaction->GetId());
        SetSuppressUpstreamSync(&batchReq->Header(), true);

        auto chunkListId = GetChunkListId(taskPart);

        auto req = batchReq->add_attach_chunk_trees_subrequests();
        ToProto(req->mutable_parent_id(), chunkListId);

        for (const auto* chunkSpec : taskPart.ChunkSpecsToExport) {
            *req->add_child_ids() = chunkSpec->chunk_id();
        }
        req->set_request_statistics(true);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error attaching chunks to %v",
            taskPart.DestinationObject.GetPath());

        const auto& batchRsp = batchRspOrError.Value();

        const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);

        taskPart.DataStatistics = rsp.statistics();

        YT_LOG_DEBUG(
            "Finished chunk upload (Destination: %v, UploadTransactionId: %v, ChunkCount: %v)",
            taskPart.DestinationObject.GetPath(),
            taskPart.UploadTransaction->GetId(),
            taskPart.ChunkSpecsToExport.size());
    }

    void EndUpload(const TTaskPart& taskPart)
    {
        YT_VERIFY(taskPart.UploadTransaction);

        auto proxy = CreateObjectServiceWriteProxy(Client_, CellTagFromId(taskPart.DestinationObject.ObjectId));

        auto req = TChunkOwnerYPathProxy::EndUpload(taskPart.DestinationObject.GetObjectIdPath());
        // COMPAT(h0pless): remove this when all masters are 24.2.
        req->set_schema_mode(ToProto<int>(ETableSchemaMode::Strong));
        *req->mutable_statistics() = taskPart.DataStatistics;

        std::vector<TSecurityTag> inferredSecurityTags;
        inferredSecurityTags.insert(
            inferredSecurityTags.end(),
            QueueObject_.SecurityTags.begin(),
            QueueObject_.SecurityTags.end());
        SortUnique(inferredSecurityTags);

        auto securityTags = taskPart.DestinationObject.Path.GetSecurityTags().value_or(inferredSecurityTags);

        ToProto(req->mutable_security_tags()->mutable_items(), securityTags);
        SetTransactionId(req, taskPart.UploadTransaction->GetId());
        GenerateMutationId(req);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, "Error ending upload to %v", taskPart.DestinationObject.GetPath());

        taskPart.UploadTransaction->Detach();
    }

    TProgressDiff UpdateCypressExportProgress(
        const TQueueExportProgressPtr& currentExportProgress,
        const TQueueExportProgressPtr& newExportProgress)
    {
        YT_VERIFY(newExportProgress->QueueObjectId == QueueObject_.ObjectId);

        TSetNodeOptions options;
        options.TransactionId = Options_.TransactionId;
        WaitFor(Client_->SetNode(
            Format("%v/@%v", ExportConfig_->ExportDirectory, ExportProgressAttributeName_),
            ConvertToYsonString(newExportProgress),
            options))
            .ThrowOnError();


        TProgressDiff diff{currentExportProgress, newExportProgress};
        YT_LOG_DEBUG("Updated export progress (ExportedRows: %v, ExportedChunks: %v)", diff.RowCount, diff.ChunkCount);
        return diff;
    }

    bool CanUseSchemaId(const TTaskPart& taskPart) const
    {
        return CellTagFromId(QueueObject_.ObjectId) == CellTagFromId(taskPart.DestinationObject.ObjectId);
    }
};

using TQueueExportTaskPtr = TIntrusivePtr<TQueueExportTask>;
DEFINE_REFCOUNTED_TYPE(TQueueExportTask)

////////////////////////////////////////////////////////////////////////////////

TQueueExportProfilingCounters::TQueueExportProfilingCounters(const TProfiler& profiler)
    : ExportedRows(profiler.Counter("/exported_rows"))
    , ExportedChunks(profiler.Counter("/exported_chunks"))
    , ExportedTables(profiler.Counter("/exported_tables"))
    , SkippedTables(profiler.Counter("/skipped_tables"))
    , ExportTaskErrors(profiler.Counter("/export_task_errors"))
    , TimeLag(profiler.TimeGauge("/time_lag"))
    , TableLag(profiler.Gauge("/table_lag"))
{ }

////////////////////////////////////////////////////////////////////////////////

class TQueueExporter
    : public virtual IQueueExporter
{
public:
    TQueueExporter(
        TString exportName,
        TCrossClusterReference queue,
        TQueueStaticExportConfigPtr exportConfig,
        TQueueExporterDynamicConfig dynamicConfig,
        TClientDirectoryPtr clientDirectory,
        IInvokerPtr invoker,
        IQueueExportManagerPtr queueExportManager,
        IAlertCollectorPtr alertCollector,
        const TProfiler& queueProfiler,
        const TLogger& logger)
        : ExportConfig_(std::move(exportConfig))
        , DynamicConfig_(std::move(dynamicConfig))
        , ExportProgress_(New<TQueueExportProgress>())
        , RetryBackoff_(DynamicConfig_.RetryBackoff)
        , ExportName_(std::move(exportName))
        , Queue_(std::move(queue))
        , ClientDirectory_(std::move(clientDirectory))
        , Invoker_(std::move(invoker))
        , QueueExportManager_(std::move(queueExportManager))
        , AlertCollector_(std::move(alertCollector))
        , ProfilingCounters_(New<TQueueExportProfilingCounters>(queueProfiler.WithPrefix("/static_export").WithTag("export_name", ExportName_)))
        , Executor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND_NO_PROPAGATE(
                &TQueueExporter::Pass,
                MakeWeak(this)),
            DynamicConfig_.GetPeriodicExecutorOptions()))
        , Logger(QueueExporterLogger().WithTag("%v, ExportName: %v",
            logger.GetTag(),
            ExportName_))
    { }

    void Initialize() const
    {
        Executor_->Start();

        YT_LOG_INFO("Queue exporter started");
    }

    TQueueExportProgressPtr GetExportProgress() const override
    {
        auto guard = Guard(Lock_);
        return ExportProgress_;
    }

    void OnExportConfigChanged(const TQueueStaticExportConfigPtr& newExportConfig) override
    {
        auto guard = Guard(Lock_);

        if (ExportConfig_->ExportDirectory != newExportConfig->ExportDirectory) {
            ExportProgress_ = New<TQueueExportProgress>();

            // NB(apachee): Restart retries from the start, since new export directory
            // might be properly configured (or become so soon).
            RetryBackoff_.Restart();
        }

        ExportConfig_ = newExportConfig;
    }

    void OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig) override
    {
        auto guard = Guard(Lock_);

        if (DynamicConfig_.GetPeriodicExecutorOptions() != newDynamicConfig.GetPeriodicExecutorOptions()) {
            Executor_->SetOptions(DynamicConfig_.GetPeriodicExecutorOptions());
        }
        if (DynamicConfig_.RetryBackoff != newDynamicConfig.RetryBackoff) {
            RetryBackoff_.UpdateOptions(newDynamicConfig.RetryBackoff);
            RetryBackoff_.Restart();
        }

        DynamicConfig_ = newDynamicConfig;
    }

    void Stop() override
    {
        AlertCollector_->Stop();
    }

    void BuildOrchidYson(NYTree::TFluentAny fluent) const override
    {
        auto guard = Guard(Lock_);

        fluent
            .BeginAttributes()
                .Item("queue_exporter_implementation_type").Value(GetImplementationType())
            .EndAttributes()
            .BeginMap()
                .Item("export_config").Value(ExportConfig_)
                .Item("progress").Value(ExportProgress_)
                .Item("last_successful_export_unix_ts").Value(LastSuccessfulExportUnixTs_.load())
                .Item("export_task_invocation_index").Value(ExportTaskInvocationIndex_)
                .Item("export_task_invocation_instant").Value(ExportTaskInvocationInstant_)
                .Item("retry_index").Value(RetryBackoff_.GetInvocationIndex())
                .Item("retry_backoff_duration").Value(RetryBackoff_.GetBackoff())
            .EndMap();
    }

    EQueueExporterImplementation GetImplementationType() const override
    {
        return EQueueExporterImplementation::New;
    }

private:
    TSpinLock Lock_;
    TQueueStaticExportConfigPtr ExportConfig_;
    TQueueExporterDynamicConfig DynamicConfig_;
    TQueueExportProgressPtr ExportProgress_;
    //! Number of times export task was invoked regardless of its result.
    i64 ExportTaskInvocationIndex_ = 0;
    //! Instant of last export task invocation.
    TInstant ExportTaskInvocationInstant_;
    //! Export unix timestamp corresponding to the last successful export.
    TBackoffStrategy RetryBackoff_;

    std::atomic<ui64> LastSuccessfulExportUnixTs_ = 0;

    const TString ExportName_;
    const TCrossClusterReference Queue_;
    const TClientDirectoryPtr ClientDirectory_;
    const IInvokerPtr Invoker_;
    const IQueueExportManagerPtr QueueExportManager_;
    const IAlertCollectorPtr AlertCollector_;
    const TQueueExportProfilingCountersPtr ProfilingCounters_;
    const TPeriodicExecutorPtr Executor_;

    const TLogger Logger;

    //! Pass that is executed regularly to check whether an export can be started.
    //!
    //! Default period is a second, so by default between consecutive export tasks there is at least 1 second delay.
    //! Furthermore, all exports within one instance share the same throttler to limit number of exports started per second.
    //!
    //! In case export task failed with any error, we use backoff to artifically increase pass period.
    //! Period is increased exponentially until it reaches 5 minutes (by default), and is reset
    //! in case of a successful export task.
    void Pass()
    {
        auto traceContextGuard = TTraceContextGuard(TTraceContext::NewRoot("QueueExporterPass"));

        auto now = TInstant::Now();

        auto exportConfig = GetExportConfig();
        auto exportUnixTs = GetExportUnixTsRange(now.Seconds(), exportConfig->ExportPeriod).first;

        if (exportUnixTs <= LastSuccessfulExportUnixTs_.load()) {
            // Too early to run new export task.
            return;
        }

        TError passError;

        try {
            GuardedExport(exportConfig, exportUnixTs);
        } catch (const std::exception& ex) {
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueueAgentQueueControllerStaticExportFailed,
                "Failed to perform static export for queue",
                /*tags*/ {{"export_name", ExportName_}},
                ex));
            passError = ex;
        }

        AlertCollector_->PublishAlerts();

        ProfileExport(exportConfig, !passError.IsOK());

        TDuration backoffDuration;
        int retryIndex = 0;
        {
            auto guard = Guard(Lock_);
            if (passError.IsOK()) {
                RetryBackoff_.Restart();
                return;
            }
            RetryBackoff_.Next();

            backoffDuration = RetryBackoff_.GetBackoff();
            retryIndex = RetryBackoff_.GetInvocationIndex();
        }

        YT_LOG_INFO("Doing retry backoff (BackoffDuration: %v, RetryIndex: %v)",
            backoffDuration,
            retryIndex);

        // TODO(apachee): Think of a way to ignore misconfigured exports completely
        // instead of artificially increasing pass period using retry backoff.
        TDelayedExecutor::WaitForDuration(backoffDuration);
    }

    void GuardedExport(const TQueueStaticExportConfigPtr& exportConfig, ui64 exportUnixTs)
    {
        // NB(apachee): It is essential that we throttle before doing any requests to the master,
        // otherwise all exports would start sending requests to master without any throttling from our side.
        // TODO(apachee): Develop more sophisticated scheme for throttling exports.
        auto throttler = QueueExportManager_->GetExportThrottler();
        WaitFor(throttler->Throttle(1))
            .ThrowOnError();

        auto dynamicConfig = GetDynamicConfig();

        {
            auto guard = Guard(Lock_);
            ++ExportTaskInvocationIndex_;
            ExportTaskInvocationInstant_ = TInstant::Now();
        }

        TQueueExportTaskPtr exportTask = New<TQueueExportTask>(
            ClientDirectory_->GetClientOrThrow(Queue_.Cluster),
            Invoker_,
            Queue_.Path,
            exportConfig,
            std::move(dynamicConfig),
            ProfilingCounters_,
            Logger);

        auto exportTaskError = WaitFor(exportTask->Run());

        auto newExportProgress = exportTask->GetExportProgress();
        bool newExportProgressNonNull = static_cast<bool>(newExportProgress);

        {
            auto guard = Guard(Lock_);
            if (exportConfig->ExportDirectory == ExportConfig_->ExportDirectory && newExportProgress) {
                ExportProgress_ = std::move(newExportProgress);
                // NB(apachee): Since we calculate export unix ts in export task separately, the one in
                // export task might be greater, and that is why we need to update the value here.
                // Even in case of 1 min / 5 min exports this matters, since throttling happens after the first export unix ts
                // calculation and before the second one.
                exportUnixTs = std::max(exportUnixTs, ExportProgress_->LastExportUnixTs);
            }
            if (!exportTaskError.IsOK() || !exportTask->GetExportError().IsOK()) {
                THROW_ERROR_EXCEPTION("Export task has errors")
                    << TErrorAttribute("task_error", exportTaskError)
                    << TErrorAttribute("export_error", exportTask->GetExportError());
            }
            if (!newExportProgressNonNull) {
                THROW_ERROR_EXCEPTION("Export task result is missing new export progress without any errors");
            }
        }
        YT_VERIFY(exportTaskError.IsOK() && exportTask->GetExportError().IsOK());

        LastSuccessfulExportUnixTs_.store(exportUnixTs);
    }

    void ProfileExport(const TQueueStaticExportConfigPtr& exportConfig, bool hasError)
    {
        auto now = TInstant::Now();
        auto exportUnixTsUpperBound = GetExportUnixTsUpperBound(now, exportConfig->ExportPeriod);

        ui64 timeLagSeconds = 0;
        auto exportProgress = GetExportProgress();
        // TODO(apachee): Consider more elaborate ways to handle if this condition is not true
        if (exportProgress->LastExportUnixTs <= exportUnixTsUpperBound) {
            timeLagSeconds = exportUnixTsUpperBound - exportProgress->LastExportUnixTs;
        }

        // NB(apachee): Table lag is rounded up, since export period could've changed after the last successful export task.
        auto exportPeriodSeconds = exportConfig->ExportPeriod.Seconds();
        ui64 tableLag = (timeLagSeconds + exportPeriodSeconds - 1) / exportPeriodSeconds;

        if (hasError) {
            ProfilingCounters_->ExportTaskErrors.Increment();
        }
        ProfilingCounters_->TimeLag.Update(TDuration::Seconds(timeLagSeconds));
        ProfilingCounters_->TableLag.Update(tableLag);
    }

    TQueueStaticExportConfigPtr GetExportConfig() const
    {
        auto guard = Guard(Lock_);
        return ExportConfig_;
    }

    TQueueExporterDynamicConfig GetDynamicConfig() const
    {
        auto guard = Guard(Lock_);
        return DynamicConfig_;
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueExporter)

////////////////////////////////////////////////////////////////////////////////

IQueueExporterPtr CreateQueueExporter(
    TString exportName,
    TCrossClusterReference queue,
    TQueueStaticExportConfigPtr exportConfig,
    TQueueExporterDynamicConfig dynamicConfig,
    TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker,
    IQueueExportManagerPtr queueExportManager,
    IAlertCollectorPtr alertCollector,
    const TProfiler& queueProfiler,
    const TLogger& logger)
{
    auto exporter =  New<TQueueExporter>(
        std::move(exportName),
        std::move(queue),
        std::move(exportConfig),
        std::move(dynamicConfig),
        std::move(clientDirectory),
        std::move(invoker),
        std::move(queueExportManager),
        std::move(alertCollector),
        queueProfiler,
        logger);
    exporter->Initialize();

    return exporter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
