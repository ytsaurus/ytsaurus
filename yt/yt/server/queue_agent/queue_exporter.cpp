#include "queue_exporter.h"

#include "helpers.h"
#include "pass_profiler.h"
#include "private.h"
#include "queue_export_manager.h"

#include <yt/yt/ytlib/api/internal_proxy/config.h>
#include <yt/yt/ytlib/api/internal_proxy/export_queue.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/queue_client/config.h>
#include <yt/yt/client/queue_client/queue_export.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NQueueAgent {

using namespace NApi;
using namespace NAlertManager;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NHiveClient;
using namespace NInternalProxy;
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

//! Export schedule described by #exportConfig.
TQueueExportSchedule GetExportSchedule(const TQueueStaticExportConfigPtr& exportConfig)
{
    if (exportConfig->ExportPeriod) {
        return TQueueExportSchedule(*exportConfig->ExportPeriod);
    }

    return TQueueExportSchedule(TQueueExportCronExpression(*exportConfig->ExportCronSchedule));
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

void TQueueExportProgress::Update(i64 tabletIndex, TChunkId chunkId, TTimestamp maxTimestamp, i64 chunkCount, i64 rowCount)
{
    auto tabletProgressIt = Tablets.find(tabletIndex);
    if (tabletProgressIt == Tablets.end()) {
        tabletProgressIt = Tablets.emplace(tabletIndex, New<TQueueTabletExportProgress>()).first;
    }

    tabletProgressIt->second->LastChunk = chunkId;
    tabletProgressIt->second->ChunkCount += chunkCount;
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
        .Default();
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
        bool isInitialInvocation,
        TQueueExportProfilingCountersPtr profilingCounters,
        const TLogger& logger)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , ProfilingCounters_(std::move(profilingCounters))
        , Queue_(std::move(queue))
        , ExportConfig_(std::move(exportConfig))
        , DynamicConfig_(std::move(dynamicConfig))
        , IsInitialInvocation_(isInitialInvocation)
        , Logger(logger
            .WithTag("ExportDirectory", ExportConfig_->ExportDirectory)
            .WithTag("ExportPeriod", ExportConfig_->ExportPeriod)
            .WithTag("ExportCronSchedule", ExportConfig_->ExportCronSchedule)
            .WithTag("IsInitialInvocation", IsInitialInvocation_))
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
    const TQueueExportProfilingCountersPtr ProfilingCounters_;

    const TYPath Queue_;
    const TQueueStaticExportConfigPtr ExportConfig_;
    const TQueueExporterDynamicConfig DynamicConfig_;

    const bool IsInitialInvocation_;

    const TLogger Logger;

    //! Options used for Cypress requests.
    TTransactionalOptions Options_;

    //! Instant of current export task.
    TInstant TaskInstant_;

    //! Upper bound on export unix ts of exported tables created in this export task.
    //! NB: We use the instant above to compute this value, i.e. it corresponds to the host's physical time.
    //! This is fine, see the comment for Run above.
    ui64 ExportUnixTsUpperBound_;
    //! Id of the queue being exported.
    TObjectId QueueObjectId_;
    // Export task part error.
    TError ExportError_;
    //! Most recent export progress.
    // Corresponds to export progress currently stored in export directory attribute.
    TQueueExportProgressPtr QueueExportProgress_;

    static constexpr TStringBuf ExportProgressAttributeName_ = "queue_static_export_progress";
    static constexpr TStringBuf ExportDestinationAttributeName_ = "queue_static_export_destination";
    static constexpr TStringBuf ExporterAttributeName_ = "queue_static_exporter";

    //! Performs the following steps:
    //!   1) Starts transaction, obtain locks on input queue (snapshot) and <export_directory>/@queue_static_exporter attribute (shared).
    //!   2) Determines export unix ts upper bound as the most recent exported table that can be created according to the current physical time.
    //!   3) Fetches chunk specs, skipping dynamic stores, already exported chunks and chunks with timestamp larger than
    //!      the export unix timestamp upper bound.
    //!   4) Groups together chunk specs by export unix ts corresponding to their export unix ts.
    //!   5) For tables with hunk chunks drops groups starting with the first one that has a reference to an unsealed hunk chunk.
    //!   6) Creates at most MaxExportedTableCountPerTask exported tables, stopping at the first failure.
    //!
    //! NB: Steps 3-6 are performed by #TQueueExportRunner.
    //!
    //! NB: We use the host's physical time to compute the unix ts of the next table to export.
    //! We rely on this time being mostly monotonous and not too different from the cluster time obtained via timestamp generation.
    //! In any case, we will only produce an output table if its unix ts is strictly greater than the one of the last exported table.
    //! If the physical time diverges from the cluster time, the only effect is that some chunks might end up being
    //! exported into later tables, which is perfectly fine.
    void DoRun()
    {
        YT_TLOG_INFO("Started export task");
        auto logFinally = Finally([&] {
            YT_TLOG_INFO("Finished export task");
        });

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        auto transactionId = transaction->GetId();

        // NB: These are only filled in the try block below, but are needed after the transaction is committed.
        TQueueExportProgressPtr newExportProgress;
        i64 preparedTableCount = 0;
        int exportedTableCount = 0;
        int skippedTableCount = 0;
        std::optional<TProgressDiff> diff;
        try {
            auto queueObjectId = WaitFor(transaction->LockNode(Queue_, ELockMode::Snapshot))
                .ValueOrThrow()
                .NodeId;

            // We take a shared lock with some fixed fictional attribute key (#ExporterAttributeName_),
            // which acts as exclusive lock for this export directory across all queue agent instances.
            WaitFor(transaction->LockNode(
                ExportConfig_->ExportDirectory,
                ELockMode::Shared,
                TLockNodeOptions{
                    .AttributeKey = std::string(ExporterAttributeName_),
                }))
                .ThrowOnError();

            Options_.TransactionId = transactionId;

            YT_TLOG_INFO("Started export transaction and locked nodes")
                .With("TransactionId", transactionId);

            TaskInstant_ = TInstant::Now();
            ExportUnixTsUpperBound_ = GetExportUnixTsUpperBound(TaskInstant_, GetExportSchedule(ExportConfig_));

            QueueObjectId_ = queueObjectId;

            auto currentExportProgress = ValidateDestinationAndFetchProgress();
            YT_VERIFY(currentExportProgress);
            QueueExportProgress_ = currentExportProgress;
            if (currentExportProgress->LastExportUnixTs >= ExportUnixTsUpperBound_) {
                if (IsInitialInvocation_) {
                    YT_TLOG_WARNING("Rows corresponding to this export unix ts have already been exported according to the last export unix ts")
                        .With("ExportUnixTsUpperBound", ExportUnixTsUpperBound_)
                        .With("LastExportUnixTs", currentExportProgress->LastExportUnixTs);
                    return;
                } else {
                    THROW_ERROR_EXCEPTION(
                        "Rows corresponding to export unix ts %v have already been exported as last export unix ts is %v",
                        ExportUnixTsUpperBound_,
                        currentExportProgress->LastExportUnixTs);
                }
            }

            auto exportResult = ExportQueue(currentExportProgress);

            CheckStartOffsets(exportResult);

            preparedTableCount = exportResult.PreparedTableCount;
            newExportProgress = CloneYsonStruct(currentExportProgress);
            newExportProgress->QueueObjectId = QueueObjectId_;

            if (exportResult.Tables.empty()) {
                // NB(apachee): New export progress is taking into account if there are chunks
                // to export, meaning in this case only last successful export task instant would
                // be changed.
                YT_TLOG_DEBUG("No chunks to export, committing export transaction prematurely")
                    .With("TransactionId", transactionId);
            } else {
                for (const auto& exportedTable : exportResult.Tables) {
                    if (!exportedTable.Error.IsOK()) {
                        YT_TLOG_ERROR("Queue export task part failed")
                            .With(exportedTable.Error);
                        ExportError_ = exportedTable.Error;
                        break;
                    }

                    AdvanceExportProgress(exportedTable, newExportProgress);

                    ++exportedTableCount;
                }
            }

            skippedTableCount = preparedTableCount - exportedTableCount;

            newExportProgress->LastExportTaskInstant = TaskInstant_;
            if (skippedTableCount == 0) {
                newExportProgress->LastSuccessfulExportTaskInstant = TaskInstant_;
                newExportProgress->LastSuccessfulExportIterationInstant = TaskInstant_;
            } else {
                YT_VERIFY(!ExportError_.IsOK());
            }

            diff = UpdateCypressExportProgress(currentExportProgress, newExportProgress);
        } catch (const std::exception&) {
            YT_UNUSED_FUTURE(transaction->Abort());
            throw;
        }

        auto commitResultOrError = WaitFor(transaction->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            commitResultOrError,
            "Error committing main export task transaction for queue %v",
            Queue_);

        QueueExportProgress_ = newExportProgress;

        YT_TLOG_DEBUG("Finished creating exported tables")
            .With("PreparedTableCount", preparedTableCount)
            .With("ExportedTableCount", exportedTableCount)
            .With("SkippedTableCount", skippedTableCount);

        ProfilingCounters_->ExportedRows.Increment(diff->RowCount);
        ProfilingCounters_->ExportedChunks.Increment(diff->ChunkCount);
        ProfilingCounters_->ExportedTables.Increment(exportedTableCount);
        ProfilingCounters_->SkippedTables.Increment(skippedTableCount);
    }

    IAttributeDictionaryPtr FetchNodeAttributes(const TYPath& path, const std::vector<TStringBuf>& attributeKeys) const
    {
        YT_TLOG_DEBUG("Started fetching attributes")
            .With("Path", path)
            .With("PathRequestedAttributes", attributeKeys);

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

        YT_TLOG_DEBUG("Finished fetching attributes")
            .With("Path", path)
            .With("PathRequestedAttributes", attributeKeys);
        return ConvertToAttributes(TYsonString(rspOrError.Value()->value()));
    }

    TQueueExportProgressPtr ValidateDestinationAndFetchProgress()
    {
        auto exportDirectoryAttributes = FetchNodeAttributes(
            ExportConfig_->ExportDirectory,
            {ExportDestinationAttributeName_, ExportProgressAttributeName_});

        auto destinationConfig = exportDirectoryAttributes->Get<TQueueStaticExportDestinationConfig>(ExportDestinationAttributeName_);
        if (destinationConfig.OriginatingQueueId != QueueObjectId_) {
            THROW_ERROR_EXCEPTION(
                "Destination config is not configured to accept exports from queue %v, configured id %v does not match queue id %v",
                Queue_,
                destinationConfig.OriginatingQueueId,
                QueueObjectId_);
        }

        auto currentExportProgress = exportDirectoryAttributes->Find<TQueueExportProgressPtr>(ExportProgressAttributeName_);

        // COMPAT(apachee): There are exports without "queue_object_id" field set. We do not want to ignore export progress in this case.
        if (currentExportProgress && !currentExportProgress->QueueObjectId) {
            currentExportProgress->QueueObjectId = QueueObjectId_;
        }

        // NB(apachee): If export progress corresponds to different queue, then assume it's the first export in this directory.
        if (currentExportProgress && currentExportProgress->QueueObjectId == QueueObjectId_) {
            return currentExportProgress;
        }

        return New<TQueueExportProgress>();
    }

    TExportQueueIntoStaticTableResult ExportQueue(const TQueueExportProgressPtr& currentExportProgress)
    {
        THashMap<i64, TChunkId> lastExportedChunkIds;
        THashMap<i64, i64> startOffsets;
        for (const auto& [tabletIndex, tabletProgress] : currentExportProgress->Tablets) {
            lastExportedChunkIds[tabletIndex] = tabletProgress->LastChunk;
            startOffsets[tabletIndex] = tabletProgress->RowCount;
        }

        TExportQueueIntoStaticTableOptions options;
        options.TransactionId = Options_.TransactionId;
        options.TableCountLimit = DynamicConfig_.MaxExportedTableCountPerTask;
        options.ExportTtl = ExportConfig_->ExportTtl;
        options.OutputTableNamePattern = ExportConfig_->OutputTableNamePattern;
        options.UseUpperBoundForTableNames = ExportConfig_->UseUpperBoundForTableNames;
        options.EnableExportFromQueueWithHunks = ExportConfig_->EnableExportFromQueueWithHunks;

        auto startTimestamp = TimestampFromUnixTime(currentExportProgress->LastExportUnixTs);

        auto exporterConfig = New<TQueueExportRunnerConfig>();
        exporterConfig->MaxTableCountPerCall = DynamicConfig_.MaxExportedTableCountPerTask;
        exporterConfig->MaxHunkChunkCountPerFetch = DynamicConfig_.MaxHunkChunkCountPerFetch;
        TQueueExportRunner exporter(std::move(exporterConfig));

        // NB: The queue is addressed by its id, since the path may have changed after it has been locked.
        return WaitFor(exporter.ExportQueueIntoStaticTable(
            Client_,
            FromObjectId(QueueObjectId_),
            ExportConfig_->ExportDirectory,
            std::move(lastExportedChunkIds),
            std::move(startOffsets),
            startTimestamp,
            GetExportSchedule(ExportConfig_),
            std::move(options),
            Logger))
            .ValueOrThrow();
    }

    //! Checks that the last exported chunk of each tablet ends exactly at the row count recorded for it,
    //! which the export reports back to us.
    //!
    //! \note Throws if the check is enabled and the two disagree. Throwing here aborts the task transaction,
    //! so none of the exported tables that have already been created within it survives.
    void CheckStartOffsets(const TExportQueueIntoStaticTableResult& exportResult)
    {
        const auto& errors = exportResult.TabletOffsetMismatchErrors;
        if (errors.empty()) {
            return;
        }

        ProfilingCounters_->RowCountMismatches.Increment(std::ssize(errors));

        if (DynamicConfig_.EnableRowCountCheck) {
            YT_TLOG_ALERT_AND_THROW("Mismatch between last chunk from tablet progress and row count from progress")
                .With("TabletOffsetMismatchErrors", errors);
        }
    }

    //! Advances #progress in-place by taking into account the tablets of an exported table.
    void AdvanceExportProgress(const TQueueExportStaticTable& exportedTable, const TQueueExportProgressPtr& progress) const
    {
        for (const auto& [tabletIndex, tabletExport] : exportedTable.Tablets) {
            progress->Update(
                tabletIndex,
                tabletExport.LastChunk,
                tabletExport.MaxTimestamp,
                tabletExport.ChunkCount,
                tabletExport.FinishOffset);
        }

        progress->LastExportUnixTs = UnixTimeFromTimestamp(exportedTable.FinishTimestamp);
    }

    TProgressDiff UpdateCypressExportProgress(
        const TQueueExportProgressPtr& currentExportProgress,
        const TQueueExportProgressPtr& newExportProgress)
    {
        YT_VERIFY(newExportProgress->QueueObjectId == QueueObjectId_);

        TSetNodeOptions options;
        options.TransactionId = Options_.TransactionId;
        WaitFor(Client_->SetNode(
            Format("%v/@%v", ExportConfig_->ExportDirectory, ExportProgressAttributeName_),
            ConvertToYsonString(newExportProgress),
            options))
            .ThrowOnError();


        TProgressDiff diff{currentExportProgress, newExportProgress};
        YT_TLOG_DEBUG("Updated export progress")
            .With("ExportedRows", diff.RowCount)
            .With("ExportedChunks", diff.ChunkCount);
        return diff;
    }
};

using TQueueExportTaskPtr = TIntrusivePtr<TQueueExportTask>;
DEFINE_REFCOUNTED_TYPE(TQueueExportTask)

////////////////////////////////////////////////////////////////////////////////

TQueueExportProfilingCounters::TQueueExportProfilingCounters(
    const TProfiler& queueProfiler,
    const TProfiler& queuePassProfiler)
    : ExportedRows(queueProfiler.Counter("/exported_rows"))
    , ExportedChunks(queueProfiler.Counter("/exported_chunks"))
    , ExportedTables(queueProfiler.Counter("/exported_tables"))
    , SkippedTables(queueProfiler.Counter("/skipped_tables"))
    , ExportTaskErrors(queueProfiler.Counter("/export_task_errors"))
    , TimeLag(queueProfiler.TimeGauge("/time_lag"))
    , TableLag(queueProfiler.Gauge("/table_lag"))
    , RowCountMismatches(queuePassProfiler.Counter("/row_count_mismatches"))
{ }

////////////////////////////////////////////////////////////////////////////////

class TQueueExporter
    : public virtual IQueueExporter
{
public:
    TQueueExporter(
        std::string exportName,
        TTablePath queue,
        TQueueStaticExportConfigPtr exportConfig,
        TQueueExporterDynamicConfig dynamicConfig,
        IInvokerPtr invoker,
        IQueueExportManagerPtr queueExportManager,
        IAlertCollectorPtr alertCollector,
        const TProfiler& queueProfiler,
        const TProfiler& queuePassProfiler,
        const TLogger& logger)
        : ExportConfig_(std::move(exportConfig))
        , DynamicConfig_(std::move(dynamicConfig))
        , RetryBackoff_(DynamicConfig_.RetryBackoff)
        , ExportName_(std::move(exportName))
        , Queue_(std::move(queue))
        , Invoker_(std::move(invoker))
        , QueueExportManager_(std::move(queueExportManager))
        , AlertCollector_(std::move(alertCollector))
        , ProfilingCounters_(New<TQueueExportProfilingCounters>(queueProfiler, queuePassProfiler))
        , PassProfiler_(queuePassProfiler)
        , Executor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND_NO_PROPAGATE(
                &TQueueExporter::Pass,
                MakeWeak(this)),
            DynamicConfig_.GetPeriodicExecutorOptions()))
        , Logger(QueueExporterLogger()
            .WithTags(logger.GetTags())
            .WithTag("ExportName", ExportName_))
    { }

    void Initialize() const
    {
        Executor_->Start();

        YT_TLOG_INFO("Queue exporter started");
    }

    TQueueExportProgressPtr GetExportProgress() const override
    {
        auto guard = Guard(Lock_);
        return ExportProgress_
            ? ExportProgress_
            : New<TQueueExportProgress>();
    }

    void OnExportConfigChanged(const TQueueStaticExportConfigPtr& newExportConfig) override
    {
        auto guard = Guard(Lock_);

        if (ExportConfig_->ExportDirectory != newExportConfig->ExportDirectory) {
            ExportProgress_.Reset();

            // NB(apachee): Restart retries from the start, since new export directory
            // might be properly configured (or become so soon).
            RetryBackoff_.Restart();

            LastSuccessfulExportUnixTs_.store(0);
        }

        ExportConfig_ = newExportConfig;
    }

    void OnDynamicConfigChanged(const TQueueExporterDynamicConfig& newDynamicConfig) override
    {
        auto guard = Guard(Lock_);

        if (DynamicConfig_.GetPeriodicExecutorOptions() != newDynamicConfig.GetPeriodicExecutorOptions()) {
            Executor_->SetOptions(DynamicConfig_.GetPeriodicExecutorOptions());
        }
        if (RetryBackoff_.GetOptions() != newDynamicConfig.RetryBackoff) {
            RetryBackoff_.UpdateOptions(newDynamicConfig.RetryBackoff);
            RetryBackoff_.Restart();
        }

        DynamicConfig_ = newDynamicConfig;
    }

    void Stop() override
    {
        AlertCollector_->Stop();
        YT_UNUSED_FUTURE(Executor_->Stop());
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
    //! Last fetched export progress. Null means that it hasn't been fetched yet.
    TQueueExportProgressPtr ExportProgress_;
    //! Number of times export task was invoked regardless of its result.
    i64 ExportTaskInvocationIndex_ = 0;
    //! Instant of last export task invocation.
    TInstant ExportTaskInvocationInstant_;
    //! Export unix timestamp corresponding to the last successful export.
    TBackoffStrategy RetryBackoff_;

    std::atomic<ui64> LastSuccessfulExportUnixTs_ = 0;

    const std::string ExportName_;
    const TTablePath Queue_;
    const IInvokerPtr Invoker_;
    const IQueueExportManagerPtr QueueExportManager_;
    const IAlertCollectorPtr AlertCollector_;
    const TQueueExportProfilingCountersPtr ProfilingCounters_;
    const TPassProfiler PassProfiler_;
    const TPeriodicExecutorPtr Executor_;

    const TLogger Logger;

    i64 PassIndex_ = -1;

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

        auto startTime = TInstant::Now();

        auto exportConfig = GetExportConfig();
        auto exportUnixTs = GetExportUnixTsUpperBound(startTime, GetExportSchedule(exportConfig));

        if (exportUnixTs <= LastSuccessfulExportUnixTs_.load()) {
            // Too early to run new export task.
            return;
        }
        ++PassIndex_;
        PassProfiler_.OnStart(PassIndex_, startTime);

        auto finalizePass = Finally([&] {
            PassProfiler_.OnFinish(TInstant::Now() - startTime);
        });

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

        YT_TLOG_INFO("Doing retry backoff")
            .With("BackoffDuration", backoffDuration)
            .With("RetryIndex", retryIndex);

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

        bool isInitialInvocation = false;
        {
            auto guard = Guard(Lock_);

            ++ExportTaskInvocationIndex_;
            ExportTaskInvocationInstant_ = TInstant::Now();

            isInitialInvocation = !static_cast<bool>(ExportProgress_);
        }

        TQueueExportTaskPtr exportTask = New<TQueueExportTask>(
            QueueExportManager_->GetQueueExportClientDirectory()->GetClientOrThrow(Queue_.GetCluster().value()),
            Invoker_,
            Queue_.GetPath(),
            exportConfig,
            std::move(dynamicConfig),
            isInitialInvocation,
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
                // NB(apachee): We store this value even in case of errors, since it might have been our first invocation.
                LastSuccessfulExportUnixTs_.store(std::max(exportUnixTs, ExportProgress_->LastExportUnixTs));
            }
            if (!exportTaskError.IsOK() || !exportTask->GetExportError().IsOK()) {
                THROW_ERROR_EXCEPTION("Export task has errors")
                    .With("task_error", exportTaskError)
                    .With("export_error", exportTask->GetExportError());
            }
            if (!newExportProgressNonNull) {
                THROW_ERROR_EXCEPTION("Export task result is missing new export progress without any errors");
            }
        }
        YT_VERIFY(exportTaskError.IsOK() && exportTask->GetExportError().IsOK());
    }

    void ProfileExport(const TQueueStaticExportConfigPtr& exportConfig, bool hasError)
    {
        auto now = TInstant::Now();
        auto exportUnixTsUpperBound = GetExportUnixTsUpperBound(now, GetExportSchedule(exportConfig));

        ui64 timeLagSeconds = 0;
        auto exportProgress = GetExportProgress();
        YT_VERIFY(exportProgress);
        // TODO(apachee): Consider more elaborate ways to handle if this condition is not true
        if (exportProgress->LastExportUnixTs <= exportUnixTsUpperBound) {
            timeLagSeconds = exportUnixTsUpperBound - exportProgress->LastExportUnixTs;
        }

        // NB(apachee): Table lag is rounded up, since export period could've changed after the last successful export task.
        // In general case, for CRON schedules the table lag is an approximation.
        auto exportPeriodSeconds = GetLastExportPeriod(exportUnixTsUpperBound, GetExportSchedule(exportConfig));
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
    std::string exportName,
    TTablePath queue,
    TQueueStaticExportConfigPtr exportConfig,
    TQueueExporterDynamicConfig dynamicConfig,
    IInvokerPtr invoker,
    IQueueExportManagerPtr queueExportManager,
    IAlertCollectorPtr alertCollector,
    IQueueExporterProfileManagerPtr profileManager,
    const TLogger& logger)
{
    auto exporter =  New<TQueueExporter>(
        std::move(exportName),
        std::move(queue),
        std::move(exportConfig),
        std::move(dynamicConfig),
        std::move(invoker),
        std::move(queueExportManager),
        std::move(alertCollector),
        profileManager->GetProfiler(EProfilerScope::Object),
        profileManager->GetProfiler(EProfilerScope::ObjectPass),
        logger);
    exporter->Initialize();

    return exporter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
