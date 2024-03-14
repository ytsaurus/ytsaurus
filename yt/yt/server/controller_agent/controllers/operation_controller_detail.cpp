#include "operation_controller_detail.h"

#include "auto_merge_task.h"
#include "job_info.h"
#include "job_helpers.h"
#include "helpers.h"
#include "sink.h"
#include "task.h"

#include <yt/yt/server/controller_agent/intermediate_chunk_scraper.h>
#include <yt/yt/server/controller_agent/counter_manager.h>
#include <yt/yt/server/controller_agent/job_profiler.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/scheduling_context.h>
#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/private.h>

#include <yt/yt/server/lib/controller_agent/job_report.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/misc/job_report.h>
#include <yt/yt/server/lib/misc/job_table_schema.h>
#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/server/lib/chunk_pools/helpers.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>
#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/event_log/event_log.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/ytlib/security_client/helpers.h>

#include <yt/yt/ytlib/query_client/functions_cache.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>
#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/api/native/proto/transaction_actions.pb.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/public.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/library/erasure/impl/codec.h>
#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <library/cpp/yt/memory/chunked_input_stream.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/vector.h>

#include <util/system/compiler.h>

#include <library/cpp/iterator/functools.h>

#include <functional>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NEventLog;
using namespace NFileClient;
using namespace NFormats;
using namespace NJobTrackerClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NRpc;
using namespace NScheduler;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NVectorHdrf;
using namespace NYPath;
using namespace NYson;
using namespace NYTProf;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

using NControllerAgent::NProto::TJobSpec;
using NJobTrackerClient::EJobState;
using NNodeTrackerClient::TNodeId;
using NProfiling::CpuInstantToInstant;
using NProfiling::TCpuInstant;
using NControllerAgent::NProto::TJobResultExt;
using NControllerAgent::NProto::TJobSpecExt;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::NProto::THeavyColumnStatisticsExt;
using NTabletNode::DefaultMaxOverlappingStoreCount;

using std::placeholders::_1;

static constexpr auto SampleChunkIdCount = 10;

////////////////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TStripeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripe);
    Persist(context, Cookie);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TInputChunkDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputStripes);
    Persist(context, InputChunks);
    Persist(context, State);
}

////////////////////////////////////////////////////////////////////////////////

TOperationControllerBase::TOperationControllerBase(
    TOperationSpecBasePtr spec,
    TControllerAgentConfigPtr config,
    TOperationOptionsPtr options,
    IOperationControllerHostPtr host,
    TOperation* operation)
    : Host(std::move(host))
    , Config(std::move(config))
    , OperationId(operation->GetId())
    , OperationType(operation->GetType())
    , StartTime_(operation->GetStartTime())
    , AuthenticatedUser(operation->GetAuthenticatedUser())
    , SecureVault(operation->GetSecureVault())
    , UserTransactionId(operation->GetUserTransactionId())
    , Logger([&] {
        auto logger = ControllerLogger;
        logger = logger.WithTag("OperationId: %v", OperationId);
        if (spec->EnableTraceLogging) {
            logger = logger.WithMinLevel(ELogLevel::Trace);
        }
        return logger;
    }())
    , CoreNotes_({Format("OperationId: %v", OperationId)})
    , Acl(operation->GetAcl())
    , ControllerEpoch(operation->GetControllerEpoch())
    , CancelableContext(New<TCancelableContext>())
    , DiagnosableInvokerPool_(CreateEnumIndexedProfiledFairShareInvokerPool<EOperationControllerQueue>(
        CreateCodicilGuardedInvoker(
            CreateSerializedInvoker(Host->GetControllerThreadPoolInvoker(), "operation_controller_base"),
            Format(
                "OperationId: %v\nAuthenticatedUser: %v",
                OperationId,
                AuthenticatedUser)),
        CreateFairShareCallbackQueue,
        Config->InvokerPoolTotalTimeAggregationPeriod,
        "OperationController"))
    , InvokerPool(DiagnosableInvokerPool_)
    , SuspendableInvokerPool(TransformInvokerPool(InvokerPool, CreateSuspendableInvoker))
    , CancelableInvokerPool(TransformInvokerPool(
        SuspendableInvokerPool,
        BIND(&TCancelableContext::CreateInvoker, CancelableContext)))
    , JobSpecBuildInvoker_(Host->GetJobSpecBuildPoolInvoker())
    , RowBuffer(New<TRowBuffer>(TRowBufferTag(), Config->ControllerRowBufferChunkSize))
    , LivePreviews_(std::make_shared<TLivePreviewMap>())
    , PoolTreeControllerSettingsMap_(operation->PoolTreeControllerSettingsMap())
    , Spec_(std::move(spec))
    , Options(std::move(options))
    , CachedRunningJobs_(
        Config->CachedRunningJobsUpdatePeriod,
        BIND(&TOperationControllerBase::DoBuildJobsYson, Unretained(this)))
    , SuspiciousJobsYsonUpdater_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdateSuspiciousJobsYson, MakeWeak(this)),
        Config->SuspiciousJobs->UpdatePeriod))
    , RunningJobStatisticsUpdateExecutor_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdateAggregatedRunningJobStatistics, MakeWeak(this)),
        Config->RunningJobStatisticsUpdatePeriod))
    , ScheduleAllocationStatistics_(New<TScheduleAllocationStatistics>(Config->ScheduleAllocationStatisticsMovingAverageWindowSize))
    , CheckTimeLimitExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckTimeLimit, MakeWeak(this)),
        Config->OperationTimeLimitCheckPeriod))
    , ExecNodesCheckExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckAvailableExecNodes, MakeWeak(this)),
        Config->AvailableExecNodesCheckPeriod))
    , AlertManager_(CreateAlertManager(this))
    , MinNeededResourcesSanityCheckExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckMinNeededResourcesSanity, MakeWeak(this)),
        Config->ResourceDemandSanityCheckPeriod))
    , PeakMemoryUsageUpdateExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdatePeakMemoryUsage, MakeWeak(this)),
        Config->MemoryWatchdog->MemoryUsageCheckPeriod))
    , ExecNodesUpdateExecutor(New<TPeriodicExecutor>(
        Host->GetExecNodesUpdateInvoker(),
        BIND(&TThis::UpdateExecNodes, MakeWeak(this)),
        Config->ControllerExecNodeInfoUpdatePeriod))
    , EventLogConsumer_(Host->GetEventLogWriter()->CreateConsumer())
    , LogProgressBackoff(DurationToCpuDuration(Config->OperationLogProgressBackoff))
    , ProgressBuildExecutor_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::BuildAndSaveProgress, MakeWeak(this)),
        Config->OperationBuildProgressPeriod))
    , CheckTentativeTreeEligibilityExecutor_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckTentativeTreeEligibility, MakeWeak(this)),
        Config->CheckTentativeTreeEligibilityPeriod))
    , MediumDirectory_(Host->GetMediumDirectory())
    , ExperimentAssignments_(operation->ExperimentAssignments())
    , UpdateAccountResourceUsageLeasesExecutor_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdateAccountResourceUsageLeases, MakeWeak(this)),
        Config->UpdateAccountResourceUsageLeasesPeriod))
    , TotalJobCounter_(New<TProgressCounter>())
    , TestingAllocationSize_(Spec_->TestingOperationOptions->AllocationSize.value_or(0))
    , AllocationReleaseDelay_(Spec_->TestingOperationOptions->AllocationReleaseDelay)
    , FastIntermediateMediumLimit_(std::min(
        Spec_->FastIntermediateMediumLimit,
        Config->FastIntermediateMediumLimit))
    , SendRunningAllocationTimeStatisticsUpdatesExecutor_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::JobEvents),
        BIND_NO_PROPAGATE(&TThis::SendRunningAllocationTimeStatisticsUpdates, MakeWeak(this)),
        Config->RunningAllocationTimeStatisticsUpdatesSendPeriod))
    , JobAbortsUntilOperationFailure_(Config->MaxJobAbortsUntilOperationFailure)
{
    // Attach user transaction if any. Don't ping it.
    TTransactionAttachOptions userAttachOptions;
    userAttachOptions.Ping = false;
    userAttachOptions.PingAncestors = false;
    UserTransaction = UserTransactionId
        ? Host->GetClient()->AttachTransaction(UserTransactionId, userAttachOptions)
        : nullptr;

    for (const auto& reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
        ExternalScheduleAllocationFailureCounts_[reason] = 0;
    }

    TSchedulingTagFilter filter(Spec_->SchedulingTagFilter);
    ExecNodesDescriptors_ = Host->GetExecNodeDescriptors(filter, /*onlineOnly*/ false);
    OnlineExecNodesDescriptors_ = Host->GetExecNodeDescriptors(filter, /*onlineOnly*/ true);

    YT_LOG_INFO("Operation controller instantiated (OperationType: %v, Address: %v)",
        OperationType,
        static_cast<void*>(this));

    YT_LOG_DEBUG("Set fast intermediate medium limit (ConfigLimit: %v, SpecLimit: %v, EffectiveLimit: %v)",
        Config->FastIntermediateMediumLimit,
        Spec_->FastIntermediateMediumLimit,
        GetFastIntermediateMediumLimit());
}

void TOperationControllerBase::BuildMemoryUsageYson(TFluentAny fluent) const
{
    fluent
        .Value(GetMemoryUsage());
}

void TOperationControllerBase::BuildStateYson(TFluentAny fluent) const
{
    fluent
        .Value(State.load());
}

void TOperationControllerBase::BuildTestingState(TFluentAny fluent) const
{
    fluent
        .BeginMap()
            .Item("commit_sleep_started").Value(CommitSleepStarted_)
        .EndMap();
}

const TProgressCounterPtr& TOperationControllerBase::GetTotalJobCounter() const
{
    return TotalJobCounter_;
}

const TScheduleAllocationStatisticsPtr& TOperationControllerBase::GetScheduleAllocationStatistics() const
{
    return ScheduleAllocationStatistics_;
}

const TAggregatedJobStatistics& TOperationControllerBase::GetAggregatedFinishedJobStatistics() const
{
    return AggregatedFinishedJobStatistics_;
}

const TAggregatedJobStatistics& TOperationControllerBase::GetAggregatedRunningJobStatistics() const
{
    return AggregatedRunningJobStatistics_;
}

std::unique_ptr<IHistogram> TOperationControllerBase::ComputeFinalPartitionSizeHistogram() const
{
    return nullptr;
}

// Resource management.
TExtendedJobResources TOperationControllerBase::GetAutoMergeResources(
    const TChunkStripeStatisticsVector& statistics) const
{
    TExtendedJobResources result;
    result.SetUserSlots(1);
    result.SetCpu(1);
    // TODO(max42): this way to estimate memory of an auto-merge job is wrong as it considers each
    // auto-merge task writing to all output tables.
    result.SetJobProxyMemory(GetFinalIOMemorySize(Spec_->AutoMerge->JobIO, AggregateStatistics(statistics)));
    return result;
}

void TOperationControllerBase::SleepInInitialize()
{
    if (auto delay = Spec_->TestingOperationOptions->DelayInsideInitialize) {
        TDelayedExecutor::WaitForDuration(*delay);
    }
}

std::vector<TTestAllocationGuard> TOperationControllerBase::TestHeap() const
{
    if (Spec_->TestingOperationOptions->AllocationSize.value_or(0) > 0) {
        auto Logger = ControllerLogger;

        constexpr i64 allocationPartSize = 1_MB;

        std::vector<TTestAllocationGuard> testHeap;

        std::function<void()> incrementer = [
                operationId = ToString(OperationId),
                Logger = Logger,
                this_ = MakeStrong(this)
            ] () mutable {
            auto size = this_->TestingAllocationSize_.fetch_add(allocationPartSize);
            YT_LOG_DEBUG("Testing allocation size was incremented (Size: %v)",
                size + allocationPartSize);
        };

        std::function<void()> decrementer = [
            OperationId = ToString(OperationId),
            Logger = Logger,
            this_ = MakeStrong(this)] () mutable {
            auto size = this_->TestingAllocationSize_.fetch_sub(allocationPartSize);
            YT_LOG_DEBUG("Testing allocation size was decremented (Size: %v)",
                size - allocationPartSize);
        };

        while (TestingAllocationSize_ > 0) {
            testHeap.emplace_back(
                allocationPartSize,
                decrementer,
                incrementer,
                AllocationReleaseDelay_.value_or(TDuration::Zero()),
                GetInvoker());
        }

        YT_LOG_DEBUG("Test heap allocation is finished (MemoryUsage: %v)",
            GetMemoryUsage());
        return testHeap;
    }

    return {};
}

void TOperationControllerBase::InitializeClients()
{
    Client = Host
        ->GetClient()
        ->GetNativeConnection()
        ->CreateNativeClient(TClientOptions::FromUser(AuthenticatedUser));
    InputClient = Client;
    OutputClient = Client;

    SchedulerClient = Host
        ->GetClient()
        ->GetNativeConnection()
        ->CreateNativeClient(TClientOptions::FromUser(SchedulerUserName));
    SchedulerInputClient = Client;
    SchedulerOutputClient = Client;
}

TOperationControllerInitializeResult TOperationControllerBase::InitializeReviving(const TControllerTransactionIds& transactions)
{
    YT_LOG_INFO("Initializing operation for revive");

    InitializeClients();

    auto attachTransaction = [&] (TTransactionId transactionId, const NNative::IClientPtr& client, bool ping) -> ITransactionPtr {
        if (!transactionId) {
            return nullptr;
        }

        try {
            return AttachTransaction(transactionId, client, ping);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error attaching operation transaction (OperationId: %v, TransactionId: %v)",
                OperationId,
                transactionId);
            return nullptr;
        }
    };

    auto inputTransaction = attachTransaction(transactions.InputId, InputClient, true);
    auto outputTransaction = attachTransaction(transactions.OutputId, OutputClient, true);
    auto debugTransaction = attachTransaction(transactions.DebugId, Client, true);
    // NB: Async and completion transactions are never reused and thus are not pinged.
    auto asyncTransaction = attachTransaction(transactions.AsyncId, Client, false);
    auto outputCompletionTransaction = attachTransaction(transactions.OutputCompletionId, OutputClient, false);
    auto debugCompletionTransaction = attachTransaction(transactions.DebugCompletionId, Client, false);

    std::vector<ITransactionPtr> nestedInputTransactions;
    THashMap<TTransactionId, ITransactionPtr> transactionIdToTransaction;
    for (auto transactionId : transactions.NestedInputIds) {
        auto it = transactionIdToTransaction.find(transactionId);
        if (it == transactionIdToTransaction.end()) {
            auto transaction = attachTransaction(transactionId, InputClient, true);
            YT_VERIFY(transactionIdToTransaction.emplace(transactionId, transaction).second);
            nestedInputTransactions.push_back(transaction);
        } else {
            nestedInputTransactions.push_back(it->second);
        }
    }

    // Check transactions.
    {
        std::vector<std::pair<ITransactionPtr, TFuture<void>>> asyncCheckResults;

        THashSet<ITransactionPtr> checkedTransactions;
        auto checkTransaction = [&] (
            const ITransactionPtr& transaction,
            ETransactionType transactionType,
            TTransactionId transactionId)
        {
            if (!transaction) {
                CleanStart = true;
                YT_LOG_INFO("Operation transaction is missing, will use clean start "
                    "(TransactionType: %v, TransactionId: %v)",
                    transactionType,
                    transactionId);
                return;
            }

            if (checkedTransactions.emplace(transaction).second) {
                asyncCheckResults.emplace_back(transaction, transaction->Ping());
            }
        };

        // NB: Async transaction is not checked.
        if (IsTransactionNeeded(ETransactionType::Input)) {
            checkTransaction(inputTransaction, ETransactionType::Input, transactions.InputId);
            for (int index = 0; index < std::ssize(nestedInputTransactions); ++index) {
                checkTransaction(nestedInputTransactions[index], ETransactionType::Input, transactions.NestedInputIds[index]);
            }
        }
        if (IsTransactionNeeded(ETransactionType::Output)) {
            checkTransaction(outputTransaction, ETransactionType::Output, transactions.OutputId);
        }
        if (IsTransactionNeeded(ETransactionType::Debug)) {
            checkTransaction(debugTransaction, ETransactionType::Debug, transactions.DebugId);
        }

        for (const auto& [transaction, asyncCheckResult] : asyncCheckResults) {
            auto error = WaitFor(asyncCheckResult);
            if (!error.IsOK()) {
                CleanStart = true;
                YT_LOG_INFO(error,
                    "Error renewing operation transaction, will use clean start (TransactionId: %v)",
                    transaction->GetId());
            }
        }
    }

    // Downloading snapshot.
    if (!CleanStart) {
        auto snapshotOrError = WaitFor(Host->DownloadSnapshot());
        if (!snapshotOrError.IsOK()) {
            YT_LOG_INFO(snapshotOrError, "Failed to download snapshot, will use clean start");
            CleanStart = true;
        } else {
            YT_LOG_INFO("Snapshot successfully downloaded");
            Snapshot = snapshotOrError.Value();
            if (Snapshot.Blocks.empty()) {
                YT_LOG_WARNING("Snapshot is empty, will use clean start");
                CleanStart = true;
            }
        }
    }

    // Abort transactions if needed.
    {
        std::vector<TFuture<void>> asyncResults;

        THashSet<ITransactionPtr> abortedTransactions;
        auto scheduleAbort = [&] (const ITransactionPtr& transaction, const NNative::IClientPtr& client) {
            if (transaction && abortedTransactions.emplace(transaction).second) {
                // Transaction object may be in incorrect state, we need to abort using only transaction id.
                asyncResults.push_back(AttachTransaction(transaction->GetId(), client)->Abort());
            }
        };

        scheduleAbort(asyncTransaction, Client);
        scheduleAbort(outputCompletionTransaction, OutputClient);
        scheduleAbort(debugCompletionTransaction, Client);

        if (CleanStart) {
            YT_LOG_INFO("Aborting operation transactions");
            // NB: Don't touch user transaction.
            scheduleAbort(inputTransaction, InputClient);
            scheduleAbort(outputTransaction, OutputClient);
            scheduleAbort(debugTransaction, Client);
            for (const auto& transaction : nestedInputTransactions) {
                scheduleAbort(transaction, InputClient);
            }
        } else {
            YT_LOG_INFO("Reusing operation transactions");
            InputTransaction = inputTransaction;
            OutputTransaction = outputTransaction;
            DebugTransaction = debugTransaction;
            AsyncTransaction = WaitFor(StartTransaction(ETransactionType::Async, Client))
                .ValueOrThrow();
            NestedInputTransactions = nestedInputTransactions;
        }

        WaitFor(AllSucceeded(asyncResults))
            .ThrowOnError();
    }


    if (CleanStart) {
        if (Spec_->FailOnJobRestart) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::OperationFailedOnJobRestart,
                "Cannot use clean restart when spec option \"fail_on_job_restart\" is set")
                << TErrorAttribute("reason", EFailOnJobRestartReason::RevivalWithCleanStart);
        }

        YT_LOG_INFO("Using clean start instead of revive");

        Snapshot = TOperationSnapshot();
        Y_UNUSED(WaitFor(Host->RemoveSnapshot()));

        StartTransactions();
        InitializeStructures();

        LockInputs();
    }

    InitUnrecognizedSpec();

    WaitFor(Host->UpdateInitializedOperationNode(CleanStart))
        .ThrowOnError();

    SleepInInitialize();

    YT_LOG_INFO("Operation initialized");

    TOperationControllerInitializeResult result;
    FillInitializeResult(&result);
    return result;
}

void TOperationControllerBase::ValidateSecureVault()
{
    if (!SecureVault) {
        return;
    }
    i64 length = ConvertToYsonString(SecureVault, EYsonFormat::Text).AsStringBuf().size();
    YT_LOG_DEBUG("Operation secure vault size detected (Size: %v)", length);
    if (length > Config->SecureVaultLengthLimit) {
        THROW_ERROR_EXCEPTION("Secure vault YSON text representation is too long")
            << TErrorAttribute("size_limit", Config->SecureVaultLengthLimit);
    }
}

TOperationControllerInitializeResult TOperationControllerBase::InitializeClean()
{
    YT_LOG_INFO("Initializing operation for clean start (Title: %v)",
        Spec_->Title);

    auto initializeAction = BIND([this_ = MakeStrong(this), this] () {
        ValidateSecureVault();
        InitializeClients();
        StartTransactions();
        InitializeStructures();
        LockInputs();
    });

    SleepInInitialize();

    auto initializeFuture = initializeAction
        .AsyncVia(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default))
        .Run()
        .WithTimeout(Config->OperationInitializationTimeout);

    WaitFor(initializeFuture)
        .ThrowOnError();

    InitUnrecognizedSpec();

    WaitFor(Host->UpdateInitializedOperationNode(/*isCleanOperationStart*/ true))
        .ThrowOnError();

    YT_LOG_INFO("Operation initialized");

    TOperationControllerInitializeResult result;
    FillInitializeResult(&result);
    return result;
}

bool TOperationControllerBase::HasUserJobFiles() const
{
    for (const auto& userJobSpec : GetUserJobSpecs()) {
        if (!userJobSpec->FilePaths.empty() || !GetLayerPaths(userJobSpec).empty()) {
            return true;
        }
    }
    return false;
}

void TOperationControllerBase::InitOutputTables()
{
    RegisterOutputTables(GetOutputTablePaths());
}

const IPersistentChunkPoolInputPtr& TOperationControllerBase::GetSink()
{
    return Sink_;
}

void TOperationControllerBase::ValidateAccountPermission(const TString& account, EPermission permission) const
{
    auto user = AuthenticatedUser;

    const auto& client = Host->GetClient();
    auto asyncResult = client->CheckPermission(
        user,
        "//sys/accounts/" + account,
        permission);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    if (result.Action == ESecurityAction::Deny) {
        THROW_ERROR_EXCEPTION("User %Qv has been denied %Qv access to intermediate account %Qv",
            user,
            permission,
            account);
    }
}

std::vector<TTransactionId> TOperationControllerBase::GetNonTrivialInputTransactionIds()
{
    // NB: keep it sync with InitializeStructures.
    std::vector<TTransactionId> inputTransactionIds;
    for (const auto& path : GetInputTablePaths()) {
        if (path.GetTransactionId()) {
            inputTransactionIds.push_back(*path.GetTransactionId());
        }
    }
    for (const auto& userJobSpec : GetUserJobSpecs()) {
        for (const auto& path : userJobSpec->FilePaths) {
            if (path.GetTransactionId()) {
                inputTransactionIds.push_back(*path.GetTransactionId());
            }
        }

        auto layerPaths = GetLayerPaths(userJobSpec);
        for (const auto& path : layerPaths) {
            if (path.GetTransactionId()) {
                inputTransactionIds.push_back(*path.GetTransactionId());
            }
        }
    }
    return inputTransactionIds;
}

void TOperationControllerBase::InitializeStructures()
{
    DataFlowGraph_->SetNodeDirectory(InputNodeDirectory_);
    DataFlowGraph_->Initialize();

    InitializeOrchid();

    // NB: keep it sync with GetNonTrivialInputTransactionIds.
    int nestedInputTransactionIndex = 0;
    for (const auto& path : GetInputTablePaths()) {
        auto table = New<TInputTable>(
            path,
            path.GetTransactionId()
            ? NestedInputTransactions[nestedInputTransactionIndex++]->GetId()
            : InputTransaction->GetId());
        table->ColumnRenameDescriptors = path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors());
        InputTables_.push_back(std::move(table));
    }

    InitOutputTables();

    if (auto stderrTablePath = GetStderrTablePath()) {
        StderrTable_ = New<TOutputTable>(*stderrTablePath, EOutputTableType::Stderr);
        DataFlowGraph_->RegisterVertex(TDataFlowGraph::StderrDescriptor);
    }

    if (auto coreTablePath = GetCoreTablePath()) {
        CoreTable_ = New<TOutputTable>(*coreTablePath, EOutputTableType::Core);
        DataFlowGraph_->RegisterVertex(TDataFlowGraph::CoreDescriptor);
    }

    InitUpdatingTables();

    for (const auto& userJobSpec : GetUserJobSpecs()) {
        auto& files = UserJobFiles_[userJobSpec];

        // Add regular files.
        for (const auto& path : userJobSpec->FilePaths) {
            files.push_back(TUserFile(
                path,
                path.GetTransactionId()
                ? NestedInputTransactions[nestedInputTransactionIndex++]->GetId()
                : InputTransaction->GetId(),
                false));
        }

        // Add layer files.
        auto layerPaths = GetLayerPaths(userJobSpec);
        for (const auto& path : layerPaths) {
            files.push_back(TUserFile(
                path,
                path.GetTransactionId()
                ? NestedInputTransactions[nestedInputTransactionIndex++]->GetId()
                : InputTransaction->GetId(),
                true));
        }
    }

    if (TLayerJobExperiment::IsEnabled(Spec_, GetUserJobSpecs()) && HasUserJobFiles()) {
        auto path = TRichYPath(*Spec_->JobExperiment->BaseLayerPath);
        if (path.GetTransactionId()) {
            THROW_ERROR_EXCEPTION("Transaction id is not supported for \"probing_base_layer_path\"");
        }
        BaseLayer_ = TUserFile(path, InputTransaction->GetId(), true);
    }

    auto maxInputTableCount = std::min(Config->MaxInputTableCount, Options->MaxInputTableCount);
    if (std::ssize(InputTables_) > maxInputTableCount) {
        THROW_ERROR_EXCEPTION(
            "Too many input tables: maximum allowed %v, actual %v",
            maxInputTableCount,
            InputTables_.size());
    }

    if (std::ssize(OutputTables_) > Config->MaxOutputTableCount) {
        THROW_ERROR_EXCEPTION(
            "Too many output tables: maximum allowed %v, actual %v",
            Config->MaxOutputTableCount,
            OutputTables_.size());
    }

    InitAccountResourceUsageLeases();

    DoInitialize();
}

void TOperationControllerBase::InitUnrecognizedSpec()
{
    UnrecognizedSpec_ = GetTypedSpec()->GetRecursiveUnrecognized();
}

void TOperationControllerBase::FillInitializeResult(TOperationControllerInitializeResult* result)
{
    result->Attributes.Mutable = BuildYsonStringFluently<EYsonType::MapFragment>()
        .Do(BIND(&TOperationControllerBase::BuildInitializeMutableAttributes, Unretained(this)))
        .Finish();
    result->Attributes.BriefSpec = BuildYsonStringFluently<EYsonType::MapFragment>()
        .Do(BIND(&TOperationControllerBase::BuildBriefSpec, Unretained(this)))
        .Finish();
    result->Attributes.FullSpec = ConvertToYsonString(Spec_);
    result->Attributes.UnrecognizedSpec = ConvertToYsonString(UnrecognizedSpec_);
    result->TransactionIds = GetTransactionIds();
    result->EraseOffloadingTrees = NeedEraseOffloadingTrees();
}

bool TOperationControllerBase::NeedEraseOffloadingTrees() const
{
    bool hasJobsAllowedForOffloading = false;
    auto userJobSpecs = GetUserJobSpecs();
    for (const auto& userJobSpec : userJobSpecs) {
        if (!userJobSpec->NetworkProject || Config->NetworkProjectsAllowedForOffloading.contains(*userJobSpec->NetworkProject)) {
            hasJobsAllowedForOffloading = true;
        }
    }
    return !userJobSpecs.empty() && !hasJobsAllowedForOffloading;
}

void TOperationControllerBase::ValidateIntermediateDataAccess(const TString& user, EPermission permission) const
{
    // Permission for IntermediateData can be only Read.
    YT_VERIFY(permission == EPermission::Read);
    Host->ValidateOperationAccess(user, EPermissionSet(permission));
}

void TOperationControllerBase::InitUpdatingTables()
{
    UpdatingTables_.clear();

    for (auto& table : OutputTables_) {
        UpdatingTables_.emplace_back(table);
    }

    if (StderrTable_) {
        UpdatingTables_.emplace_back(StderrTable_);
    }

    if (CoreTable_) {
        UpdatingTables_.emplace_back(CoreTable_);
    }
}

void TOperationControllerBase::InitializeOrchid()
{
    YT_LOG_DEBUG("Initializing orchid");

    using TLivePreviewMapService = NYTree::TCollectionBoundMapService<TLivePreviewMap>;
    LivePreviewService_ = New<TLivePreviewMapService>(std::weak_ptr<TLivePreviewMap>(LivePreviews_));

    auto createService = [&] (auto fluentMethod, const TString& key) {
        return IYPathService::FromProducer(BIND(
            [
                =,
                fluentMethod = std::move(fluentMethod),
                this,
                weakThis = MakeWeak(this)
            ] (IYsonConsumer* consumer) {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError, "Operation controller was destroyed");
                }

                YT_LOG_DEBUG(
                    "Handling orchid request in controller (Key: %v)",
                    key);

                BuildYsonFluently(consumer)
                    .Do(fluentMethod);
            }),
            Config->ControllerStaticOrchidUpdatePeriod
        );
    };

    // Methods like BuildProgress, BuildBriefProgress and buildJobsYson build map fragment,
    // so we have to enclose them with a map in order to pass into createService helper.
    // TODO(max42): get rid of this when GetOperationInfo is not stopping us from changing Build* signatures any more.
    auto wrapWithMap = [=] (auto fluentMethod) {
        return [=, fluentMethod = std::move(fluentMethod)] (TFluentAny fluent) {
            fluent
                .BeginMap()
                    .Do(fluentMethod)
                .EndMap();
        };
    };

    auto createServiceWithInvoker = [&] (auto fluentMethod, const TString& key) -> IYPathServicePtr {
        return createService(std::move(fluentMethod), key)
            ->Via(InvokerPool->GetInvoker(EOperationControllerQueue::Default));
    };

    auto createMapServiceWithInvoker = [&] (auto fluentMethod, const TString& key) -> IYPathServicePtr {
        return createServiceWithInvoker(wrapWithMap(std::move(fluentMethod)), key);
    };

    // NB: we may safely pass unretained this below as all the callbacks are wrapped with a createService helper
    // that takes care on checking the controller presence and properly replying in case it is already destroyed.
    auto service = New<TCompositeMapService>()
        ->AddChild(
            "progress",
            createMapServiceWithInvoker(BIND(&TOperationControllerBase::BuildProgress, Unretained(this)), "progress"))
        ->AddChild(
            "brief_progress",
            createMapServiceWithInvoker(BIND(&TOperationControllerBase::BuildBriefProgress, Unretained(this)), "brief_progress"))
        ->AddChild(
            "running_jobs",
            createMapServiceWithInvoker(BIND(&TOperationControllerBase::BuildJobsYson, Unretained(this)), "running_jobs"))
        ->AddChild(
            "retained_finished_jobs",
            createMapServiceWithInvoker(BIND(&TOperationControllerBase::BuildRetainedFinishedJobsYson, Unretained(this)), "retained_finished_jobs"))
        ->AddChild(
            "unavailable_input_chunks",
            createServiceWithInvoker(BIND(&TOperationControllerBase::BuildUnavailableInputChunksYson, Unretained(this)), "unavailable_input_chunks"))
        ->AddChild(
            "memory_usage",
            createService(BIND(&TOperationControllerBase::BuildMemoryUsageYson, Unretained(this)), "memory_usage"))
        ->AddChild(
            "state",
            createService(BIND(&TOperationControllerBase::BuildStateYson, Unretained(this)), "state"))
        ->AddChild(
            "data_flow_graph",
            DataFlowGraph_->GetService()
                ->WithPermissionValidator(BIND(&TOperationControllerBase::ValidateIntermediateDataAccess, MakeWeak(this))))
        ->AddChild(
            "live_previews",
            LivePreviewService_
                ->WithPermissionValidator(BIND(&TOperationControllerBase::ValidateIntermediateDataAccess, MakeWeak(this))))
        ->AddChild(
            "testing",
            createService(BIND(&TOperationControllerBase::BuildTestingState, Unretained(this)), "testing"));
    service->SetOpaque(false);
    Orchid_.Store(service
        ->Via(InvokerPool->GetInvoker(EOperationControllerQueue::Default)));

    YT_LOG_DEBUG("Orchid initialized");
}

void TOperationControllerBase::DoInitialize()
{ }

void TOperationControllerBase::LockInputs()
{
    // TODO(max42): why is this done during initialization?
    // Consider moving this call to preparation phase.
    PrepareInputTables();
    LockInputTables();
    LockUserFiles();
}

void TOperationControllerBase::SleepInPrepare()
{
    auto delay = Spec_->TestingOperationOptions->DelayInsidePrepare;
    if (delay) {
        TDelayedExecutor::WaitForDuration(*delay);
    }
}

void TOperationControllerBase::CreateOutputTables(
    const NApi::NNative::IClientPtr& client,
    const std::vector<TUserObject*>& tables,
    TTransactionId defaultTransactionId,
    EOutputTableType outputTableType,
    EObjectType desiredType)
{
    std::vector<TUserObject*> tablesToCreate;
    for (auto* table : tables) {
        if (table->Path.GetCreate()) {
            tablesToCreate.push_back(table);
        }
    }
    if (tablesToCreate.empty()) {
        return;
    }

    YT_LOG_DEBUG("Creating output tables (TableCount: %v, OutputTableType: %v)",
        tablesToCreate.size(),
        outputTableType);

    auto proxy = CreateObjectServiceWriteProxy(client);
    auto batchReq = proxy.ExecuteBatch();

    for (auto* table : tablesToCreate) {
        auto req = TCypressYPathProxy::Create(table->Path.GetPath());
        req->set_ignore_existing(true);
        req->set_type(ToProto<int>(desiredType));

        NCypressClient::SetTransactionId(req, table->TransactionId.value_or(defaultTransactionId));
        GenerateMutationId(req);

        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error creating output tables");
}

TOperationControllerPrepareResult TOperationControllerBase::SafePrepare()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    SleepInPrepare();

    // Testing purpose code.
    if (Config->EnableControllerFailureSpecOption &&
        Spec_->TestingOperationOptions->ControllerFailure)
    {
        YT_VERIFY(*Spec_->TestingOperationOptions->ControllerFailure !=
            EControllerFailureType::AssertionFailureInPrepare);
    }

    // Process input tables.
    if (!GetInputTablePaths().empty()) {
        GetInputTablesAttributes();
    } else {
        YT_LOG_INFO("Operation has no input tables");
    }

    PrepareInputQuery();

    // Process files.
    if (HasUserJobFiles()) {
        GetUserFilesAttributes();
    } else {
        YT_LOG_INFO("Operation has no input files");
    }

    // Process output and stderr tables.
    if (!OutputTables_.empty()) {
        auto userObjectList = MakeUserObjectList(OutputTables_);
        CreateOutputTables(
            OutputClient,
            userObjectList,
            OutputTransaction->GetId(),
            EOutputTableType::Output,
            GetOutputTableDesiredType());
        GetUserObjectBasicAttributes(
            OutputClient,
            userObjectList,
            OutputTransaction->GetId(),
            Logger,
            EPermission::Write);
    } else {
        YT_LOG_INFO("Operation has no output tables");
    }

    if (StderrTable_) {
        CreateOutputTables(
            Client,
            {StderrTable_.Get()},
            DebugTransaction->GetId(),
            EOutputTableType::Stderr,
            EObjectType::Table);
        GetUserObjectBasicAttributes(
            Client,
            {StderrTable_.Get()},
            DebugTransaction->GetId(),
            Logger,
            EPermission::Write);
    } else {
        YT_LOG_INFO("Operation has no stderr table");
    }

    if (CoreTable_) {
        CreateOutputTables(
            Client,
            {CoreTable_.Get()},
            DebugTransaction->GetId(),
            EOutputTableType::Core,
            EObjectType::Table);
        GetUserObjectBasicAttributes(
            Client,
            {CoreTable_.Get()},
            DebugTransaction->GetId(),
            Logger,
            EPermission::Write);
    } else {
        YT_LOG_INFO("Operation has no core table");
    }

    {
        ValidateUpdatingTablesTypes();

        THashSet<TObjectId> updatingTableIds;
        for (const auto& table : UpdatingTables_) {
            bool insertedNew = updatingTableIds.insert(table->ObjectId).second;
            if (!insertedNew) {
                THROW_ERROR_EXCEPTION("Output table %v is specified multiple times",
                    table->GetPath());
            }
        }

        GetOutputTablesSchema();

        std::vector<TTableSchemaPtr> outputTableSchemas;
        outputTableSchemas.resize(OutputTables_.size());
        for (int outputTableIndex = 0; outputTableIndex < std::ssize(OutputTables_); ++outputTableIndex) {
            const auto& table = OutputTables_[outputTableIndex];
            if (table->Dynamic) {
                outputTableSchemas[outputTableIndex] = table->TableUploadOptions.GetUploadSchema();
            }
        }

        PrepareOutputTables();

        for (int outputTableIndex = 0; outputTableIndex < std::ssize(OutputTables_); ++outputTableIndex) {
            const auto& table = OutputTables_[outputTableIndex];
            if (table->Dynamic && *outputTableSchemas[outputTableIndex] != *table->TableUploadOptions.GetUploadSchema()) {
                THROW_ERROR_EXCEPTION(
                    "Schema of output dynamic table %v unexpectedly changed during preparation phase. "
                    "Please send the link to the operation to yt-admin@",
                    table->Path)
                    << TErrorAttribute("original_schema", *outputTableSchemas[outputTableIndex])
                    << TErrorAttribute("upload_schema", *table->TableUploadOptions.GetUploadSchema());
            }
        }

        LockOutputTablesAndGetAttributes();
    }

    InitializeStandardStreamDescriptors();

    CustomPrepare();

    // NB: these calls must be after CustomPrepare() since some controllers may alter input table ranges
    // (e.g. TEraseController which inverts the user-provided range).
    InferInputRanges();
    InitInputStreamDirectory();

    YT_LOG_INFO("Operation prepared");

    TOperationControllerPrepareResult result;
    FillPrepareResult(&result);
    return result;
}

TOperationControllerMaterializeResult TOperationControllerBase::SafeMaterialize()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    TOperationControllerMaterializeResult result;

    try {
        PeakMemoryUsageUpdateExecutor->Start();

        // NB(coteeq): Allocate new chunk lists early, so we can actually schedule the first job,
        //             when scheduler gives us one.
        PickIntermediateDataCells();
        InitChunkListPools();

        FetchInputTables();
        FetchUserFiles();
        ValidateUserFileSizes();

        SuppressLivePreviewIfNeeded();


        CreateLivePreviewTables();

        CollectTotals();

        InitializeJobExperiment();

        AlertManager_->StartPeriodicActivity();

        CustomMaterialize();

        InitializeHistograms();

        InitializeSecurityTags();

        YT_LOG_INFO("Tasks prepared (RowBufferCapacity: %v)", RowBuffer->GetCapacity());

        if (IsCompleted()) {
            // Possible reasons:
            // - All input chunks are unavailable && Strategy == Skip
            // - Merge decided to teleport all input chunks
            // - Anything else?
            YT_LOG_INFO("No jobs needed");
            DoCompleteOperation(/*interrupted*/ false);
            return result;
        } else {
            RegisterUnavailableInputChunks();
            if (!UnavailableInputChunkIds.empty()) {
                YT_LOG_INFO("Found unavailable input chunks during materialization (UnavailableInputChunkCount: %v, SampleUnavailableInputChunkIds: %v)",
                    UnavailableInputChunkIds.size(),
                    MakeShrunkFormattableView(UnavailableInputChunkIds, TDefaultFormatter(), SampleChunkIdCount));
            }
        }

        UpdateAllTasks();

        if (Config->TestingOptions->EnableSnapshotCycleAfterMaterialization) {
            TStringStream stringStream;
            SaveSnapshot(&stringStream);
            TOperationSnapshot snapshot;
            snapshot.Version = ToUnderlying(GetCurrentSnapshotVersion());
            snapshot.Blocks = {TSharedRef::FromString(stringStream.Str())};
            DoLoadSnapshot(snapshot);
            AlertManager_->StartPeriodicActivity();
        }

        // Input chunk scraper initialization should be the last step to avoid races,
        // because input chunk scraper works in control thread.
        InitInputChunkScraper();
        InitIntermediateChunkScraper();

        UpdateMinNeededAllocationResources();
        // NB(eshcherbin): This update is done to ensure that needed resources amount is computed.
        UpdateAllTasks();

        CheckTimeLimitExecutor->Start();
        ProgressBuildExecutor_->Start();
        ExecNodesCheckExecutor->Start();
        SuspiciousJobsYsonUpdater_->Start();
        MinNeededResourcesSanityCheckExecutor->Start();
        ExecNodesUpdateExecutor->Start();
        CheckTentativeTreeEligibilityExecutor_->Start();
        UpdateAccountResourceUsageLeasesExecutor_->Start();
        RunningJobStatisticsUpdateExecutor_->Start();
        SendRunningAllocationTimeStatisticsUpdatesExecutor_->Start();

        if (auto maybeDelay = Spec_->TestingOperationOptions->DelayInsideMaterialize) {
            TDelayedExecutor::WaitForDuration(*maybeDelay);
        }

        if (State != EControllerState::Preparing) {
            return result;
        }
        State = EControllerState::Running;

        LogProgress(/*force*/ true);
    } catch (const std::exception& ex) {
        auto wrappedError = TError(EErrorCode::MaterializationFailed, "Materialization failed")
            << ex;
        YT_LOG_INFO(wrappedError);
        DoFailOperation(wrappedError);
        return result;
    }

    InitialMinNeededResources_ = GetMinNeededAllocationResources();

    result.Suspend = Spec_->SuspendOperationAfterMaterialization;
    result.InitialNeededResources = GetNeededResources();
    result.InitialMinNeededResources = InitialMinNeededResources_;

    YT_LOG_INFO("Materialization finished");

    OnOperationReady();

    return result;
}

void TOperationControllerBase::SaveSnapshot(IZeroCopyOutput* output)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TSaveContext context(output);

    Save(context, this);

    context.Finish();
}

void TOperationControllerBase::SleepInRevive()
{
    if (auto delay = Spec_->TestingOperationOptions->DelayInsideRevive) {
        TDelayedExecutor::WaitForDuration(*delay);
    }
}

TOperationControllerReviveResult TOperationControllerBase::Revive()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    // A fast path to stop revival if fail_on_job_restart = %true and
    // this is not a vanilla operation.
    ValidateRevivalAllowed();

    if (CleanStart) {
        TOperationControllerReviveResult result;
        result.RevivedFromSnapshot = false;
        result.ControllerEpoch = ControllerEpoch;
        static_cast<TOperationControllerPrepareResult&>(result) = Prepare();
        return result;
    }

    SleepInRevive();

    DoLoadSnapshot(Snapshot);

    // Once again check that revival is allowed (now having the loaded snapshot).
    ValidateSnapshot();

    Snapshot = TOperationSnapshot();

    TOperationControllerReviveResult result;
    result.RevivedFromSnapshot = true;
    result.ControllerEpoch = ControllerEpoch;
    result.RevivedBannedTreeIds = BannedTreeIds_;
    FillPrepareResult(&result);

    InitChunkListPools();

    SuppressLivePreviewIfNeeded();
    CreateLivePreviewTables();

    if (IsCompleted()) {
        DoCompleteOperation(/*interrupted*/ false);
        return result;
    }

    UpdateAllTasks();

    RegisterUnavailableInputChunks();

    // Input chunk scraper initialization should be the last step to avoid races.
    InitInputChunkScraper();
    InitIntermediateChunkScraper();

    if (UnavailableIntermediateChunkCount > 0) {
        IntermediateChunkScraper->Start();
    }

    UpdateMinNeededAllocationResources();
    // NB(eshcherbin): This update is done to ensure that needed resources amount is computed.
    UpdateAllTasks();

    result.NeededResources = GetNeededResources();

    ReinstallLivePreview();

    if (!Config->EnableJobRevival) {
        if (Spec_->FailOnJobRestart && !JobletMap.empty()) {
            OnJobUniquenessViolated(TError(
                NScheduler::EErrorCode::OperationFailedOnJobRestart,
                "Reviving operation without job revival; failing operation since \"fail_on_job_restart\" spec option is set")
                << TErrorAttribute("reason", EFailOnJobRestartReason::JobRevivalDisabled));
            return result;
        }

        AbortAllJoblets(EAbortReason::JobRevivalDisabled, /*honestly*/ true);
    }

    ShouldUpdateProgressAttributesInCypress_ = true;
    ShouldUpdateLightOperationAttributes_ = true;

    CheckTimeLimitExecutor->Start();
    ProgressBuildExecutor_->Start();
    ExecNodesCheckExecutor->Start();
    SuspiciousJobsYsonUpdater_->Start();
    AlertManager_->StartPeriodicActivity();
    MinNeededResourcesSanityCheckExecutor->Start();
    ExecNodesUpdateExecutor->Start();
    CheckTentativeTreeEligibilityExecutor_->Start();
    UpdateAccountResourceUsageLeasesExecutor_->Start();
    RunningJobStatisticsUpdateExecutor_->Start();
    SendRunningAllocationTimeStatisticsUpdatesExecutor_->Start();

    result.RevivedAllocations.reserve(std::size(JobletMap));

    for (const auto& [_, joblet] : JobletMap) {
        result.RevivedAllocations.push_back(TOperationControllerReviveResult::TRevivedAllocation{
            .AllocationId = AllocationIdFromJobId(joblet->JobId),
            .StartTime = joblet->StartTime,
            .PreemptibleProgressStartTime = joblet->NodeJobStartTime,
            .ResourceLimits = joblet->ResourceLimits,
            .DiskQuota = joblet->DiskQuota,
            .TreeId = joblet->TreeId,
            .NodeId = joblet->NodeDescriptor.Id,
            .NodeAddress = joblet->NodeDescriptor.Address,
        });
    }

    result.MinNeededResources = GetMinNeededAllocationResources();
    result.InitialMinNeededResources = InitialMinNeededResources_;

    // Monitoring tags are transient by design.
    // So after revive we do reset the corresponding alert.
    SetOperationAlert(EOperationAlertType::UserJobMonitoringLimited, TError());

    YT_LOG_INFO("Operation revived");

    State = EControllerState::Running;

    OnOperationReady();

    return result;
}

void TOperationControllerBase::AbortAllJoblets(EAbortReason abortReason, bool honestly)
{
    YT_LOG_DEBUG("Aborting all joblets (AbortReason: %v)", abortReason);

    std::vector<TJobToRelease> jobsToRelease;
    jobsToRelease.reserve(std::size(JobletMap));

    auto now = TInstant::Now();
    for (const auto& [_, joblet] : JobletMap) {
        auto jobSummary = TAbortedJobSummary(joblet->JobId, abortReason);
        jobSummary.FinishTime = now;
        UpdateJobletFromSummary(jobSummary, joblet);
        LogFinishedJobFluently(ELogEventType::JobAborted, joblet)
            .Item("reason").Value(abortReason);
        UpdateAggregatedFinishedJobStatistics(joblet, jobSummary);

        Host->GetJobProfiler()->ProfileAbortedJob(*joblet, jobSummary);

        if (honestly) {
            joblet->Task->OnJobAborted(joblet, jobSummary);
        }

        Host->AbortJob(
            joblet->JobId,
            abortReason);

        jobsToRelease.push_back({joblet->JobId, {}});
    }
    JobletMap.clear();

    if (!std::empty(jobsToRelease)) {
        YT_LOG_DEBUG(
            "Releasing aborted jobs (JobCount: %v)",
            std::size(jobsToRelease));

        Host->ReleaseJobs(std::move(jobsToRelease));
    }
}

bool TOperationControllerBase::IsTransactionNeeded(ETransactionType type) const
{
    switch (type) {
        case ETransactionType::Async:
            return IsLegacyIntermediateLivePreviewSupported() || IsLegacyOutputLivePreviewSupported() || GetStderrTablePath();
        case ETransactionType::Input:
            return !GetInputTablePaths().empty() || HasUserJobFiles() || HasDiskRequestsWithSpecifiedAccount();
        case ETransactionType::Output:
        case ETransactionType::OutputCompletion:
            // NB: cannot replace with OutputTables_.empty() here because output tables are not ready yet.
            return !GetOutputTablePaths().empty();
        case ETransactionType::Debug:
        case ETransactionType::DebugCompletion:
            return GetStderrTablePath() || GetCoreTablePath();
        default:
            YT_ABORT();
    }
}

ITransactionPtr TOperationControllerBase::AttachTransaction(
    TTransactionId transactionId,
    const NNative::IClientPtr& client,
    bool ping)
{
    TTransactionAttachOptions options;
    options.Ping = ping;
    options.PingAncestors = false;
    options.PingPeriod = Config->OperationTransactionPingPeriod;
    return client->AttachTransaction(transactionId, options);
}

void TOperationControllerBase::StartTransactions()
{
    std::vector<TFuture<NNative::ITransactionPtr>> asyncResults = {
        StartTransaction(ETransactionType::Async, Client),
        StartTransaction(ETransactionType::Input, InputClient, GetInputTransactionParentId()),
        StartTransaction(ETransactionType::Output, OutputClient, GetOutputTransactionParentId()),
        // NB: we do not start Debug transaction under User transaction since we want to save debug results
        // even if user transaction is aborted.
        StartTransaction(ETransactionType::Debug, Client),
    };

    THashMap<TTransactionId, int> inputTransactionIdToResultIndex;
    for (auto transactionId : GetNonTrivialInputTransactionIds()) {
        if (!inputTransactionIdToResultIndex.contains(transactionId)) {
            inputTransactionIdToResultIndex[transactionId] = asyncResults.size();
            asyncResults.push_back(StartTransaction(ETransactionType::Input, InputClient, transactionId));
        }
    }

    auto results = WaitFor(AllSet(asyncResults))
        .ValueOrThrow();

    {
        AsyncTransaction = results[0].ValueOrThrow();
        InputTransaction = results[1].ValueOrThrow();
        OutputTransaction = results[2].ValueOrThrow();
        DebugTransaction = results[3].ValueOrThrow();
        for (auto transactionId : GetNonTrivialInputTransactionIds()) {
            NestedInputTransactions.push_back(results[inputTransactionIdToResultIndex[transactionId]].ValueOrThrow());
        }
    }
}

void TOperationControllerBase::InitInputStreamDirectory()
{
    std::vector<NChunkPools::TInputStreamDescriptor> inputStreams;
    inputStreams.reserve(InputTables_.size());
    for (const auto& [tableIndex, inputTable] : Enumerate(InputTables_)) {
        for (const auto& [rangeIndex, range] : Enumerate(inputTable->Path.GetRanges())) {
            auto& descriptor = inputStreams.emplace_back(
                inputTable->Teleportable,
                inputTable->IsPrimary(),
                /*isVersioned*/ inputTable->Dynamic);
            descriptor.SetTableIndex(tableIndex);
            descriptor.SetRangeIndex(rangeIndex);
        }
    }
    InputStreamDirectory_ = TInputStreamDirectory(std::move(inputStreams));

    YT_LOG_INFO("Input stream directory prepared (InputStreamCount: %v)", InputStreamDirectory_.GetDescriptorCount());
}

const TInputStreamDirectory& TOperationControllerBase::GetInputStreamDirectory() const
{
    return InputStreamDirectory_;
}

int TOperationControllerBase::GetPrimaryInputTableCount() const
{
    return std::count_if(
        InputTables_.begin(),
        InputTables_.end(),
        [] (const TInputTablePtr& table) { return table->IsPrimary(); });
}

IFetcherChunkScraperPtr TOperationControllerBase::CreateFetcherChunkScraper() const
{
    return Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Wait
        ? NChunkClient::CreateFetcherChunkScraper(
            Config->ChunkScraper,
            GetCancelableInvoker(),
            Host->GetChunkLocationThrottlerManager(),
            InputClient,
            InputNodeDirectory_,
            Logger)
        : nullptr;
}

TTransactionId TOperationControllerBase::GetInputTransactionParentId()
{
    return UserTransactionId;
}

TTransactionId TOperationControllerBase::GetOutputTransactionParentId()
{
    return UserTransactionId;
}

TAutoMergeDirector* TOperationControllerBase::GetAutoMergeDirector()
{
    return AutoMergeDirector_.get();
}

TFuture<NNative::ITransactionPtr> TOperationControllerBase::StartTransaction(
    ETransactionType type,
    const NNative::IClientPtr& client,
    TTransactionId parentTransactionId)
{
    if (!IsTransactionNeeded(type)) {
        YT_LOG_INFO("Skipping transaction as it is not needed (Type: %v)", type);
        return MakeFuture(NNative::ITransactionPtr());
    }

    auto collectTableCellTags = [] (const std::vector<TOutputTablePtr>& tables) {
        TCellTagList result;

        for (const auto& table : tables) {
            if (!table) {
                continue;
            }

            if (table->ExternalCellTag == NObjectClient::InvalidCellTag) {
                result.push_back(CellTagFromId(table->ObjectId));
            } else {
                result.push_back(table->ExternalCellTag);
            }

            for (const auto& [chunkStripeKey, id] : table->OutputChunkTreeIds) {
                if (TypeFromId(id) == EObjectType::ChunkList) {
                    continue;
                }
                result.push_back(CellTagFromId(id));
            }
        }

        return result;
    };

    TCellTagList replicateToCellTags;
    switch (type) {
        // NB: these transactions are started when no basic attributes have been
        // fetched yet and collecting cell tags is therefore useless.
        case ETransactionType::Async:
        case ETransactionType::Input:
        case ETransactionType::Output:
        case ETransactionType::Debug:
            break;

        case ETransactionType::OutputCompletion:
            replicateToCellTags = collectTableCellTags(OutputTables_);
            break;

        case ETransactionType::DebugCompletion:
            replicateToCellTags = collectTableCellTags({StderrTable_, CoreTable_});
            break;

        default:
            YT_ABORT();
    }
    SortUnique(replicateToCellTags);

    YT_LOG_INFO("Starting transaction (Type: %v, ParentId: %v, ReplicateToCellTags: %v)",
        type,
        parentTransactionId,
        replicateToCellTags);

    TTransactionStartOptions options;
    options.AutoAbort = false;
    options.PingAncestors = false;
    auto attributes = CreateEphemeralAttributes();
    attributes->Set(
        "title",
        Format("Scheduler %Qlv transaction for operation %v",
            type,
            OperationId));
    attributes->Set("operation_id", OperationId);
    if (Spec_->Title) {
        attributes->Set("operation_title", Spec_->Title);
    }
    attributes->Set("operation_type", GetOperationType());
    options.Attributes = std::move(attributes);
    options.ParentId = parentTransactionId;
    options.Timeout = Config->OperationTransactionTimeout;
    options.PingPeriod = Config->OperationTransactionPingPeriod;
    options.ReplicateToMasterCellTags = std::move(replicateToCellTags);

    auto transactionFuture = client->StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        options);

    return transactionFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<NNative::ITransactionPtr>& transactionOrError) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            transactionOrError,
            "Error starting %Qlv transaction",
            type);

        auto transaction = transactionOrError.Value();

        YT_LOG_INFO("Transaction started (Type: %v, TransactionId: %v)",
            type,
            transaction->GetId());

        return transaction;
    }));
}

void TOperationControllerBase::PickIntermediateDataCells()
{
    if (GetOutputTablePaths().empty()) {
        return;
    }

    WaitForFast(OutputClient
        ->GetNativeConnection()
        ->GetMasterCellDirectorySynchronizer()
        ->RecentSync())
        .ThrowOnError();

    IntermediateOutputCellTagList = OutputClient
        ->GetNativeConnection()
        ->GetMasterCellDirectory()
        ->GetMasterCellTagsWithRole(NCellMasterClient::EMasterCellRole::ChunkHost);
    if (IntermediateOutputCellTagList.empty()) {
        THROW_ERROR_EXCEPTION("No master cells with chunk host role found");
    }

    int intermediateDataCellCount = std::min<int>(Config->IntermediateOutputMasterCellCount, IntermediateOutputCellTagList.size());
    // TODO(max42, gritukan): Remove it when new live preview will be ready.
    if (IsLegacyIntermediateLivePreviewSupported()) {
        intermediateDataCellCount = 1;
    }

    PartialShuffle(
        IntermediateOutputCellTagList.begin(),
        IntermediateOutputCellTagList.begin() + intermediateDataCellCount,
        IntermediateOutputCellTagList.end());
    IntermediateOutputCellTagList.resize(intermediateDataCellCount);

    YT_LOG_DEBUG("Intermediate data cells picked (CellTags: %v)",
        IntermediateOutputCellTagList);
}

void TOperationControllerBase::InitChunkListPools()
{
    if (!GetOutputTablePaths().empty()) {
        OutputChunkListPool_ = New<TChunkListPool>(
            Config,
            OutputClient,
            CancelableInvokerPool,
            OperationId,
            OutputTransaction->GetId());

        CellTagToRequiredOutputChunkListCount_.clear();
        for (const auto& table : OutputTables_) {
            ++CellTagToRequiredOutputChunkListCount_[table->ExternalCellTag];
        }

        for (auto cellTag : IntermediateOutputCellTagList) {
            ++CellTagToRequiredOutputChunkListCount_[cellTag];
        }
    }

    if (DebugTransaction) {
        DebugChunkListPool_ = New<TChunkListPool>(
            Config,
            OutputClient,
            CancelableInvokerPool,
            OperationId,
            DebugTransaction->GetId());
    }

    CellTagToRequiredDebugChunkListCount_.clear();
    if (StderrTable_) {
        ++CellTagToRequiredDebugChunkListCount_[StderrTable_->ExternalCellTag];
    }
    if (CoreTable_) {
        ++CellTagToRequiredDebugChunkListCount_[CoreTable_->ExternalCellTag];
    }

    YT_LOG_DEBUG("Preallocating chunk lists");
    for (const auto& [cellTag, count] : CellTagToRequiredOutputChunkListCount_) {
        Y_UNUSED(OutputChunkListPool_->HasEnough(cellTag, count));
    }
    for (const auto& [cellTag, count] : CellTagToRequiredDebugChunkListCount_) {
        YT_VERIFY(DebugChunkListPool_);
        Y_UNUSED(DebugChunkListPool_->HasEnough(cellTag, count));
    }
}

void TOperationControllerBase::InitInputChunkScraper()
{
    THashSet<TChunkId> chunkIds;
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap) {
        if (!IsDynamicTabletStoreType(TypeFromId(chunkId))) {
            chunkIds.insert(chunkId);
        }
    }

    YT_VERIFY(!InputChunkScraper);
    InputChunkScraper = New<TChunkScraper>(
        Config->ChunkScraper,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        Host->GetChunkLocationThrottlerManager(),
        InputClient,
        InputNodeDirectory_,
        std::move(chunkIds),
        BIND(&TThis::OnInputChunkLocated, MakeWeak(this)),
        Logger);

    if (!UnavailableInputChunkIds.empty()) {
        YT_LOG_INFO("Waiting for unavailable input chunks (Count: %v, SampleIds: %v)",
            UnavailableInputChunkIds.size(),
            MakeShrunkFormattableView(UnavailableInputChunkIds, TDefaultFormatter(), SampleChunkIdCount));
        InputChunkScraper->Start();
    }
}

void TOperationControllerBase::InitIntermediateChunkScraper()
{
    IntermediateChunkScraper = New<TIntermediateChunkScraper>(
        Config->ChunkScraper,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        CancelableInvokerPool,
        Host->GetChunkLocationThrottlerManager(),
        InputClient,
        InputNodeDirectory_,
        [weakThis = MakeWeak(this)] () {
            if (auto this_ = weakThis.Lock()) {
                return this_->GetAliveIntermediateChunks();
            } else {
                return THashSet<TChunkId>();
            }
        },
        BIND(&TThis::OnIntermediateChunkLocated, MakeWeak(this)),
        Logger);
}

bool TOperationControllerBase::TryInitAutoMerge(int outputChunkCountEstimate)
{
    AutoMergeEnabled_.resize(OutputTables_.size(), false);

    const auto& autoMergeSpec = Spec_->AutoMerge;
    auto mode = autoMergeSpec->Mode;

    if (mode == EAutoMergeMode::Disabled) {
        return false;
    }

    auto autoMergeError = GetAutoMergeError();
    if (!autoMergeError.IsOK()) {
        SetOperationAlert(EOperationAlertType::AutoMergeDisabled, autoMergeError);
        return false;
    }

    i64 maxIntermediateChunkCount;
    i64 chunkCountPerMergeJob;
    switch (mode) {
        case EAutoMergeMode::Relaxed:
            maxIntermediateChunkCount = std::numeric_limits<int>::max() / 4;
            chunkCountPerMergeJob = 500;
            break;
        case EAutoMergeMode::Economy:
            maxIntermediateChunkCount = std::max(500, static_cast<int>(2.5 * sqrt(outputChunkCountEstimate)));
            chunkCountPerMergeJob = maxIntermediateChunkCount / 10;
            maxIntermediateChunkCount *= OutputTables_.size();
            break;
        case EAutoMergeMode::Manual:
            maxIntermediateChunkCount = *autoMergeSpec->MaxIntermediateChunkCount;
            chunkCountPerMergeJob = *autoMergeSpec->ChunkCountPerMergeJob;
            break;
        default:
            YT_ABORT();
    }
    i64 desiredChunkSize = autoMergeSpec->JobIO->TableWriter->DesiredChunkSize;
    auto upperWeightLimit = std::min<i64>(autoMergeSpec->JobIO->TableWriter->DesiredChunkWeight, Spec_->MaxDataWeightPerJob / 2);
    i64 desiredChunkDataWeight = std::clamp<i64>(desiredChunkSize / InputCompressionRatio, 1, upperWeightLimit);
    i64 dataWeightPerJob = desiredChunkDataWeight;

    // NB: if row count limit is set on any output table, we do not
    // enable auto merge as it prematurely stops the operation
    // because wrong statistics are currently used when checking row count.
    for (int index = 0; index < std::ssize(OutputTables_); ++index) {
        if (OutputTables_[index]->Path.GetRowCountLimit()) {
            YT_LOG_INFO("Output table has row count limit, force disabling auto merge (TableIndex: %v)", index);
            auto error = TError("Output table has row count limit, force disabling auto merge")
                << TErrorAttribute("table_index", index);
            SetOperationAlert(EOperationAlertType::AutoMergeDisabled, error);
            return false;
        }
    }

    YT_LOG_INFO("Auto merge parameters calculated ("
        "Mode: %v, OutputChunkCountEstimate: %v, MaxIntermediateChunkCount: %v, ChunkCountPerMergeJob: %v, "
        "ChunkSizeThreshold: %v, DesiredChunkSize: %v, DesiredChunkDataWeight: %v, IntermediateChunkUnstageMode: %v)",
        mode,
        outputChunkCountEstimate,
        maxIntermediateChunkCount,
        chunkCountPerMergeJob,
        autoMergeSpec->ChunkSizeThreshold,
        desiredChunkSize,
        desiredChunkDataWeight,
        GetIntermediateChunkUnstageMode());

    AutoMergeDirector_ = std::make_unique<TAutoMergeDirector>(
        maxIntermediateChunkCount,
        chunkCountPerMergeJob,
        Logger);

    bool sortedOutputAutoMergeRequired = false;

    const auto standardStreamDescriptors = GetStandardStreamDescriptors();

    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors;
    outputStreamDescriptors.reserve(OutputTables_.size());
    for (int index = 0; index < std::ssize(OutputTables_); ++index) {
        const auto& outputTable = OutputTables_[index];
        if (outputTable->Path.GetAutoMerge()) {
            if (outputTable->TableUploadOptions.TableSchema->IsSorted()) {
                sortedOutputAutoMergeRequired = true;
            } else {
                auto streamDescriptor = standardStreamDescriptors[index]->Clone();
                // Auto-merge jobs produce single output, so we override the table
                // index in writer options with 0.
                streamDescriptor->TableWriterOptions = CloneYsonStruct(streamDescriptor->TableWriterOptions);
                streamDescriptor->TableWriterOptions->TableIndex = 0;
                outputStreamDescriptors.push_back(std::move(streamDescriptor));
                AutoMergeEnabled_[index] = true;
            }
        }
    }

    bool autoMergeEnabled = !outputStreamDescriptors.empty();
    if (autoMergeEnabled) {
        AutoMergeTask_ = New<TAutoMergeTask>(
            /*taskHost*/ this,
            chunkCountPerMergeJob,
            autoMergeSpec->ChunkSizeThreshold,
            dataWeightPerJob,
            Spec_->MaxDataWeightPerJob,
            std::move(outputStreamDescriptors),
            std::vector<TInputStreamDescriptorPtr>{});
        RegisterTask(AutoMergeTask_);
    }

    if (sortedOutputAutoMergeRequired && !autoMergeEnabled) {
        auto error = TError("Sorted output with auto merge is not supported for now, it will be done in YT-8024");
        SetOperationAlert(EOperationAlertType::AutoMergeDisabled, error);
    }

    return autoMergeEnabled;
}

NLogging::TLogger TOperationControllerBase::GetLogger() const
{
    return Logger;
}

IYPathServicePtr TOperationControllerBase::BuildZombieOrchid()
{
    IYPathServicePtr orchid;
    if (auto controllerOrchid = GetOrchid()) {
        auto ysonOrError = WaitFor(AsyncYPathGet(controllerOrchid, ""));
        if (!ysonOrError.IsOK()) {
            return nullptr;
        }
        auto yson = ysonOrError.Value();
        if (!yson) {
            return nullptr;
        }
        auto producer = TYsonProducer(BIND([yson = std::move(yson)] (IYsonConsumer* consumer) {
            consumer->OnRaw(yson);
        }));
        orchid = IYPathService::FromProducer(std::move(producer))
            ->Via(Host->GetControllerThreadPoolInvoker());
    }
    return orchid;
}

std::vector<TOutputStreamDescriptorPtr> TOperationControllerBase::GetAutoMergeStreamDescriptors()
{
    auto streamDescriptors = GetStandardStreamDescriptors();
    YT_VERIFY(GetAutoMergeDirector());

    std::optional<TString> intermediateDataAccount;
    if (Spec_->AutoMerge->UseIntermediateDataAccount) {
        ValidateAccountPermission(Spec_->IntermediateDataAccount, EPermission::Use);
        intermediateDataAccount = Spec_->IntermediateDataAccount;
    }

    int autoMergeTaskTableIndex = 0;
    for (int index = 0; index < std::ssize(streamDescriptors); ++index) {
        if (AutoMergeEnabled_[index]) {
            streamDescriptors[index] = streamDescriptors[index]->Clone();
            streamDescriptors[index]->DestinationPool = AutoMergeTask_->GetChunkPoolInput();
            streamDescriptors[index]->ChunkMapping = AutoMergeTask_->GetChunkMapping();
            streamDescriptors[index]->ImmediatelyUnstageChunkLists = true;
            streamDescriptors[index]->RequiresRecoveryInfo = true;
            streamDescriptors[index]->IsFinalOutput = false;
            // NB. The vertex descriptor for auto merge task must be empty, as TAutoMergeTask builds both input
            // and output edges. The underlying operation must not build an output edge, as it doesn't know
            // whether the resulting vertex is shallow_auto_merge or auto_merge.
            streamDescriptors[index]->TargetDescriptor = TDataFlowGraph::TVertexDescriptor();
            streamDescriptors[index]->PartitionTag = autoMergeTaskTableIndex++;
            if (intermediateDataAccount) {
                streamDescriptors[index]->TableWriterOptions->Account = *intermediateDataAccount;
            }
        }
    }
    return streamDescriptors;
}

THashSet<TChunkId> TOperationControllerBase::GetAliveIntermediateChunks() const
{
    THashSet<TChunkId> intermediateChunks;

    for (const auto& [chunkId, job] : ChunkOriginMap) {
        if (!job->Suspended || !job->Restartable) {
            intermediateChunks.insert(chunkId);
        }
    }

    return intermediateChunks;
}

void TOperationControllerBase::ReinstallLivePreview()
{
    if (IsLegacyOutputLivePreviewSupported()) {
        for (const auto& table : OutputTables_) {
            std::vector<TChunkTreeId> childIds;
            childIds.reserve(table->OutputChunkTreeIds.size());
            for (const auto& [key, chunkTreeId] : table->OutputChunkTreeIds) {
                childIds.push_back(chunkTreeId);
            }
            YT_UNUSED_FUTURE(Host->AttachChunkTreesToLivePreview(
                AsyncTransaction->GetId(),
                table->LivePreviewTableId,
                childIds));
        }
    }

    if (IsLegacyIntermediateLivePreviewSupported()) {
        std::vector<TChunkTreeId> childIds;
        childIds.reserve(ChunkOriginMap.size());
        for (const auto& [chunkId, job] : ChunkOriginMap) {
            if (!job->Suspended) {
                childIds.push_back(chunkId);
            }
        }
        YT_UNUSED_FUTURE(Host->AttachChunkTreesToLivePreview(
            AsyncTransaction->GetId(),
            IntermediateTable->LivePreviewTableId,
            childIds));
    }
}

void TOperationControllerBase::DoLoadSnapshot(const TOperationSnapshot& snapshot)
{
    YT_LOG_INFO("Started loading snapshot (Size: %v, BlockCount: %v, Version: %v)",
        GetByteSize(snapshot.Blocks),
        snapshot.Blocks.size(),
        snapshot.Version);

    // Deserialization errors must be fatal.
    TCrashOnDeserializationErrorGuard crashOnDeserializationErrorGuard;

    // Snapshot loading must be synchronous.
    TOneShotContextSwitchGuard oneShotContextSwitchGuard(
        BIND([this, this_ = MakeStrong(this)] {
            TStringBuilder stackTrace;
            DumpStackTrace([&stackTrace] (TStringBuf str) {
                stackTrace.AppendString(str);
            });
            YT_LOG_WARNING("Context switch while loading snapshot (StackTrace: %v)",
                stackTrace.Flush());
        }));

    TChunkedInputStream input(snapshot.Blocks);

    TLoadContext context(
        &input,
        RowBuffer,
        static_cast<ESnapshotVersion>(snapshot.Version));

    NPhoenix::TSerializer::InplaceLoad(context, this);

    for (const auto& task : Tasks) {
        task->Initialize();
    }
    InitializeOrchid();

    YT_LOG_INFO("Finished loading snapshot");
}

void TOperationControllerBase::StartOutputCompletionTransaction()
{
    if (!OutputTransaction) {
        return;
    }

    OutputCompletionTransaction = WaitFor(StartTransaction(
        ETransactionType::OutputCompletion,
        OutputClient,
        OutputTransaction->GetId()))
        .ValueOrThrow();

    // Set transaction id to Cypress.
    {
        const auto& client = Host->GetClient();
        auto proxy = CreateObjectServiceWriteProxy(client);

        auto path = GetOperationPath(OperationId) + "/@output_completion_transaction_id";
        auto req = TYPathProxy::Set(path);
        req->set_value(ConvertToYsonStringNestingLimited(OutputCompletionTransaction->GetId()).ToString());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }
}

void TOperationControllerBase::CommitOutputCompletionTransaction()
{
    auto outputCompletionTransactionId = OutputCompletionTransaction
        ? OutputCompletionTransaction->GetId()
        : NullTransactionId;

    YT_LOG_INFO("Committing output completion transaction and setting committed attribute (TransactionId: %v)",
        outputCompletionTransactionId);

    auto setCommittedViaCypressTransactionAction = GetConfig()->SetCommittedAttributeViaTransactionAction;

    auto fetchAttributeAsObjectId = [&, this] (
        const TYPath& path,
        const TString& attribute,
        TTransactionId transactionId = NullTransactionId)
    {
        auto getRequest = TYPathProxy::Get(Format("%v/@%v", path, attribute));
        if (transactionId != NullTransactionId) {
            SetTransactionId(getRequest, transactionId);
        }

        auto proxy = CreateObjectServiceReadProxy(
            Host->GetClient(),
            EMasterChannelKind::Follower);
        auto getResponse = WaitFor(proxy.Execute(getRequest))
            .ValueOrThrow();

        return ConvertTo<TObjectId>(TYsonString(getResponse->value()));
    };

    auto operationCypressNodeId = fetchAttributeAsObjectId(
        GetOperationPath(OperationId),
        IdAttributeName,
        outputCompletionTransactionId);
    if (setCommittedViaCypressTransactionAction && OutputCompletionTransaction) {
        // NB: Transaction action cannot be executed on node's native cell
        // directly because it leads to distributed transaction commit which
        // cannot be done with prerequisites.

        NNative::NProto::TReqSetAttributeOnTransactionCommit action;
        ToProto(action.mutable_node_id(), operationCypressNodeId);
        action.set_attribute(CommittedAttribute);
        action.set_value(ConvertToYsonStringNestingLimited(true).ToString());

        auto transactionCoordinatorCellTag = CellTagFromId(OutputCompletionTransaction->GetId());
        auto connection = Client->GetNativeConnection();
        OutputCompletionTransaction->AddAction(
            connection->GetMasterCellId(transactionCoordinatorCellTag),
            MakeTransactionActionData(action));
    } else {
        auto proxy = CreateObjectServiceWriteProxy(Host->GetClient());

        auto path = GetOperationPath(OperationId) + "/@" + CommittedAttribute;
        auto req = TYPathProxy::Set(path);
        SetTransactionId(req, outputCompletionTransactionId);
        req->set_value(ConvertToYsonStringNestingLimited(true).ToString());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }

    if (OutputCompletionTransaction) {
        // NB: Every set to `@committed` acquires lock which is promoted to
        // user's transaction on scheduler's transaction commit. To avoid this
        // we manually merge branched node and detach it from transaction.

        std::optional<TTransactionId> parentTransactionId;
        if (Config->CommitOperationCypressNodeChangesViaSystemTransaction &&
            !setCommittedViaCypressTransactionAction)
        {
            parentTransactionId = fetchAttributeAsObjectId(
                FromObjectId(outputCompletionTransactionId),
                ParentIdAttributeName);
        }

        TTransactionCommitOptions options;
        options.PrerequisiteTransactionIds.push_back(IncarnationIdToTransactionId(Host->GetIncarnationId()));

        WaitFor(OutputCompletionTransaction->Commit(options))
            .ThrowOnError();
        OutputCompletionTransaction.Reset();

        if (parentTransactionId) {
            ManuallyMergeBranchedCypressNode(operationCypressNodeId, *parentTransactionId);
        }
    }

    CommitFinished = true;

    YT_LOG_INFO("Output completion transaction committed and committed attribute set (TransactionId: %v)",
        outputCompletionTransactionId);
}

void TOperationControllerBase::ManuallyMergeBranchedCypressNode(
    NCypressClient::TNodeId nodeId,
    TTransactionId transactionId)
{
    YT_LOG_DEBUG(
        "Trying to merge operation Cypress node manually to reduce transaction lock count "
        "(CypressNodeId: %v, TransactionId: %v, OperationId: %v)",
        nodeId,
        transactionId,
        OperationId);

    try {
        // It is just a way to run custom logic at master.
        // Note that output completion transaction cannot be used here because
        // it's a Cypress transaction while a system is required to run
        // transaction actions.
        auto helperTransaction = WaitFor(Host->GetClient()->StartNativeTransaction(
            NTransactionClient::ETransactionType::Master,
            {
                .CoordinatorMasterCellTag = CellTagFromId(nodeId),
                .StartCypressTransaction = false,
            }))
            .ValueOrThrow();
        NNative::NProto::TReqMergeToTrunkAndUnlockNode reqCommitBranchNode;
        ToProto(reqCommitBranchNode.mutable_transaction_id(), transactionId);
        ToProto(reqCommitBranchNode.mutable_node_id(), nodeId);
        helperTransaction->AddAction(
            Client->GetNativeConnection()->GetMasterCellId(CellTagFromId(nodeId)),
            MakeTransactionActionData(reqCommitBranchNode));

        TTransactionCommitOptions options;
        options.PrerequisiteTransactionIds = {transactionId};
        WaitFor(helperTransaction->Commit(options))
            .ThrowOnError();
    } catch (const std::exception& ex) {
        YT_LOG_ALERT(ex,
            "Failed to manually merge branched operation Cypress node (CypressNodeId: %v, TransactionId: %v, OperationId: %v)",
            nodeId,
            transactionId,
            OperationId);
    }
}

void TOperationControllerBase::StartDebugCompletionTransaction()
{
    if (!DebugTransaction) {
        return;
    }

    DebugCompletionTransaction = WaitFor(StartTransaction(
        ETransactionType::DebugCompletion,
        OutputClient,
        DebugTransaction->GetId()))
        .ValueOrThrow();

    // Set transaction id to Cypress.
    {
        auto proxy = CreateObjectServiceWriteProxy(Host->GetClient());

        auto path = GetOperationPath(OperationId) + "/@debug_completion_transaction_id";
        auto req = TYPathProxy::Set(path);
        req->set_value(ConvertToYsonStringNestingLimited(DebugCompletionTransaction->GetId()).ToString());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }
}

void TOperationControllerBase::CommitDebugCompletionTransaction()
{
    if (!DebugTransaction) {
        return;
    }

    auto debugCompletionTransactionId = DebugCompletionTransaction->GetId();

    YT_LOG_INFO("Committing debug completion transaction (TransactionId: %v)",
        debugCompletionTransactionId);

    TTransactionCommitOptions options;
    options.PrerequisiteTransactionIds.push_back(IncarnationIdToTransactionId(Host->GetIncarnationId()));
    WaitFor(DebugCompletionTransaction->Commit(options))
        .ThrowOnError();
    DebugCompletionTransaction.Reset();

    YT_LOG_INFO("Debug completion transaction committed (TransactionId: %v)",
        debugCompletionTransactionId);
}

void TOperationControllerBase::SleepInCommitStage(EDelayInsideOperationCommitStage desiredStage)
{
    auto delay = Spec_->TestingOperationOptions->DelayInsideOperationCommit;
    auto stage = Spec_->TestingOperationOptions->DelayInsideOperationCommitStage;
    auto skipOnSecondEntrance = Spec_->TestingOperationOptions->NoDelayOnSecondEntranceToCommit;

    {
        auto proxy = CreateObjectServiceWriteProxy(Host->GetClient());

        auto path = GetOperationPath(OperationId) + "/@testing";
        auto req = TYPathProxy::Get(path);
        auto rspOrError = WaitFor(proxy.Execute(req));
        if (rspOrError.IsOK()) {
            auto rspNode = ConvertToNode(NYson::TYsonString(rspOrError.ValueOrThrow()->value()));
            CommitSleepStarted_ = rspNode->AsMap()->GetChildValueOrThrow<bool>("commit_sleep_started");
        }
    }

    if (delay && stage && *stage == desiredStage && (!CommitSleepStarted_ || !skipOnSecondEntrance)) {
        CommitSleepStarted_ = true;
        TDelayedExecutor::WaitForDuration(*delay);
    }
}

i64 TOperationControllerBase::GetPartSize(EOutputTableType tableType)
{
    if (tableType == EOutputTableType::Stderr) {
        return GetStderrTableWriterConfig()->MaxPartSize;
    }
    if (tableType == EOutputTableType::Core) {
        return GetCoreTableWriterConfig()->MaxPartSize;
    }

    YT_ABORT();
}

void TOperationControllerBase::BuildFeatureYson(TFluentAny fluent) const
{
    fluent.BeginList()
        .Item().Value(ControllerFeatures_)
        .DoFor(Tasks, [] (TFluentList fluent, const TTaskPtr& task) {
            fluent.Item().Do(BIND(&TTask::BuildFeatureYson, task));
        })
    .EndList();
}

void TOperationControllerBase::CommitFeatures()
{
    LogStructuredEventFluently(ControllerFeatureStructuredLogger, NLogging::ELogLevel::Info)
        .Item("operation_id").Value(ToString(GetOperationId()))
        .Item("start_time").Value(StartTime_)
        .Item("finish_time").Value(FinishTime_)
        .Item("experiment_assignment_names").DoListFor(
            ExperimentAssignments_,
            [] (TFluentList fluent, const TExperimentAssignmentPtr& experiment) {
                fluent.Item().Value(experiment->GetName());
            })
        .Item("features").Do(
            BIND(&TOperationControllerBase::BuildFeatureYson, Unretained(this)));

    auto featureYson = BuildYsonStringFluently().Do(
        BIND(&TOperationControllerBase::BuildFeatureYson, Unretained(this)));
    ValidateYson(featureYson, GetYsonNestingLevelLimit());

    WaitFor(Host->UpdateControllerFeatures(featureYson))
        .ThrowOnError();
}

void TOperationControllerBase::FinalizeFeatures()
{
    FinishTime_ = TInstant::Now();
    for (const auto& task : Tasks) {
        task->FinalizeFeatures();
    }

    ControllerFeatures_.AddTag("authenticated_user", GetAuthenticatedUser());
    ControllerFeatures_.AddTag("operation_type", GetOperationType());
    ControllerFeatures_.AddTag("total_estimated_input_data_weight", TotalEstimatedInputDataWeight);
    ControllerFeatures_.AddTag("total_estimated_input_row_count", TotalEstimatedInputRowCount);
    ControllerFeatures_.AddTag("total_estimated_input_value_count", TotalEstimatedInputValueCount);
    ControllerFeatures_.AddTag("total_estimated_input_chunk_count", TotalEstimatedInputChunkCount);
    ControllerFeatures_.AddTag("total_estimated_input_compressed_data_size", TotalEstimatedInputCompressedDataSize);
    ControllerFeatures_.AddTag("total_estimated_input_uncompressed_data_size", TotalEstimatedInputUncompressedDataSize);
    ControllerFeatures_.AddTag("total_job_count", GetTotalJobCount());

    ControllerFeatures_.AddSingular("operation_count", 1);
    ControllerFeatures_.AddSingular("wall_time", (FinishTime_ - StartTime_).MilliSeconds());
    ControllerFeatures_.AddSingular("peak_controller_memory_usage", PeakMemoryUsage_);

    ControllerFeatures_.AddSingular(
        "job_count",
        BuildYsonNodeFluently().Value(GetTotalJobCounter()));
}

void TOperationControllerBase::SafeCommit()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    YT_LOG_INFO("Committing results");

    SleepInCommitStage(EDelayInsideOperationCommitStage::Start);

    RemoveRemainingJobsOnOperationFinished();

    FinalizeFeatures();

    StartOutputCompletionTransaction();
    StartDebugCompletionTransaction();

    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage1);
    BeginUploadOutputTables(UpdatingTables_);
    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage2);
    TeleportOutputChunks();
    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage3);
    AttachOutputChunks(UpdatingTables_);
    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage4);
    EndUploadOutputTables(UpdatingTables_);
    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage5);

    CustomCommit();
    CommitFeatures();

    LockOutputDynamicTables();
    CommitOutputCompletionTransaction();
    CommitDebugCompletionTransaction();
    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage6);
    CommitTransactions();

    CancelableContext->Cancel(TError("Operation committed"));

    YT_LOG_INFO("Results committed");
}

void TOperationControllerBase::LockOutputDynamicTables()
{
    if (auto explicitOption = Spec_->LockOutputDynamicTables) {
        if (!*explicitOption) {
            YT_LOG_DEBUG("Will not lock output dynamic tables since locking is disabled in spec");
            return;
        }
    } else {
        if (Spec_->Atomicity == EAtomicity::None && !Config->LockNonAtomicOutputDynamicTables) {
            YT_LOG_DEBUG("Will not lock output tables with atomicity %Qlv", EAtomicity::None);
            return;
        }
    }

    if (OperationType == EOperationType::RemoteCopy) {
        return;
    }

    THashMap<TCellTag, std::vector<TOutputTablePtr>> externalCellTagToTables;
    for (const auto& table : UpdatingTables_) {
        if (table->Dynamic && !table->Path.GetOutputTimestamp()) {
            externalCellTagToTables[table->ExternalCellTag].push_back(table);
        }
    }

    if (externalCellTagToTables.empty()) {
        return;
    }

    YT_LOG_INFO("Locking output dynamic tables");

    const auto& timestampProvider = OutputClient->GetNativeConnection()->GetTimestampProvider();
    auto currentTimestampOrError = WaitFor(timestampProvider->GenerateTimestamps());
    THROW_ERROR_EXCEPTION_IF_FAILED(currentTimestampOrError, "Error generating timestamp to lock output dynamic tables");
    auto currentTimestamp = currentTimestampOrError.Value();

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    std::vector<TCellTag> externalCellTags;
    for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
        auto proxy = CreateObjectServiceWriteProxy(OutputClient, externalCellTag);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& table : tables) {
            auto req = TTableYPathProxy::LockDynamicTable(table->GetObjectIdPath());
            req->set_timestamp(currentTimestamp);
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
        externalCellTags.push_back(externalCellTag);
    }

    auto combinedResultOrError = WaitFor(AllSet(asyncResults));
    THROW_ERROR_EXCEPTION_IF_FAILED(combinedResultOrError, "Error locking output dynamic tables");
    auto& combinedResult = combinedResultOrError.Value();

    for (const auto& batchRspOrError : combinedResult) {
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error locking output dynamic tables");
    }

    YT_LOG_INFO("Waiting for dynamic tables lock to complete");

    std::vector<TError> innerErrors;
    auto sleepDuration = Config->DynamicTableLockCheckingIntervalDurationMin;
    for (int attempt = 0; attempt < Config->DynamicTableLockCheckingAttemptCountLimit; ++attempt) {
        asyncResults.clear();
        externalCellTags.clear();
        innerErrors.clear();

        for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
            auto proxy = CreateObjectServiceReadProxy(
                OutputClient,
                EMasterChannelKind::Follower,
                externalCellTag);
            auto batchReq = proxy.ExecuteBatch();

            for (const auto& table : tables) {
                auto objectIdPath = FromObjectId(table->ObjectId);
                auto req = TTableYPathProxy::CheckDynamicTableLock(objectIdPath);
                AddCellTagToSyncWith(req, table->ObjectId);
                SetTransactionId(req, table->ExternalTransactionId);
                batchReq->AddRequest(req);
            }

            asyncResults.push_back(batchReq->Invoke());
            externalCellTags.push_back(externalCellTag);
        }

        auto combinedResultOrError = WaitFor(AllSet(asyncResults));
        if (!combinedResultOrError.IsOK()) {
            innerErrors.push_back(combinedResultOrError);
            continue;
        }
        auto& combinedResult = combinedResultOrError.Value();

        for (size_t cellIndex = 0; cellIndex < externalCellTags.size(); ++cellIndex) {
            auto& batchRspOrError = combinedResult[cellIndex];
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                if (cumulativeError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                    cumulativeError.ThrowOnError();
                }
                innerErrors.push_back(cumulativeError);
                YT_LOG_DEBUG(cumulativeError, "Error while checking dynamic table lock");
                continue;
            }

            const auto& batchRsp = batchRspOrError.Value();
            auto checkLockRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspCheckDynamicTableLock>();

            auto& tables = externalCellTagToTables[externalCellTags[cellIndex]];
            std::vector<TOutputTablePtr> pendingTables;

            for (size_t index = 0; index < tables.size(); ++index) {
                const auto& rspOrError = checkLockRspsOrError[index];
                const auto& table = tables[index];

                if (!rspOrError.IsOK()) {
                    if (rspOrError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                        rspOrError.ThrowOnError();
                    }
                    innerErrors.push_back(rspOrError);
                    YT_LOG_DEBUG(rspOrError, "Error while checking dynamic table lock");
                }

                if (!rspOrError.IsOK() || !rspOrError.Value()->confirmed()) {
                    pendingTables.push_back(table);
                }
            }

            tables = std::move(pendingTables);
            if (tables.empty()) {
                externalCellTagToTables.erase(externalCellTags[cellIndex]);
            }
        }

        if (externalCellTagToTables.empty()) {
            break;
        }

        TDelayedExecutor::WaitForDuration(sleepDuration);

        sleepDuration = std::min(
            sleepDuration * Config->DynamicTableLockCheckingIntervalScale,
            Config->DynamicTableLockCheckingIntervalDurationMax);
    }

    if (!innerErrors.empty()) {
        THROW_ERROR_EXCEPTION("Could not lock output dynamic tables")
            << std::move(innerErrors);
    }

    YT_LOG_INFO("Dynamic tables locking completed");
}

void TOperationControllerBase::CommitTransactions()
{
    YT_LOG_INFO("Committing scheduler transactions");

    std::vector<TFuture<TTransactionCommitResult>> commitFutures;

    if (OutputTransaction) {
        commitFutures.push_back(OutputTransaction->Commit());
    }

    SleepInCommitStage(EDelayInsideOperationCommitStage::Stage7);

    if (DebugTransaction) {
        commitFutures.push_back(DebugTransaction->Commit());
    }

    WaitFor(AllSucceeded(commitFutures))
        .ThrowOnError();

    YT_LOG_INFO("Scheduler transactions committed");

    THashSet<ITransactionPtr> abortedTransactions;
    auto abortTransaction = [&] (const ITransactionPtr& transaction) {
        if (transaction && abortedTransactions.emplace(transaction).second) {
            // Fire-and-forget.
            YT_UNUSED_FUTURE(transaction->Abort());
        }
    };
    abortTransaction(InputTransaction);
    abortTransaction(AsyncTransaction);
    for (const auto& transaction : NestedInputTransactions) {
        abortTransaction(transaction);
    }
}

void TOperationControllerBase::TeleportOutputChunks()
{
    if (OutputTables_.empty()) {
        return;
    }

    auto teleporter = New<TChunkTeleporter>(
        Config->ChunkTeleporter,
        OutputClient,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        OutputCompletionTransaction->GetId(),
        Logger);

    for (auto& table : OutputTables_) {
        for (const auto& [key, chunkTreeId] : table->OutputChunkTreeIds) {
            if (IsPhysicalChunkType(TypeFromId(chunkTreeId))) {
                teleporter->RegisterChunk(chunkTreeId, table->ExternalCellTag);
            }
        }
    }

    WaitFor(teleporter->Run())
        .ThrowOnError();
}

void TOperationControllerBase::VerifySortedOutput(TOutputTablePtr table)
{
    const auto& path = table->Path.GetPath();
    YT_LOG_DEBUG("Sorting output chunk tree ids by boundary keys (ChunkTreeCount: %v, Table: %v)",
        table->OutputChunkTreeIds.size(),
        path);

    YT_VERIFY(table->TableUploadOptions.TableSchema->IsSorted());
    const auto& comparator = table->TableUploadOptions.TableSchema->ToComparator();

    std::stable_sort(
        table->OutputChunkTreeIds.begin(),
        table->OutputChunkTreeIds.end(),
        [&] (const auto& lhs, const auto& rhs) -> bool {
            auto lhsBoundaryKeys = lhs.first.AsBoundaryKeys();
            auto rhsBoundaryKeys = rhs.first.AsBoundaryKeys();
            auto minKeyResult = comparator.CompareKeys(lhsBoundaryKeys.MinKey, rhsBoundaryKeys.MinKey);
            if (minKeyResult != 0) {
                return minKeyResult < 0;
            }
            return comparator.CompareKeys(lhsBoundaryKeys.MaxKey, rhsBoundaryKeys.MaxKey) < 0;
        });

    if (!table->OutputChunkTreeIds.empty() &&
        table->TableUploadOptions.UpdateMode == EUpdateMode::Append &&
        table->LastKey)
    {
        YT_LOG_DEBUG(
            "Comparing table last key against first chunk min key (LastKey: %v, MinKey: %v, Comparator: %v)",
            table->LastKey,
            table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey,
            comparator);

        int cmp = comparator.CompareKeys(
            table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey,
            table->LastKey);

        if (cmp < 0) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SortOrderViolation,
                "Output table %v is not sorted: job outputs overlap with original table",
                table->GetPath())
                << TErrorAttribute("table_max_key", table->LastKey)
                << TErrorAttribute("job_output_min_key", table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey)
                << TErrorAttribute("comparator", comparator);
        }

        if (cmp == 0 && table->TableWriterOptions->ValidateUniqueKeys) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SortOrderViolation,
                "Output table %v contains duplicate keys: job outputs overlap with original table",
                table->GetPath())
                << TErrorAttribute("table_max_key", table->LastKey)
                << TErrorAttribute("job_output_min_key", table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey)
                << TErrorAttribute("comparator", comparator);
        }
    }

    for (auto current = table->OutputChunkTreeIds.begin(); current != table->OutputChunkTreeIds.end(); ++current) {
        auto next = current + 1;
        if (next != table->OutputChunkTreeIds.end()) {
            int cmp = comparator.CompareKeys(next->first.AsBoundaryKeys().MinKey, current->first.AsBoundaryKeys().MaxKey);

            if (cmp < 0) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::SortOrderViolation,
                    "Output table %v is not sorted: job outputs have overlapping key ranges",
                    table->GetPath())
                    << TErrorAttribute("current_range_max_key", current->first.AsBoundaryKeys().MaxKey)
                    << TErrorAttribute("next_range_min_key", next->first.AsBoundaryKeys().MinKey)
                    << TErrorAttribute("comparator", comparator);
            }

            if (cmp == 0 && table->TableWriterOptions->ValidateUniqueKeys) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::UniqueKeyViolation,
                    "Output table %v contains duplicate keys: job outputs have overlapping key ranges",
                    table->GetPath())
                    << TErrorAttribute("current_range_max_key", current->first.AsBoundaryKeys().MaxKey)
                    << TErrorAttribute("next_range_min_key", next->first.AsBoundaryKeys().MinKey)
                    << TErrorAttribute("comparator", comparator);
            }
        }
    }
}


void TOperationControllerBase::AttachOutputChunks(const std::vector<TOutputTablePtr>& tableList)
{
    for (const auto& table : tableList) {
        const auto& path = table->GetPath();

        YT_LOG_INFO("Attaching output chunks (Path: %v)",
            path);

        auto channel = OutputClient->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            table->ExternalCellTag);
        TChunkServiceProxy proxy(channel);

        // Split large outputs into separate requests.
        // For static tables there is always exactly one subrequest. For dynamic tables
        // there may be multiple subrequests, each corresponding to the whole tablet.
        NChunkClient::NProto::TReqAttachChunkTrees* req = nullptr;
        TChunkServiceProxy::TReqExecuteBatchPtr batchReq;

        auto flushSubrequest = [&] (bool requestStatistics) {
            if (req) {
                req->set_request_statistics(requestStatistics);
                req = nullptr;
            }
        };

        auto flushRequest = [&] (bool requestStatistics) {
            if (!batchReq) {
                return;
            }

            if (!requestStatistics && table->Dynamic) {
                YT_ASSERT(!req);
            }
            flushSubrequest(requestStatistics);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to output table %v",
                path);

            const auto& batchRsp = batchRspOrError.Value();
            const auto& subresponses = batchRsp->attach_chunk_trees_subresponses();
            if (!table->Dynamic) {
                YT_VERIFY(subresponses.size() == 1);
            }
            if (requestStatistics) {
                for (const auto& rsp : subresponses) {
                    // NB: For a static table statistics are requested only once at the end.
                    // For a dynamic table statistics are requested for each tablet separately.
                    table->DataStatistics += rsp.statistics();
                }
            }

            batchReq.Reset();
        };

        i64 currentRequestSize = 0;

        auto addChunkTree = [&] (TChunkTreeId chunkTreeId) {
            if (batchReq && currentRequestSize >= Config->MaxChildrenPerAttachRequest) {
                // NB: Static tables do not need statistics for intermediate requests.
                // Dynamic tables need them for each subrequest, so we ensure that
                // the whole request is flushed only when there is no opened subrequest.
                if (!table->Dynamic || !req) {
                    flushRequest(false);
                    currentRequestSize = 0;
                }
            }

            ++currentRequestSize;

            if (!req) {
                if (!batchReq) {
                    batchReq = proxy.ExecuteBatch();
                    GenerateMutationId(batchReq);
                    SetSuppressUpstreamSync(&batchReq->Header(), true);
                    // COMPAT(shakurov): prefer proto ext (above).
                    batchReq->set_suppress_upstream_sync(true);
                }
                req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), table->OutputChunkListId);
                if (table->Dynamic && OperationType != EOperationType::RemoteCopy && !table->Path.GetOutputTimestamp()) {
                    ToProto(req->mutable_transaction_id(), table->ExternalTransactionId);
                }
            }

            ToProto(req->add_child_ids(), chunkTreeId);
        };

        if (table->TableUploadOptions.TableSchema->IsSorted() && (table->Dynamic || ShouldVerifySortedOutput())) {
            // Sorted output generated by user operation requires rearranging.

            if (!table->TableUploadOptions.PartiallySorted && ShouldVerifySortedOutput()) {
                VerifySortedOutput(table);
            } else {
                YT_VERIFY(table->Dynamic);
            }

            if (!table->Dynamic) {
                for (const auto& [key, chunkTreeId] : table->OutputChunkTreeIds) {
                    addChunkTree(chunkTreeId);
                }
            } else {
                std::vector<std::vector<TChunkTreeId>> tabletChunks(table->PivotKeys.size());
                for (const auto& chunk : table->OutputChunks) {
                    auto chunkId  = chunk->GetChunkId();
                    auto& minKey = chunk->BoundaryKeys()->MinKey;
                    auto& maxKey = chunk->BoundaryKeys()->MaxKey;

                    auto start = BinarySearch(0, tabletChunks.size(), [&] (size_t index) {
                        return CompareRows(table->PivotKeys[index], minKey) <= 0;
                    });
                    if (start > 0) {
                        --start;
                    }

                    auto end = BinarySearch(0, tabletChunks.size() - 1, [&] (size_t index) {
                        return CompareRows(table->PivotKeys[index], maxKey) <= 0;
                    });

                    if (CompareRows(table->PivotKeys[end], maxKey) <= 0) {
                        ++end;
                    }

                    for (auto index = start; index < end; ++index) {
                        tabletChunks[index].push_back(chunkId);
                    }
                }

                for (int index = 0; index < std::ssize(tabletChunks); ++index) {
                    table->OutputChunkListId = table->TabletChunkListIds[index];
                    for (auto& chunkTree : tabletChunks[index]) {
                        addChunkTree(chunkTree);
                    }
                    flushSubrequest(true);
                }
            }
        } else if (auto outputOrder = GetOutputOrder()) {
            YT_LOG_DEBUG("Sorting output chunk tree ids according to a given output order (ChunkTreeCount: %v, Table: %v)",
                table->OutputChunkTreeIds.size(),
                path);
            std::vector<std::pair<TOutputOrder::TEntry, TChunkTreeId>> chunkTreeIds;
            for (const auto& [key, chunkTreeId] : table->OutputChunkTreeIds) {
                chunkTreeIds.emplace_back(std::move(key.AsOutputOrderEntry()), chunkTreeId);
            }

            auto outputChunkTreeIds = outputOrder->ArrangeOutputChunkTrees(std::move(chunkTreeIds));
            for (const auto& chunkTreeId : outputChunkTreeIds) {
                addChunkTree(chunkTreeId);
            }
        } else {
            YT_LOG_DEBUG("Sorting output chunk tree ids by integer keys (ChunkTreeCount: %v, Table: %v)",
                table->OutputChunkTreeIds.size(), path);
            std::stable_sort(
                table->OutputChunkTreeIds.begin(),
                table->OutputChunkTreeIds.end(),
                [&] (const auto& lhs, const auto& rhs) -> bool {
                    auto lhsKey = lhs.first.AsIndex();
                    auto rhsKey = rhs.first.AsIndex();
                    return lhsKey < rhsKey;
                }
            );
            for (const auto& [key, chunkTreeId] : table->OutputChunkTreeIds) {
                addChunkTree(chunkTreeId);
            }
        }

        // NB: Don't forget to ask for the statistics in the last request.
        flushRequest(true);

        YT_LOG_INFO("Output chunks attached (Path: %v, Statistics: %v)",
            path,
            table->DataStatistics);
    }
}

void TOperationControllerBase::CustomCommit()
{ }

void TOperationControllerBase::EndUploadOutputTables(const std::vector<TOutputTablePtr>& tables)
{
    THashMap<TCellTag, std::vector<TOutputTablePtr>> nativeCellTagToTables;
    for (const auto& table : tables) {
        nativeCellTagToTables[CellTagFromId(table->ObjectId)].push_back(table);

        YT_LOG_INFO("Finishing upload to output table (Path: %v, Schema: %v)",
            table->GetPath(),
            *table->TableUploadOptions.TableSchema);
    }

    {
        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [nativeCellTag, tables] : nativeCellTagToTables) {
            auto proxy = CreateObjectServiceWriteProxy(OutputClient, nativeCellTag);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                {
                    auto req = TTableYPathProxy::EndUpload(table->GetObjectIdPath());
                    SetTransactionId(req, table->UploadTransactionId);
                    GenerateMutationId(req);
                    *req->mutable_statistics() = table->DataStatistics;

                    if (!table->IsFile()) {
                        // COMPAT(h0pless): remove this when masters will receive schema in BeginUpload.
                        ToProto(req->mutable_table_schema(), table->TableUploadOptions.TableSchema.Get());
                        req->set_schema_mode(ToProto<int>(table->TableUploadOptions.SchemaMode));

                        req->set_optimize_for(ToProto<int>(table->TableUploadOptions.OptimizeFor));
                        if (table->TableUploadOptions.ChunkFormat) {
                            req->set_chunk_format(ToProto<int>(*table->TableUploadOptions.ChunkFormat));
                        }
                    }
                    req->set_compression_codec(ToProto<int>(table->TableUploadOptions.CompressionCodec));
                    req->set_erasure_codec(ToProto<int>(table->TableUploadOptions.ErasureCodec));
                    if (table->TableUploadOptions.SecurityTags) {
                        ToProto(req->mutable_security_tags()->mutable_items(), *table->TableUploadOptions.SecurityTags);
                    }
                    batchReq->AddRequest(req);
                }
                if (table->OutputType == EOutputTableType::Stderr || table->OutputType == EOutputTableType::Core) {
                    auto req = TYPathProxy::Set(table->GetObjectIdPath() + "/@part_size");
                    SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                    req->set_value(ConvertToYsonStringNestingLimited(GetPartSize(table->OutputType)).ToString());
                    batchReq->AddRequest(req);
                }
                if (table->OutputType == EOutputTableType::Core) {
                    auto req = TYPathProxy::Set(table->GetObjectIdPath() + "/@sparse");
                    SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                    req->set_value(ConvertToYsonStringNestingLimited(true).ToString());
                    batchReq->AddRequest(req);
                }
            }

            asyncResults.push_back(batchReq->Invoke());
        }

        auto checkError = [] (const auto& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error finishing upload to output tables");
        };

        auto result = WaitFor(AllSucceeded(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
        }
    }

    YT_LOG_INFO("Upload to output tables finished");
}

void TOperationControllerBase::SafeOnJobStarted(const TJobletPtr& joblet)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job started, ignored (JobId: %v)", joblet->JobId);
        return;
    }

    YT_LOG_DEBUG("Job started (JobId: %v)", joblet->JobId);

    joblet->LastActivityTime = TInstant::Now();
    joblet->TaskName = joblet->Task->GetVertexDescriptor();

    Host->GetJobProfiler()->ProfileStartedJob(*joblet);

    YT_VERIFY(!std::exchange(joblet->JobState, EJobState::Waiting));

    if (!joblet->Revived) {
        Host->RegisterJob(
            TStartedJobInfo{joblet->JobId, joblet->NodeDescriptor.Address});
    }

    IncreaseAccountResourceUsageLease(joblet->DiskRequestAccount, joblet->DiskQuota);

    ReportJobCookieToArchive(joblet);
    ReportControllerStateToArchive(joblet, EJobState::Running);

    LogEventFluently(ELogEventType::JobStarted)
        .Item("job_id").Value(joblet->JobId)
        .Item("operation_id").Value(OperationId)
        .Item("resource_limits").Value(joblet->ResourceLimits)
        .Item("node_address").Value(joblet->NodeDescriptor.Address)
        .Item("job_type").Value(joblet->JobType)
        .Item("tree_id").Value(joblet->TreeId);

    LogProgress();
}

void TOperationControllerBase::InitializeHistograms()
{
    if (IsInputDataSizeHistogramSupported()) {
        EstimatedInputDataSizeHistogram_ = CreateHistogram();
        InputDataSizeHistogram_ = CreateHistogram();
    }
}

void TOperationControllerBase::AddValueToEstimatedHistogram(const TJobletPtr& joblet)
{
    if (EstimatedInputDataSizeHistogram_) {
        EstimatedInputDataSizeHistogram_->AddValue(joblet->InputStripeList->TotalDataWeight);
    }
}

void TOperationControllerBase::RemoveValueFromEstimatedHistogram(const TJobletPtr& joblet)
{
    if (EstimatedInputDataSizeHistogram_) {
        EstimatedInputDataSizeHistogram_->RemoveValue(joblet->InputStripeList->TotalDataWeight);
    }
}

void TOperationControllerBase::UpdateActualHistogram(const TCompletedJobSummary& jobSummary)
{
    if (InputDataSizeHistogram_ && jobSummary.TotalInputDataStatistics) {
        auto dataWeight = jobSummary.TotalInputDataStatistics->data_weight();
        if (dataWeight > 0) {
            InputDataSizeHistogram_->AddValue(dataWeight);
        }
    }
}

void TOperationControllerBase::InitializeSecurityTags()
{
    std::vector<TSecurityTag> inferredSecurityTags;
    auto addTags = [&] (const auto& moreTags) {
        inferredSecurityTags.insert(inferredSecurityTags.end(), moreTags.begin(), moreTags.end());
    };

    addTags(Spec_->AdditionalSecurityTags);

    for (const auto& table : InputTables_) {
        addTags(table->SecurityTags);
    }

    for (const auto& [userJobSpec, files] : UserJobFiles_) {
        for (const auto& file : files) {
            addTags(file.SecurityTags);
        }
    }

    if (BaseLayer_) {
        addTags(BaseLayer_->SecurityTags);
    }

    SortUnique(inferredSecurityTags);

    for (const auto& table : OutputTables_) {
        if (auto explicitSecurityTags = table->Path.GetSecurityTags()) {
            // TODO(babenko): audit
            YT_LOG_INFO("Output table is assigned explicit security tags (Path: %v, InferredSecurityTags: %v, ExplicitSecurityTags: %v)",
                table->GetPath(),
                inferredSecurityTags,
                explicitSecurityTags);
            table->TableUploadOptions.SecurityTags = *explicitSecurityTags;
        } else {
            YT_LOG_INFO("Output table is assigned automatically-inferred security tags (Path: %v, SecurityTags: %v)",
                table->GetPath(),
                inferredSecurityTags);
            table->TableUploadOptions.SecurityTags = inferredSecurityTags;
        }
    }
}

void TOperationControllerBase::ProcessJobFinishedResult(const TJobFinishedResult& result)
{
    if (!result.OperationFailedError.IsOK()) {
        OnOperationFailed(result.OperationFailedError);
    }

    for (const auto& treeId : result.NewlyBannedTrees) {
        MaybeBanInTentativeTree(treeId);
    }
}

void TOperationControllerBase::OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    YT_VERIFY(jobSummary);

    auto jobId = jobSummary->Id;
    const auto abandoned = jobSummary->Abandoned;

    if (!ShouldProcessJobEvents()) {
        YT_LOG_DEBUG("Stale job completed, ignored (JobId: %v)", jobId);
        return;
    }

    auto joblet = GetJoblet(jobId);

    if (!JobAbortsUntilOperationFailure_.empty()) {
        JobAbortsUntilOperationFailure_.clear();
    }

    YT_LOG_DEBUG(
        "Job completed (JobId: %v, ResultSize: %v, Abandoned: %v, InterruptReason: %v, Interruptible: %v)",
        jobId,
        jobSummary->Result ? std::make_optional(jobSummary->GetJobResult().ByteSizeLong()) : std::nullopt,
        jobSummary->Abandoned,
        jobSummary->InterruptReason,
        joblet->JobInterruptible);

    // Testing purpose code.
    if (Config->EnableControllerFailureSpecOption &&
        Spec_->TestingOperationOptions->ControllerFailure &&
        *Spec_->TestingOperationOptions->ControllerFailure == EControllerFailureType::ExceptionThrownInOnJobCompleted)
    {
        THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::TestingError, "Testing exception");
    }

    // TODO(max42): this code is overcomplicated, rethink it.
    if (!abandoned) {
        const auto& jobResultExt = jobSummary->GetJobResultExt();
        bool restartNeeded = false;
        // TODO(max42): always send restart_needed?
        if (jobResultExt.has_restart_needed()) {
            restartNeeded = jobResultExt.restart_needed();
        } else {
            restartNeeded = jobResultExt.unread_chunk_specs_size() > 0;
        }

        if (restartNeeded) {
            if (joblet->Revived) {
                // NB: We lose the original interrupt reason during the revival,
                // so we set it to Unknown.
                jobSummary->InterruptReason = EInterruptReason::Unknown;
                YT_LOG_DEBUG(
                    "Overriding job interrupt reason due to revival (JobId: %v, InterruptReason: %v)",
                    jobId,
                    jobSummary->InterruptReason);
            } else {
                YT_LOG_DEBUG("Job restart is needed (JobId: %v)", jobId);
            }
        } else if (jobSummary->InterruptReason != EInterruptReason::None) {
            jobSummary->InterruptReason = EInterruptReason::None;
            YT_LOG_DEBUG(
                "Overriding job interrupt reason due to unneeded restart (JobId: %v, InterruptReason: %v)",
                jobId,
                jobSummary->InterruptReason);
        }

        YT_VERIFY(
            (jobSummary->InterruptReason == EInterruptReason::None && jobResultExt.unread_chunk_specs_size() == 0) ||
            (jobSummary->InterruptReason != EInterruptReason::None && (
                jobResultExt.unread_chunk_specs_size() != 0 ||
                jobResultExt.restart_needed())));

        // Validate all node ids of the output chunks and populate the local node directory.
        // In case any id is not known, abort the job.
        for (const auto& chunkSpec : jobResultExt.output_chunk_specs()) {
            if (auto abortedJobSummary = RegisterOutputChunkReplicas(*jobSummary, chunkSpec)) {
                OnJobAborted(std::move(abortedJobSummary));
                return;
            }
        }
    }

    // Controller should abort job if its competitor has already completed.
    if (auto maybeAbortReason = joblet->Task->ShouldAbortCompletingJob(joblet)) {
        YT_LOG_DEBUG("Job is considered aborted since its competitor has already completed (JobId: %v)", jobId);
        OnJobAborted(std::make_unique<TAbortedJobSummary>(*jobSummary, *maybeAbortReason));
        return;
    }

    TJobFinishedResult taskJobResult;
    std::optional<i64> optionalRowCount;

    {
        // NB: We want to process finished job changes atomically.
        // It is needed to prevent inconsistencies of operation controller state.
        // Such inconsistencies blocks saving job results in case of operation failure
        TForbidContextSwitchGuard guard;

        // NB: We should not explicitly tell node to remove abandoned job because it may be still
        // running at the node.
        if (!abandoned) {
            CompletedJobIdsReleaseQueue_.Push(jobId);
        }

        if (jobSummary->InterruptReason != EInterruptReason::None) {
            ExtractInterruptDescriptor(*jobSummary, joblet);
            YT_LOG_DEBUG(
                "Job interrupted (JobId: %v, InterruptReason: %v, UnreadDataSliceCount: %v, ReadDataSliceCount: %v)",
                jobId,
                jobSummary->InterruptReason,
                jobSummary->UnreadInputDataSlices.size(),
                jobSummary->ReadInputDataSlices.size());
        }

        UpdateJobletFromSummary(*jobSummary, joblet);

        joblet->Task->UpdateMemoryDigests(joblet, /*resourceOverdraft*/ false);
        UpdateActualHistogram(*jobSummary);

        if (joblet->ShouldLogFinishedEvent()) {
            LogFinishedJobFluently(ELogEventType::JobCompleted, joblet);
        }

        UpdateJobMetrics(joblet, *jobSummary, /*isJobFinished*/ true);
        UpdateAggregatedFinishedJobStatistics(joblet, *jobSummary);

        taskJobResult = joblet->Task->OnJobCompleted(joblet, *jobSummary);

        if (!abandoned) {
            if ((JobSpecCompletedArchiveCount_ < Config->GuaranteedArchivedJobSpecCountPerOperation ||
                jobSummary->TimeStatistics.ExecDuration.value_or(TDuration()) > Config->MinJobDurationToArchiveJobSpec) &&
                JobSpecCompletedArchiveCount_ < Config->MaxArchivedJobSpecCountPerOperation)
            {
                ++JobSpecCompletedArchiveCount_;
                jobSummary->ReleaseFlags.ArchiveJobSpec = true;
            }
        }

        // We want to know row count before moving jobSummary to OnJobFinished.
        if (RowCountLimitTableIndex && jobSummary->OutputDataStatistics) {
            optionalRowCount = VectorAtOr(*jobSummary->OutputDataStatistics, *RowCountLimitTableIndex).row_count();
        }

        Host->GetJobProfiler()->ProfileCompletedJob(*joblet, *jobSummary);

        OnJobFinished(std::move(jobSummary), /*retainJob*/ false);

        if (abandoned) {
            ReleaseJobs({jobId});
        }

        UnregisterJoblet(joblet);
    }

    ProcessJobFinishedResult(taskJobResult);

    UpdateTask(joblet->Task);
    LogProgress();

    if (IsCompleted()) {
        OnOperationCompleted(/*interrupted*/ false);
        return;
    }

    if (RowCountLimitTableIndex && optionalRowCount) {
        switch (joblet->JobType) {
            case EJobType::Map:
            case EJobType::OrderedMap:
            case EJobType::SortedReduce:
            case EJobType::JoinReduce:
            case EJobType::PartitionReduce:
            case EJobType::OrderedMerge:
            case EJobType::UnorderedMerge:
            case EJobType::SortedMerge:
            case EJobType::FinalSort: {
                RegisterOutputRows(*optionalRowCount, *RowCountLimitTableIndex);
                break;
            }
            default:
                break;
        }
    }

    CheckFailedJobsStatusReceived();
}

void TOperationControllerBase::OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    auto jobId = jobSummary->Id;

    if (!ShouldProcessJobEvents()) {
        YT_LOG_DEBUG("Stale job failed, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Job failed (JobId: %v)", jobId);

    auto joblet = GetJoblet(jobId);

    if (!JobAbortsUntilOperationFailure_.empty()) {
        JobAbortsUntilOperationFailure_.clear();
    }

    if (Spec_->IgnoreJobFailuresAtBannedNodes && BannedNodeIds_.find(joblet->NodeDescriptor.Id) != BannedNodeIds_.end()) {
        YT_LOG_DEBUG("Job is considered aborted since it has failed at a banned node "
            "(JobId: %v, Address: %v)",
            jobId,
            joblet->NodeDescriptor.Address);
        auto abortedJobSummary = std::make_unique<TAbortedJobSummary>(*jobSummary, EAbortReason::NodeBanned);
        OnJobAborted(std::move(abortedJobSummary));
        return;
    }

    if (joblet->CompetitionType == EJobCompetitionType::Experiment) {
        YT_LOG_DEBUG("Failed layer probing job is considered aborted "
            "(JobId: %v)",
            jobId);
        auto abortedJobSummary = std::make_unique<TAbortedJobSummary>(*jobSummary, EAbortReason::JobTreatmentFailed);
        OnJobAborted(std::move(abortedJobSummary));
        return;
    }

    auto error = jobSummary->GetError();

    TJobFinishedResult taskJobResult;

    {
        // NB: We want to process finished job changes atomically.
        // It is needed to prevent inconsistencies of operation controller state.
        // Such inconsistencies blocks saving job results in case of operation failure
        TForbidContextSwitchGuard guard;

        ++FailedJobCount_;
        if (FailedJobCount_ == 1) {
            ShouldUpdateLightOperationAttributes_ = true;
            ShouldUpdateProgressAttributesInCypress_ = true;
        }

        UpdateJobletFromSummary(*jobSummary, joblet);

        LogFinishedJobFluently(ELogEventType::JobFailed, joblet)
            .Item("error").Value(error);

        UpdateJobMetrics(joblet, *jobSummary, /*isJobFinished*/ true);
        UpdateAggregatedFinishedJobStatistics(joblet, *jobSummary);

        taskJobResult = joblet->Task->OnJobFailed(joblet, *jobSummary);

        jobSummary->ReleaseFlags.ArchiveJobSpec = true;

        Host->GetJobProfiler()->ProfileFailedJob(*joblet, *jobSummary);

        OnJobFinished(std::move(jobSummary), /*retainJob*/ true);

        auto finallyGuard = Finally([&] {
            // TODO(pogorelov): Remove current exception checking (YT-18911).
            if (std::uncaught_exceptions() == 0 || Config->ReleaseFailedJobOnException) {
                ReleaseJobs({jobId});
            }
        });

        UnregisterJoblet(joblet);
    }

    ProcessJobFinishedResult(taskJobResult);

    // This failure case has highest priority for users. Therefore check must be performed as early as possible.
    if (Spec_->FailOnJobRestart) {
        OnJobUniquenessViolated(TError(NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Job failed; failing operation since \"fail_on_job_restart\" spec option is set")
            << TErrorAttribute("job_id", joblet->JobId)
            << TErrorAttribute("reason", EFailOnJobRestartReason::JobFailed)
            << error);
        return;
    }

    if (error.Attributes().Get<bool>("fatal", false)) {
        auto wrappedError = TError("Job failed with fatal error") << error;
        OnOperationFailed(wrappedError);
        return;
    }

    int maxFailedJobCount = Spec_->MaxFailedJobCount;
    if (FailedJobCount_ >= maxFailedJobCount) {
        auto failedJobsLimitExceededError = TError(NScheduler::EErrorCode::MaxFailedJobsLimitExceeded, "Failed jobs limit exceeded")
                << TErrorAttribute("max_failed_job_count", maxFailedJobCount);
        auto operationFailedError = [&] {
            if (IsFailingByTimeout()) {
                return GetTimeLimitError()
                    << failedJobsLimitExceededError
                    << error;
            } else {
                return failedJobsLimitExceededError
                    << error;
            }
        }();

        OnOperationFailed(operationFailedError);
        return;
    }

    CheckFailedJobsStatusReceived();

    if (Spec_->BanNodesWithFailedJobs) {
        if (BannedNodeIds_.insert(joblet->NodeDescriptor.Id).second) {
            YT_LOG_DEBUG("Node banned due to failed job (JobId: %v, NodeId: %v, Address: %v)",
                jobId,
                joblet->NodeDescriptor.Id,
                joblet->NodeDescriptor.Address);
        }
    }

    if (Spec_->SuspendOnJobFailure) {
        Host->OnOperationSuspended(TError("Job failed with error") << error);
    }

    UpdateTask(joblet->Task);
    LogProgress();

    if (IsCompleted()) {
        OnOperationCompleted(/*interrupted*/ false);
    }
}

void TOperationControllerBase::OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    auto jobId = jobSummary->Id;
    const auto abortReason = jobSummary->AbortReason;

    if (!ShouldProcessJobEvents()) {
        YT_LOG_DEBUG("Stale job aborted, ignored (JobId: %v)", jobId);
        return;
    }

    if (abortReason == EAbortReason::FailedChunks) {
        YT_LOG_DEBUG(
            "Job aborted (JobId: %v, AbortReason: %v, SampleFailedChunkIds: %v)",
            jobId,
            abortReason,
            MakeShrunkFormattableView(UnavailableInputChunkIds, TDefaultFormatter(), SampleChunkIdCount));
    } else {
        YT_LOG_DEBUG(
            "Job aborted (JobId: %v, AbortReason: %v)",
            jobId,
            abortReason);
    }

    auto joblet = GetJoblet(jobId);

    auto error = jobSummary->Error;

    TJobFinishedResult taskJobResult;
    std::vector<TChunkId> failedChunkIds;
    bool wasScheduled = jobSummary->Scheduled;

    {
        // NB: We want to process finished job changes atomically.
        // It is needed to prevent inconsistencies of operation controller state.
        // Such inconsistencies blocks saving job results in case of operation failure
        TForbidContextSwitchGuard guard;

        UpdateJobletFromSummary(*jobSummary, joblet);

        if (abortReason == EAbortReason::ResourceOverdraft) {
            joblet->Task->UpdateMemoryDigests(joblet, /*resourceOverdraft*/ true);
        }

        if (wasScheduled) {
            if (joblet->ShouldLogFinishedEvent()) {
                auto fluent = LogFinishedJobFluently(ELogEventType::JobAborted, joblet)
                    .Item("reason").Value(abortReason)
                    .DoIf(jobSummary->Error.has_value(), [&] (TFluentMap fluent) {
                        fluent.Item("error").Value(jobSummary->Error);
                    })
                    .DoIf(jobSummary->PreemptedFor.has_value(), [&] (TFluentMap fluent) {
                        fluent.Item("preempted_for").Value(jobSummary->PreemptedFor);
                    });
            }
            UpdateAggregatedFinishedJobStatistics(joblet, *jobSummary);
        }

        UpdateJobMetrics(joblet, *jobSummary, /*isJobFinished*/ true);

        if (abortReason == EAbortReason::FailedChunks) {
            const auto& jobResultExt = jobSummary->GetJobResultExt();
            failedChunkIds = FromProto<std::vector<TChunkId>>(jobResultExt.failed_chunk_ids());
        }
        taskJobResult = joblet->Task->OnJobAborted(joblet, *jobSummary);

        bool retainJob = (abortReason == EAbortReason::UserRequest) || WasJobGracefullyAborted(jobSummary);

        Host->GetJobProfiler()->ProfileAbortedJob(*joblet, *jobSummary);

        OnJobFinished(std::move(jobSummary), retainJob);

        auto finallyGuard = Finally([&] {
            ReleaseJobs({jobId});
        });

        UnregisterJoblet(joblet);
    }

    ProcessJobFinishedResult(taskJobResult);

    for (auto chunkId : failedChunkIds) {
        OnChunkFailed(chunkId, jobId);
    }

    // This failure case has highest priority for users. Therefore check must be performed as early as possible.
    if (Spec_->FailOnJobRestart &&
        wasScheduled &&
        joblet->IsStarted() &&
        abortReason != EAbortReason::GetSpecFailed)
    {
        OnJobUniquenessViolated(TError(
            NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Job aborted; failing operation since \"fail_on_job_restart\" spec option is set")
            << TErrorAttribute("job_id", joblet->JobId)
            << TErrorAttribute("reason", EFailOnJobRestartReason::JobAborted)
            << TErrorAttribute("job_abort_reason", abortReason));
    }

    if (auto it = JobAbortsUntilOperationFailure_.find(abortReason); it != JobAbortsUntilOperationFailure_.end()) {
        if (--it->second == 0) {
            JobAbortsUntilOperationFailure_.clear();
            auto wrappedError = TError("Fail operation due to excessive successive job aborts");
            if (error) {
                wrappedError <<= *error;
            }
            OnOperationFailed(wrappedError);
            return;
        }
    }

    if (abortReason == EAbortReason::AccountLimitExceeded) {
        Host->OnOperationSuspended(TError("Account limit exceeded"));
    }

    CheckFailedJobsStatusReceived();
    UpdateTask(joblet->Task);
    LogProgress();

    if (IsCompleted()) {
        OnOperationCompleted(/*interrupted*/ false);
    }
}

bool TOperationControllerBase::WasJobGracefullyAborted(const std::unique_ptr<TAbortedJobSummary>& jobSummary)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    if (!jobSummary->Result) {
        return false;
    }

    const auto& error = jobSummary->GetError();
    if (auto innerError = error.FindMatching(NExecNode::EErrorCode::AbortByControllerAgent)) {
        return innerError->Attributes().Get("graceful_abort", false);
    }

    return false;
}

void TOperationControllerBase::OnJobStartTimeReceived(
    const TJobletPtr& joblet,
    const std::unique_ptr<TRunningJobSummary>& jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    auto jobId = joblet->JobId;

    auto nodeJobStartTime = TInstant();

    if (auto jobStartTime = jobSummary->StartTime) {
        nodeJobStartTime = jobStartTime;
    } else if (
        const auto& timeStatistics = jobSummary->TimeStatistics;
        timeStatistics.ExecDuration ||
        timeStatistics.PrepareDuration)
    {
        auto totalDuration =
            timeStatistics.PrepareDuration.value_or(TDuration::Zero()) +
            timeStatistics.ExecDuration.value_or(TDuration::Zero());

        nodeJobStartTime = TInstant::Now() - totalDuration;
    }

    if (nodeJobStartTime && !joblet->IsJobStartedOnNode()) {
        joblet->NodeJobStartTime = nodeJobStartTime;
        RunningAllocationPreemptibleProgressStartTimes_[AllocationIdFromJobId(jobId)] = nodeJobStartTime;
    }
}

void TOperationControllerBase::SafeOnAllocationAborted(TAbortedAllocationSummary&& abortedAllocationSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    YT_LOG_DEBUG(
        "Allocation aborted event processing (JobId: %v)",
        abortedAllocationSummary.Id);

    auto joblet = FindJoblet(abortedAllocationSummary.Id);
    if (!joblet) {
        YT_LOG_DEBUG(
            "Joblet is not found, ignore allocation aborted event (JobId: %v)",
            abortedAllocationSummary.Id);

        return;
    }

    Host->AbortJob(
        joblet->JobId,
        abortedAllocationSummary.AbortReason);

    auto jobSummary = CreateAbortedJobSummary(joblet->JobId, std::move(abortedAllocationSummary));
    OnJobAborted(std::move(jobSummary));
}

void TOperationControllerBase::OnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    auto jobId = jobSummary->Id;

    if (Spec_->TestingOperationOptions->CrashControllerAgent) {
        bool canCrashControllerAgent = false;
        {
            const auto& client = Host->GetClient();
            auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Cache);
            TMasterReadOptions readOptions{
                .ReadFrom = EMasterChannelKind::Cache
            };

            auto userClosure = GetSubjectClosure(
                AuthenticatedUser,
                proxy,
                client->GetNativeConnection(),
                readOptions);

            canCrashControllerAgent = userClosure.contains(RootUserName) || userClosure.contains(SuperusersGroupName);
        }

        if (canCrashControllerAgent) {
            YT_LOG_ERROR("Crashing controller agent");
            YT_ABORT();
        } else {
            auto error = TError(
                "User %Qv is not a superuser but tried to crash controller agent using testing options in spec; "
                "this incident will be reported",
                AuthenticatedUser);
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
    }

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job running event ignored because controller is not running (JobId: %v, State: %v)", jobId, State.load());
        return;
    }

    auto joblet = GetJoblet(jobSummary->Id);

    if (jobSummary->StatusTimestamp <= joblet->LastUpdateTime) {
        YT_LOG_DEBUG(
            "Stale job running event ignored because its timestamp is older than joblet last update time "
            "(JobId: %v, JobletLastUpdateTime: %v, StatusTimestamp: %v)",
            jobSummary->Id,
            joblet->LastUpdateTime,
            jobSummary->StatusTimestamp);
        return;
    }

    UpdateJobletFromSummary(*jobSummary, joblet);

    Host->GetJobProfiler()->ProfileRunningJob(*joblet);

    joblet->JobState = EJobState::Running;

    joblet->Task->OnJobRunning(joblet, *jobSummary);

    OnJobStartTimeReceived(joblet, jobSummary);

    if (jobSummary->Statistics) {
        // We actually got fresh running job statistics.

        UpdateJobMetrics(joblet, *jobSummary, /*isJobFinished*/ false);

        TErrorOr<TBriefJobStatisticsPtr> briefStatisticsOrError;

        try {
            briefStatisticsOrError = BuildBriefStatistics(std::move(jobSummary));
        } catch (const std::exception& ex) {
            briefStatisticsOrError = TError(ex);
        }

        AnalyzeBriefStatistics(
            joblet,
            Config->SuspiciousJobs,
            briefStatisticsOrError);
    }
}

void TOperationControllerBase::SafeAbandonJob(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    YT_LOG_DEBUG("Abandon job (JobId: %v)", jobId);

    if (State != EControllerState::Running) {
        THROW_ERROR_EXCEPTION(
            "Operation %v is not running",
            OperationId);
    }

    auto joblet = GetJobletOrThrow(jobId);

    switch (joblet->JobType) {
        case EJobType::Map:
        case EJobType::OrderedMap:
        case EJobType::SortedReduce:
        case EJobType::JoinReduce:
        case EJobType::PartitionMap:
        case EJobType::ReduceCombiner:
        case EJobType::PartitionReduce:
        case EJobType::Vanilla:
            break;
        default:
            THROW_ERROR_EXCEPTION(
                "Cannot abandon job %v of operation %v since it has type %Qlv",
                jobId,
                OperationId,
                joblet->JobType);
    }

    if (!ShouldProcessJobEvents()) {
        THROW_ERROR_EXCEPTION(
            "Cannot abandon job %v of operation %v that is not running",
            jobId,
            OperationId);
    }

    Host->AbortJob(jobId, EAbortReason::Abandoned);

    OnJobCompleted(CreateAbandonedJobSummary(jobId));
}

void TOperationControllerBase::SafeInterruptJobByUserRequest(TJobId jobId, TDuration timeout)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    YT_LOG_DEBUG(
        "Interrupting job (JobId: %v, Timeout: %v)",
        jobId,
        timeout);

    if (State != EControllerState::Running) {
        THROW_ERROR_EXCEPTION(
            "Operation %v is not running",
            OperationId);
    }

    auto joblet = GetJobletOrThrow(jobId);

    if (!joblet->JobInterruptible) {
        THROW_ERROR_EXCEPTION(
            "Cannot interrupt job %v of type %Qlv "
            "because it does not support interruption or \"interruption_signal\" is not set",
            jobId,
            joblet->JobType);
    }

    InterruptJob(jobId, EInterruptReason::UserRequest, timeout);
}

void TOperationControllerBase::BuildJobAttributes(
    const TJobletPtr& joblet,
    EJobState state,
    i64 stderrSize,
    TFluentMap fluent) const
{
    YT_LOG_DEBUG("Building job attributes");
    fluent
        .Item("job_type").Value(joblet->JobType)
        .Item("state").Value(state)
        .Item("address").Value(joblet->NodeDescriptor.Address)
        .Item("start_time").Value(joblet->StartTime)
        .Item("account").Value(joblet->DebugArtifactsAccount)
        .Item("progress").Value(joblet->Progress)

        // We use Int64 for `stderr_size' to be consistent with
        // compressed_data_size / uncompressed_data_size attributes.
        .Item("stderr_size").Value(stderrSize)
        .Item("brief_statistics").Value(joblet->BriefStatistics)
        .Item("statistics").Value(joblet->BuildCombinedStatistics())
        .Item("suspicious").Value(joblet->Suspicious)
        .Item("job_competition_id").Value(joblet->CompetitionIds[EJobCompetitionType::Speculative])
        .Item("probing_job_competition_id").Value(joblet->CompetitionIds[EJobCompetitionType::Probing])
        .Item("has_competitors").Value(joblet->HasCompetitors[EJobCompetitionType::Speculative])
        .Item("has_probing_competitors").Value(joblet->HasCompetitors[EJobCompetitionType::Probing])
        .Item("probing").Value(joblet->CompetitionType == EJobCompetitionType::Probing)
        .Item("speculative").Value(joblet->CompetitionType == EJobCompetitionType::Speculative)
        .Item("task_name").Value(joblet->TaskName)
        .Item("job_cookie").Value(joblet->OutputCookie)
        .DoIf(joblet->PredecessorType != EPredecessorType::None, [&] (TFluentMap fluent) {
            fluent
                .Item("predecessor_type").Value(joblet->PredecessorType)
                .Item("predecessor_job_id").Value(joblet->PredecessorJobId);
        });
}

void TOperationControllerBase::BuildFinishedJobAttributes(
    const TJobletPtr& joblet,
    TJobSummary* jobSummary,
    bool hasStderr,
    bool hasFailContext,
    TFluentMap fluent) const
{
    auto stderrSize = hasStderr
        // Report nonzero stderr size as we are sure it is saved.
        ? std::max(joblet->StderrSize, static_cast<i64>(1))
        : 0;

    i64 failContextSize = hasFailContext ? 1 : 0;

    BuildJobAttributes(joblet, jobSummary->State, stderrSize, fluent);

    bool includeError = jobSummary->State == EJobState::Failed ||
        jobSummary->State == EJobState::Aborted;
    fluent
        .Item("finish_time").Value(joblet->FinishTime)
        .DoIf(includeError, [&] (TFluentMap fluent) {
            fluent.Item("error").Value(jobSummary->GetError());
        })
        .DoIf(jobSummary->GetJobResult().HasExtension(TJobResultExt::job_result_ext),
            [&] (TFluentMap fluent)
        {
            const auto& jobResultExt = jobSummary->GetJobResultExt();
            fluent.Item("core_infos").Value(jobResultExt.core_infos());
        })
        .Item("fail_context_size").Value(failContextSize);
}

TFluentLogEvent TOperationControllerBase::LogFinishedJobFluently(
    ELogEventType eventType,
    const TJobletPtr& joblet)
{
    auto statistics = joblet->BuildCombinedStatistics();
    // Table rows cannot have top-level attributes, so we drop statistics timestamp here.
    statistics.SetTimestamp(std::nullopt);

    return LogEventFluently(eventType)
        .Item("job_id").Value(joblet->JobId)
        .Item("operation_id").Value(OperationId)
        .Item("start_time").Value(joblet->StartTime)
        .Item("finish_time").Value(joblet->FinishTime)
        .Item("resource_limits").Value(joblet->ResourceLimits)
        .Item("statistics").Value(statistics)
        .Item("node_address").Value(joblet->NodeDescriptor.Address)
        .Item("job_type").Value(joblet->JobType)
        .Item("job_competition_id").Value(joblet->CompetitionIds[EJobCompetitionType::Speculative])
        .Item("probing_job_competition_id").Value(joblet->CompetitionIds[EJobCompetitionType::Probing])
        .Item("has_competitors").Value(joblet->HasCompetitors[EJobCompetitionType::Speculative])
        .Item("has_probing_competitors").Value(joblet->HasCompetitors[EJobCompetitionType::Probing])
        .Item("tree_id").Value(joblet->TreeId)
        .DoIf(joblet->PredecessorType != EPredecessorType::None, [&] (TFluentMap fluent) {
            fluent
                .Item("predecessor_type").Value(joblet->PredecessorType)
                .Item("predecessor_job_id").Value(joblet->PredecessorJobId);
        });
}

IYsonConsumer* TOperationControllerBase::GetEventLogConsumer()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EventLogConsumer_.get();
}

const TLogger* TOperationControllerBase::GetEventLogger()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return &ControllerEventLogger;
}

void TOperationControllerBase::OnChunkFailed(TChunkId chunkId, TJobId jobId)
{
    if (chunkId == NullChunkId) {
        YT_LOG_WARNING("Incompatible unavailable chunk found; deprecated node version");
        return;
    }

    // Dynamic stores cannot be located by the controller, let the job do its job.
    if (IsDynamicTabletStoreType(TypeFromId(chunkId))) {
        return;
    }

    auto it = InputChunkMap.find(chunkId);
    if (it == InputChunkMap.end()) {
        YT_LOG_DEBUG("Intermediate chunk has failed (ChunkId: %v, JobId: %v)", chunkId, jobId);
        if (!OnIntermediateChunkUnavailable(chunkId)) {
            return;
        }

        IntermediateChunkScraper->Start();
    } else {
        YT_LOG_DEBUG("Input chunk has failed (ChunkId: %v, JobId: %v)", chunkId, jobId);
        OnInputChunkUnavailable(chunkId, &it->second);
    }
}

void TOperationControllerBase::SafeOnIntermediateChunkLocated(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas,
    bool missing)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (missing) {
        // We can unstage intermediate chunks (e.g. in automerge) - just skip them.
        return;
    }

    // Intermediate chunks are always replicated.
    if (IsUnavailable(replicas, NErasure::ECodec::None, GetChunkAvailabilityPolicy())) {
        OnIntermediateChunkUnavailable(chunkId);
    } else {
        OnIntermediateChunkAvailable(chunkId, replicas);
    }
}

void TOperationControllerBase::SafeOnInputChunkLocated(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas,
    bool missing)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (missing) {
        // We must have locked all the relevant input chunks, but when user transaction is aborted
        // there can be a race between operation completion and chunk scraper.
        OnOperationFailed(TError("Input chunk %v is missing", chunkId));
        return;
    }

    ++ChunkLocatedCallCount;
    if (ChunkLocatedCallCount >= Config->ChunkScraper->MaxChunksPerRequest) {
        ChunkLocatedCallCount = 0;
        YT_LOG_DEBUG("Located another batch of chunks (Count: %v, UnavailableInputChunkCount: %v, SampleUnavailableInputChunkIds: %v)",
            Config->ChunkScraper->MaxChunksPerRequest,
            UnavailableInputChunkIds.size(),
            MakeShrunkFormattableView(UnavailableInputChunkIds, TDefaultFormatter(), SampleChunkIdCount));
    }

    auto& descriptor = GetOrCrash(InputChunkMap, chunkId);
    YT_VERIFY(!descriptor.InputChunks.empty());

    const auto& chunkSpec = descriptor.InputChunks.front();
    auto codecId = chunkSpec->GetErasureCodec();

    if (IsUnavailable(replicas, codecId, GetChunkAvailabilityPolicy())) {
        OnInputChunkUnavailable(chunkId, &descriptor);
    } else {
        OnInputChunkAvailable(chunkId, replicas, &descriptor);
    }
}

void TOperationControllerBase::OnInputChunkAvailable(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas,
    TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Waiting) {
        return;
    }

    UnregisterUnavailableInputChunk(chunkId);

    if (UnavailableInputChunkIds.empty()) {
        YT_UNUSED_FUTURE(InputChunkScraper->Stop());
    }

    // Update replicas in place for all input chunks with current chunkId.
    for (const auto& chunkSpec : descriptor->InputChunks) {
        chunkSpec->SetReplicaList(replicas);
    }

    descriptor->State = EInputChunkState::Active;

    for (const auto& inputStripe : descriptor->InputStripes) {
        --inputStripe.Stripe->WaitingChunkCount;
        if (inputStripe.Stripe->WaitingChunkCount > 0) {
            continue;
        }

        const auto& task = inputStripe.Task;
        task->GetChunkPoolInput()->Resume(inputStripe.Cookie);
        UpdateTask(task);
    }
}

void TOperationControllerBase::OnInputChunkUnavailable(TChunkId chunkId, TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Active) {
        return;
    }

    RegisterUnavailableInputChunk(chunkId);

    switch (Spec_->UnavailableChunkTactics) {
        case EUnavailableChunkAction::Fail:
            OnOperationFailed(TError(NChunkClient::EErrorCode::ChunkUnavailable, "Input chunk %v is unavailable",
                chunkId));
            break;

        case EUnavailableChunkAction::Skip: {
            descriptor->State = EInputChunkState::Skipped;
            for (const auto& inputStripe : descriptor->InputStripes) {
                inputStripe.Stripe->DataSlices.erase(
                    std::remove_if(
                        inputStripe.Stripe->DataSlices.begin(),
                        inputStripe.Stripe->DataSlices.end(),
                        [&] (TLegacyDataSlicePtr slice) {
                            try {
                                return chunkId == slice->GetSingleUnversionedChunk()->GetChunkId();
                            } catch (const std::exception& ex) {
                                //FIXME(savrus) allow data slices to be unavailable.
                                OnOperationFailed(TError(NChunkClient::EErrorCode::ChunkUnavailable, "Dynamic table chunk became unavailable")
                                    << ex);
                                return true;
                            }
                        }),
                    inputStripe.Stripe->DataSlices.end());

                // Store information that chunk disappeared in the chunk mapping.
                for (const auto& chunk : descriptor->InputChunks) {
                    inputStripe.Task->GetChunkMapping()->OnChunkDisappeared(chunk);
                }

                UpdateTask(inputStripe.Task);
            }
            InputChunkScraper->Start();
            break;
        }

        case EUnavailableChunkAction::Wait: {
            descriptor->State = EInputChunkState::Waiting;
            for (const auto& inputStripe : descriptor->InputStripes) {
                if (inputStripe.Stripe->WaitingChunkCount == 0) {
                    inputStripe.Task->GetChunkPoolInput()->Suspend(inputStripe.Cookie);
                }
                ++inputStripe.Stripe->WaitingChunkCount;
            }
            InputChunkScraper->Start();
            break;
        }

        default:
            YT_ABORT();
    }
}

bool TOperationControllerBase::OnIntermediateChunkUnavailable(TChunkId chunkId)
{
    auto& completedJob = GetOrCrash(ChunkOriginMap, chunkId);

    YT_LOG_DEBUG(
        "Intermediate chunk is lost (ChunkId: %v, JobId: %v, Restartable: %v, Suspended: %v)",
        chunkId,
        completedJob->JobId,
        completedJob->Restartable,
        completedJob->Suspended);

    // If completedJob->Restartable == false, that means that source pool/task don't support lost jobs
    // and we have to use scraper to find new replicas of intermediate chunks.

    if (!completedJob->Restartable && Spec_->UnavailableChunkTactics == EUnavailableChunkAction::Fail) {
        auto error = TError("Intermediate chunk is unavailable")
            << TErrorAttribute("chunk_id", chunkId);
        OnOperationFailed(error, true);
        return false;
    }

    // If job is replayable, we don't track individual unavailable chunks,
    // since we will regenerate them all anyway.
    if (!completedJob->Restartable &&
        completedJob->UnavailableChunks.insert(chunkId).second)
    {
        ++UnavailableIntermediateChunkCount;
    }

    if (completedJob->Suspended) {
        return false;
    }

    YT_LOG_DEBUG(
        "Job is lost (Address: %v, JobId: %v, SourceTask: %v, OutputCookie: %v, InputCookie: %v, "
        "Restartable: %v, ChunkId: %v, UnavailableIntermediateChunkCount: %v)",
        completedJob->NodeDescriptor.Address,
        completedJob->JobId,
        completedJob->SourceTask->GetTitle(),
        completedJob->OutputCookie,
        completedJob->InputCookie,
        completedJob->Restartable,
        chunkId,
        UnavailableIntermediateChunkCount);

    completedJob->Suspended = true;
    completedJob->DestinationPool->Suspend(completedJob->InputCookie);

    if (completedJob->Restartable) {
        TForbidContextSwitchGuard guard;

        completedJob->SourceTask->GetChunkPoolOutput()->Lost(completedJob->OutputCookie);
        completedJob->SourceTask->OnJobLost(completedJob, chunkId);
        UpdateTask(completedJob->SourceTask);
    }

    return true;
}

void TOperationControllerBase::OnIntermediateChunkAvailable(
    TChunkId chunkId,
    const TChunkReplicaWithMediumList& replicas)
{
    auto& completedJob = GetOrCrash(ChunkOriginMap, chunkId);

    if (completedJob->Restartable || !completedJob->Suspended) {
        // Job will either be restarted or all chunks are fine.
        return;
    }

    if (completedJob->UnavailableChunks.erase(chunkId) == 1) {
        for (auto& dataSlice : completedJob->InputStripe->DataSlices) {
            // Intermediate chunks are always unversioned.
            auto inputChunk = dataSlice->GetSingleUnversionedChunk();
            if (inputChunk->GetChunkId() == chunkId) {
                inputChunk->SetReplicaList(replicas);
            }
        }
        --UnavailableIntermediateChunkCount;

        YT_VERIFY(UnavailableIntermediateChunkCount > 0 ||
            (UnavailableIntermediateChunkCount == 0 && completedJob->UnavailableChunks.empty()));
        if (completedJob->UnavailableChunks.empty()) {
            YT_LOG_DEBUG("Job result is resumed (JobId: %v, InputCookie: %v, UnavailableIntermediateChunkCount: %v)",
                completedJob->JobId,
                completedJob->InputCookie,
                UnavailableIntermediateChunkCount);

            completedJob->Suspended = false;
            completedJob->DestinationPool->Resume(completedJob->InputCookie);

            // TODO(psushin).
            // Unfortunately we don't know what task we are resuming, so
            // we update them all.
            UpdateAllTasks();
        }
    }
}

bool TOperationControllerBase::AreForeignTablesSupported() const
{
    return false;
}

bool TOperationControllerBase::IsLegacyOutputLivePreviewSupported() const
{
    return !IsLegacyLivePreviewSuppressed &&
        (GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare ||
        GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::ExplicitlyEnabled);
}

bool TOperationControllerBase::IsOutputLivePreviewSupported() const
{
    return !OutputTables_.empty();
}

bool TOperationControllerBase::IsLegacyIntermediateLivePreviewSupported() const
{
    return !IsLegacyLivePreviewSuppressed &&
        (GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare ||
        GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::ExplicitlyEnabled);
}

bool TOperationControllerBase::IsIntermediateLivePreviewSupported() const
{
    return false;
}

ELegacyLivePreviewMode TOperationControllerBase::GetLegacyOutputLivePreviewMode() const
{
    return ELegacyLivePreviewMode::NotSupported;
}

ELegacyLivePreviewMode TOperationControllerBase::GetLegacyIntermediateLivePreviewMode() const
{
    return ELegacyLivePreviewMode::NotSupported;
}

bool TOperationControllerBase::CheckUserTransactionAlive()
{
    if (!UserTransaction) {
        return true;
    }

    auto result = WaitFor(UserTransaction->Ping());
    if (result.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
        OnOperationAborted(GetUserTransactionAbortedError(UserTransaction->GetId()));
        return false;
    }

    return true;
}

void TOperationControllerBase::OnTransactionsAborted(const std::vector<TTransactionId>& transactionIds)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (!CheckUserTransactionAlive()) {
        return;
    }

    DoFailOperation(
        GetSchedulerTransactionsAbortedError(transactionIds),
        /*flush*/ false);
}

TControllerTransactionIds TOperationControllerBase::GetTransactionIds()
{
    auto getId = [] (const NApi::ITransactionPtr& transaction) {
        return transaction ? transaction->GetId() : NTransactionClient::TTransactionId();
    };

    TControllerTransactionIds transactionIds;
    transactionIds.AsyncId = getId(AsyncTransaction);
    transactionIds.InputId = getId(InputTransaction);
    transactionIds.OutputId = getId(OutputTransaction);
    transactionIds.DebugId = getId(DebugTransaction);
    transactionIds.OutputCompletionId = getId(OutputCompletionTransaction);
    transactionIds.DebugCompletionId = getId(DebugCompletionTransaction);
    for (const auto& transaction : NestedInputTransactions) {
        transactionIds.NestedInputIds.push_back(getId(transaction));
    }

    return transactionIds;
}

bool TOperationControllerBase::IsInputDataSizeHistogramSupported() const
{
    return false;
}

void TOperationControllerBase::SafeTerminate(EControllerState finalState)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    YT_LOG_INFO("Terminating operation controller");

    RemoveRemainingJobsOnOperationFinished();

    if (Spec_->TestingOperationOptions->ThrowExceptionDuringOperationAbort) {
        THROW_ERROR_EXCEPTION("Test exception");
    }

    // NB: Errors ignored since we cannot do anything with it.
    Y_UNUSED(WaitFor(Host->FlushOperationNode()));

    bool debugTransactionCommitted = false;

    // Skip committing anything if operation controller already tried to commit results.
    if (!CommitFinished) {
        try {
            FinalizeFeatures();
            CommitFeatures();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to finalize and commit features");
        }

        std::vector<TOutputTablePtr> tables;
        if (StderrTable_ && StderrTable_->IsPrepared()) {
            tables.push_back(StderrTable_);
        }
        if (CoreTable_ && CoreTable_->IsPrepared()) {
            tables.push_back(CoreTable_);
        }

        if (!tables.empty()) {
            YT_VERIFY(DebugTransaction);

            try {
                StartDebugCompletionTransaction();
                BeginUploadOutputTables(tables);
                AttachOutputChunks(tables);
                EndUploadOutputTables(tables);
                CommitDebugCompletionTransaction();

                WaitFor(DebugTransaction->Commit())
                    .ThrowOnError();
                debugTransactionCommitted = true;
            } catch (const std::exception& ex) {
                // Bad luck we can't commit transaction.
                // Such a pity can happen for example if somebody aborted our transaction manually.
                YT_LOG_ERROR(ex, "Failed to commit debug transaction");
                // Intentionally do not wait for abort.
                // Transaction object may be in incorrect state, we need to abort using only transaction id.
                YT_UNUSED_FUTURE(AttachTransaction(DebugTransaction->GetId(), Client)->Abort());
            }
        }
    }

    std::vector<TFuture<void>> abortTransactionFutures;
    THashMap<ITransactionPtr, TFuture<void>> transactionToAbortFuture;
    auto abortTransaction = [&] (const ITransactionPtr& transaction, const NNative::IClientPtr& client, bool sync = true) {
        if (transaction) {
            TFuture<void> abortFuture;
            auto it = transactionToAbortFuture.find(transaction);
            if (it == transactionToAbortFuture.end()) {
                // Transaction object may be in incorrect state, we need to abort using only transaction id.
                abortFuture = AttachTransaction(transaction->GetId(), client)->Abort();
                YT_VERIFY(transactionToAbortFuture.emplace(transaction, abortFuture).second);
            } else {
                abortFuture = it->second;
            }

            if (sync) {
                abortTransactionFutures.push_back(abortFuture);
            }
        }
    };

    // NB: We do not abort input transaction synchronously since
    // it can belong to an unavailable remote cluster.
    // Moreover if input transaction abort failed it does not harm anything.
    abortTransaction(InputTransaction, SchedulerInputClient, /*sync*/ false);
    abortTransaction(OutputTransaction, SchedulerOutputClient);
    abortTransaction(AsyncTransaction, SchedulerClient, /*sync*/ false);
    if (!debugTransactionCommitted) {
        abortTransaction(DebugTransaction, SchedulerClient, /*sync*/ false);
    }
    for (const auto& transaction : NestedInputTransactions) {
        abortTransaction(transaction, SchedulerInputClient, /*sync*/ false);
    }

    WaitFor(AllSucceeded(abortTransactionFutures))
        .ThrowOnError();

    YT_VERIFY(finalState == EControllerState::Aborted || finalState == EControllerState::Failed);
    State = finalState;

    LogProgress(/*force*/ true);

    YT_LOG_INFO("Operation controller terminated");
}

void TOperationControllerBase::SafeComplete()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    OnOperationCompleted(true);
}

void TOperationControllerBase::CheckTimeLimit()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    auto timeLimit = GetTimeLimit();
    if (timeLimit) {
        if (TInstant::Now() - StartTime_ > *timeLimit) {
            OnOperationTimeLimitExceeded();
        }
    }
}

void TOperationControllerBase::CheckAvailableExecNodes()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (ShouldSkipSanityCheck()) {
        return;
    }

    // If no available nodes were seen then re-check all nodes on each tick.
    // After such nodes were discovered, only re-check within BannedExecNodesCheckPeriod.
    auto now = TInstant::Now();
    if (AvailableExecNodesObserved_ && now < LastAvailableExecNodesCheckTime_ + Config->BannedExecNodesCheckPeriod) {
        return;
    }
    LastAvailableExecNodesCheckTime_ = now;

    TString observedExecNodeAddress;
    bool foundMatching = false;
    bool foundMatchingNotBanned = false;
    int nonMatchingFilterNodeCount = 0;
    THashMap<TString, THashMap<TString, i64>> matchingButInsufficientResourcesNodeCountPerTask;
    for (const auto& nodePair : GetExecNodeDescriptors()) {
        const auto& descriptor = nodePair.second;

        bool hasSuitableTree = false;
        for (const auto& [treeName, settings] : PoolTreeControllerSettingsMap_) {
            if (descriptor->CanSchedule(settings.SchedulingTagFilter)) {
                hasSuitableTree = true;
                break;
            }
        }
        if (!hasSuitableTree) {
            ++nonMatchingFilterNodeCount;
            continue;
        }

        bool hasNonTrivialTasks = false;
        bool hasEnoughResources = false;
        for (const auto& task : Tasks) {
            if (task->HasNoPendingJobs()) {
                continue;
            }
            hasNonTrivialTasks = true;

            const auto& neededResources = task->GetMinNeededResources();
            bool taskHasEnoughResources = true;
            TEnumIndexedArray<EJobResourceType, bool> taskHasEnoughResourcesPerResource;

            auto processJobResourceType = [&] (auto resourceLimit, auto resource, EJobResourceType type) {
                if (resource > resourceLimit) {
                    taskHasEnoughResources = false;
                } else {
                    taskHasEnoughResourcesPerResource[type] = true;
                }
            };

            #define XX(name, Name) processJobResourceType( \
                descriptor->ResourceLimits.Get##Name(), \
                neededResources.ToJobResources().Get##Name(), \
                EJobResourceType::Name);
            ITERATE_JOB_RESOURCES(XX)
            #undef XX

            bool taskCanSatisfyDiskQuotaRequest = CanSatisfyDiskQuotaRequest(descriptor->DiskResources, neededResources.DiskQuota(), /*considerUsage*/ false);
            hasEnoughResources |= taskHasEnoughResources && taskCanSatisfyDiskQuotaRequest;

            if (hasEnoughResources) {
                break;
            }

            for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
                matchingButInsufficientResourcesNodeCountPerTask[task->GetVertexDescriptor()][FormatEnum(resourceType)] +=
                    !taskHasEnoughResourcesPerResource[resourceType];
            }
            matchingButInsufficientResourcesNodeCountPerTask[task->GetVertexDescriptor()]["disk_space"] += !taskCanSatisfyDiskQuotaRequest;
        }
        if (hasNonTrivialTasks && !hasEnoughResources) {
            continue;
        }

        observedExecNodeAddress = descriptor->Address;
        foundMatching = true;
        if (BannedNodeIds_.find(descriptor->Id) == BannedNodeIds_.end()) {
            foundMatchingNotBanned = true;
            // foundMatchingNotBanned also implies foundMatching, hence we interrupt.
            break;
        }
    }

    if (foundMatching) {
        AvailableExecNodesObserved_ = true;
    }

    if (!AvailableExecNodesObserved_) {
        DoFailOperation(TError(
            EErrorCode::NoOnlineNodeToScheduleAllocation,
            "No online nodes that match operation scheduling tag filter %Qv "
            "and have sufficient resources to schedule an allocation found in trees %v",
            Spec_->SchedulingTagFilter.GetFormula(),
            GetKeys(PoolTreeControllerSettingsMap_))
            << TErrorAttribute("non_matching_filter_node_count", nonMatchingFilterNodeCount)
            << TErrorAttribute("matching_but_insufficient_resources_node_count_per_task", matchingButInsufficientResourcesNodeCountPerTask));
        return;
    }

    if (foundMatching && !foundMatchingNotBanned && Spec_->FailOnAllNodesBanned) {
        TStringBuilder errorMessageBuilder;
        errorMessageBuilder.AppendFormat(
            "All online nodes that match operation scheduling tag filter %Qv were banned in trees %v",
            Spec_->SchedulingTagFilter.GetFormula(),
            GetKeys(PoolTreeControllerSettingsMap_));
        // NB(eshcherbin): This should happen always, currently this option could be the only reason to ban a node.
        if (Spec_->BanNodesWithFailedJobs) {
            errorMessageBuilder.AppendString(
                " (\"ban_nodes_with_failed_jobs\" spec option is set, try investigating your job failures)");
        }
        auto errorMessage = errorMessageBuilder.Flush();
        DoFailOperation(TError(errorMessage));
        return;
    }

    YT_LOG_DEBUG("Available exec nodes check succeeded (ObservedNodeAddress: %v)",
        observedExecNodeAddress);
}

void TOperationControllerBase::CheckMinNeededResourcesSanity()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (ShouldSkipSanityCheck()) {
        return;
    }

    for (const auto& task : Tasks) {
        if (task->HasNoPendingJobs()) {
            continue;
        }

        const auto& neededResources = task->GetMinNeededResources();
        if (!Dominates(*CachedMaxAvailableExecNodeResources_, neededResources.ToJobResources())) {
            DoFailOperation(
                TError(
                    EErrorCode::NoOnlineNodeToScheduleAllocation,
                    "No online node can satisfy the resource demand")
                    << TErrorAttribute("task_name", task->GetTitle())
                    << TErrorAttribute("needed_resources", neededResources.ToJobResources())
                    << TErrorAttribute("max_available_resources", *CachedMaxAvailableExecNodeResources_));
        }
    }
}

TControllerScheduleAllocationResultPtr TOperationControllerBase::SafeScheduleAllocation(
    ISchedulingContext* context,
    const TJobResources& jobLimits,
    const TString& treeId)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->ScheduleAllocationControllerQueue));

    MaybeDelay(Spec_->TestingOperationOptions->ScheduleAllocationDelay);

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale schedule job attempt");
        return nullptr;
    }

    // SafeScheduleAllocation must be synchronous; context switches are prohibited.
    TForbidContextSwitchGuard contextSwitchGuard;

    TWallTimer timer;
    auto scheduleJobResult = New<TControllerScheduleAllocationResult>();
    DoScheduleAllocation(context, jobLimits, treeId, scheduleJobResult.Get());
    auto scheduleJobDuration = timer.GetElapsedTime();
    if (scheduleJobResult->StartDescriptor) {
        AvailableExecNodesObserved_ = true;
    }
    scheduleJobResult->Duration = scheduleJobDuration;
    scheduleJobResult->ControllerEpoch = ControllerEpoch;

    ScheduleAllocationStatistics_->RecordJobResult(*scheduleJobResult);
    scheduleJobResult->NextDurationEstimate = ScheduleAllocationStatistics_->SuccessfulDurationMovingAverage().GetAverage();

    auto now = NProfiling::GetCpuInstant();
    if (now > ScheduleAllocationStatisticsLogDeadline_) {
        AccountExternalScheduleAllocationFailures();

        YT_LOG_DEBUG(
            "Schedule allocation statistics (Count: %v, TotalDuration: %v, SuccessfulDurationEstimate: %v, FailureReasons: %v)",
            ScheduleAllocationStatistics_->GetCount(),
            ScheduleAllocationStatistics_->GetTotalDuration(),
            ScheduleAllocationStatistics_->SuccessfulDurationMovingAverage().GetAverage(),
            ScheduleAllocationStatistics_->Failed());

        ScheduleAllocationStatisticsLogDeadline_ = now + NProfiling::DurationToCpuDuration(Config->ScheduleAllocationStatisticsLogBackoff);
    }

    return scheduleJobResult;
}

bool TOperationControllerBase::IsThrottling() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto now = TInstant::Now();

    bool forceLogging = LastControllerJobSchedulingThrottlingLogTime_ + Config->ControllerThrottlingLogBackoff < now;
    if (forceLogging) {
        LastControllerJobSchedulingThrottlingLogTime_ = now;
    }

    // Check job spec limits.
    bool jobSpecThrottlingActive = false;
    {
        auto buildingJobSpecCount = BuildingJobSpecCount_.load();
        auto totalBuildingJobSpecSliceCount = TotalBuildingJobSpecSliceCount_.load();
        auto avgSliceCount = totalBuildingJobSpecSliceCount / std::max<double>(1.0, buildingJobSpecCount);
        if (Options->ControllerBuildingJobSpecCountLimit) {
            jobSpecThrottlingActive |= buildingJobSpecCount > *Options->ControllerBuildingJobSpecCountLimit;
        }
        if (Options->ControllerTotalBuildingJobSpecSliceCountLimit) {
            jobSpecThrottlingActive |= totalBuildingJobSpecSliceCount > *Options->ControllerTotalBuildingJobSpecSliceCountLimit;
        }

        if (jobSpecThrottlingActive || forceLogging) {
            YT_LOG_DEBUG(
                "Throttling status for building job specs (JobSpecCount: %v, JobSpecCountLimit: %v, TotalJobSpecSliceCount: %v, "
                "TotalJobSpecSliceCountLimit: %v, AvgJobSpecSliceCount: %v, JobSpecThrottlingActive: %v)",
                buildingJobSpecCount,
                Options->ControllerBuildingJobSpecCountLimit,
                totalBuildingJobSpecSliceCount,
                Options->ControllerTotalBuildingJobSpecSliceCountLimit,
                avgSliceCount,
                jobSpecThrottlingActive);
        }
    }

    // Check invoker wait time.
    bool waitTimeThrottlingActive = false;
    {
        auto scheduleAllocationInvokerStatistics = GetInvokerStatistics(Config->ScheduleAllocationControllerQueue);
        auto scheduleJobWaitTime = scheduleAllocationInvokerStatistics.TotalTimeEstimate;
        waitTimeThrottlingActive = scheduleJobWaitTime > Config->ScheduleAllocationTotalTimeThreshold;

        if (waitTimeThrottlingActive || forceLogging) {
            YT_LOG_DEBUG(
                "Throttling status for wait time "
                "(ScheduleAllocationWaitTime: %v, Threshold: %v, WaitTimeThrottlingActive: %v)",
                scheduleJobWaitTime,
                Config->ScheduleAllocationTotalTimeThreshold,
                waitTimeThrottlingActive);
        }
    }

    return jobSpecThrottlingActive || waitTimeThrottlingActive;
}

bool TOperationControllerBase::ShouldSkipRunningJobEvents() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto now = TInstant::Now();

    bool forceLogging = LastControllerJobEventThrottlingLogTime_ + Config->ControllerThrottlingLogBackoff < now;
    if (forceLogging) {
        LastControllerJobEventThrottlingLogTime_ = now;
    }

    // Check invoker wait time.
    bool waitTimeThrottlingActive = false;
    {
        auto jobEventsInvokerStatistics = GetInvokerStatistics(Config->JobEventsControllerQueue);
        auto jobEventsWaitTime = jobEventsInvokerStatistics.TotalTimeEstimate;
        waitTimeThrottlingActive = jobEventsWaitTime > Config->JobEventsTotalTimeThreshold;

        if (waitTimeThrottlingActive || forceLogging) {
            YT_LOG_DEBUG(
                "Throttling status for job events wait time "
                "(JobEventsWaitTime: %v, Threshold: %v, WaitTimeThrottlingActive: %v)",
                jobEventsWaitTime,
                Config->JobEventsTotalTimeThreshold,
                waitTimeThrottlingActive);
        }
    }

    return waitTimeThrottlingActive;
}

void TOperationControllerBase::RecordScheduleAllocationFailure(EScheduleAllocationFailReason reason) noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    ExternalScheduleAllocationFailureCounts_[reason].fetch_add(1);
}

void TOperationControllerBase::AccountBuildingJobSpecDelta(int countDelta, i64 totalSliceCountDelta) noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    BuildingJobSpecCount_.fetch_add(countDelta);
    TotalBuildingJobSpecSliceCount_.fetch_add(totalSliceCountDelta);
}

void TOperationControllerBase::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    Config = config;

    RunningJobStatisticsUpdateExecutor_->SetPeriod(config->RunningJobStatisticsUpdatePeriod);
    SendRunningAllocationTimeStatisticsUpdatesExecutor_->SetPeriod(config->RunningAllocationTimeStatisticsUpdatesSendPeriod);

    ScheduleAllocationStatistics_->SetMovingAverageWindowSize(config->ScheduleAllocationStatisticsMovingAverageWindowSize);
    DiagnosableInvokerPool_->UpdateActionTimeRelevancyHalflife(config->InvokerPoolTotalTimeAggregationPeriod);
}

void TOperationControllerBase::CustomizeJoblet(const TJobletPtr& /*joblet*/)
{ }

void TOperationControllerBase::CustomizeJobSpec(const TJobletPtr& joblet, TJobSpec* jobSpec) const
{
    VERIFY_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);

    jobSpecExt->set_testing_options(ConvertToYsonString(Spec_->JobTestingOptions).ToString());

    jobSpecExt->set_enable_prefetching_job_throttler(Spec_->EnablePrefetchingJobThrottler);

    if (OutputTransaction) {
        ToProto(jobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
    }

    if (joblet->EnabledJobProfiler) {
        auto* profiler = jobSpecExt->add_job_profilers();
        ToProto(profiler, *joblet->EnabledJobProfiler);
    }

    if (joblet->Task->GetUserJobSpec()) {
        InitUserJobSpec(
            jobSpecExt->mutable_user_job_spec(),
            joblet);
    }

    jobSpecExt->set_acl(ConvertToYsonString(Acl).ToString());
}

void TOperationControllerBase::RegisterTask(TTaskPtr task)
{
    task->Initialize();
    task->Prepare();
    task->RegisterCounters(TotalJobCounter_);
    Tasks.emplace_back(std::move(task));
}

void TOperationControllerBase::UpdateTask(const TTaskPtr& task)
{
    if (!task) {
        return;
    }

    auto oldPendingJobCount = CachedPendingJobCount.Load();
    auto newPendingJobCount = CachedPendingJobCount.Load() + task->GetPendingJobCountDelta();
    CachedPendingJobCount.Store(newPendingJobCount);

    int oldTotalJobCount = CachedTotalJobCount;
    int newTotalJobCount = CachedTotalJobCount + task->GetTotalJobCountDelta();
    CachedTotalJobCount = newTotalJobCount;

    IncreaseNeededResources(task->GetTotalNeededResourcesDelta());

    // TODO(max42): move this logging into pools.
    YT_LOG_DEBUG_IF(
        newPendingJobCount != oldPendingJobCount || newTotalJobCount != oldTotalJobCount,
        "Task updated (Task: %v, PendingJobCount: %v -> %v, TotalJobCount: %v -> %v, NeededResources: %v)",
        task->GetTitle(),
        oldPendingJobCount,
        newPendingJobCount,
        oldTotalJobCount,
        newTotalJobCount,
        CachedNeededResources);

    task->CheckCompleted();
}

void TOperationControllerBase::UpdateAllTasks()
{
    for (const auto& task : Tasks) {
        UpdateTask(task);
    }
}

void TOperationControllerBase::UpdateAllTasksIfNeeded()
{
    auto now = NProfiling::GetCpuInstant();
    if (now < TaskUpdateDeadline_) {
        return;
    }
    UpdateAllTasks();
    TaskUpdateDeadline_ = now + NProfiling::DurationToCpuDuration(Config->TaskUpdatePeriod);
}

void TOperationControllerBase::ResetTaskLocalityDelays()
{
    YT_LOG_DEBUG("Task locality delays are reset");
    for (const auto& task : Tasks) {
        task->SetDelayedTime(std::nullopt);
    }
}

void TOperationControllerBase::DoScheduleAllocation(
    ISchedulingContext* context,
    const TJobResources& jobLimits,
    const TString& treeId,
    TControllerScheduleAllocationResult* scheduleJobResult)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->ScheduleAllocationControllerQueue));

    if (!IsRunning()) {
        YT_LOG_TRACE("Operation is not running, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleAllocationFailReason::OperationNotRunning);
        return;
    }

    if (GetPendingJobCount().GetJobCountFor(treeId) == 0) {
        YT_LOG_TRACE("No pending jobs left, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleAllocationFailReason::NoPendingJobs);
        return;
    }

    if (BannedNodeIds_.find(context->GetNodeDescriptor()->Id) != BannedNodeIds_.end()) {
        YT_LOG_TRACE("Node is banned, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleAllocationFailReason::NodeBanned);
        return;
    }

    MaybeDelay(Spec_->TestingOperationOptions->InsideScheduleAllocationDelay);

    TryScheduleAllocation(context, jobLimits, treeId, scheduleJobResult, /*scheduleLocalJob*/ true);
    if (!scheduleJobResult->StartDescriptor) {
        TryScheduleAllocation(context, jobLimits, treeId, scheduleJobResult, /*scheduleLocalJob*/ false);
    }
}

void TOperationControllerBase::TryScheduleAllocation(
    ISchedulingContext* context,
    const TJobResources& jobLimits,
    const TString& treeId,
    TControllerScheduleAllocationResult* scheduleAllocationResult,
    bool scheduleLocalJob)
{
    if (!IsRunning()) {
        scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::OperationNotRunning);
        return;
    }

    const auto& address = context->GetNodeDescriptor()->Address;
    auto nodeId = context->GetNodeDescriptor()->Id;
    auto now = NProfiling::CpuInstantToInstant(context->GetNow());

    for (const auto& task : Tasks) {
        if (scheduleAllocationResult->IsScheduleStopNeeded()) {
            break;
        }

        // NB: we do not consider disk resources occupied by jobs that had already be scheduled in
        // current heartbeat. This check would be performed in scheduler.
        auto minNeededResources = task->GetMinNeededResources();
        if (!Dominates(jobLimits, minNeededResources.ToJobResources()) ||
            !CanSatisfyDiskQuotaRequest(context->DiskResources(), minNeededResources.DiskQuota()))
        {
            scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::NotEnoughResources);
            continue;
        }

        auto locality = task->GetLocality(nodeId);

        if (scheduleLocalJob) {
            // Make sure that the task has positive locality.
            if (locality <= 0) {
                // NB: This is one of the possible reasons, hopefully the most probable.
                scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::NoLocalJobs);
                continue;
            }
        } else {
            if (!task->GetDelayedTime()) {
                task->SetDelayedTime(now);
            }

            auto deadline = *task->GetDelayedTime() + task->GetLocalityTimeout();
            if (deadline > now) {
                YT_LOG_DEBUG(
                    "Task delayed (Task: %v, Deadline: %v)",
                    task->GetTitle(),
                    deadline);
                scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::TaskDelayed);
                continue;
            }
        }

        if (task->HasNoPendingJobs(treeId)) {
            UpdateTask(task);
            continue;
        }

        YT_LOG_DEBUG(
            "Attempting to schedule job (Kind: %v, Task: %v, Address: %v, Locality: %v, JobLimits: %v, "
            "PendingDataWeight: %v, PendingJobCount: %v)",
            scheduleLocalJob ? "Local" : "NonLocal",
            task->GetTitle(),
            address,
            locality,
            jobLimits,
            task->GetPendingDataWeight(),
            task->GetPendingJobCount());

        if (!HasEnoughChunkLists(task->IsStderrTableEnabled(), task->IsCoreTableEnabled())) {
            YT_LOG_DEBUG("Job chunk list demand is not met");
            scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::NotEnoughChunkLists);
            break;
        }

        task->ScheduleAllocation(context, jobLimits, treeId, IsTreeTentative(treeId), IsTreeProbing(treeId), scheduleAllocationResult);
        if (scheduleAllocationResult->StartDescriptor) {
            RegisterTestingSpeculativeJobIfNeeded(task, scheduleAllocationResult->StartDescriptor->Id);
            UpdateTask(task);
            return;
        }
    }

    scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::NoCandidateTasks);
}

bool TOperationControllerBase::IsTreeTentative(const TString& treeId) const
{
    return GetOrCrash(PoolTreeControllerSettingsMap_, treeId).Tentative;
}

bool TOperationControllerBase::IsTreeProbing(const TString& treeId) const
{
    return GetOrCrash(PoolTreeControllerSettingsMap_, treeId).Probing;
}

bool TOperationControllerBase::IsIdleCpuPolicyAllowedInTree(const TString& treeId) const
{
    return GetOrCrash(PoolTreeControllerSettingsMap_, treeId).AllowIdleCpuPolicy;
}

void TOperationControllerBase::MaybeBanInTentativeTree(const TString& treeId)
{
    if (!BannedTreeIds_.insert(treeId).second) {
        return;
    }

    Host->OnOperationBannedInTentativeTree(
        treeId,
        GetAllocationIdsByTreeId(treeId));

    auto error = TError("Operation was banned from tentative tree")
        << TErrorAttribute("tree_id", treeId);
    SetOperationAlert(EOperationAlertType::OperationBannedInTentativeTree, error);
}

TCancelableContextPtr TOperationControllerBase::GetCancelableContext() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableContext;
}

IInvokerPtr TOperationControllerBase::GetInvoker(EOperationControllerQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SuspendableInvokerPool->GetInvoker(queue);
}

IInvokerPtr TOperationControllerBase::GetCancelableInvoker(EOperationControllerQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableInvokerPool->GetInvoker(queue);
}

IInvokerPtr TOperationControllerBase::GetJobSpecBuildInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobSpecBuildInvoker_;
}

TDiagnosableInvokerPool::TInvokerStatistics TOperationControllerBase::GetInvokerStatistics(EOperationControllerQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DiagnosableInvokerPool_->GetInvokerStatistics(queue);
}

TFuture<void> TOperationControllerBase::Suspend()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (Spec_->TestingOperationOptions->DelayInsideSuspend) {
        return AllSucceeded(std::vector<TFuture<void>> {
            SuspendInvokerPool(SuspendableInvokerPool),
            TDelayedExecutor::MakeDelayed(*Spec_->TestingOperationOptions->DelayInsideSuspend)});
    }

    return SuspendInvokerPool(SuspendableInvokerPool);
}

void TOperationControllerBase::Resume()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ResumeInvokerPool(SuspendableInvokerPool);
}

void TOperationControllerBase::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableContext->Cancel(TError("Operation controller canceled"));

    YT_LOG_INFO("Operation controller canceled");
}

TCompositePendingJobCount TOperationControllerBase::GetPendingJobCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Avoid accessing the state while not prepared.
    if (!IsPrepared()) {
        return TCompositePendingJobCount{};
    }

    // NB: For suspended operations we still report proper pending job count
    // but zero demand.
    if (!IsRunning()) {
        return TCompositePendingJobCount{};
    }

    return CachedPendingJobCount.Load();
}

i64 TOperationControllerBase::GetFailedJobCount() const
{
    return FailedJobCount_;
}

bool TOperationControllerBase::ShouldUpdateLightOperationAttributes() const
{
    return ShouldUpdateLightOperationAttributes_;
}

void TOperationControllerBase::SetLightOperationAttributesUpdated()
{
    ShouldUpdateLightOperationAttributes_ = false;
}

void TOperationControllerBase::IncreaseNeededResources(const TCompositeNeededResources& resourcesDelta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(CachedNeededResourcesLock);
    CachedNeededResources = CachedNeededResources + resourcesDelta;
}

void TOperationControllerBase::IncreaseAccountResourceUsageLease(const std::optional<TString>& account, const TDiskQuota& delta)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    if (!account || !EnableMasterResourceUsageAccounting_) {
        return;
    }

    auto& info = GetOrCrash(AccountResourceUsageLeaseMap_, *account);

    YT_LOG_DEBUG("Increasing account resource usage lease (Account: %v, CurrentDiskQuota: %v, Delta: %v)",
        account,
        info.DiskQuota,
        delta);

    info.DiskQuota += delta;
    YT_VERIFY(!info.DiskQuota.DiskSpaceWithoutMedium.has_value());
}

void TOperationControllerBase::UpdateAccountResourceUsageLeases()
{
    for (const auto& [account, info] : AccountResourceUsageLeaseMap_) {
        auto it = LastUpdatedAccountResourceUsageLeaseMap_.find(account);
        if (it != LastUpdatedAccountResourceUsageLeaseMap_.end() && info.DiskQuota == it->second.DiskQuota) {
            continue;
        }

        LastUpdatedAccountResourceUsageLeaseMap_[account] = info;

        auto error = WaitFor(Host->UpdateAccountResourceUsageLease(info.LeaseId, info.DiskQuota));
        if (!error.IsOK()) {
            if (!CheckUserTransactionAlive()) {
                return;
            }

            if (error.FindMatching(NSecurityClient::EErrorCode::AccountLimitExceeded) ||
                error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError) ||
                error.FindMatching(NYTree::EErrorCode::ResolveError))
            {
                DoFailOperation(
                    TError("Failed to update account usage lease")
                        << TErrorAttribute("account", account)
                        << TErrorAttribute("lease_id", info.LeaseId)
                        << TErrorAttribute("operation_id", OperationId)
                        << TErrorAttribute("resource_usage", info.DiskQuota)
                        << error);
            } else {
                Host->Disconnect(
                    TError("Failed to update account usage lease")
                        << TErrorAttribute("account", account)
                        << TErrorAttribute("lease_id", info.LeaseId)
                        << TErrorAttribute("operation_id", OperationId)
                        << TErrorAttribute("resource_usage", info.DiskQuota)
                        << error);
            }
            return;
        } else {
            YT_LOG_DEBUG("Account resource usage lease updated (Account: %v, LeaseId: %v, DiskQuota: %v)",
                account,
                info.LeaseId,
                info.DiskQuota);
        }
    }
}

TCompositeNeededResources TOperationControllerBase::GetNeededResources() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(CachedNeededResourcesLock);
    return CachedNeededResources;
}

TJobResourcesWithQuotaList TOperationControllerBase::GetMinNeededAllocationResources() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedMinNeededAllocationResources.Load();
}

void TOperationControllerBase::SafeUpdateMinNeededAllocationResources()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    THashMap<TString, TJobResourcesWithQuota> minNeededJobResourcesPerTask;
    for (const auto& task : Tasks) {
        if (task->HasNoPendingJobs()) {
            UpdateTask(task);
            continue;
        }

        TJobResourcesWithQuota resources;
        try {
            resources = task->GetMinNeededResources();
        } catch (const std::exception& ex) {
            auto error = TError("Failed to update minimum needed resources")
                << ex;
            DoFailOperation(error);
            return;
        }

        EmplaceOrCrash(minNeededJobResourcesPerTask, task->GetTitle(), resources);
    }

    TJobResourcesWithQuotaList minNeededResources;
    for (const auto& [taskTitle, resources] : minNeededJobResourcesPerTask) {
        minNeededResources.push_back(resources);

        YT_LOG_DEBUG(
            "Aggregated minimum needed resources for allocations (Task: %v, MinNeededResources: %v)",
            taskTitle,
            FormatResources(resources));
    }

    CachedMinNeededAllocationResources.Exchange(minNeededResources);
}

TJobResources TOperationControllerBase::GetAggregatedMinNeededAllocationResources() const
{
    auto result = GetNeededResources().DefaultResources;
    for (const auto& allocationResources : GetMinNeededAllocationResources()) {
        result = Min(result, allocationResources.ToJobResources());
    }
    return result;
}

void TOperationControllerBase::FlushOperationNode(bool checkFlushResult)
{
    YT_LOG_DEBUG("Flushing operation node");
    // Some statistics are reported only on operation end so
    // we need to synchronously check everything and set
    // appropriate alerts before flushing operation node.
    // Flush of newly calculated statistics is guaranteed by OnOperationFailed.
    AlertManager_->Analyze();

    auto flushResult = WaitFor(Host->FlushOperationNode());
    if (checkFlushResult && !flushResult.IsOK()) {
        // We do not want to complete operation if progress flush has failed.
        DoFailOperation(flushResult, /*flush*/ false);
    }

    YT_LOG_DEBUG("Operation node flushed");
}

void TOperationControllerBase::OnOperationCompleted(bool interrupted)
{
    YT_UNUSED_FUTURE(BIND(&TOperationControllerBase::DoCompleteOperation, MakeStrong(this))
        .AsyncVia(GetCancelableInvoker())
        .Run(interrupted));
}

void TOperationControllerBase::DoCompleteOperation(bool /*interrupted*/)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    // This can happen if operation failed during completion in derived class (e.g. SortController).
    if (IsFinished()) {
        return;
    }

    State = EControllerState::Completed;

    BuildAndSaveProgress();
    FlushOperationNode(/*checkFlushResult*/ true);

    LogProgress(/*force*/ true);

    Host->OnOperationCompleted();
}

void TOperationControllerBase::OnOperationFailed(const TError& error, bool flush, bool abortAllJoblets)
{
    YT_UNUSED_FUTURE(BIND(&TOperationControllerBase::DoFailOperation, MakeStrong(this))
        .AsyncVia(GetCancelableInvoker())
        .Run(error, flush, abortAllJoblets));
}

void TOperationControllerBase::DoFailOperation(const TError& error, bool flush, bool abortAllJoblets)
{
    VERIFY_INVOKER_POOL_AFFINITY(InvokerPool);

    WaitFor(BIND([=, this, this_ = MakeStrong(this)] {
        YT_LOG_DEBUG(error, "Operation controller failed (Flush: %v)", flush);

        // During operation failing job aborting can lead to another operation fail, we don't want to invoke it twice.
        if (IsFinished()) {
            return;
        }

        State = EControllerState::Failed;

        if (abortAllJoblets) {
            AbortAllJoblets(EAbortReason::OperationFailed, /*honestly*/ true);
        }

        for (const auto& task : Tasks) {
            task->StopTiming();
        }

        BuildAndSaveProgress();
        LogProgress(/*force*/ true);

        if (flush) {
            // NB: Error ignored since we cannot do anything with it.
            FlushOperationNode(/*checkFlushResult*/ false);
        }

        Error_ = error;

        YT_LOG_DEBUG("Notifying host about operation controller failure");
        Host->OnOperationFailed(error);
        YT_LOG_DEBUG("Host notified about operation controller failure");
    })
        .AsyncVia(GetInvoker())
        .Run()
        .ToUncancelable())
        .ThrowOnError();
}

void TOperationControllerBase::OnOperationAborted(const TError& error)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    // Cf. OnOperationFailed.
    if (IsFinished()) {
        return;
    }

    State = EControllerState::Aborted;

    Host->OnOperationAborted(error);
}

std::optional<TDuration> TOperationControllerBase::GetTimeLimit() const
{
    auto timeLimit = Config->OperationTimeLimit;
    if (Spec_->TimeLimit) {
        timeLimit = Spec_->TimeLimit;
    }
    return timeLimit;
}

TError TOperationControllerBase::GetTimeLimitError() const
{
    return TError("Operation is running for too long, aborted")
        << TErrorAttribute("time_limit", GetTimeLimit());
}

void TOperationControllerBase::OnOperationTimeLimitExceeded()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG(
            "Attempt to report time limit expiration of an operation which is not running (State: %v)",
            State.load());
        return;
    }

    OperationTimedOut_ = true;

    YT_LOG_DEBUG("Operation timed out");

    GracefullyFailOperation(GetTimeLimitError());
}

void TOperationControllerBase::OnJobUniquenessViolated(TError error)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    if (Config->JobTracker->EnableGracefulAbort) {
        GracefullyFailOperation(std::move(error));
    } else {
        DoFailOperation(std::move(error));
    }
}

void TOperationControllerBase::GracefullyFailOperation(TError error)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    // NB(arkady-e1ppa): If we enter here when operations is finished
    // this entire call does nothing since JobletMap is empty and OnOperationFailed
    // short-circs if IsFinished == true.
    // Entering here with Failing state would just duplicate graceful abort request
    // as this call is the only one which assigns such a state.
    if (State != EControllerState::Running) {
        YT_LOG_DEBUG(
            "Attempt to gracefully fail operation which is not running (State: %v)",
            State.load());

        return;
    }

    State = EControllerState::Failing;

    YT_LOG_INFO("Operation gracefully failing");

    bool hasJobsToFail = false;

    // NB: joblet abort will remove it from map invalidating iterator.
    auto jobletMapCopy = JobletMap;

    for (const auto& [_, joblet] : jobletMapCopy) {
        switch (joblet->JobType) {
            // TODO(ignat): YT-11247, add helper with list of job types with user code.
            case EJobType::Map:
            case EJobType::OrderedMap:
            case EJobType::SortedReduce:
            case EJobType::JoinReduce:
            case EJobType::PartitionMap:
            case EJobType::ReduceCombiner:
            case EJobType::PartitionReduce:
            case EJobType::Vanilla:
                hasJobsToFail = true;
                Host->RequestJobGracefulAbort(joblet->JobId, EAbortReason::OperationFailed);
                break;
            default:
                AbortJob(joblet->JobId, EAbortReason::OperationFailed);
        }
    }

    if (hasJobsToFail) {
        YT_LOG_DEBUG("Postpone operation failure to handle failed jobs");

        YT_UNUSED_FUTURE(TDelayedExecutor::MakeDelayed(Spec_->TimeLimitJobFailTimeout)
            .Apply(BIND(
                &TOperationControllerBase::DoFailOperation,
                MakeWeak(this),
                error,
                /*flush*/ true,
                /*abortAllJoblets*/ true)
            .Via(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default))));
    } else {
        DoFailOperation(error, /*flush*/ true);
    }
}

void TOperationControllerBase::CheckFailedJobsStatusReceived()
{
    if (IsFailing() && JobletMap.empty()) {
        auto error = GetTimeLimitError();
        OnOperationFailed(error, /*flush*/ true);
    }
}

const std::vector<TOutputStreamDescriptorPtr>& TOperationControllerBase::GetStandardStreamDescriptors() const
{
    return StandardStreamDescriptors_;
}

void TOperationControllerBase::InitializeStandardStreamDescriptors()
{
    StandardStreamDescriptors_.resize(OutputTables_.size());
    for (int index = 0; index < std::ssize(OutputTables_); ++index) {
        StandardStreamDescriptors_[index] = OutputTables_[index]->GetStreamDescriptorTemplate(index)->Clone();
        StandardStreamDescriptors_[index]->DestinationPool = GetSink();
        StandardStreamDescriptors_[index]->IsFinalOutput = true;
        StandardStreamDescriptors_[index]->LivePreviewIndex = index;
        StandardStreamDescriptors_[index]->TargetDescriptor = TDataFlowGraph::SinkDescriptor;
        StandardStreamDescriptors_[index]->PartitionTag = index;
    }
}

void TOperationControllerBase::AddChunksToUnstageList(std::vector<TInputChunkPtr> chunks)
{
    std::vector<TChunkId> chunkIds;
    for (const auto& chunk : chunks) {
        auto it = LivePreviewChunks_.find(chunk);
        YT_VERIFY(it != LivePreviewChunks_.end());
        auto livePreviewDescriptor = it->second;
        DataFlowGraph_->UnregisterLivePreviewChunk(
            livePreviewDescriptor.VertexDescriptor,
            livePreviewDescriptor.LivePreviewIndex,
            chunk);
        chunkIds.push_back(chunk->GetChunkId());
        YT_LOG_DEBUG("Releasing intermediate chunk (ChunkId: %v, VertexDescriptor: %v, LivePreviewIndex: %v)",
            chunk->GetChunkId(),
            livePreviewDescriptor.VertexDescriptor,
            livePreviewDescriptor.LivePreviewIndex);
        LivePreviewChunks_.erase(it);
    }
    Host->AddChunkTreesToUnstageList(std::move(chunkIds), /*recursive*/ false);
}

void TOperationControllerBase::ProcessSafeException(const std::exception& ex)
{
    auto error = TError("Exception thrown in operation controller that led to operation failure")
        << ex;

    YT_LOG_ERROR(error);

    OnOperationFailed(error, /*flush*/ false, /*abortAllJoblets*/ false);
}

void TOperationControllerBase::ProcessSafeException(const TAssertionFailedException& ex)
{
    TControllerAgentCounterManager::Get()->IncrementAssertionsFailed(OperationType);

    auto error = TError(
        NScheduler::EErrorCode::OperationControllerCrashed,
        "Operation controller crashed; please file a ticket at YTADMINREQ and attach a link to this operation")
        << TErrorAttribute("failed_condition", ex.GetExpression())
        << TErrorAttribute("stack_trace", ex.GetStackTrace())
        << TErrorAttribute("core_path", ex.GetCorePath())
        << TErrorAttribute("operation_id", OperationId);

    YT_LOG_ERROR(error);

    OnOperationFailed(error, /*flush*/ false, /*abortAllJoblets*/ false);
}

void TOperationControllerBase::OnJobFinished(std::unique_ptr<TJobSummary> summary, bool retainJob)
{
    auto jobId = summary->Id;

    auto joblet = GetJoblet(jobId);
    if (!joblet->IsStarted()) {
        return;
    }

    bool hasStderr = false;
    bool hasFailContext = false;
    int coreInfoCount = 0;

    if (summary->Result) {
        const auto& jobResultExtension = summary->GetJobResult().GetExtension(TJobResultExt::job_result_ext);

        if (jobResultExtension.has_has_stderr()) {
            hasStderr = jobResultExtension.has_stderr();
        } else {
            auto stderrChunkId = FromProto<TChunkId>(jobResultExtension.stderr_chunk_id());
            if (stderrChunkId) {
                Host->AddChunkTreesToUnstageList({stderrChunkId}, /*recursive*/ false);
            }
            hasStderr = static_cast<bool>(stderrChunkId);
        }

        if (jobResultExtension.has_has_fail_context()) {
            hasFailContext = jobResultExtension.has_fail_context();
        } else {
            auto failContextChunkId = FromProto<TChunkId>(jobResultExtension.fail_context_chunk_id());
            hasFailContext = static_cast<bool>(failContextChunkId);
        }

        coreInfoCount = jobResultExtension.core_infos().size();
    }

    ReportControllerStateToArchive(joblet, summary->State);

    bool shouldRetainJob =
        (retainJob && RetainedJobCount_ < Config->MaxRetainedJobsPerOperation) ||
        (hasStderr && RetainedJobWithStderrCount_ < Spec_->MaxStderrCount) ||
        (coreInfoCount > 0 && RetainedJobsCoreInfoCount_ + coreInfoCount <= Spec_->MaxCoreInfoCount);

    auto releaseJobFlags = summary->ReleaseFlags;
    if (hasStderr && shouldRetainJob) {
        releaseJobFlags.ArchiveStderr = true;
        // Job spec is necessary for ACL checks for stderr.
        releaseJobFlags.ArchiveJobSpec = true;
    }
    if (hasFailContext && shouldRetainJob) {
        releaseJobFlags.ArchiveFailContext = true;
        // Job spec is necessary for ACL checks for fail context.
        releaseJobFlags.ArchiveJobSpec = true;
    }
    releaseJobFlags.ArchiveProfile = true;

    // TODO(gritukan, prime): This is always true.
    if (releaseJobFlags.IsNonTrivial()) {
        JobIdToReleaseFlags_.emplace(jobId, releaseJobFlags);
    }

    if (shouldRetainJob) {
        auto attributesFragment = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do([&] (TFluentMap fluent) {
                BuildFinishedJobAttributes(
                    joblet,
                    summary.get(),
                    hasStderr,
                    hasFailContext,
                    fluent);
            })
            .Finish();

        {
            auto attributes = BuildYsonStringFluently()
                .DoMap([&] (TFluentMap fluent) {
                    fluent.GetConsumer()->OnRaw(attributesFragment);
                });
            RetainedFinishedJobs_.emplace_back(jobId, std::move(attributes));
        }

        if (hasStderr) {
            ++RetainedJobWithStderrCount_;
        }
        if (retainJob) {
            ++RetainedJobCount_;
        }
        RetainedJobsCoreInfoCount_ += coreInfoCount;
    }

    if (joblet->IsStarted()) {
        IncreaseAccountResourceUsageLease(joblet->DiskRequestAccount, -joblet->DiskQuota);
    }
}

bool TOperationControllerBase::IsPrepared() const
{
    return State != EControllerState::Preparing;
}

bool TOperationControllerBase::IsRunning() const
{
    return State == EControllerState::Running;
}

bool TOperationControllerBase::IsFailing() const
{
    return State == EControllerState::Failing;
}

bool TOperationControllerBase::IsFailingByTimeout() const
{
    return IsFailing() && OperationTimedOut_;
}

bool TOperationControllerBase::IsFinished() const
{
    return State == EControllerState::Completed ||
        State == EControllerState::Failed ||
        State == EControllerState::Aborted;
}

std::pair<ITransactionPtr, TString> TOperationControllerBase::GetIntermediateMediumTransaction()
{
    return {nullptr, {}};
}

void TOperationControllerBase::UpdateIntermediateMediumUsage(i64 /*usage*/)
{
    YT_UNIMPLEMENTED();
}

const std::vector<TString>& TOperationControllerBase::GetOffloadingPoolTrees()
{
    if (!OffloadingPoolTrees_) {
        OffloadingPoolTrees_.emplace();
        for (const auto& [poolTree, settings]: PoolTreeControllerSettingsMap_) {
            if (settings.Offloading) {
                OffloadingPoolTrees_.value().push_back(poolTree);
            }
        }
    }
    return *OffloadingPoolTrees_;
}

void TOperationControllerBase::InitializeJobExperiment()
{
    if (Spec_->JobExperiment) {
        if (TLayerJobExperiment::IsEnabled(Spec_, GetUserJobSpecs()) && BaseLayer_) {
            JobExperiment_ = New<TLayerJobExperiment>(
                *Spec_->DefaultBaseLayerPath,
                *BaseLayer_,
                Config->EnableBypassArtifactCache,
                Logger);
        } else if (TMtnJobExperiment::IsEnabled(Spec_, GetUserJobSpecs())) {
            JobExperiment_ = New<TMtnJobExperiment>(
                Host->GetClient(),
                GetAuthenticatedUser(),
                *Spec_->JobExperiment->NetworkProject,
                Logger);
        }
    }
}

TJobExperimentBasePtr TOperationControllerBase::GetJobExperiment()
{
    return JobExperiment_;
}

TJobId TOperationControllerBase::GenerateJobId(NScheduler::TAllocationId allocationId)
{
    return TJobId(allocationId.Underlying());
}

void TOperationControllerBase::AsyncAbortJob(TJobId jobId, EAbortReason abortReason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue)->Invoke(
        BIND(
            &TOperationControllerBase::AbortJob,
            MakeWeak(this),
            jobId,
            abortReason));
}

void TOperationControllerBase::SafeAbortJobByJobTracker(TJobId jobId, EAbortReason abortReason)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    if (!FindJoblet(jobId)) {
        YT_LOG_DEBUG(
            "Ignore stale job abort request from job tracker (JobId: %v, AbortReason: %v)",
            jobId,
            abortReason);

        return;
    }

    YT_LOG_DEBUG("Aborting job by job tracker request (JobId: %v)", jobId);

    DoAbortJob(jobId, abortReason, /*requestJobTrackerJobAbortion*/ false);
}

void TOperationControllerBase::SuppressLivePreviewIfNeeded()
{
    if (GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::NotSupported &&
        GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::NotSupported)
    {
        YT_LOG_INFO("Legacy live preview is not supported for this operation");
        return;
    }

    std::vector<TError> suppressionErrors;

    const auto& connection = Host->GetClient()->GetNativeConnection();
    for (const auto& table : OutputTables_) {
        if (table->Dynamic) {
            suppressionErrors.emplace_back("Output table %v is dynamic", table->Path);
            break;
        }
    }

    for (const auto& table : OutputTables_) {
        if (table->ExternalCellTag == connection->GetPrimaryMasterCellTag() &&
            !connection->GetSecondaryMasterCellTags().empty())
        {
            suppressionErrors.emplace_back(
                "Output table %v is non-external and cluster is multicell",
                table->Path);
            break;
        }
    }

    // TODO(ifsmirnov): YT-11498. This is not the suppression you are looking for.
    for (const auto& table : InputTables_) {
        if (table->Schema->HasNontrivialSchemaModification()) {
            suppressionErrors.emplace_back(
                "Input table %v has non-trivial schema modification",
                table->Path);
            break;
        }
    }

    if (GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare ||
        GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare)
    {
        // Some live preview normally should appear, but user did not request anything explicitly.
        // We should check if user is not in legacy live preview blacklist in order to inform him
        // if he is in a blacklist.
        if (NRe2::TRe2::FullMatch(
            NRe2::StringPiece(AuthenticatedUser.data()),
            *Config->LegacyLivePreviewUserBlacklist))
        {
            suppressionErrors.emplace_back(TError(
                "User %Qv belongs to legacy live preview suppression blacklist; in order "
                "to overcome this suppression reason, explicitly specify enable_legacy_live_preview = %%true "
                "in operation spec", AuthenticatedUser)
                    << TErrorAttribute(
                        "legacy_live_preview_blacklist_regex",
                        Config->LegacyLivePreviewUserBlacklist->pattern()));
        }
    }

    if (IntermediateOutputCellTagList.size() != 1 && IsLegacyIntermediateLivePreviewSupported() && suppressionErrors.empty()) {
        suppressionErrors.emplace_back(TError(
            "Legacy live preview appears to have been disabled in the controller agents config when the operation started"));
    }

    IsLegacyLivePreviewSuppressed = !suppressionErrors.empty();
    if (IsLegacyLivePreviewSuppressed) {
        auto combinedSuppressionError = TError("Legacy live preview is suppressed due to the following reasons")
            << suppressionErrors
            << TErrorAttribute("output_live_preview_mode", GetLegacyOutputLivePreviewMode())
            << TErrorAttribute("intermediate_live_preview_mode", GetLegacyIntermediateLivePreviewMode());
        YT_LOG_INFO("Suppressing live preview due to some reasons (CombinedError: %v)", combinedSuppressionError);
        SetOperationAlert(EOperationAlertType::LegacyLivePreviewSuppressed, combinedSuppressionError);
    } else {
        YT_LOG_INFO("Legacy live preview is not suppressed");
    }
}

void TOperationControllerBase::CreateLivePreviewTables()
{
    const auto& client = Host->GetClient();
    auto connection = client->GetNativeConnection();

    // NB: use root credentials.
    auto proxy = CreateObjectServiceWriteProxy(client);
    auto batchReq = proxy.ExecuteBatch();

    auto addRequest = [&] (
        const TString& path,
        TCellTag cellTag,
        int replicationFactor,
        NCompression::ECodec compressionCodec,
        const std::optional<TString>& account,
        const TString& key,
        const TYsonString& acl,
        const TTableSchemaPtr& schema)
    {
        if (!AsyncTransaction) {
            YT_LOG_INFO("Creating transaction required for the legacy live preview (Type: %v)", ETransactionType::Async);
            AsyncTransaction = WaitFor(StartTransaction(ETransactionType::Async, Client))
                .ValueOrThrow();
        }

        auto req = TCypressYPathProxy::Create(path);
        req->set_type(ToProto<int>(EObjectType::Table));
        req->set_ignore_existing(true);

        const auto nestingLevelLimit = Host
            ->GetClient()
            ->GetNativeConnection()
            ->GetConfig()
            ->CypressWriteYsonNestingLevelLimit;
        auto attributes = CreateEphemeralAttributes(nestingLevelLimit);
        attributes->Set("replication_factor", replicationFactor);
        // Does this affect anything or is this for viewing only? Should we set the 'media' ('primary_medium') property?
        attributes->Set("compression_codec", compressionCodec);
        if (cellTag == connection->GetPrimaryMasterCellTag()) {
            attributes->Set("external", false);
        } else {
            attributes->Set("external", true);
            attributes->Set("external_cell_tag", cellTag);
        }
        attributes->Set("acl", acl);
        attributes->Set("inherit_acl", false);
        if (schema) {
            attributes->Set("schema", *schema);
        }
        if (account) {
            attributes->Set("account", *account);
        }

        ToProto(req->mutable_node_attributes(), *attributes);
        GenerateMutationId(req);
        SetTransactionId(req, AsyncTransaction->GetId());

        batchReq->AddRequest(req, key);
    };

    if (IsLegacyOutputLivePreviewSupported()) {
        YT_LOG_INFO("Creating live preview for output tables");

        for (int index = 0; index < std::ssize(OutputTables_); ++index) {
            auto& table = OutputTables_[index];
            auto path = GetOperationPath(OperationId) + "/output_" + ToString(index);
            addRequest(
                path,
                table->ExternalCellTag,
                table->TableWriterOptions->ReplicationFactor,
                table->TableWriterOptions->CompressionCodec,
                table->TableWriterOptions->Account,
                "create_output",
                table->EffectiveAcl,
                table->TableUploadOptions.TableSchema.Get());
        }
    }

    for (int index = 0; index < std::ssize(OutputTables_); ++index) {
        RegisterLivePreviewTable("output_" + ToString(index), OutputTables_[index]);
    }

    if (StderrTable_) {
        YT_LOG_INFO("Creating live preview for stderr table");

        auto name = "stderr";
        auto path = GetOperationPath(OperationId) + "/" + name;

        RegisterLivePreviewTable(name, StderrTable_);

        addRequest(
            path,
            StderrTable_->ExternalCellTag,
            StderrTable_->TableWriterOptions->ReplicationFactor,
            StderrTable_->TableWriterOptions->CompressionCodec,
            /*account*/ std::nullopt,
            "create_stderr",
            StderrTable_->EffectiveAcl,
            StderrTable_->TableUploadOptions.TableSchema.Get());
    }

    if (IsIntermediateLivePreviewSupported()) {
        auto name = "intermediate";
        IntermediateTable->LivePreviewTableName = name;
        (*LivePreviews_)[name] = New<TLivePreview>(
            New<TTableSchema>(),
            InputNodeDirectory_,
            OperationId,
            name);
    }

    if (CoreTable_) {
        RegisterLivePreviewTable("core", CoreTable_);
    }

    if (IsLegacyIntermediateLivePreviewSupported()) {
        YT_LOG_INFO("Creating live preview for intermediate table");

        auto path = GetOperationPath(OperationId) + "/intermediate";

        auto intermediateDataAcl = MakeOperationArtifactAcl(Acl);
        if (Config->AllowUsersGroupReadIntermediateData) {
            intermediateDataAcl.Entries.emplace_back(
                ESecurityAction::Allow,
                std::vector<TString>{UsersGroupName},
                EPermissionSet(EPermission::Read));
        }
        YT_VERIFY(IntermediateOutputCellTagList.size() == 1);
        addRequest(
            path,
            IntermediateOutputCellTagList.front(),
            1,
            Spec_->IntermediateCompressionCodec,
            Spec_->IntermediateDataAccount,
            "create_intermediate",
            ConvertToYsonStringNestingLimited(intermediateDataAcl),
            nullptr);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error creating live preview tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto handleResponse = [&] (TLivePreviewTableBase& table, TCypressYPathProxy::TRspCreatePtr rsp) {
        table.LivePreviewTableId = FromProto<NCypressClient::TNodeId>(rsp->node_id());
    };

    if (IsLegacyOutputLivePreviewSupported()) {
        auto rspsOrError = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_output");
        YT_VERIFY(rspsOrError.size() == OutputTables_.size());

        for (int index = 0; index < std::ssize(OutputTables_); ++index) {
            handleResponse(*OutputTables_[index], rspsOrError[index].Value());
        }

        YT_LOG_INFO("Live preview for output tables created");
    }

    if (StderrTable_) {
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>("create_stderr");
        handleResponse(*StderrTable_, rsp.Value());

        YT_LOG_INFO("Live preview for stderr table created");
    }

    if (IsLegacyIntermediateLivePreviewSupported()) {
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>("create_intermediate");
        handleResponse(*IntermediateTable, rsp.Value());

        YT_LOG_INFO("Live preview for intermediate table created");
    }
}

void TOperationControllerBase::FetchInputTables()
{
    TPeriodicYielder yielder(PrepareYieldPeriod);

    i64 totalChunkCount = 0;
    i64 totalExtensionSize = 0;

    YT_LOG_INFO(
        "Started fetching input tables (TableCount: %v)",
        InputTables_.size());

    auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        InputClient,
        TColumnarStatisticsFetcher::TOptions{
            .Config = Config->Fetcher,
            .NodeDirectory = InputNodeDirectory_,
            .ChunkScraper = CreateFetcherChunkScraper(),
            .Mode = Spec_->InputTableColumnarStatistics->Mode,
            .EnableEarlyFinish = Config->EnableColumnarStatisticsEarlyFinish,
            .Logger = Logger,
        });

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        InputClient,
        TMasterReadOptions{},
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        Config->MaxChunksPerFetch,
        Config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int tableIndex) {
            const auto& table = InputTables_[tableIndex];
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

            if (table->Path.GetColumns() && Spec_->InputTableColumnarStatistics->Enabled.value_or(Config->UseColumnarStatisticsDefault)) {
                req->add_extension_tags(TProtoExtensionTag<THeavyColumnStatisticsExt>::Value);
            }
            if (table->Dynamic || IsBoundaryKeysFetchEnabled()) {
                req->add_extension_tags(TProtoExtensionTag<TBoundaryKeysExt>::Value);
            }
            if (table->Dynamic) {
                if (!Spec_->EnableDynamicStoreRead.value_or(true)) {
                    req->set_omit_dynamic_stores(true);
                }
                if (OperationType == EOperationType::RemoteCopy) {
                    req->set_throw_on_chunk_views(true);
                }
            }
            // NB: we always fetch parity replicas since
            // erasure reader can repair data on flight.
            req->set_fetch_parity_replicas(true);
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
        },
        Logger);

    for (int tableIndex = 0; tableIndex < std::ssize(InputTables_); ++tableIndex) {
        yielder.TryYield();

        auto& table = InputTables_[tableIndex];
        auto ranges = table->Path.GetNewRanges(table->Comparator, table->Schema->GetKeyColumnTypes());

        // XXX(max42): does this ever happen?
        if (ranges.empty()) {
            continue;
        }

        bool hasColumnSelectors = table->Path.GetColumns().operator bool();

        if (std::ssize(ranges) > Config->MaxRangesOnTable) {
            THROW_ERROR_EXCEPTION(
                "Too many ranges on table: maximum allowed %v, actual %v",
                Config->MaxRangesOnTable,
                ranges.size())
                << TErrorAttribute("table_path", table->Path);
        }

        YT_LOG_DEBUG("Adding input table for fetch (Path: %v, Id: %v, Dynamic: %v, ChunkCount: %v, RangeCount: %v, "
            "HasColumnSelectors: %v, EnableDynamicStoreRead: %v)",
            table->GetPath(),
            table->ObjectId,
            table->Dynamic,
            table->ChunkCount,
            ranges.size(),
            hasColumnSelectors,
            Spec_->EnableDynamicStoreRead);

        chunkSpecFetcher->Add(
            table->ObjectId,
            table->ExternalCellTag,
            table->Dynamic && !table->Schema->IsSorted() ? -1 : table->ChunkCount,
            tableIndex,
            ranges);
    }

    YT_LOG_INFO("Fetching input tables");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("Input tables fetched");

    for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        yielder.TryYield();

        int tableIndex = chunkSpec.table_index();
        auto& table = InputTables_[tableIndex];

        auto inputChunk = New<TInputChunk>(
            chunkSpec,
            /*keyColumnCount*/ table->Comparator.GetLength());
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetChunkIndex(totalChunkCount++);

        if (inputChunk->IsDynamicStore() && !table->Schema->IsSorted()) {
            if (!InputHasOrderedDynamicStores_) {
                YT_LOG_DEBUG("Operation input has ordered dynamic stores, job interrupts "
                    "are disabled (TableId: %v, TablePath: %v)",
                    table->ObjectId,
                    table->GetPath());
                InputHasOrderedDynamicStores_ = true;
            }
        }

        if (inputChunk->GetRowCount() > 0 || inputChunk->IsFile()) {
            // Input chunks may have zero row count in case of unsensible read range with coinciding
            // lower and upper row index. We skip such chunks.
            // NB(coteeq): File chunks have zero rows as well, but we want to be able to remote_copy them.
            table->Chunks.emplace_back(inputChunk);
            for (const auto& extension : chunkSpec.chunk_meta().extensions().extensions()) {
                totalExtensionSize += extension.data().size();
            }
            RegisterInputChunk(table->Chunks.back());

            // We fetch columnar statistics only for the tables that have column selectors specified.
            auto hasColumnSelectors = table->Path.GetColumns().operator bool();
            bool shouldSkip = IsUnavailable(inputChunk, GetChunkAvailabilityPolicy()) && Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Skip;
            if (hasColumnSelectors && Spec_->InputTableColumnarStatistics->Enabled.value_or(Config->UseColumnarStatisticsDefault) && !shouldSkip) {
                auto stableColumnNames = MapNamesToStableNames(
                    *table->Schema,
                    *table->Path.GetColumns(),
                    NonexistentColumnName);
                columnarStatisticsFetcher->AddChunk(inputChunk, stableColumnNames);
            }

            if (hasColumnSelectors) {
                inputChunk->SetValuesPerRow(table->Path.GetColumns()->size());
            } else if (table->Schema && table->Schema->GetStrict()) {
                inputChunk->SetValuesPerRow(table->Schema->Columns().size());
            }
        }
    }

    if (columnarStatisticsFetcher->GetChunkCount() > 0) {
        YT_LOG_INFO("Fetching chunk columnar statistics for tables with column selectors (ChunkCount: %v)",
            columnarStatisticsFetcher->GetChunkCount());
        MaybeCancel(ECancelationStage::ColumnarStatisticsFetch);
        columnarStatisticsFetcher->SetCancelableContext(GetCancelableContext());
        WaitFor(columnarStatisticsFetcher->Fetch())
            .ThrowOnError();
        YT_LOG_INFO("Columnar statistics fetched");
        columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
    }

    YT_LOG_INFO("Finished fetching input tables (TotalChunkCount: %v, TotalExtensionSize: %v, MemoryUsage: %v)",
        totalChunkCount,
        totalExtensionSize,
        GetMemoryUsage());

    // TODO(galtsev): remove after YT-20281 is fixed
    if (AnyOf(InputTables_, IsStaticTableWithHunks)) {
        InputHasStaticTableWithHunks_ = true;
        YT_LOG_INFO("Static tables with hunks found, disabling job splitting");
    }
}

void TOperationControllerBase::RegisterInputChunk(const TInputChunkPtr& inputChunk)
{
    auto chunkId = inputChunk->GetChunkId();

    // Insert an empty TInputChunkDescriptor if a new chunkId is encountered.
    auto& chunkDescriptor = InputChunkMap[chunkId];
    chunkDescriptor.InputChunks.push_back(inputChunk);

    if (IsUnavailable(inputChunk, GetChunkAvailabilityPolicy())) {
        chunkDescriptor.State = EInputChunkState::Waiting;
    }
}

void TOperationControllerBase::LockInputTables()
{
    //! TODO(ignat): Merge in with lock input files method.
    YT_LOG_INFO("Locking input tables");

    auto proxy = CreateObjectServiceWriteProxy(InputClient);
    auto batchReq = proxy.ExecuteBatchWithRetries(InputClient->GetNativeConnection()->GetConfig()->ChunkFetchRetries);

    for (const auto& table : InputTables_) {
        auto req = TTableYPathProxy::Lock(table->GetPath());
        req->Tag() = table;
        req->set_mode(ToProto<int>(ELockMode::Snapshot));
        SetTransactionId(req, *table->TransactionId);
        GenerateMutationId(req);
        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error locking input tables");

    const auto& batchRsp = batchRspOrError.Value();
    for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspLock>()) {
        const auto& rsp = rspOrError.Value();
        auto table = std::any_cast<TInputTablePtr>(rsp->Tag());
        table->ObjectId = FromProto<TObjectId>(rsp->node_id());
        table->Revision = rsp->revision();
        table->ExternalCellTag = FromProto<TCellTag>(rsp->external_cell_tag());
        table->ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *table->TransactionId;
        PathToInputTables_[table->GetPath()].push_back(table);
    }
}

template <class TTable, class TTransactionIdFunc>
void TOperationControllerBase::FetchTableSchemas(
    const NApi::NNative::IClientPtr& client,
    const TRange<TTable>& tables,
    TTransactionIdFunc tableToTransactionId,
    bool fetchFromExternalCells) const
{
    // The fetchFromExternalCells parameter allows us to choose whether to fetch the schema from native or external cell.
    // Ideally, we want to fetch schemas only from external cells, but it is not possible now. For output
    // tables, lock is acquired after the schema is fetched. This behavior is bad as it may lead to races.
    // Once locking output tables is fixed, we will always fetch the schemas from external cells, and the
    // fetchFromExternalCells parameter will be removed. See also YT-15269.
    // TODO(gepardo): always fetch schemas from external cells.
    auto tableToCellTag = [&] (const TTable& table) {
        return fetchFromExternalCells
            ? table->ExternalCellTag
            : CellTagFromId(table->ObjectId);
    };

    THashMap<TGuid, std::vector<TTable>> schemaIdToTables;
    THashMap<TCellTag, std::vector<TGuid>> cellTagToSchemaIds;
    for (const auto& table : tables) {
        const auto& schemaId = table->SchemaId;
        schemaIdToTables[schemaId].push_back(table);
    }

    for (const auto& [schemaId, tablesWithIdenticalSchema] : schemaIdToTables) {
        YT_VERIFY(!tablesWithIdenticalSchema.empty());
        auto cellTag = tableToCellTag(tablesWithIdenticalSchema.front());
        cellTagToSchemaIds[cellTag].push_back(schemaId);
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    for (auto& [cellTag, schemaIds] : cellTagToSchemaIds) {
        auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower, cellTag);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& schemaId : schemaIds) {
            // TODO(gepardo): fetch schema by schema ID directly, without using Get for the corresponding table.
            auto table = schemaIdToTables[schemaId][0];
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@schema");
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, tableToTransactionId(table));
            req->Tag() = schemaId;
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
    }

    auto checkError = [] (const auto& error) {
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error fetching table schemas");
    };

    auto result = WaitFor(AllSucceeded(asyncResults));
    checkError(result);

    for (const auto& batchRsp : result.Value()) {
        checkError(GetCumulativeError(batchRsp));
        for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            auto schema = ConvertTo<TTableSchemaPtr>(TYsonString(rsp->value()));
            auto schemaId = std::any_cast<TGuid>(rsp->Tag());
            for (const auto& table : schemaIdToTables[schemaId]) {
                table->Schema = schema;
            }
        }
    }
}

void TOperationControllerBase::SafeOnJobInfoReceivedFromNode(std::unique_ptr<TJobSummary> jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    auto jobId = jobSummary->Id;

    YT_LOG_DEBUG(
        "Job info received from node (JobId: %v, JobState: %v, HasStatistics: %v, FinishTime: %v)",
        jobId,
        jobSummary->State,
        static_cast<bool>(jobSummary->Statistics),
        jobSummary->FinishTime);

    auto joblet = FindJoblet(jobId);

    if (!joblet) {
        YT_LOG_DEBUG(
            "Received job info for unknown job (JobId: %v)",
            jobId);
        return;
    }

    if (!joblet->IsStarted()) {
        YT_VERIFY(joblet->Revived);

        YT_LOG_DEBUG(
            "Received revived job info for job that is not marked started in snapshot; processing job start "
            "(JobId: %v, JobState: %v)",
            jobId,
            jobSummary->State);

        OnJobStarted(joblet);
    }

    switch (jobSummary->State) {
        case EJobState::Waiting:
            break;
        case EJobState::Running:
            OnJobRunning(SummaryCast<TRunningJobSummary>(std::move(jobSummary)));
            break;
        case EJobState::Completed:
            OnJobCompleted(
                SummaryCast<TCompletedJobSummary>(std::move(jobSummary)));
            break;
        case EJobState::Failed:
            OnJobFailed(
                SummaryCast<TFailedJobSummary>(std::move(jobSummary)));
            break;
        case EJobState::Aborted:
            OnJobAborted(
                SummaryCast<TAbortedJobSummary>(std::move(jobSummary)));
            break;
        default:
            YT_ABORT();
    }
}

void TOperationControllerBase::ValidateInputTablesTypes() const
{
    for (const auto& table : InputTables_) {
        if (table->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                table->GetPath(),
                EObjectType::Table,
                table->Type);
        }
    }
}

void TOperationControllerBase::ValidateUpdatingTablesTypes() const
{
    for (const auto& table : UpdatingTables_) {
        if (table->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                table->GetPath(),
                EObjectType::Table,
                table->Type);
        }
    }
}

EObjectType TOperationControllerBase::GetOutputTableDesiredType() const
{
    return EObjectType::Table;
}

void TOperationControllerBase::GetInputTablesAttributes()
{
    YT_LOG_INFO("Getting input tables attributes");

    GetUserObjectBasicAttributes(
        InputClient,
        MakeUserObjectList(InputTables_),
        InputTransaction->GetId(),
        Logger,
        EPermission::Read,
        TGetUserObjectBasicAttributesOptions{
            .OmitInaccessibleColumns = Spec_->OmitInaccessibleColumns,
            .PopulateSecurityTags = true
        });

    ValidateInputTablesTypes();

    std::vector<TYsonString> omittedInaccessibleColumnsList;
    for (const auto& table : InputTables_) {
        if (!table->OmittedInaccessibleColumns.empty()) {
            omittedInaccessibleColumnsList.push_back(BuildYsonStringFluently()
                .BeginMap()
                    .Item("path").Value(table->GetPath())
                    .Item("columns").Value(table->OmittedInaccessibleColumns)
                .EndMap());
        }
    }
    if (!omittedInaccessibleColumnsList.empty()) {
        auto error = TError("Some columns of input tables are inaccessible and were omitted")
            << TErrorAttribute("input_tables", omittedInaccessibleColumnsList);
        SetOperationAlert(EOperationAlertType::OmittedInaccessibleColumnsInInputTables, error);
    }

    THashMap<TCellTag, std::vector<TInputTablePtr>> externalCellTagToTables;
    for (const auto& table : InputTables_) {
        externalCellTagToTables[table->ExternalCellTag].push_back(table);
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
        auto proxy = CreateObjectServiceReadProxy(InputClient, EMasterChannelKind::Follower, externalCellTag);
        auto batchReq = proxy.ExecuteBatch();
        for (const auto& table : tables) {
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "dynamic",
                "chunk_count",
                "retained_timestamp",
                "schema_mode",
                "schema_id",
                "unflushed_timestamp",
                "content_revision",
                "enable_dynamic_store_read",
                "tablet_state",
                "account",
            });
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
            req->Tag() = table;
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
    }

    auto checkError = [] (const auto& error) {
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error getting attributes of input tables");
    };

    auto result = WaitFor(AllSucceeded(asyncResults));
    checkError(result);

    THashMap<TInputTablePtr, IAttributeDictionaryPtr> tableAttributes;
    for (const auto& batchRsp : result.Value()) {
        checkError(GetCumulativeError(batchRsp));
        for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
            auto table = std::any_cast<TInputTablePtr>(rsp->Tag());
            tableAttributes.emplace(std::move(table), std::move(attributes));
        }
    }

    bool needFetchSchemas = !InputTables_.empty() && InputTables_[0]->Type == EObjectType::Table;

    if (needFetchSchemas) {
        // Fetch the schemas based on schema IDs. We didn't fetch the schemas initially to allow deduplication
        // if there are multiple tables sharing same schema.
        for (const auto& [table, attributes] : tableAttributes) {
            table->SchemaId = attributes->Get<TGuid>("schema_id");
        }

        FetchTableSchemas(
            InputClient,
            MakeRange(InputTables_),
            [] (const auto& table) { return table->ExternalTransactionId; },
            /*fetchFromExternalCells*/ true);
    }

    bool haveTablesWithEnabledDynamicStoreRead = false;

    for (const auto& [table, attributes] : tableAttributes) {
        table->ChunkCount = attributes->Get<int>("chunk_count");
        table->ContentRevision = attributes->Get<NHydra::TRevision>("content_revision");
        table->Account = attributes->Get<TString>("account");
        if (table->Type == EObjectType::File) {
            // NB(coteeq): Files have none of the folllowing attributes.
            continue;
        }

        table->Dynamic = attributes->Get<bool>("dynamic");
        if (table->Schema->IsSorted()) {
            table->Comparator = table->Schema->ToComparator();
        }
        table->SchemaMode = attributes->Get<ETableSchemaMode>("schema_mode");

        haveTablesWithEnabledDynamicStoreRead |= attributes->Get<bool>("enable_dynamic_store_read", false);

        // Validate that timestamp is correct.
        ValidateDynamicTableTimestamp(
            table->Path,
            table->Dynamic,
            *table->Schema,
            *attributes,
            !Spec_->EnableDynamicStoreRead.value_or(true));

        YT_LOG_INFO("Input table locked (Path: %v, ObjectId: %v, Schema: %v, Dynamic: %v, ChunkCount: %v, SecurityTags: %v, "
            "Revision: %x, ContentRevision: %x)",
            table->GetPath(),
            table->ObjectId,
            *table->Schema,
            table->Dynamic,
            table->ChunkCount,
            table->SecurityTags,
            table->Revision,
            table->ContentRevision);

        if (!table->ColumnRenameDescriptors.empty()) {
            if (table->Path.GetTeleport()) {
                THROW_ERROR_EXCEPTION("Cannot rename columns in table with teleport")
                    << TErrorAttribute("table_path", table->Path);
            }
            YT_LOG_DEBUG("Start renaming columns of input table");
            auto description = Format("input table %v", table->GetPath());
            table->Schema = RenameColumnsInSchema(
                description,
                table->Schema,
                table->Dynamic,
                table->ColumnRenameDescriptors,
                /*changeStableName*/ !Config->EnableTableColumnRenaming);
            YT_LOG_DEBUG("Columns of input table are renamed (Path: %v, NewSchema: %v)",
                table->GetPath(),
                *table->Schema);
        }

        if (table->Dynamic && OperationType == EOperationType::RemoteCopy) {
            if (!Config->EnableVersionedRemoteCopy) {
                THROW_ERROR_EXCEPTION("Remote copy for dynamic tables is disabled");
            }

            auto tabletState = attributes->Get<ETabletState>("tablet_state");
            if (tabletState != ETabletState::Frozen && tabletState != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION("Input table has tablet state %Qlv: expected %Qlv or %Qlv",
                    tabletState,
                    ETabletState::Frozen,
                    ETabletState::Unmounted)
                    << TErrorAttribute("table_path", table->Path);
            }
        }

        // TODO(ifsmirnov): YT-20044
        if (table->Schema->HasHunkColumns() && OperationType == EOperationType::RemoteCopy) {
            if (!Spec_->BypassHunkRemoteCopyProhibition.value_or(false)) {
                THROW_ERROR_EXCEPTION("Table with hunk columns cannot be copied to another cluster")
                    << TErrorAttribute("table_path", table->Path);
            }
        }
    }

    if (Spec_->EnableDynamicStoreRead == true && !haveTablesWithEnabledDynamicStoreRead) {
        SetOperationAlert(
            EOperationAlertType::NoTablesWithEnabledDynamicStoreRead,
            TError(
                "enable_dynamic_store_read in operation spec set to true, "
                "but no input tables have @enable_dynamic_store_read attribute set"));
    }
}

void TOperationControllerBase::GetOutputTablesSchema()
{
    YT_LOG_INFO("Getting output tables schema");

    auto proxy = CreateObjectServiceReadProxy(OutputClient, EMasterChannelKind::Follower);
    auto batchReq = proxy.ExecuteBatch();

    static const auto AttributeKeys = [] {
        return ConcatVectors(
            GetTableUploadOptionsAttributeKeys(),
            std::vector<TString>{
                "schema_id"
            });
    }();

    YT_LOG_DEBUG("Fetching output tables schema information from primary cell");

    for (const auto& table : UpdatingTables_) {
        auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), AttributeKeys);
        req->Tag() = table;
        SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of output tables from native cell");
    const auto& batchRsp = batchRspOrError.Value();

    THashMap<TOutputTablePtr, IAttributeDictionaryPtr> tableAttributes;
    auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>();
    for (const auto& rspOrError : rspsOrError) {
        const auto& rsp = rspOrError.Value();

        auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
        tableAttributes.emplace(std::move(table), std::move(attributes));
    }

    YT_LOG_DEBUG("Finished fetching output tables schema information from primary cell");

    bool needFetchSchemas = !UpdatingTables_.empty() && !UpdatingTables_[0]->IsFile();
    if (needFetchSchemas) {
        // Fetch the schemas based on schema IDs. We didn't fetch the schemas initially to allow deduplication
        // if there are multiple tables sharing same schema.
        for (const auto& [table, attributes] : tableAttributes) {
            table->SchemaId = attributes->Get<TGuid>("schema_id");
        }

        // TODO(h0pless): Try fetching schema from external cells.
        // With schemas being externalized it became possible to do so.
        FetchTableSchemas(
            OutputClient,
            MakeRange(UpdatingTables_),
            [this] (const auto& table) { return GetTransactionForOutputTable(table)->GetId(); },
            /*fetchFromExternalCells*/ false);
    }

    for (const auto& [table, attributes] : tableAttributes) {
        const auto& path = table->Path;

        if (table->IsFile()) {
            table->TableUploadOptions = GetFileUploadOptions(path, *attributes);
            continue;
        } else {
            table->Dynamic = attributes->Get<bool>("dynamic");
            table->TableUploadOptions = GetTableUploadOptions(
                path,
                *attributes,
                table->Schema,
                0); // Here we assume zero row count, we will do additional check later.
        }

        // Will be used by AddOutputTableSpecs.
        table->TableUploadOptions.SchemaId = table->SchemaId;

        // Saving it here to make sure that we notice table schema change in PrepareOutputTables (if there is any).
        table->OriginalTableSchemaRevision = table->TableUploadOptions.TableSchema.GetRevision();

        if (table->Dynamic) {
            if (!table->TableUploadOptions.TableSchema->IsSorted()) {
                THROW_ERROR_EXCEPTION("Only sorted dynamic table can be updated")
                    << TErrorAttribute("table_path", path);
            }

            // Check if bulk insert is enabled for a certain user.
            if (!Config->EnableBulkInsertForEveryone && OperationType != EOperationType::RemoteCopy) {
                TGetNodeOptions options;
                options.ReadFrom = EMasterChannelKind::Cache;
                options.Attributes = {"enable_bulk_insert"};

                auto path = "//sys/users/" + ToYPathLiteral(AuthenticatedUser);
                auto rspOrError = WaitFor(OutputClient->GetNode(path, options));
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to check if bulk insert is enabled");
                auto rsp = ConvertTo<INodePtr>(rspOrError.Value());
                const auto& attributes = rsp->Attributes();
                if (!attributes.Get<bool>("enable_bulk_insert", false)) {
                    THROW_ERROR_EXCEPTION("Bulk insert is disabled for user %Qv, contact yt-admin@ for enabling",
                        AuthenticatedUser);
                }
            }
        }

        if (path.GetOutputTimestamp()) {
            if (table->Dynamic && table->TableUploadOptions.SchemaModification != ETableSchemaModification::None) {
                THROW_ERROR_EXCEPTION("Cannot set \"output_timestamp\" attribute to the dynamic table with nontrivial schema modification");
            }
            auto outputTimestamp = *path.GetOutputTimestamp();
            if (outputTimestamp < MinTimestamp || outputTimestamp > MaxTimestamp) {
                THROW_ERROR_EXCEPTION("Attribute \"output_timestamp\" value is out of range [%v, %v]",
                    MinTimestamp,
                    MaxTimestamp)
                    << TErrorAttribute("output_timestamp", outputTimestamp)
                    << TErrorAttribute("table_path", path);

            }

            table->Timestamp = outputTimestamp;
        } else {
            // TODO(savrus): I would like to see commit ts here. But as for now, start ts suffices.
            table->Timestamp = GetTransactionForOutputTable(table)->GetStartTimestamp();
        }

        // NB(psushin): This option must be set before PrepareOutputTables call.
        table->TableWriterOptions->EvaluateComputedColumns = table->TableUploadOptions.TableSchema->HasComputedColumns();

        table->TableWriterOptions->SchemaModification = table->TableUploadOptions.SchemaModification;

        YT_LOG_DEBUG("Received output table schema (Path: %v, Schema: %v, SchemaMode: %v, LockMode: %v)",
            path,
            *table->TableUploadOptions.TableSchema,
            table->TableUploadOptions.SchemaMode,
            table->TableUploadOptions.LockMode);
    }

    if (StderrTable_) {
        StderrTable_->TableUploadOptions.TableSchema = GetStderrBlobTableSchema().ToTableSchema();
        StderrTable_->TableUploadOptions.SchemaMode = ETableSchemaMode::Strong;
        if (StderrTable_->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
            THROW_ERROR_EXCEPTION("Cannot write stderr table in append mode");
        }
    }

    if (CoreTable_) {
        CoreTable_->TableUploadOptions.TableSchema = GetCoreBlobTableSchema().ToTableSchema();
        CoreTable_->TableUploadOptions.SchemaMode = ETableSchemaMode::Strong;
        if (CoreTable_->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
            THROW_ERROR_EXCEPTION("Cannot write core table in append mode");
        }
    }
}

void TOperationControllerBase::PrepareInputTables()
{
    if (!AreForeignTablesSupported()) {
        for (const auto& table : InputTables_) {
            if (table->IsForeign()) {
                THROW_ERROR_EXCEPTION("Foreign tables are not supported in %Qlv operation", OperationType)
                    << TErrorAttribute("foreign_table", table->GetPath());
            }
        }
    }
}

void TOperationControllerBase::PrepareOutputTables()
{ }

void TOperationControllerBase::LockOutputTablesAndGetAttributes()
{
    YT_LOG_INFO("Locking output tables");

    {
        auto proxy = CreateObjectServiceWriteProxy(OutputClient);
        auto batchReq = proxy.ExecuteBatch();
        for (const auto& table : UpdatingTables_) {
            auto req = TTableYPathProxy::Lock(table->GetObjectIdPath());
            SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
            GenerateMutationId(req);
            req->set_mode(ToProto<int>(table->TableUploadOptions.LockMode));
            req->Tag() = table;
            batchReq->AddRequest(req);
        }
        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error locking output tables");

        const auto& batchRsp = batchRspOrError.Value();
        for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspLock>()) {
            const auto& rsp = rspOrError.Value();
            const auto& table = std::any_cast<TOutputTablePtr>(rsp->Tag());;

            auto objectId = FromProto<TObjectId>(rsp->node_id());
            auto revision = rsp->revision();

            table->ExternalTransactionId = rsp->has_external_transaction_id()
                ? FromProto<TTransactionId>(rsp->external_transaction_id())
                : GetTransactionForOutputTable(table)->GetId();

            YT_LOG_INFO("Output table locked (Path: %v, ObjectId: %v, Schema: %v, ExternalTransactionId: %v, Revision: %x)",
                table->GetPath(),
                objectId,
                *table->TableUploadOptions.TableSchema,
                table->ExternalTransactionId,
                revision);

            if (auto it = PathToInputTables_.find(table->GetPath())) {
                for (const auto& inputTable : it->second) {
                    // NB: remote copy is a special case.
                    if (CellTagFromId(inputTable->ObjectId) != CellTagFromId(objectId)) {
                        continue;
                    }
                    if (inputTable->ObjectId != objectId || inputTable->Revision != revision) {
                        THROW_ERROR_EXCEPTION(
                            NScheduler::EErrorCode::OperationFailedWithInconsistentLocking,
                            "Table %v has changed between taking input and output locks",
                            inputTable->GetPath())
                            << TErrorAttribute("input_object_id", inputTable->ObjectId)
                            << TErrorAttribute("input_revision", inputTable->Revision)
                            << TErrorAttribute("output_object_id", objectId)
                            << TErrorAttribute("output_revision", revision);
                    }
                }
            }
        }
    }

    YT_LOG_INFO("Getting output tables attributes");

    {
        THashMap<TCellTag, TVector<TOutputTablePtr>> perCellUpdatingTables;
        for (const auto& table : UpdatingTables_) {
            perCellUpdatingTables[table->ExternalCellTag].push_back(table);
        }

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> futures;
        for (const auto& [externalCellTag, tables] : perCellUpdatingTables) {
            auto proxy = CreateObjectServiceReadProxy(OutputClient, EMasterChannelKind::Follower, externalCellTag);
            auto batchReq = proxy.ExecuteBatch();

            YT_LOG_DEBUG("Fetching attributes of output tables from external cell (CellTag: %v, NodeCount: %v)",
                externalCellTag,
                tables.size());

            for (const auto& table : tables) {
                auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
                ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                    "schema_id",
                    "account",
                    "chunk_writer",
                    "primary_medium",
                    "replication_factor",
                    "row_count",
                    "vital",
                    "enable_skynet_sharing",
                    "atomicity",
                });
                req->Tag() = table;
                SetTransactionId(req, table->ExternalTransactionId);

                batchReq->AddRequest(req);
            }

            futures.push_back(batchReq->Invoke());
        }

        auto checkErrorExternalCells = [] (const auto& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error getting attributes of output tables from external cells");
        };

        auto responses = WaitFor(AllSucceeded(futures));
        checkErrorExternalCells(responses);

        THashMap<TOutputTablePtr, IAttributeDictionaryPtr> tableAttributes;
        for (const auto& response : responses.Value()) {
            checkErrorExternalCells(GetCumulativeError(response));
            auto rspsOrErrors = response->GetResponses<TTableYPathProxy::TRspGet>();
            for (const auto& rspOrError : rspsOrErrors) {
                const auto& rsp = rspOrError.Value();

                auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
                auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                tableAttributes.emplace(std::move(table), std::move(attributes));
            }
        }

        YT_LOG_DEBUG("Finished fetching output tables schema from external cells");

        for (const auto& [table, attributes] : tableAttributes) {
            if (table->IsFile()) {
                continue;
            }
            auto receivedSchemaId = attributes->GetAndRemove<TGuid>("schema_id");
            if (receivedSchemaId != table->SchemaId) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::OperationFailedWithInconsistentLocking,
                    "Schema of an output table %v has changed between schema fetch and lock acquisition",
                    table->GetPath())
                        << TErrorAttribute("expected_schema_id", table->SchemaId)
                        << TErrorAttribute("received_schema_id", receivedSchemaId);
            }
        }

        // Getting attributes from primary cell
        auto proxy = CreateObjectServiceReadProxy(OutputClient, EMasterChannelKind::Follower);
        auto batchReq = proxy.ExecuteBatch();

        YT_LOG_DEBUG("Fetching attributes of output tables from native cell");

        for (const auto& table : UpdatingTables_) {
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "effective_acl",
                "tablet_state",
                "backup_state",
                "tablet_statistics",
                "max_overlapping_store_count",
            });
            req->Tag() = table;
            SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error getting attributes of output tables from native cell");
        const auto& batchRsp = batchRspOrError.Value();

        auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>();
        for (const auto& rspOrError : rspsOrError) {
            const auto& rsp = rspOrError.Value();

            auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

            auto it = tableAttributes.find(table);
            YT_VERIFY(it != tableAttributes.end());
            it->second->MergeFrom(*attributes.Get());
        }

        YT_LOG_DEBUG("Finished fetching output tables schema from native cell");

        YT_LOG_DEBUG("Fetching max heavy columns from master");

        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;

        auto maxHeavyColumnsRspOrError = WaitFor(OutputClient->GetNode("//sys/@config/chunk_manager/max_heavy_columns", options));
        THROW_ERROR_EXCEPTION_IF_FAILED(maxHeavyColumnsRspOrError, "Failed to get max heavy columns from master");

        auto maxHeavyColumns = ConvertTo<int>(maxHeavyColumnsRspOrError.Value());

        YT_LOG_DEBUG("Finished fetching max heavy columns (MaxHeavyColumns: %v)", maxHeavyColumns);

        for (const auto& [table, attributes] : tableAttributes) {
            const auto& path = table->GetPath();

            if (table->Dynamic) {
                auto tabletState = attributes->Get<ETabletState>("tablet_state");
                if (OperationType == EOperationType::RemoteCopy) {
                    if (tabletState != ETabletState::Unmounted) {
                        THROW_ERROR_EXCEPTION("Remote copy is only allowed to unmounted table, "
                            "while output table %v has tablet state %Qv",
                            path,
                            tabletState);
                    }
                } else {
                    if (tabletState != ETabletState::Mounted && tabletState != ETabletState::Frozen) {
                        THROW_ERROR_EXCEPTION("Output table %v tablet state %Qv does not allow to write into it",
                            path,
                            tabletState);
                    }
                }

                auto backupState = attributes->Get<ETableBackupState>("backup_state", ETableBackupState::None);
                if (backupState != ETableBackupState::None) {
                    THROW_ERROR_EXCEPTION("Output table %v backup state %Qlv does not allow to write into it",
                        path,
                        backupState);
                }

                if (UserTransactionId) {
                    THROW_ERROR_EXCEPTION(
                        "Operations with output to dynamic tables cannot be run under user transaction")
                        << TErrorAttribute("user_transaction_id", UserTransactionId);
                }

                auto atomicity = attributes->Get<EAtomicity>("atomicity");
                if (atomicity != Spec_->Atomicity) {
                    THROW_ERROR_EXCEPTION("Output table %lv atomicity %Qv does not match spec atomicity %Qlv",
                        path,
                        atomicity,
                        Spec_->Atomicity);
                }

                if (table->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
                    auto overlappingStoreCount = TryGetInt64(
                        attributes->GetYson("tablet_statistics").ToString(),
                        "/overlapping_store_count");
                    if (!overlappingStoreCount) {
                        THROW_ERROR_EXCEPTION("Output table %v does not have @tablet_statistics/overlapping_store_count attribute",
                            path);
                    }
                    auto maxOverlappingStoreCount = attributes->Get<int>(
                        "max_overlapping_store_count",
                        DefaultMaxOverlappingStoreCount);

                    if (*overlappingStoreCount >= maxOverlappingStoreCount) {
                        THROW_ERROR_EXCEPTION(
                            "Cannot write to output table %v since overlapping store count limit is exceeded",
                            path)
                            << TErrorAttribute("overlapping_store_count", *overlappingStoreCount)
                            << TErrorAttribute("max_overlapping_store_count", maxOverlappingStoreCount);
                    }
                }
            }

            table->Account = attributes->Get<TString>("account");

            if (table->TableUploadOptions.TableSchema->IsSorted()) {
                table->TableWriterOptions->ValidateSorted = true;
                table->TableWriterOptions->ValidateUniqueKeys = table->TableUploadOptions.TableSchema->GetUniqueKeys();
            } else {
                table->TableWriterOptions->ValidateSorted = false;
            }

            table->TableWriterOptions->CompressionCodec = table->TableUploadOptions.CompressionCodec;
            table->TableWriterOptions->ErasureCodec = table->TableUploadOptions.ErasureCodec;
            table->TableWriterOptions->EnableStripedErasure = table->TableUploadOptions.EnableStripedErasure;
            table->TableWriterOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
            table->TableWriterOptions->MediumName = attributes->Get<TString>("primary_medium");
            table->TableWriterOptions->Account = attributes->Get<TString>("account");
            table->TableWriterOptions->ChunksVital = attributes->Get<bool>("vital");
            table->TableWriterOptions->OptimizeFor = table->TableUploadOptions.OptimizeFor;
            table->TableWriterOptions->ChunkFormat = table->TableUploadOptions.ChunkFormat;
            table->TableWriterOptions->EnableSkynetSharing = attributes->Get<bool>("enable_skynet_sharing", false);
            table->TableWriterOptions->MaxHeavyColumns = maxHeavyColumns;

            // Workaround for YT-5827.
            if (table->TableUploadOptions.TableSchema->Columns().empty() &&
                table->TableUploadOptions.TableSchema->GetStrict())
            {
                table->TableWriterOptions->OptimizeFor = EOptimizeFor::Lookup;
                table->TableWriterOptions->ChunkFormat = {};
            }

            table->EffectiveAcl = attributes->GetYson("effective_acl");
            table->WriterConfig = attributes->FindYson("chunk_writer");

            YT_LOG_INFO("Output table attributes fetched (Path: %v, Options: %v, UploadTransactionId: %v)",
                path,
                ConvertToYsonString(table->TableWriterOptions, EYsonFormat::Text).ToString(),
                table->UploadTransactionId);
        }
    }
}

void TOperationControllerBase::BeginUploadOutputTables(const std::vector<TOutputTablePtr>& tables)
{
    THashMap<TCellTag, std::vector<TOutputTablePtr>> nativeCellTagToTables;
    for (const auto& table : tables) {
        nativeCellTagToTables[CellTagFromId(table->ObjectId)].push_back(table);
    }

    THashMap<TCellTag, std::vector<TOutputTablePtr>> externalCellTagToTables;
    for (const auto& table : tables) {
        externalCellTagToTables[table->ExternalCellTag].push_back(table);
    }

    {
        YT_LOG_INFO("Starting upload for output tables");

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [nativeCellTag, tables] : nativeCellTagToTables) {
            auto proxy = CreateObjectServiceWriteProxy(OutputClient, nativeCellTag);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                auto req = TTableYPathProxy::BeginUpload(table->GetObjectIdPath());
                SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                GenerateMutationId(req);
                req->Tag() = table;

                if (!table->IsFile()) {
                    auto schemaChanged = table->OriginalTableSchemaRevision != table->TableUploadOptions.TableSchema.GetRevision();
                    if (!schemaChanged) {
                        YT_VERIFY(table->TableUploadOptions.SchemaId);
                        ToProto(req->mutable_table_schema_id(), table->TableUploadOptions.SchemaId);
                    } else {
                        // Sending schema, since in this case it might be not registered on master yet.
                        YT_LOG_DEBUG("Sending full table schema to master during begin upload");
                        ToProto(req->mutable_table_schema(), table->TableUploadOptions.TableSchema.Get());
                    }

                    req->set_schema_mode(ToProto<int>(table->TableUploadOptions.SchemaMode));
                }
                req->set_update_mode(ToProto<int>(table->TableUploadOptions.UpdateMode));
                req->set_lock_mode(ToProto<int>(table->TableUploadOptions.LockMode));
                req->set_upload_transaction_title(Format("Upload to %v from operation %v",
                    table->GetPath(),
                    OperationId));
                batchReq->AddRequest(req);
            }

            asyncResults.push_back(batchReq->Invoke());
        }

        auto checkError = [] (const auto& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error starting upload for output tables");
        };

        auto result = WaitFor(AllSucceeded(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
            for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspBeginUpload>()) {
                const auto& rsp = rspOrError.Value();

                auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
                table->UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
                table->SchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());
            }
        }
    }

    {
        YT_LOG_INFO("Getting output tables upload parameters");

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
            auto proxy = CreateObjectServiceReadProxy(OutputClient, EMasterChannelKind::Follower, externalCellTag);
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                auto req = TTableYPathProxy::GetUploadParams(table->GetObjectIdPath());
                SetTransactionId(req, table->UploadTransactionId);
                req->Tag() = table;
                if (table->TableUploadOptions.TableSchema->IsSorted() &&
                    !table->Dynamic &&
                    table->TableUploadOptions.UpdateMode == EUpdateMode::Append)
                {
                    req->set_fetch_last_key(true);
                }
                batchReq->AddRequest(req);
            }

            asyncResults.push_back(batchReq->Invoke());
        }

        auto checkError = [] (const auto& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error getting upload parameters of output tables");
        };

        auto result = WaitFor(AllSucceeded(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
            for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGetUploadParams>()) {
                const auto& rsp = rspOrError.Value();
                auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());

                if (table->Dynamic) {
                    table->PivotKeys = FromProto<std::vector<TLegacyOwningKey>>(rsp->pivot_keys());
                    table->TabletChunkListIds = FromProto<std::vector<TChunkListId>>(rsp->tablet_chunk_list_ids());
                } else {
                    const auto& schema = table->TableUploadOptions.TableSchema;
                    table->OutputChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
                    if (schema->IsSorted() && table->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
                        TUnversionedOwningRow row;
                        FromProto(&row, rsp->last_key());
                        auto fixedRow = LegacyKeyToKeyFriendlyOwningRow(row, schema->GetKeyColumnCount());
                        if (row != fixedRow) {
                            YT_LOG_DEBUG(
                                "Table last key fixed (Path: %v, LastKey: %v -> %v)",
                                table->GetPath(),
                                row,
                                fixedRow);
                            row = fixedRow;
                        }
                        auto capturedRow = RowBuffer->CaptureRow(row);
                        table->LastKey = TKey::FromRowUnchecked(capturedRow, schema->GetKeyColumnCount());
                        YT_LOG_DEBUG(
                            "Writing to table in sorted append mode (Path: %v, LastKey: %v)",
                            table->GetPath(),
                            table->LastKey);
                    }
                }

                YT_LOG_INFO("Upload parameters of output table received (Path: %v, ChunkListId: %v)",
                    table->GetPath(),
                    table->OutputChunkListId);
            }
        }
    }
}

void TOperationControllerBase::FetchUserFiles()
{
    std::vector<TUserFile*> userFiles;

    auto chunkSpecFetcher = New<TMasterChunkSpecFetcher>(
        InputClient,
        TMasterReadOptions{},
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        Config->MaxChunksPerFetch,
        Config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int fileIndex) {
            const auto& file = *userFiles[fileIndex];
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            if (file.Type == EObjectType::File && file.Path.GetColumns() && Spec_->UserFileColumnarStatistics->Enabled) {
                req->add_extension_tags(TProtoExtensionTag<THeavyColumnStatisticsExt>::Value);
            }
            if (file.Dynamic || IsBoundaryKeysFetchEnabled()) {
                req->add_extension_tags(TProtoExtensionTag<TBoundaryKeysExt>::Value);
            }
            if (file.Dynamic) {
                if (!Spec_->EnableDynamicStoreRead.value_or(true)) {
                    req->set_omit_dynamic_stores(true);
                }
                if (OperationType == EOperationType::RemoteCopy) {
                    req->set_throw_on_chunk_views(true);
                }
            }
            // NB: we always fetch parity replicas since
            // erasure reader can repair data on flight.
            req->set_fetch_parity_replicas(true);
            AddCellTagToSyncWith(req, file.ObjectId);
            SetTransactionId(req, file.ExternalTransactionId);
        },
        Logger);

    auto addFileForFetching = [&userFiles, &chunkSpecFetcher] (TUserFile& file) {
        int fileIndex = userFiles.size();
        userFiles.push_back(&file);

        std::vector<TReadRange> readRanges;
        switch (file.Type) {
            case EObjectType::Table:
                readRanges = file.Path.GetNewRanges(file.Schema->ToComparator(), file.Schema->GetKeyColumnTypes());
                break;
            case EObjectType::File:
                readRanges = {TReadRange()};
                break;
            default:
                YT_ABORT();
        }

        chunkSpecFetcher->Add(
            file.ObjectId,
            file.ExternalCellTag,
            file.ChunkCount,
            fileIndex,
            readRanges);
    };

    for (auto& [userJobSpec, files] : UserJobFiles_) {
        for (auto& file : files) {
            YT_LOG_INFO("Adding user file for fetch (Path: %v, TaskTitle: %v)",
                file.Path,
                userJobSpec->TaskTitle);

            addFileForFetching(file);
        }
    }

    if (BaseLayer_) {
        YT_LOG_INFO("Adding base layer for fetch (Path: %v)",
            BaseLayer_->Path);

        addFileForFetching(*BaseLayer_);
    }

    YT_LOG_INFO("Fetching user files");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("User files fetched (ChunkCount: %v)",
        chunkSpecFetcher->ChunkSpecs().size());

    for (auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        if (IsDynamicTabletStoreType(TypeFromId(chunkId))) {
            const auto& fileName = userFiles[chunkSpec.table_index()]->Path;
            THROW_ERROR_EXCEPTION(
                "Dynamic store read is not supported for user files but it is "
                "enabled for user file %Qv; consider disabling dynamic store read "
                "in operation spec by setting \"enable_dynamic_store_read\" option "
                "to false or disable dynamic store read for table by setting attribute "
                "\"enable_dynamic_store_read\" to false and remounting table.",
                fileName);
        }

        // NB(gritukan): all user files chunks should have table_index = 0.
        int tableIndex = chunkSpec.table_index();
        chunkSpec.set_table_index(0);

        userFiles[tableIndex]->ChunkSpecs.push_back(chunkSpec);
    }
}

void TOperationControllerBase::ValidateUserFileSizes()
{
    YT_LOG_INFO("Validating user file sizes");
    auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        InputClient,
        TColumnarStatisticsFetcher::TOptions{
            .Config = Config->Fetcher,
            .NodeDirectory = InputNodeDirectory_,
            .ChunkScraper = CreateFetcherChunkScraper(),
            .Mode = Spec_->UserFileColumnarStatistics->Mode,
            .EnableEarlyFinish = Config->EnableColumnarStatisticsEarlyFinish,
            .Logger = Logger,
        });

    // Collect columnar statistics for table files with column selectors.
    for (auto& [_, files] : UserJobFiles_) {
        for (auto& file : files) {
            if (file.Type == EObjectType::Table) {
                for (const auto& chunkSpec : file.ChunkSpecs) {
                    auto chunk = New<TInputChunk>(chunkSpec);
                    file.Chunks.emplace_back(chunk);
                    if (file.Path.GetColumns() && Spec_->UserFileColumnarStatistics->Enabled) {
                        auto stableColumnNames = MapNamesToStableNames(
                            *file.Schema,
                            *file.Path.GetColumns(),
                            NonexistentColumnName);
                        columnarStatisticsFetcher->AddChunk(chunk, stableColumnNames);
                    }
                }
            }
        }
    }

    if (columnarStatisticsFetcher->GetChunkCount() > 0) {
        YT_LOG_INFO("Fetching columnar statistics for table files with column selectors (ChunkCount: %v)",
            columnarStatisticsFetcher->GetChunkCount());
        columnarStatisticsFetcher->SetCancelableContext(GetCancelableContext());
        WaitFor(columnarStatisticsFetcher->Fetch())
            .ThrowOnError();
        columnarStatisticsFetcher->ApplyColumnSelectivityFactors();
    }

    auto updateOptional = [] (auto& updated, auto patch, auto defaultValue)
    {
        if (!updated.has_value()) {
            if (patch.has_value()) {
                updated = patch;
            } else {
                updated = defaultValue;
            }
        } else if (patch.has_value()) {
            updated = std::min(updated.value(), patch.value());
        }
    };

    auto userFileLimitsPatch = New<TUserFileLimitsPatchConfig>();
    for (const auto& [treeName, _] : PoolTreeControllerSettingsMap_) {
        auto it = Config->UserFileLimitsPerTree.find(treeName);
        bool found = it != Config->UserFileLimitsPerTree.end();
        updateOptional(
            userFileLimitsPatch->MaxSize,
            found ? it->second->MaxSize : std::optional<i64>(),
            Config->UserFileLimits->MaxSize);
        updateOptional(
            userFileLimitsPatch->MaxTableDataWeight,
            found ? it->second->MaxTableDataWeight : std::optional<i64>(),
            Config->UserFileLimits->MaxTableDataWeight);
        updateOptional(
            userFileLimitsPatch->MaxChunkCount,
            found ? it->second->MaxChunkCount : std::optional<i64>(),
            Config->UserFileLimits->MaxChunkCount);
    }

    auto userFileLimits = New<TUserFileLimitsConfig>();
    userFileLimits->MaxSize = userFileLimitsPatch->MaxSize.value();
    userFileLimits->MaxTableDataWeight = userFileLimitsPatch->MaxTableDataWeight.value();
    userFileLimits->MaxChunkCount = userFileLimitsPatch->MaxChunkCount.value();

    auto validateFile = [&userFileLimits, this] (const TUserFile& file) {
        YT_LOG_DEBUG("Validating user file (FileName: %v, Path: %v, Type: %v, HasColumns: %v)",
            file.FileName,
            file.Path,
            file.Type,
            file.Path.GetColumns().operator bool());
        auto chunkCount = file.Type == NObjectClient::EObjectType::File ? file.ChunkCount : file.Chunks.size();
        if (static_cast<i64>(chunkCount) > userFileLimits->MaxChunkCount) {
            THROW_ERROR_EXCEPTION(
                "User file %v exceeds chunk count limit: %v > %v",
                file.Path,
                chunkCount,
                userFileLimits->MaxChunkCount);
        }
        if (file.Type == NObjectClient::EObjectType::Table) {
            i64 dataWeight = 0;
            for (const auto& chunk : file.Chunks) {
                dataWeight += chunk->GetDataWeight();
            }
            if (dataWeight > userFileLimits->MaxTableDataWeight) {
                THROW_ERROR_EXCEPTION(
                    "User file table %v exceeds data weight limit: %v > %v",
                    file.Path,
                    dataWeight,
                    userFileLimits->MaxTableDataWeight);
            }
        } else {
            i64 uncompressedSize = 0;
            for (const auto& chunkSpec : file.ChunkSpecs) {
                uncompressedSize += GetChunkUncompressedDataSize(chunkSpec);
            }
            if (uncompressedSize > userFileLimits->MaxSize) {
                THROW_ERROR_EXCEPTION(
                    "User file %v exceeds size limit: %v > %v",
                    file.Path,
                    uncompressedSize,
                    userFileLimits->MaxSize);
            }
        }
    };

    for (auto& [_, files] : UserJobFiles_) {
        for (const auto& file : files) {
            validateFile(file);
        }
    }

    if (BaseLayer_) {
        validateFile(*BaseLayer_);
    }
}

void TOperationControllerBase::LockUserFiles()
{
    YT_LOG_INFO("Locking user files");

    auto proxy = CreateObjectServiceWriteProxy(OutputClient);
    auto batchReq = proxy.ExecuteBatch();

    auto lockFile = [&batchReq] (TUserFile& file) {
        auto req = TFileYPathProxy::Lock(file.Path.GetPath());
        req->set_mode(ToProto<int>(ELockMode::Snapshot));
        GenerateMutationId(req);
        SetTransactionId(req, *file.TransactionId);
        req->Tag() = &file;
        batchReq->AddRequest(req);
    };

    for (auto& [userJobSpec, files] : UserJobFiles_) {
        for (auto& file : files) {
            lockFile(file);
        }
    }

    if (BaseLayer_) {
        lockFile(*BaseLayer_);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error locking user files");

    const auto& batchRsp = batchRspOrError.Value();
    for (const auto& rspOrError : batchRsp->GetResponses<TCypressYPathProxy::TRspLock>()) {
        const auto& rsp = rspOrError.Value();
        auto* file = std::any_cast<TUserFile*>(rsp->Tag());
        file->ObjectId = FromProto<TObjectId>(rsp->node_id());
        file->ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *file->TransactionId;
    }
}

void TOperationControllerBase::GetUserFilesAttributes()
{
    // XXX(babenko): refactor; in particular, request attributes from external cells
    YT_LOG_INFO("Getting user files attributes");

    for (auto& [userJobSpec, files] : UserJobFiles_) {
        GetUserObjectBasicAttributes(
            Client,
            MakeUserObjectList(files),
            InputTransaction->GetId(),
            Logger.WithTag("TaskTitle: %v", userJobSpec->TaskTitle),
            EPermission::Read,
            TGetUserObjectBasicAttributesOptions{
                .PopulateSecurityTags = true
            });
    }

    if (BaseLayer_) {
        std::vector<TUserObject*> layers(1, &*BaseLayer_);

        GetUserObjectBasicAttributes(
            Client,
            layers,
            InputTransaction->GetId(),
            Logger,
            EPermission::Read,
            TGetUserObjectBasicAttributesOptions{
                .PopulateSecurityTags = true
            });
    }

    for (const auto& files : GetValues(UserJobFiles_)) {
        for (const auto& file : files) {
            const auto& path = file.Path.GetPath();
            if (!file.Layer && file.Type != EObjectType::Table && file.Type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("User file %v has invalid type: expected %Qlv or %Qlv, actual %Qlv",
                    path,
                    EObjectType::Table,
                    EObjectType::File,
                    file.Type);
            } else if (file.Layer && file.Type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("User layer %v has invalid type: expected %Qlv, actual %Qlv",
                    path,
                    EObjectType::File,
                    file.Type);
            }
        }
    }

    if (BaseLayer_ && BaseLayer_->Type != EObjectType::File) {
        THROW_ERROR_EXCEPTION("User layer %v has invalid type: expected %Qlv, actual %Qlv",
            BaseLayer_->Path,
            EObjectType::File,
            BaseLayer_->Type);
    }


    auto proxy = CreateObjectServiceReadProxy(OutputClient, EMasterChannelKind::Follower);
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& files : GetValues(UserJobFiles_)) {
        for (const auto& file : files) {
            {
                auto req = TYPathProxy::Get(file.GetObjectIdPath() + "/@");
                SetTransactionId(req, *file.TransactionId);
                std::vector<TString> attributeKeys;
                attributeKeys.push_back("file_name");
                attributeKeys.push_back("account");
                switch (file.Type) {
                    case EObjectType::File:
                        attributeKeys.push_back("executable");
                        attributeKeys.push_back("filesystem");
                        attributeKeys.push_back("access_method");
                        break;

                    case EObjectType::Table:
                        attributeKeys.push_back("format");
                        attributeKeys.push_back("dynamic");
                        attributeKeys.push_back("schema");
                        attributeKeys.push_back("retained_timestamp");
                        attributeKeys.push_back("unflushed_timestamp");
                        attributeKeys.push_back("enable_dynamic_store_read");
                        break;

                    default:
                        YT_ABORT();
                }
                attributeKeys.push_back("key");
                attributeKeys.push_back("chunk_count");
                attributeKeys.push_back("content_revision");
                ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
                batchReq->AddRequest(req, "get_attributes");
            }

            {
                auto req = TYPathProxy::Get(file.Path.GetPath() + "&/@");
                SetTransactionId(req, *file.TransactionId);
                ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                    "key",
                    "file_name"
                });
                batchReq->AddRequest(req, "get_link_attributes");
            }
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting attributes of user files");
    const auto& batchRsp = batchRspOrError.Value();

    auto getAttributesRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_attributes");
    auto getLinkAttributesRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_link_attributes");

    int index = 0;
    for (auto& [userJobSpec, files] : UserJobFiles_) {
        THashSet<TString> userFileNames;
        try {
            for (auto& file : files) {
                const auto& path = file.Path.GetPath();

                {
                    const auto& rspOrError = getAttributesRspsOrError[index];
                    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting attributes of user file ", path);
                    const auto& rsp = rspOrError.Value();
                    const auto& linkRsp = getLinkAttributesRspsOrError[index];
                    index++;

                    file.Attributes = ConvertToAttributes(TYsonString(rsp->value()));
                    const auto& attributes = *file.Attributes;

                    try {
                        if (const auto& fileNameFromPath = file.Path.GetFileName()) {
                            file.FileName = *fileNameFromPath;
                        } else {
                            const auto* actualAttributes = &attributes;
                            IAttributeDictionaryPtr linkAttributes;
                            if (linkRsp.IsOK()) {
                                linkAttributes = ConvertToAttributes(TYsonString(linkRsp.Value()->value()));
                                actualAttributes = linkAttributes.Get();
                            }
                            if (const auto& fileNameAttribute = actualAttributes->Find<TString>("file_name")) {
                                file.FileName = *fileNameAttribute;
                            } else if (const auto& keyAttribute = actualAttributes->Find<TString>("key")) {
                                file.FileName = *keyAttribute;
                            } else {
                                THROW_ERROR_EXCEPTION("Couldn't infer file name for user file");
                            }
                        }
                    } catch (const std::exception& ex) {
                        // NB: Some of the above Gets and Finds may throw due to, e.g., type mismatch.
                        THROW_ERROR_EXCEPTION("Error parsing attributes of user file %v",
                            path) << ex;
                    }

                    switch (file.Type) {
                        case EObjectType::File:
                            file.Executable = attributes.Get<bool>("executable", false);
                            file.Executable = file.Path.GetExecutable().value_or(file.Executable);

                            if (file.Layer) {
                                // Get access_method and filesystem attributes only for layers.
                                auto accessMethod = attributes.Find<TString>("access_method").value_or(ToString(ELayerAccessMethod::Local));
                                try {
                                    file.AccessMethod = TEnumTraits<ELayerAccessMethod>::FromString(accessMethod);
                                } catch (const std::exception& ex) {
                                    THROW_ERROR_EXCEPTION("Attribute 'access_method' of file %v has invalid value %Qv",
                                        file.Path,
                                        accessMethod) << ex;
                                }

                                auto filesystem = attributes.Find<TString>("filesystem").value_or(ToString(ELayerFilesystem::Archive));
                                try {
                                    file.Filesystem = TEnumTraits<ELayerFilesystem>::FromString(filesystem);
                                } catch (const std::exception& ex) {
                                    THROW_ERROR_EXCEPTION("Attribute 'filesystem' of file %v has invalid value %Qv",
                                        file.Path,
                                        filesystem) << ex;
                                }

                                // Some access_method, filesystem combinations are invalid as of now.
                                if (!AreCompatible(*file.AccessMethod, *file.Filesystem)) {
                                    THROW_ERROR_EXCEPTION("File %v has incompatible access method %Qv and filesystem %Qv",
                                        file.Path,
                                        *file.AccessMethod,
                                        *file.Filesystem);
                                }
                            }
                            break;

                        case EObjectType::Table:
                            file.Dynamic = attributes.Get<bool>("dynamic");
                            file.Schema = attributes.Get<TTableSchemaPtr>("schema");
                            if (auto renameDescriptors = file.Path.GetColumnRenameDescriptors()) {
                                YT_LOG_DEBUG("Start renaming columns of user file");
                                auto description = Format("user file %v", file.GetPath());
                                file.Schema = RenameColumnsInSchema(
                                    description,
                                    file.Schema,
                                    file.Dynamic,
                                    *renameDescriptors,
                                    /*changeStableName*/ !Config->EnableTableColumnRenaming);
                                YT_LOG_DEBUG("Columns of user file are renamed (Path: %v, NewSchema: %v)",
                                    file.GetPath(),
                                    *file.Schema);
                            }
                            file.Format = attributes.FindYson("format");
                            if (!file.Format) {
                                file.Format = file.Path.GetFormat();
                            }
                            // Validate that format is correct.
                            try {
                                if (!file.Format) {
                                    THROW_ERROR_EXCEPTION("Format is not specified");
                                }
                                ConvertTo<TFormat>(file.Format);
                            } catch (const std::exception& ex) {
                                THROW_ERROR_EXCEPTION("Failed to parse format of table file %v",
                                    file.Path) << ex;
                            }
                            // Validate that timestamp is correct.
                            ValidateDynamicTableTimestamp(file.Path, file.Dynamic, *file.Schema, attributes);
                            break;

                        default:
                            YT_ABORT();
                    }

                    file.Account = attributes.Get<TString>("account");

                    file.ChunkCount = attributes.Get<i64>("chunk_count");
                    file.ContentRevision = attributes.Get<NHydra::TRevision>("content_revision");

                    YT_LOG_INFO("User file locked (Path: %v, TaskTitle: %v, FileName: %v, SecurityTags: %v, ContentRevision: %x)",
                        path,
                        userJobSpec->TaskTitle,
                        file.FileName,
                        file.SecurityTags,
                        file.ContentRevision);
                }

                if (!file.Layer) {
                    const auto& path = file.Path.GetPath();
                    const auto& fileName = file.FileName;

                    if (fileName.empty()) {
                        THROW_ERROR_EXCEPTION("Empty user file name for %v",
                            path);
                    }

                    if (!NFS::IsPathRelativeAndInvolvesNoTraversal(fileName)) {
                        THROW_ERROR_EXCEPTION("User file name %Qv for %v does not point inside the sandbox directory",
                            fileName,
                            path);
                    }

                    if (!userFileNames.insert(fileName).second) {
                        THROW_ERROR_EXCEPTION("Duplicate user file name %Qv for %v",
                            fileName,
                            path);
                    }
                }
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error getting user file attributes")
                << TErrorAttribute("task_title", userJobSpec->TaskTitle)
                << ex;
        }
    }
}

void TOperationControllerBase::PrepareInputQuery()
{ }

void TOperationControllerBase::ParseInputQuery(
    const TString& queryString,
    const std::optional<TTableSchema>& schema)
{
    for (const auto& table : InputTables_) {
        if (table->Path.GetColumns()) {
            THROW_ERROR_EXCEPTION("Column filter and QL filter cannot appear in the same operation");
        }
    }

    auto externalCGInfo = New<TExternalCGInfo>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());

        std::vector<TString> externalNames;
        for (const auto& name : names) {
            auto found = typeInferrers->find(name);
            if (found == typeInferrers->end()) {
                externalNames.push_back(name);
            }
        }

        if (externalNames.empty()) {
            return;
        }

        if (!Config->UdfRegistryPath) {
            THROW_ERROR_EXCEPTION("External UDF registry is not configured")
                << TErrorAttribute("external_names", externalNames);
        }

        std::vector<std::pair<TString, TString>> keys;
        for (const auto& name : externalNames) {
            keys.emplace_back(*Config->UdfRegistryPath, name);
        }

        auto descriptors = LookupAllUdfDescriptors(keys, Host->GetClient());

        AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
    };

    auto inferSchema = [&] {
        std::vector<TTableSchemaPtr> schemas;
        for (const auto& table : InputTables_) {
            schemas.push_back(table->Schema);
        }
        return InferInputSchema(schemas, false);
    };

    auto query = PrepareJobQuery(
        queryString,
        schema ? New<TTableSchema>(*schema) : inferSchema(),
        fetchFunctions);

    auto getColumns = [] (const TTableSchema& desiredSchema, const TTableSchema& tableSchema) {
        std::vector<TString> columns;
        for (const auto& column : desiredSchema.Columns()) {
            auto columnName = column.Name();
            if (tableSchema.FindColumn(columnName)) {
                columns.push_back(columnName);
            }
        }

        return std::ssize(columns) == tableSchema.GetColumnCount()
            ? std::optional<std::vector<TString>>()
            : std::make_optional(std::move(columns));
    };

    // Use query column filter for input tables.
    for (auto table : InputTables_) {
        auto columns = getColumns(*query->GetReadSchema(), *table->Schema);
        if (columns) {
            table->Path.SetColumns(*columns);
        }
    }

    InputQuery.emplace();
    InputQuery->Query = std::move(query);
    InputQuery->ExternalCGInfo = std::move(externalCGInfo);

    ValidateTableSchema(
        *InputQuery->Query->GetTableSchema(),
        /*isTableDynamic*/ false,
        /*allowUnversionedUpdateColumns*/ true);
}

void TOperationControllerBase::WriteInputQueryToJobSpec(TJobSpecExt* jobSpecExt)
{
    auto* querySpec = jobSpecExt->mutable_input_query_spec();
    ToProto(querySpec->mutable_query(), InputQuery->Query);
    querySpec->mutable_query()->set_input_row_limit(std::numeric_limits<i64>::max() / 4);
    querySpec->mutable_query()->set_output_row_limit(std::numeric_limits<i64>::max() / 4);
    ToProto(querySpec->mutable_external_functions(), InputQuery->ExternalCGInfo->Functions);
}

void TOperationControllerBase::CollectTotals()
{
    // This is the sum across all input chunks not accounting lower/upper read limits.
    // Used to calculate compression ratio.
    i64 totalInputDataWeight = 0;
    for (const auto& table : InputTables_) {
        for (const auto& inputChunk : table->Chunks) {
            if (IsUnavailable(inputChunk, GetChunkAvailabilityPolicy())) {
                auto chunkId = inputChunk->GetChunkId();

                switch (Spec_->UnavailableChunkStrategy) {
                    case EUnavailableChunkAction::Fail:
                        THROW_ERROR_EXCEPTION("Input chunk %v is unavailable",
                            chunkId);

                    case EUnavailableChunkAction::Skip:
                        YT_LOG_TRACE("Skipping unavailable chunk (ChunkId: %v)",
                            chunkId);
                        continue;

                    case EUnavailableChunkAction::Wait:
                        // Do nothing.
                        break;

                    default:
                        YT_ABORT();
                }
            }

            if (table->IsPrimary()) {
                PrimaryInputDataWeight += inputChunk->GetDataWeight();
            } else {
                ForeignInputDataWeight += inputChunk->GetDataWeight();
            }

            totalInputDataWeight += inputChunk->GetTotalDataWeight();
            TotalEstimatedInputUncompressedDataSize += inputChunk->GetUncompressedDataSize();
            TotalEstimatedInputRowCount += inputChunk->GetRowCount();
            TotalEstimatedInputValueCount += inputChunk->GetValuesPerRow() * inputChunk->GetRowCount();
            TotalEstimatedInputCompressedDataSize += inputChunk->GetCompressedDataSize();
            TotalEstimatedInputDataWeight += inputChunk->GetDataWeight();
            ++TotalEstimatedInputChunkCount;
        }
    }

    InputCompressionRatio = static_cast<double>(TotalEstimatedInputCompressedDataSize) / TotalEstimatedInputDataWeight;
    DataWeightRatio = static_cast<double>(totalInputDataWeight) / TotalEstimatedInputUncompressedDataSize;

    YT_LOG_INFO("Estimated input totals collected (ChunkCount: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v, DataWeight: %v, TotalDataWeight: %v, TotalValueCount: %v)",
        TotalEstimatedInputChunkCount,
        TotalEstimatedInputRowCount,
        TotalEstimatedInputUncompressedDataSize,
        TotalEstimatedInputCompressedDataSize,
        TotalEstimatedInputDataWeight,
        totalInputDataWeight,
        TotalEstimatedInputValueCount);
}

bool TOperationControllerBase::HasDiskRequestsWithSpecifiedAccount() const
{
    for (const auto& userJobSpec : GetUserJobSpecs()) {
        if (userJobSpec->DiskRequest && userJobSpec->DiskRequest->Account) {
            return true;
        }
    }
    return false;
}

void TOperationControllerBase::InitAccountResourceUsageLeases()
{
    THashSet<TString> accounts;

    for (const auto& userJobSpec : GetUserJobSpecs()) {
        if (auto& diskRequest = userJobSpec->DiskRequest) {
            auto mediumDirectory = GetMediumDirectory();
            if (!diskRequest->MediumName) {
                continue;
            }
            auto mediumName = *diskRequest->MediumName;
            auto* mediumDescriptor = mediumDirectory->FindByName(mediumName);
            if (!mediumDescriptor) {
                THROW_ERROR_EXCEPTION("Unknown medium %Qv", mediumName);
            }
            diskRequest->MediumIndex = mediumDescriptor->Index;

            if (Config->ObligatoryAccountMedia.contains(mediumName)) {
                if (!diskRequest->Account) {
                    THROW_ERROR_EXCEPTION("Account must be specified for disk request with given medium")
                        << TErrorAttribute("medium_name", mediumName);
                }
            }
            if (Config->DeprecatedMedia.contains(mediumName)) {
                THROW_ERROR_EXCEPTION("Medium is deprecated to be used in disk requests")
                    << TErrorAttribute("medium_name", mediumName);
            }
            if (diskRequest->Account) {
                accounts.insert(*diskRequest->Account);
            }
        }
    }

    EnableMasterResourceUsageAccounting_ = Config->EnableMasterResourceUsageAccounting;
    if (EnableMasterResourceUsageAccounting_) {
        // TODO(ignat): use batching here.
        for (const auto& account : accounts) {
            try {
                ValidateAccountPermission(account, EPermission::Use);

                auto proxy = CreateObjectServiceWriteProxy(OutputClient);

                auto req = TMasterYPathProxy::CreateObject();
                SetPrerequisites(req, TPrerequisiteOptions{
                    .PrerequisiteTransactionIds = {InputTransaction->GetId()},
                });

                req->set_type(ToProto<int>(EObjectType::AccountResourceUsageLease));

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("account", account);
                attributes->Set("transaction_id", InputTransaction->GetId());
                ToProto(req->mutable_object_attributes(), *attributes);

                auto rsp = WaitFor(proxy.Execute(req))
                    .ValueOrThrow();

                AccountResourceUsageLeaseMap_[account] = {
                    .LeaseId = FromProto<TAccountResourceUsageLeaseId>(rsp->object_id()),
                    .DiskQuota = TDiskQuota(),
                };
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to create account resource usage lease")
                    << TErrorAttribute("account", account)
                    << ex;
            }
        }
    }
}

void TOperationControllerBase::CustomPrepare()
{ }

void TOperationControllerBase::CustomMaterialize()
{ }

void TOperationControllerBase::InferInputRanges()
{
    TPeriodicYielder yielder(PrepareYieldPeriod);

    if (!InputQuery) {
        return;
    }

    YT_LOG_INFO("Inferring ranges for input tables");

    TQueryOptions queryOptions;
    queryOptions.VerboseLogging = true;
    queryOptions.RangeExpansionLimit = Config->MaxRangesOnTable;

    for (auto& table : InputTables_) {
        yielder.TryYield();

        auto ranges = table->Path.GetNewRanges(table->Comparator, table->Schema->GetKeyColumnTypes());

        // XXX(max42): does this ever happen?
        if (ranges.empty()) {
            continue;
        }

        if (!table->Schema->IsSorted()) {
            continue;
        }

        auto rangeInferrer = CreateRangeInferrer(
            InputQuery->Query->WhereClause,
            table->Schema,
            table->Schema->GetKeyColumns(),
            Host->GetClient()->GetNativeConnection()->GetColumnEvaluatorCache(),
            GetBuiltinRangeExtractors(),
            queryOptions);

        std::vector<TReadRange> inferredRanges;
        for (const auto& range : ranges) {
            yielder.TryYield();

            auto legacyRange = ReadRangeToLegacyReadRange(range);
            auto lower = legacyRange.LowerLimit().HasLegacyKey()
                ? legacyRange.LowerLimit().GetLegacyKey()
                : MinKey();
            auto upper = legacyRange.UpperLimit().HasLegacyKey()
                ? legacyRange.UpperLimit().GetLegacyKey()
                : MaxKey();
            auto result = rangeInferrer(TRowRange(lower.Get(), upper.Get()), RowBuffer);
            for (const auto& inferred : result) {
                auto inferredRange = legacyRange;
                inferredRange.LowerLimit().SetLegacyKey(TLegacyOwningKey(inferred.first));
                inferredRange.UpperLimit().SetLegacyKey(TLegacyOwningKey(inferred.second));
                inferredRanges.push_back(ReadRangeFromLegacyReadRange(inferredRange, table->Comparator.GetLength()));
            }
        }
        table->Path.SetRanges(inferredRanges);

        YT_LOG_DEBUG("Input table ranges inferred (Path: %v, RangeCount: %v, InferredRangeCount: %v)",
            table->GetPath(),
            ranges.size(),
            inferredRanges.size());
    }
}

TError TOperationControllerBase::GetAutoMergeError() const
{
    return TError("Automatic output merge is not supported for %lv operations", OperationType);
}

void TOperationControllerBase::FillPrepareResult(TOperationControllerPrepareResult* result)
{
    result->Attributes = BuildYsonStringFluently<EYsonType::MapFragment>()
        .Do(BIND(&TOperationControllerBase::BuildPrepareAttributes, Unretained(this)))
        .Finish();
}

// NB: must preserve order of chunks in the input tables, no shuffling.
std::vector<TInputChunkPtr> TOperationControllerBase::CollectPrimaryChunks(bool versioned) const
{
    std::vector<TInputChunkPtr> result;
    for (const auto& table : InputTables_) {
        if (!table->IsForeign() && ((table->Dynamic && table->Schema->IsSorted()) == versioned)) {
            for (const auto& chunk : table->Chunks) {
                if (IsUnavailable(chunk, GetChunkAvailabilityPolicy())) {
                    switch (Spec_->UnavailableChunkStrategy) {
                        case EUnavailableChunkAction::Skip:
                            continue;

                        case EUnavailableChunkAction::Wait:
                            // Do nothing.
                            break;

                        default:
                            YT_ABORT();
                    }
                }
                result.push_back(chunk);
            }
        }
    }
    return result;
}

std::vector<TInputChunkPtr> TOperationControllerBase::CollectPrimaryUnversionedChunks() const
{
    return CollectPrimaryChunks(false);
}

std::vector<TInputChunkPtr> TOperationControllerBase::CollectPrimaryVersionedChunks() const
{
    return CollectPrimaryChunks(true);
}

std::vector<TLegacyDataSlicePtr> TOperationControllerBase::CollectPrimaryVersionedDataSlices(i64 sliceSize)
{
    auto createScraperForFetcher = [&] () -> IFetcherChunkScraperPtr {
        if (Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
            auto scraper = CreateFetcherChunkScraper();
            DataSliceFetcherChunkScrapers.push_back(scraper);
            return scraper;
        } else {
            return IFetcherChunkScraperPtr();
        }
    };

    i64 totalDataWeightBefore = 0;

    std::vector<TFuture<void>> asyncResults;
    std::vector<TComparator> comparators;
    std::vector<IChunkSliceFetcherPtr> fetchers;

    for (const auto& table : InputTables_) {
        if (!table->IsForeign() && table->Dynamic && table->Schema->IsSorted()) {
            auto fetcher = CreateChunkSliceFetcher(
                Config->ChunkSliceFetcher,
                InputNodeDirectory_,
                GetCancelableInvoker(),
                createScraperForFetcher(),
                Host->GetClient(),
                RowBuffer,
                Logger);

            YT_VERIFY(table->Comparator);

            for (const auto& chunk : table->Chunks) {
                if (IsUnavailable(chunk, GetChunkAvailabilityPolicy()) &&
                    Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Skip)
                {
                    continue;
                }

                auto chunkSlice = CreateInputChunkSlice(chunk);
                InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer);
                auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(dataSlice->GetTableIndex(), dataSlice->GetRangeIndex()));
                dataSlice->TransformToNew(RowBuffer, table->Comparator.GetLength());
                fetcher->AddDataSliceForSlicing(dataSlice, table->Comparator, sliceSize, true);
                totalDataWeightBefore += dataSlice->GetDataWeight();
            }

            fetcher->SetCancelableContext(GetCancelableContext());
            asyncResults.emplace_back(fetcher->Fetch());
            fetchers.emplace_back(std::move(fetcher));
            YT_VERIFY(table->Comparator);
            comparators.push_back(table->Comparator);
        }
    }

    YT_LOG_INFO("Collecting primary versioned data slices");

    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    i64 totalDataSliceCount = 0;
    i64 totalDataWeightAfter = 0;

    std::vector<TLegacyDataSlicePtr> result;
    for (const auto& [fetcher, comparator] : Zip(fetchers, comparators)) {
        for (const auto& chunkSlice : fetcher->GetChunkSlices()) {
            YT_VERIFY(!chunkSlice->IsLegacy);
        }
        auto dataSlices = CombineVersionedChunkSlices(fetcher->GetChunkSlices(), comparator);
        for (auto& dataSlice : dataSlices) {
            YT_LOG_TRACE("Added dynamic table slice (TablePath: %v, Range: %v..%v, ChunkIds: %v)",
                InputTables_[dataSlice->GetTableIndex()]->GetPath(),
                dataSlice->LowerLimit(),
                dataSlice->UpperLimit(),
                dataSlice->ChunkSlices);

            dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(dataSlice->GetTableIndex(), dataSlice->GetRangeIndex()));
            totalDataWeightAfter += dataSlice->GetDataWeight();
            result.emplace_back(std::move(dataSlice));
            ++totalDataSliceCount;
        }
    }

    YT_LOG_INFO(
        "Collected versioned data slices (Count: %v, DataWeight: %v -> %v)",
        totalDataSliceCount,
        totalDataWeightBefore,
        totalDataWeightAfter);

    if (Spec_->AdjustDynamicTableDataSlices) {
        double scaleFactor = totalDataWeightBefore / std::max<double>(1.0, totalDataWeightAfter);
        i64 totalDataWeightAdjusted = 0;
        for (const auto& dataSlice : result) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                chunkSlice->ApplySamplingSelectivityFactor(scaleFactor);
            }
            totalDataWeightAdjusted += dataSlice->GetDataWeight();
        }
        YT_LOG_INFO("Adjusted dynamic table data slices (AdjutedDataWeight: %v)", totalDataWeightAdjusted);
    }

    DataSliceFetcherChunkScrapers.clear();

    return result;
}

std::vector<TLegacyDataSlicePtr> TOperationControllerBase::CollectPrimaryInputDataSlices(i64 versionedSliceSize)
{
    std::vector<std::vector<TLegacyDataSlicePtr>> dataSlicesByTableIndex(InputTables_.size());
    for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

        const auto& inputTable = InputTables_[dataSlice->GetTableIndex()];
        dataSlice->TransformToNew(RowBuffer, inputTable->Comparator);

        dataSlicesByTableIndex[dataSlice->GetTableIndex()].emplace_back(std::move(dataSlice));
    }

    if (OperationType == EOperationType::RemoteCopy) {
        for (const auto& chunk : CollectPrimaryVersionedChunks()) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

            const auto& inputTable = InputTables_[dataSlice->GetTableIndex()];
            dataSlice->TransformToNew(RowBuffer, inputTable->Comparator);

            dataSlicesByTableIndex[dataSlice->GetTableIndex()].emplace_back(std::move(dataSlice));
        }
    } else {
        for (auto& dataSlice : CollectPrimaryVersionedDataSlices(versionedSliceSize)) {
            dataSlicesByTableIndex[dataSlice->GetTableIndex()].emplace_back(std::move(dataSlice));
        }
    }

    std::vector<TLegacyDataSlicePtr> dataSlices;
    for (auto& tableDataSlices : dataSlicesByTableIndex) {
        std::move(tableDataSlices.begin(), tableDataSlices.end(), std::back_inserter(dataSlices));
    }
    return dataSlices;
}

std::vector<std::deque<TLegacyDataSlicePtr>> TOperationControllerBase::CollectForeignInputDataSlices(int foreignKeyColumnCount) const
{
    std::vector<std::deque<TLegacyDataSlicePtr>> result;
    for (const auto& table : InputTables_) {
        if (table->IsForeign()) {
            result.push_back(std::deque<TLegacyDataSlicePtr>());

            if (table->Dynamic && table->Schema->IsSorted()) {
                std::vector<TInputChunkSlicePtr> chunkSlices;
                chunkSlices.reserve(table->Chunks.size());
                YT_VERIFY(table->Comparator);
                for (const auto& chunkSpec : table->Chunks) {
                    auto& chunkSlice = chunkSlices.emplace_back(CreateInputChunkSlice(
                        chunkSpec,
                        RowBuffer->CaptureRow(chunkSpec->BoundaryKeys()->MinKey.Get()),
                        GetKeySuccessor(chunkSpec->BoundaryKeys()->MaxKey.Get(), RowBuffer)));

                    chunkSlice->TransformToNew(RowBuffer, table->Comparator.GetLength());
                }

                YT_VERIFY(table->Comparator);
                auto dataSlices = CombineVersionedChunkSlices(chunkSlices, table->Comparator);
                for (auto& dataSlice : dataSlices) {
                    dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(dataSlice->GetTableIndex(), dataSlice->GetRangeIndex()));

                    if (IsUnavailable(dataSlice, GetChunkAvailabilityPolicy())) {
                        switch (Spec_->UnavailableChunkStrategy) {
                            case EUnavailableChunkAction::Skip:
                                continue;

                            case EUnavailableChunkAction::Wait:
                                // Do nothing.
                                break;

                            default:
                                YT_ABORT();
                        }
                    }
                    result.back().push_back(dataSlice);
                }
            } else {
                for (const auto& inputChunk : table->Chunks) {
                    if (IsUnavailable(inputChunk, GetChunkAvailabilityPolicy())) {
                        switch (Spec_->UnavailableChunkStrategy) {
                            case EUnavailableChunkAction::Skip:
                                continue;

                            case EUnavailableChunkAction::Wait:
                                // Do nothing.
                                break;

                            default:
                                YT_ABORT();
                        }
                    }
                    auto& dataSlice = result.back().emplace_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(
                        inputChunk,
                        GetKeyPrefix(inputChunk->BoundaryKeys()->MinKey.Get(), foreignKeyColumnCount, RowBuffer),
                        GetKeyPrefixSuccessor(inputChunk->BoundaryKeys()->MaxKey.Get(), foreignKeyColumnCount, RowBuffer))));
                    dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(dataSlice->GetTableIndex(), dataSlice->GetRangeIndex()));

                    YT_VERIFY(table->Comparator);

                    dataSlice->TransformToNew(RowBuffer, table->Comparator.GetLength());
                }
            }
        }
    }
    return result;
}

bool TOperationControllerBase::InputHasVersionedTables() const
{
    for (const auto& table : InputTables_) {
        if (table->Dynamic && table->Schema->IsSorted()) {
            return true;
        }
    }
    return false;
}

bool TOperationControllerBase::InputHasReadLimits() const
{
    for (const auto& table : InputTables_) {
        if (table->Path.HasNontrivialRanges()) {
            return true;
        }
    }
    return false;
}

bool TOperationControllerBase::InputHasDynamicStores() const
{
    for (const auto& table : InputTables_) {
        if (table->Dynamic) {
            for (const auto& chunk : table->Chunks) {
                if (chunk->IsDynamicStore()) {
                    return true;
                }
            }
        }
    }

    return false;
}

bool TOperationControllerBase::IsLocalityEnabled() const
{
    return Config->EnableLocality && TotalEstimatedInputDataWeight > Spec_->MinLocalityInputDataWeight;
}

TString TOperationControllerBase::GetLoggingProgress() const
{
    const auto& jobCounter = GetTotalJobCounter();
    return Format(
        "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, I: %v}, "
        "UnavailableInputChunks: %v",
        jobCounter->GetTotal(),
        jobCounter->GetRunning(),
        jobCounter->GetCompletedTotal(),
        GetPendingJobCount(),
        jobCounter->GetFailed(),
        jobCounter->GetAbortedTotal(),
        jobCounter->GetInterruptedTotal(),
        GetUnavailableInputChunkCount());
}

void TOperationControllerBase::ExtractInterruptDescriptor(TCompletedJobSummary& jobSummary, const TJobletPtr& joblet) const
{
    std::vector<TLegacyDataSlicePtr> dataSliceList;

    const auto& jobResultExt = jobSummary.GetJobResultExt();

    std::vector<TDataSliceDescriptor> unreadDataSliceDescriptors;
    std::vector<TDataSliceDescriptor> readDataSliceDescriptors;
    if (jobResultExt.unread_chunk_specs_size() > 0) {
        FromProto(
            &unreadDataSliceDescriptors,
            jobResultExt.unread_chunk_specs(),
            jobResultExt.chunk_spec_count_per_unread_data_slice(),
            jobResultExt.virtual_row_index_per_unread_data_slice());
    }
    if (jobResultExt.read_chunk_specs_size() > 0) {
        FromProto(
            &readDataSliceDescriptors,
            jobResultExt.read_chunk_specs(),
            jobResultExt.chunk_spec_count_per_read_data_slice(),
            jobResultExt.virtual_row_index_per_read_data_slice());
    }

    auto extractDataSlice = [&] (const TDataSliceDescriptor& dataSliceDescriptor) {
        std::vector<TInputChunkSlicePtr> chunkSliceList;
        chunkSliceList.reserve(dataSliceDescriptor.ChunkSpecs.size());

        // TODO(gritukan): One day we will do interrupts in non-input tasks.
        TComparator comparator;
        if (joblet->Task->GetIsInput()) {
            comparator = GetInputTable(dataSliceDescriptor.GetDataSourceIndex())->Comparator;
        }

        bool dynamic = InputTables_[dataSliceDescriptor.GetDataSourceIndex()]->Dynamic;
        for (const auto& protoChunkSpec : dataSliceDescriptor.ChunkSpecs) {
            auto chunkId = FromProto<TChunkId>(protoChunkSpec.chunk_id());
            const auto& inputChunks = GetOrCrash(InputChunkMap, chunkId).InputChunks;
            auto chunkIt = std::find_if(
                inputChunks.begin(),
                inputChunks.end(),
                [&] (const TInputChunkPtr& inputChunk) -> bool {
                    return inputChunk->GetChunkIndex() == protoChunkSpec.chunk_index();
                });
            YT_VERIFY(chunkIt != inputChunks.end());
            auto chunkSlice = New<TInputChunkSlice>(*chunkIt, RowBuffer, protoChunkSpec);
            // NB: Dynamic tables use legacy slices for now, so we do not convert dynamic table
            // slices into new.
            if (!dynamic) {
                if (comparator) {
                    chunkSlice->TransformToNew(RowBuffer, comparator.GetLength());
                    InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer, std::nullopt, comparator);
                } else {
                    chunkSlice->TransformToNewKeyless();
                }
            }
            chunkSliceList.emplace_back(std::move(chunkSlice));
        }
        TLegacyDataSlicePtr dataSlice;
        if (dynamic) {
            dataSlice = CreateVersionedInputDataSlice(chunkSliceList);
            if (comparator) {
                dataSlice->TransformToNew(RowBuffer, comparator.GetLength());
            } else {
                dataSlice->TransformToNewKeyless();
            }
        } else {
            YT_VERIFY(chunkSliceList.size() == 1);
            dataSlice = CreateUnversionedInputDataSlice(chunkSliceList[0]);
        }

        YT_VERIFY(!dataSlice->IsLegacy);
        if (comparator) {
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer, comparator);
        }

        dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
        dataSlice->Tag = dataSliceDescriptor.GetTag();
        return dataSlice;
    };

    for (const auto& dataSliceDescriptor : unreadDataSliceDescriptors) {
        jobSummary.UnreadInputDataSlices.emplace_back(extractDataSlice(dataSliceDescriptor));
    }
    for (const auto& dataSliceDescriptor : readDataSliceDescriptors) {
        jobSummary.ReadInputDataSlices.emplace_back(extractDataSlice(dataSliceDescriptor));
    }
}

TSortColumns TOperationControllerBase::CheckInputTablesSorted(
    const TSortColumns& sortColumns,
    std::function<bool(const TInputTablePtr& table)> inputTableFilter)
{
    YT_VERIFY(!InputTables_.empty());

    for (const auto& table : InputTables_) {
        if (inputTableFilter(table) && !table->Schema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Input table %v is not sorted",
                table->GetPath());
        }
    }

    auto validateColumnFilter = [] (const TInputTablePtr& table, const TSortColumns& sortColumns) {
        auto columns = table->Path.GetColumns();
        if (!columns) {
            return;
        }

        auto columnSet = THashSet<TString>(columns->begin(), columns->end());
        for (const auto& sortColumn : sortColumns) {
            if (columnSet.find(sortColumn.Name) == columnSet.end()) {
                THROW_ERROR_EXCEPTION("Column filter for input table %v must include key column %Qv",
                    table->GetPath(),
                    sortColumn.Name);
            }
        }
    };

    if (!sortColumns.empty()) {
        for (const auto& table : InputTables_) {
            if (!inputTableFilter(table)) {
                continue;
            }

            if (!CheckSortColumnsCompatible(table->Schema->GetSortColumns(), sortColumns)) {
                THROW_ERROR_EXCEPTION("Input table %v is sorted by columns %v that are not compatible "
                    "with the requested columns %v",
                    table->GetPath(),
                    table->Schema->GetSortColumns(),
                    sortColumns);
            }
            validateColumnFilter(table, sortColumns);
        }
        return sortColumns;
    } else {
        for (const auto& referenceTable : InputTables_) {
            if (inputTableFilter(referenceTable)) {
                for (const auto& table : InputTables_) {
                    if (!inputTableFilter(table)) {
                        continue;
                    }

                    if (table->Schema->GetSortColumns() != referenceTable->Schema->GetSortColumns()) {
                        THROW_ERROR_EXCEPTION("Sort columns do not match: input table %v is sorted by columns %v "
                            "while input table %v is sorted by columns %v",
                            table->GetPath(),
                            table->Schema->GetSortColumns(),
                            referenceTable->GetPath(),
                            referenceTable->Schema->GetSortColumns());
                    }
                    validateColumnFilter(table, referenceTable->Schema->GetSortColumns());
                }
                return referenceTable->Schema->GetSortColumns();
            }
        }
    }
    YT_ABORT();
}

bool TOperationControllerBase::CheckKeyColumnsCompatible(
    const TKeyColumns& fullColumns,
    const TKeyColumns& prefixColumns)
{
    if (fullColumns.size() < prefixColumns.size()) {
        return false;
    }

    return std::equal(prefixColumns.begin(), prefixColumns.end(), fullColumns.begin());
}

bool TOperationControllerBase::CheckSortColumnsCompatible(
    const TSortColumns& fullColumns,
    const TSortColumns& prefixColumns)
{
    if (fullColumns.size() < prefixColumns.size()) {
        return false;
    }

    return std::equal(prefixColumns.begin(), prefixColumns.end(), fullColumns.begin());
}

bool TOperationControllerBase::ShouldVerifySortedOutput() const
{
    return true;
}

TOutputOrderPtr TOperationControllerBase::GetOutputOrder() const
{
    return nullptr;
}

EChunkAvailabilityPolicy TOperationControllerBase::GetChunkAvailabilityPolicy() const
{
    return Spec_->ChunkAvailabilityPolicy;
}

bool TOperationControllerBase::IsBoundaryKeysFetchEnabled() const
{
    return false;
}

void TOperationControllerBase::RegisterLivePreviewTable(TString name, const TOutputTablePtr& table)
{
    // COMPAT(galtsev)
    if (name.Empty()) {
        return;
    }

    auto schema = table->TableUploadOptions.TableSchema.Get();
    LivePreviews_->emplace(
        name,
        New<TLivePreview>(std::move(schema), InputNodeDirectory_, OperationId, name, table->Path.GetPath()));
    table->LivePreviewTableName = std::move(name);
}

void TOperationControllerBase::AttachToIntermediateLivePreview(TInputChunkPtr chunk)
{
    if (IsLegacyIntermediateLivePreviewSupported()) {
        AttachToLivePreview(chunk->GetChunkId(), IntermediateTable->LivePreviewTableId);
    }
    AttachToLivePreview(IntermediateTable->LivePreviewTableName, chunk);
}

void TOperationControllerBase::AttachToLivePreview(
    TChunkTreeId chunkTreeId,
    NCypressClient::TNodeId tableId)
{
    YT_UNUSED_FUTURE(Host->AttachChunkTreesToLivePreview(
        AsyncTransaction->GetId(),
        tableId,
        {chunkTreeId}));
}

void TOperationControllerBase::AttachToLivePreview(
    TStringBuf tableName,
    TInputChunkPtr chunk)
{
    // COMPAT(galtsev)
    if (tableName.Empty()) {
        return;
    }

    InsertOrCrash((*LivePreviews_)[tableName]->Chunks(), std::move(chunk));
}

void TOperationControllerBase::AttachToLivePreview(
    TStringBuf tableName,
    const TChunkStripePtr& stripe)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            AttachToLivePreview(tableName, chunkSlice->GetInputChunk());
        }
    }
}

void TOperationControllerBase::RegisterStderr(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    if (!joblet->StderrTableChunkListId) {
        return;
    }

    YT_VERIFY(StderrTable_);

    const auto& chunkListId = joblet->StderrTableChunkListId;

    const auto& jobResultExt = jobSummary.GetJobResultExt();

    YT_VERIFY(jobResultExt.has_stderr_result());

    const auto& stderrResult = jobResultExt.stderr_result();
    if (stderrResult.empty()) {
        return;
    }
    auto key = BuildBoundaryKeysFromOutputResult(stderrResult, StderrTable_->GetStreamDescriptorTemplate(), RowBuffer);
    if (!key.MinKey || key.MinKey.GetLength() == 0 || !key.MaxKey || key.MaxKey.GetLength() == 0) {
        YT_LOG_DEBUG("Dropping empty stderr chunk tree (JobId: %v, NodeAddress: %v, ChunkListId: %v)",
            joblet->JobId,
            joblet->NodeDescriptor.Address,
            chunkListId);
        return;
    }

    for (const auto& chunkSpec : stderrResult.chunk_specs()) {
        RegisterOutputChunkReplicas(jobSummary, chunkSpec);

        auto chunk = New<TInputChunk>(chunkSpec);
        AttachToLivePreview(StderrTable_->LivePreviewTableName, chunk);
        RegisterLivePreviewChunk(TDataFlowGraph::StderrDescriptor, /*index*/ 0, std::move(chunk));
    }

    StderrTable_->OutputChunkTreeIds.emplace_back(key, chunkListId);

    YT_LOG_DEBUG("Stderr chunk tree registered (JobId: %v, NodeAddress: %v, ChunkListId: %v, ChunkCount: %v)",
        joblet->JobId,
        joblet->NodeDescriptor.Address,
        chunkListId,
        stderrResult.chunk_specs().size());
}

void TOperationControllerBase::RegisterCores(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    if (!joblet->CoreTableChunkListId) {
        return;
    }

    YT_VERIFY(CoreTable_);

    const auto& chunkListId = joblet->CoreTableChunkListId;

    const auto& jobResultExt = jobSummary.GetJobResultExt();

    for (const auto& coreInfo : jobResultExt.core_infos()) {
        YT_LOG_DEBUG("Core file (JobId: %v, ProcessId: %v, ExecutableName: %v, Size: %v, Error: %v, Cuda: %v)",
            joblet->JobId,
            coreInfo.process_id(),
            coreInfo.executable_name(),
            coreInfo.size(),
            coreInfo.has_error() ? FromProto<TError>(coreInfo.error()) : TError(),
            coreInfo.cuda());
    }

    if (!jobResultExt.has_core_result()) {
        return;
    }
    const auto& coreResult = jobResultExt.core_result();
    if (coreResult.empty()) {
        return;
    }
    auto key = BuildBoundaryKeysFromOutputResult(coreResult, CoreTable_->GetStreamDescriptorTemplate(), RowBuffer);
    if (!key.MinKey || key.MinKey.GetLength() == 0 || !key.MaxKey || key.MaxKey.GetLength() == 0) {
        YT_LOG_DEBUG("Dropping empty core chunk tree (JobId: %v, NodeAddress: %v, ChunkListId: %v)",
            joblet->JobId,
            joblet->NodeDescriptor.Address,
            chunkListId);
        return;
    }
    CoreTable_->OutputChunkTreeIds.emplace_back(key, chunkListId);

    for (const auto& chunkSpec : coreResult.chunk_specs()) {
        RegisterOutputChunkReplicas(jobSummary, chunkSpec);

        auto chunk = New<TInputChunk>(chunkSpec);
        AttachToLivePreview(CoreTable_->LivePreviewTableName, chunk);
        RegisterLivePreviewChunk(TDataFlowGraph::CoreDescriptor, /*index*/ 0, std::move(chunk));
    }

    YT_LOG_DEBUG("Core chunk tree registered (JobId: %v, NodeAddress: %v, ChunkListId: %v, ChunkCount: %v)",
        joblet->JobId,
        joblet->NodeDescriptor.Address,
        chunkListId,
        coreResult.chunk_specs().size());
}

const ITransactionPtr TOperationControllerBase::GetTransactionForOutputTable(const TOutputTablePtr& table) const
{
    if (table->OutputType == EOutputTableType::Output) {
        if (OutputCompletionTransaction) {
            return OutputCompletionTransaction;
        } else {
            return OutputTransaction;
        }
    } else {
        YT_VERIFY(table->OutputType == EOutputTableType::Stderr || table->OutputType == EOutputTableType::Core);
        if (DebugCompletionTransaction) {
            return DebugCompletionTransaction;
        } else {
            return DebugTransaction;
        }
    }
}

void TOperationControllerBase::RegisterTeleportChunk(
    TInputChunkPtr chunk,
    TChunkStripeKey key,
    int tableIndex)
{
    auto& table = OutputTables_[tableIndex];

    if (table->TableUploadOptions.TableSchema->IsSorted() && ShouldVerifySortedOutput()) {
        YT_VERIFY(chunk->BoundaryKeys());
        YT_VERIFY(chunk->GetRowCount() > 0);
        YT_VERIFY(chunk->GetUniqueKeys() || !table->TableWriterOptions->ValidateUniqueKeys);

        NControllerAgent::NProto::TOutputResult resultBoundaryKeys;
        resultBoundaryKeys.set_empty(false);
        resultBoundaryKeys.set_sorted(true);
        resultBoundaryKeys.set_unique_keys(chunk->GetUniqueKeys());
        ToProto(resultBoundaryKeys.mutable_min(), chunk->BoundaryKeys()->MinKey);
        ToProto(resultBoundaryKeys.mutable_max(), chunk->BoundaryKeys()->MaxKey);

        key = BuildBoundaryKeysFromOutputResult(resultBoundaryKeys, StandardStreamDescriptors_[tableIndex], RowBuffer);
    }

    table->OutputChunkTreeIds.emplace_back(key, chunk->GetChunkId());

    if (table->Dynamic) {
        table->OutputChunks.push_back(chunk);
    }

    if (IsLegacyOutputLivePreviewSupported()) {
        AttachToLivePreview(chunk->GetChunkId(), table->LivePreviewTableId);
    }
    AttachToLivePreview(table->LivePreviewTableName, chunk);

    RegisterOutputRows(chunk->GetRowCount(), tableIndex);

    YT_LOG_DEBUG("Teleport chunk registered (Table: %v, ChunkId: %v, Key: %v)",
        tableIndex,
        chunk->GetChunkId(),
        key);
}

void TOperationControllerBase::RegisterInputStripe(const TChunkStripePtr& stripe, const TTaskPtr& task)
{
    THashSet<TChunkId> visitedChunks;

    TStripeDescriptor stripeDescriptor;
    stripeDescriptor.Stripe = stripe;
    stripeDescriptor.Task = task;
    stripeDescriptor.Cookie = task->GetChunkPoolInput()->Add(stripe);

    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& slice : dataSlice->ChunkSlices) {
            auto inputChunk = slice->GetInputChunk();
            auto chunkId = inputChunk->GetChunkId();

            if (!visitedChunks.insert(chunkId).second) {
                continue;
            }

            auto& chunkDescriptor = GetOrCrash(InputChunkMap, chunkId);
            chunkDescriptor.InputStripes.push_back(stripeDescriptor);

            if (chunkDescriptor.State == EInputChunkState::Waiting) {
                ++stripe->WaitingChunkCount;
            }
        }
    }

    if (stripe->WaitingChunkCount > 0) {
        task->GetChunkPoolInput()->Suspend(stripeDescriptor.Cookie);
    }
}

void TOperationControllerBase::RegisterRecoveryInfo(
    const TCompletedJobPtr& completedJob,
    const TChunkStripePtr& stripe)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        // NB: intermediate slice must be trivial.
        auto chunkId = dataSlice->GetSingleUnversionedChunk()->GetChunkId();
        YT_VERIFY(ChunkOriginMap.emplace(chunkId, completedJob).second);
    }

    IntermediateChunkScraper->Restart();
}

TRowBufferPtr TOperationControllerBase::GetRowBuffer()
{
    return RowBuffer;
}

TSnapshotCookie TOperationControllerBase::OnSnapshotStarted()
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    int snapshotIndex = SnapshotIndex_++;
    if (RecentSnapshotIndex_) {
        YT_LOG_WARNING("Starting next snapshot without completing previous one (SnapshotIndex: %v)",
            snapshotIndex);
    }
    RecentSnapshotIndex_ = snapshotIndex;

    CompletedJobIdsSnapshotCookie_ = CompletedJobIdsReleaseQueue_.Checkpoint();
    IntermediateStripeListSnapshotCookie_ = IntermediateStripeListReleaseQueue_.Checkpoint();
    ChunkTreeSnapshotCookie_ = ChunkTreeReleaseQueue_.Checkpoint();
    YT_LOG_INFO("Storing snapshot cookies (CompletedJobIdsSnapshotCookie: %v, StripeListSnapshotCookie: %v, "
        "ChunkTreeSnapshotCookie: %v, SnapshotIndex: %v)",
        CompletedJobIdsSnapshotCookie_,
        IntermediateStripeListSnapshotCookie_,
        ChunkTreeSnapshotCookie_,
        *RecentSnapshotIndex_);

    TSnapshotCookie result;
    result.SnapshotIndex = *RecentSnapshotIndex_;
    return result;
}

void TOperationControllerBase::SafeOnSnapshotCompleted(const TSnapshotCookie& cookie)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    // OnSnapshotCompleted should match the most recent OnSnapshotStarted.
    YT_VERIFY(RecentSnapshotIndex_);
    YT_VERIFY(cookie.SnapshotIndex == *RecentSnapshotIndex_);

    // Completed job ids.
    {
        auto headCookie = CompletedJobIdsReleaseQueue_.GetHeadCookie();
        auto jobIdsToRelease = CompletedJobIdsReleaseQueue_.Release(CompletedJobIdsSnapshotCookie_);
        YT_LOG_INFO("Releasing jobs on snapshot completion (SnapshotCookie: %v, HeadCookie: %v, JobCount: %v, SnapshotIndex: %v)",
            CompletedJobIdsSnapshotCookie_,
            headCookie,
            jobIdsToRelease.size(),
            cookie.SnapshotIndex);
        ReleaseJobs(jobIdsToRelease);
    }

    // Stripe lists.
    {
        auto headCookie = IntermediateStripeListReleaseQueue_.GetHeadCookie();
        auto stripeListsToRelease = IntermediateStripeListReleaseQueue_.Release(IntermediateStripeListSnapshotCookie_);
        YT_LOG_INFO("Releasing stripe lists (SnapshotCookie: %v, HeadCookie: %v, StripeListCount: %v, SnapshotIndex: %v)",
            IntermediateStripeListSnapshotCookie_,
            headCookie,
            stripeListsToRelease.size(),
            cookie.SnapshotIndex);

        for (const auto& stripeList : stripeListsToRelease) {
            auto chunks = GetStripeListChunks(stripeList);
            AddChunksToUnstageList(std::move(chunks));
            OnChunksReleased(stripeList->TotalChunkCount);
        }
    }

    // Chunk trees.
    {
        auto headCookie = ChunkTreeReleaseQueue_.GetHeadCookie();
        auto chunkTreeIdsToRelease = ChunkTreeReleaseQueue_.Release(ChunkTreeSnapshotCookie_);
        YT_LOG_INFO("Releasing chunk trees (SnapshotCookie: %v, HeadCookie: %v, ChunkTreeCount: %v, SnapshotIndex: %v)",
            ChunkTreeSnapshotCookie_,
            headCookie,
            chunkTreeIdsToRelease.size(),
            cookie.SnapshotIndex);

        Host->AddChunkTreesToUnstageList(chunkTreeIdsToRelease, /*recursive*/ true);
    }

    RecentSnapshotIndex_.reset();
    LastSuccessfulSnapshotTime_ = TInstant::Now();
}

bool TOperationControllerBase::HasSnapshot() const
{
    return SnapshotIndex_.load();
}

void TOperationControllerBase::Dispose()
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    YT_VERIFY(IsFinished());

    YT_VERIFY(std::empty(JobletMap));
    // Check that all jobs released.
    YT_VERIFY(CompletedJobIdsReleaseQueue_.Checkpoint() == CompletedJobIdsReleaseQueue_.GetHeadCookie());
    {
        auto guard = Guard(JobMetricsDeltaPerTreeLock_);

        for (auto& [treeId, metrics] : JobMetricsDeltaPerTree_) {
            auto totalTime = GetOrCrash(TotalTimePerTree_, treeId);
            auto mainResourceConsumption = GetOrCrash(MainResourceConsumptionPerTree_, treeId);

            switch (State) {
                case EControllerState::Completed:
                    metrics.Values()[EJobMetricName::TotalTimeOperationCompleted] = totalTime;
                    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationCompleted] = mainResourceConsumption;
                    break;

                case EControllerState::Aborted:
                    metrics.Values()[EJobMetricName::TotalTimeOperationAborted] = totalTime;
                    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationAborted] = mainResourceConsumption;
                    break;

                case EControllerState::Failed:
                    metrics.Values()[EJobMetricName::TotalTimeOperationFailed] = totalTime;
                    metrics.Values()[EJobMetricName::MainResourceConsumptionOperationFailed] = mainResourceConsumption;
                    break;

                default:
                    YT_ABORT();
            }
        }

        YT_LOG_DEBUG(
            "Adding total time per tree to residual job metrics on controller disposal (FinalState: %v, TotalTimePerTree: %v)",
            State.load(),
            TotalTimePerTree_);
    }
}

void TOperationControllerBase::UpdateRuntimeParameters(const TOperationRuntimeParametersUpdatePtr& update)
{
    if (update->Acl) {
        Acl = *update->Acl;
    }
}

TOperationJobMetrics TOperationControllerBase::PullJobMetricsDelta(bool force)
{
    auto guard = Guard(JobMetricsDeltaPerTreeLock_);

    auto now = NProfiling::GetCpuInstant();
    if (!force && LastJobMetricsDeltaReportTime_ + DurationToCpuDuration(Config->JobMetricsReportPeriod) > now) {
        return {};
    }

    TOperationJobMetrics result;
    for (auto& [treeId, delta] : JobMetricsDeltaPerTree_) {
        if (!delta.IsEmpty()) {
            result.push_back({treeId, delta});
            delta = TJobMetrics();
        }
    }
    LastJobMetricsDeltaReportTime_ = now;

    YT_LOG_DEBUG_UNLESS(result.empty(), "Non-zero job metrics reported");

    return result;
}

TOperationAlertMap TOperationControllerBase::GetAlerts()
{
    auto guard = Guard(AlertsLock_);
    return Alerts_;
}

TOperationInfo TOperationControllerBase::BuildOperationInfo()
{
    // NB: BuildOperationInfo called by GetOperationInfo RPC method, that used scheduler at operation finalization.
    BuildAndSaveProgress();

    TOperationInfo result;

    result.Progress =
        BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do(std::bind(&TOperationControllerBase::BuildProgress, this, _1))
        .Finish();

    result.BriefProgress =
        BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do(std::bind(&TOperationControllerBase::BuildBriefProgress, this, _1))
        .Finish();

    result.Alerts =
        BuildYsonStringFluently<EYsonType::MapFragment>()
            .DoFor(GetAlerts(), [&] (TFluentMap fluent, const auto& pair) {
                auto alertType = pair.first;
                const auto& error = pair.second;
                if (!error.IsOK()) {
                    fluent
                        .Item(FormatEnum(alertType)).Value(error);
                }
            })
        .Finish();

    result.RunningJobs =
        BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do(std::bind(&TOperationControllerBase::BuildJobsYson, this, _1))
        .Finish();

    result.MemoryUsage = GetMemoryUsage();

    result.ControllerState = State;

    return result;
}

i64 TOperationControllerBase::GetMemoryUsage() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto snapshot = GetMemoryUsageSnapshot();
    YT_VERIFY(snapshot);

    return snapshot->GetUsage(OperationIdTag, ToString(OperationId));
}

bool TOperationControllerBase::HasEnoughChunkLists(bool isWritingStderrTable, bool isWritingCoreTable)
{
    // We use this "result" variable to make sure that we have enough chunk lists
    // for every cell tag and start allocating them all in advance and simultaneously.
    bool result = true;
    for (auto [cellTag, count] : CellTagToRequiredOutputChunkListCount_) {
        if (count > 0 && !OutputChunkListPool_->HasEnough(cellTag, count)) {
            result = false;
        }
    }
    for (auto [cellTag, count] : CellTagToRequiredDebugChunkListCount_) {
        if (StderrTable_ && !isWritingStderrTable && StderrTable_->ExternalCellTag == cellTag) {
            --count;
        }
        if (CoreTable_ && !isWritingCoreTable && CoreTable_->ExternalCellTag == cellTag) {
            --count;
        }
        YT_VERIFY(DebugChunkListPool_);
        if (count > 0 && !DebugChunkListPool_->HasEnough(cellTag, count)) {
            result = false;
        }
    }
    return result;
}

TChunkListId TOperationControllerBase::ExtractOutputChunkList(TCellTag cellTag)
{
    return OutputChunkListPool_->Extract(cellTag);
}

TChunkListId TOperationControllerBase::ExtractDebugChunkList(TCellTag cellTag)
{
    YT_VERIFY(DebugChunkListPool_);
    return DebugChunkListPool_->Extract(cellTag);
}

void TOperationControllerBase::ReleaseChunkTrees(
    const std::vector<TChunkListId>& chunkTreeIds,
    bool unstageRecursively,
    bool waitForSnapshot)
{
    if (waitForSnapshot) {
        YT_VERIFY(unstageRecursively);
        for (const auto& chunkTreeId : chunkTreeIds) {
            ChunkTreeReleaseQueue_.Push(chunkTreeId);
        }
    } else {
        Host->AddChunkTreesToUnstageList(chunkTreeIds, unstageRecursively);
    }
}

void TOperationControllerBase::RegisterJoblet(const TJobletPtr& joblet)
{
    EmplaceOrCrash(JobletMap, AllocationIdFromJobId(joblet->JobId), joblet);
}

TJobletPtr TOperationControllerBase::FindJoblet(TAllocationId allocationId) const
{
    auto it = JobletMap.find(allocationId);
    return it == JobletMap.end() ? nullptr : it->second;
}

TJobletPtr TOperationControllerBase::FindJoblet(TJobId jobId) const
{
    return FindJoblet(AllocationIdFromJobId(jobId));
}

TJobletPtr TOperationControllerBase::GetJoblet(TJobId jobId) const
{
    auto joblet = FindJoblet(jobId);
    YT_VERIFY(joblet);
    return joblet;
}

TJobletPtr TOperationControllerBase::GetJobletOrThrow(TJobId jobId) const
{
    auto joblet = FindJoblet(jobId);
    if (!joblet) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchJob,
            "No such job %v",
            jobId);
    }
    return joblet;
}

std::optional<TJobMonitoringDescriptor> TOperationControllerBase::RegisterJobForMonitoring(TJobId jobId)
{
    std::optional<TJobMonitoringDescriptor> descriptor;
    if (MonitoringDescriptorIndexPool_.empty()) {
        descriptor = RegisterNewMonitoringDescriptor();
    } else {
        auto it = MonitoringDescriptorIndexPool_.begin();
        auto index = *it;
        MonitoringDescriptorIndexPool_.erase(it);
        descriptor = TJobMonitoringDescriptor{Host->GetIncarnationId(), index};
    }
    if (descriptor) {
        YT_LOG_DEBUG("Monitoring descriptor assigned to job (JobId: %v, MonitoringDescriptor: %v)",
            jobId,
            descriptor);

        EmplaceOrCrash(JobIdToMonitoringDescriptor_, jobId, *descriptor);
        ++MonitoredUserJobCount_;
    } else {
        YT_LOG_DEBUG("Failed to assign monitoring descriptor to job (JobId: %v)", jobId);
    }
    return descriptor;
}

std::optional<TJobMonitoringDescriptor> TOperationControllerBase::RegisterNewMonitoringDescriptor()
{
    ++MonitoredUserJobAttemptCount_;
    if (MonitoredUserJobCount_ >= Config->UserJobMonitoring->ExtendedMaxMonitoredUserJobsPerOperation) {
        SetOperationAlert(
            EOperationAlertType::UserJobMonitoringLimited,
            TError("Limit of monitored user jobs per operation reached, some jobs may be not monitored")
                << TErrorAttribute("operation_type", OperationType)
                << TErrorAttribute("limit_per_operation", Config->UserJobMonitoring->ExtendedMaxMonitoredUserJobsPerOperation));
        return {};
    }
    if (MonitoredUserJobCount_ >= Config->UserJobMonitoring->DefaultMaxMonitoredUserJobsPerOperation &&
        !GetOrDefault(Config->UserJobMonitoring->EnableExtendedMaxMonitoredUserJobsPerOperation, OperationType))
    {
        YT_LOG_DEBUG("Limit of monitored user jobs per operation reached "
            "(OperationType: %v, LimitPerOperation: %v)",
            OperationType,
            Config->UserJobMonitoring->DefaultMaxMonitoredUserJobsPerOperation);
        return {};
    }

    auto descriptor = Host->TryAcquireJobMonitoringDescriptor(OperationId);
    if (!descriptor) {
        SetOperationAlert(
            EOperationAlertType::UserJobMonitoringLimited,
            TError("Limit of monitored user jobs per controller agent reached, some jobs may be not monitored")
                << TErrorAttribute("limit_per_controller_agent", Config->UserJobMonitoring->MaxMonitoredUserJobsPerAgent));
        return {};
    }
    ++RegisteredMonitoringDescriptorCount_;
    return descriptor;
}

void TOperationControllerBase::UnregisterJobForMonitoring(const TJobletPtr& joblet)
{
    const auto& userJobSpec = joblet->Task->GetUserJobSpec();
    if (userJobSpec && userJobSpec->Monitoring->Enable) {
        --MonitoredUserJobAttemptCount_;
    }
    if (joblet->UserJobMonitoringDescriptor) {
        InsertOrCrash(MonitoringDescriptorIndexPool_, joblet->UserJobMonitoringDescriptor->Index);
        EraseOrCrash(JobIdToMonitoringDescriptor_, joblet->JobId);
        --MonitoredUserJobCount_;
        // NB: we do not want to remove index, but old version of logic can be done with the following call.
        // Host->ReleaseJobMonitoringDescriptor(OperationId, joblet->UserJobMonitoringDescriptor->Index);
    }
    if (MonitoredUserJobCount_ <= MonitoredUserJobAttemptCount_) {
        SetOperationAlert(EOperationAlertType::UserJobMonitoringLimited, TError());
    }
}

int TOperationControllerBase::GetMonitoredUserJobCount() const
{
    return MonitoredUserJobCount_;
}

int TOperationControllerBase::GetRegisteredMonitoringDescriptorCount() const
{
    return RegisteredMonitoringDescriptorCount_;
}

void TOperationControllerBase::UnregisterJoblet(const TJobletPtr& joblet)
{
    UnregisterJobForMonitoring(joblet);

    auto allocationJobletIt = GetIteratorOrCrash(JobletMap, AllocationIdFromJobId(joblet->JobId));
    YT_VERIFY(joblet == allocationJobletIt->second);
    JobletMap.erase(allocationJobletIt);
}

std::vector<TAllocationId> TOperationControllerBase::GetAllocationIdsByTreeId(const TString& treeId)
{
    std::vector<TAllocationId> allocationIds;
    for (const auto& [_, joblet] : JobletMap) {
        if (joblet->TreeId == treeId) {
            allocationIds.push_back(AllocationIdFromJobId(joblet->JobId));
        }
    }
    return allocationIds;
}

void TOperationControllerBase::SetProgressAttributesUpdated()
{
    ShouldUpdateProgressAttributesInCypress_ = false;
}

bool TOperationControllerBase::ShouldUpdateProgressAttributes() const
{
    return ShouldUpdateProgressAttributesInCypress_;
}

bool TOperationControllerBase::HasProgress() const
{
    if (!IsPrepared()) {
        return false;
    }

    {
        auto guard = Guard(ProgressLock_);
        return ProgressString_ && BriefProgressString_;
    }
}

void TOperationControllerBase::BuildInitializeMutableAttributes(TFluentMap fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    fluent
        .Item("async_scheduler_transaction_id").Value(AsyncTransaction ? AsyncTransaction->GetId() : NullTransactionId)
        .Item("input_transaction_id").Value(InputTransaction ? InputTransaction->GetId() : NullTransactionId)
        .Item("output_transaction_id").Value(OutputTransaction ? OutputTransaction->GetId() : NullTransactionId)
        .Item("debug_transaction_id").Value(DebugTransaction ? DebugTransaction->GetId() : NullTransactionId)
        .Item("nested_input_transaction_ids").DoListFor(NestedInputTransactions,
            [] (TFluentList fluent, const ITransactionPtr& transaction) {
                fluent
                    .Item().Value(transaction->GetId());
            }
        );
}

void TOperationControllerBase::BuildPrepareAttributes(TFluentMap fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    fluent
        .DoIf(static_cast<bool>(AutoMergeDirector_), [&] (TFluentMap fluent) {
            fluent
                .Item("auto_merge").BeginMap()
                    .Item("max_intermediate_chunk_count").Value(AutoMergeDirector_->GetMaxIntermediateChunkCount())
                    .Item("chunk_count_per_merge_job").Value(AutoMergeDirector_->GetChunkCountPerMergeJob())
                .EndMap();
        });
}

void TOperationControllerBase::BuildBriefSpec(TFluentMap fluent) const
{
    std::vector<TYPath> inputPaths;
    for (const auto& path : GetInputTablePaths()) {
        inputPaths.push_back(path.GetPath());
    }

    std::vector<TYPath> outputPaths;
    for (const auto& path : GetOutputTablePaths()) {
        outputPaths.push_back(path.GetPath());
    }

    fluent
        .OptionalItem("title", Spec_->Title)
        .OptionalItem("alias", Spec_->Alias)
        .Item("user_transaction_id").Value(UserTransactionId)
        .Item("input_table_paths").ListLimited(inputPaths, 1)
        .Item("output_table_paths").ListLimited(outputPaths, 1);
}

void TOperationControllerBase::BuildProgress(TFluentMap fluent) const
{
    if (!IsPrepared()) {
        return;
    }

    AccountExternalScheduleAllocationFailures();

    auto fullJobStatistics = MergeJobStatistics(
        AggregatedFinishedJobStatistics_,
        AggregatedRunningJobStatistics_);

    fluent
        .Item("state").Value(State.load())
        .Item("build_time").Value(TInstant::Now())
        .Item("ready_job_count").Value(GetPendingJobCount())
        .Item("job_statistics_v2").Value(fullJobStatistics)
        .Item("job_statistics").Do([this] (TFluentAny fluent) {
            AggregatedFinishedJobStatistics_.SerializeLegacy(fluent.GetConsumer());
        })
        .Item("peak_memory_usage").Value(PeakMemoryUsage_)
        .Item("estimated_input_statistics").BeginMap()
            .Item("chunk_count").Value(TotalEstimatedInputChunkCount)
            .Item("uncompressed_data_size").Value(TotalEstimatedInputUncompressedDataSize)
            .Item("compressed_data_size").Value(TotalEstimatedInputCompressedDataSize)
            .Item("data_weight").Value(TotalEstimatedInputDataWeight)
            .Item("row_count").Value(TotalEstimatedInputRowCount)
            .Item("unavailable_chunk_count").Value(GetUnavailableInputChunkCount() + UnavailableIntermediateChunkCount)
            .Item("data_slice_count").Value(GetDataSliceCount())
        .EndMap()
        .Item("live_preview").BeginMap()
            .Item("output_supported").Value(IsLegacyOutputLivePreviewSupported())
            .Item("intermediate_supported").Value(IsLegacyIntermediateLivePreviewSupported())
            .Item("stderr_supported").Value(static_cast<bool>(StderrTable_))
            .Item("core_supported").Value(static_cast<bool>(CoreTable_))
            .Item("virtual_table_format").BeginMap()
                .Item("output_supported").Value(IsOutputLivePreviewSupported())
                .Item("intermediate_supported").Value(IsIntermediateLivePreviewSupported())
                .Item("stderr_supported").Value(static_cast<bool>(StderrTable_))
                .Item("core_supported").Value(static_cast<bool>(CoreTable_))
            .EndMap()
        .EndMap()
        .Item("schedule_job_statistics").BeginMap()
            .Item("count").Value(ScheduleAllocationStatistics_->GetCount())
            .Item("total_duration").Value(ScheduleAllocationStatistics_->GetTotalDuration())
            // COMPAT(eshcherbin)
            .Item("duration").Value(ScheduleAllocationStatistics_->GetTotalDuration())
            .Item("failed").Value(ScheduleAllocationStatistics_->Failed())
            .Item("successful_duration_estimate_us").Value(
                ScheduleAllocationStatistics_->SuccessfulDurationMovingAverage().GetAverage().value_or(TDuration::Zero()).MicroSeconds())
        .EndMap()
        // COMPAT(gritukan): Drop it in favour of "total_job_counter".
        .Item("jobs").Value(GetTotalJobCounter())
        .Item("total_job_counter").Value(GetTotalJobCounter())
        .Item("data_flow_graph").BeginMap()
            .Do(BIND(&TDataFlowGraph::BuildLegacyYson, DataFlowGraph_))
        .EndMap()
        // COMPAT(gritukan): Drop it in favour of per-task histograms.
        .DoIf(static_cast<bool>(EstimatedInputDataSizeHistogram_), [&] (TFluentMap fluent) {
            EstimatedInputDataSizeHistogram_->BuildHistogramView();
            fluent
                .Item("estimated_input_data_size_histogram").Value(*EstimatedInputDataSizeHistogram_);
        })
        .DoIf(static_cast<bool>(InputDataSizeHistogram_), [&] (TFluentMap fluent) {
            InputDataSizeHistogram_->BuildHistogramView();
            fluent
                .Item("input_data_size_histogram").Value(*InputDataSizeHistogram_);
        })
        .Item("snapshot_index").Value(SnapshotIndex_.load())
        .Item("recent_snapshot_index").Value(RecentSnapshotIndex_)
        .Item("last_successful_snapshot_time").Value(LastSuccessfulSnapshotTime_)
        .Item("tasks").DoListFor(GetTopologicallyOrderedTasks(), [=] (TFluentList fluent, const TTaskPtr& task) {
            fluent.Item()
                .BeginMap()
                    .Do(BIND(&TTask::BuildTaskYson, task))
                .EndMap();
        })
        .Item("data_flow").BeginList()
            .Do(BIND(&TDataFlowGraph::BuildDataFlowYson, DataFlowGraph_))
        .EndList();
}

void TOperationControllerBase::BuildBriefProgress(TFluentMap fluent) const
{
    if (IsPrepared()) {
        fluent
            .Item("state").Value(State.load())
            // COMPAT(gritukan): Drop it in favour of "total_job_counter".
            .Item("jobs").Do(BIND([&] (TFluentAny fluent) {
                SerializeBriefVersion(GetTotalJobCounter(), fluent.GetConsumer());
            }))
            .Item("total_job_counter").Do(BIND([&] (TFluentAny fluent) {
                SerializeBriefVersion(GetTotalJobCounter(), fluent.GetConsumer());
            }))
            .Item("build_time").Value(TInstant::Now())
            .Item("registered_monitoring_descriptor_count").Value(GetRegisteredMonitoringDescriptorCount())
            .Item("input_transaction_id").Value(InputTransaction ? InputTransaction->GetId() : NullTransactionId)
            .Item("output_transaction_id").Value(OutputTransaction ? OutputTransaction->GetId() : NullTransactionId);
    }
}

void TOperationControllerBase::BuildAndSaveProgress()
{
    YT_LOG_DEBUG("Building and saving progress");

    auto progressString = BuildYsonStringFluently()
        .BeginMap()
        .Do([&] (TFluentMap fluent) {
            auto asyncResult = WaitFor(
                BIND(&TOperationControllerBase::BuildProgress, MakeStrong(this))
                    .AsyncVia(GetInvoker())
                    .Run(fluent));
                asyncResult
                    .ThrowOnError();
            })
        .EndMap();

    auto briefProgressString = BuildYsonStringFluently()
        .BeginMap()
            .Do([&] (TFluentMap fluent) {
                auto asyncResult = WaitFor(
                    BIND(&TOperationControllerBase::BuildBriefProgress, MakeStrong(this))
                        .AsyncVia(GetInvoker())
                        .Run(fluent));
                asyncResult
                    .ThrowOnError();
            })
        .EndMap();

    {
        auto guard = Guard(ProgressLock_);
        if (!ProgressString_ || ProgressString_ != progressString ||
            !BriefProgressString_ || BriefProgressString_ != briefProgressString)
        {
            ShouldUpdateProgressAttributesInCypress_ = true;
            YT_LOG_DEBUG("New progress is different from previous one, should update progress");
        }
        ProgressString_ = progressString;
        BriefProgressString_ = briefProgressString;
    }
    YT_LOG_DEBUG("Progress built and saved");
}

TYsonString TOperationControllerBase::GetProgress() const
{
    auto guard = Guard(ProgressLock_);
    return ProgressString_;
}

TYsonString TOperationControllerBase::GetBriefProgress() const
{
    auto guard = Guard(ProgressLock_);
    return BriefProgressString_;
}

IYPathServicePtr TOperationControllerBase::GetOrchid() const
{
    return Orchid_.Acquire();
}

void TOperationControllerBase::ZombifyOrchid()
{
    Orchid_.Store(BuildZombieOrchid());
}

const std::vector<NScheduler::TJobShellPtr>& TOperationControllerBase::GetJobShells() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Spec_->JobShells;
}

NYson::TYsonString TOperationControllerBase::DoBuildJobsYson()
{
    return BuildYsonStringFluently<EYsonType::MapFragment>()
        .DoFor(JobletMap, [&] (TFluentMap fluent, const std::pair<TAllocationId, TJobletPtr>& pair) {
            const auto& joblet = pair.second;

            if (joblet->IsStarted()) {
                fluent.Item(ToString(joblet->JobId)).BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        BuildJobAttributes(
                            joblet,
                            *joblet->JobState,
                            joblet->StderrSize,
                            fluent);
                    })
                .EndMap();
            }
        })
        .Finish();
}

void TOperationControllerBase::BuildJobsYson(TFluentMap fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));
    fluent.GetConsumer()->OnRaw(CachedRunningJobs_.GetValue());
}

void TOperationControllerBase::BuildRetainedFinishedJobsYson(TFluentMap fluent) const
{
    for (const auto& [jobId, attributes] : RetainedFinishedJobs_) {
        fluent
            .Item(ToString(jobId)).Value(attributes);
    }
}

void TOperationControllerBase::BuildUnavailableInputChunksYson(TFluentAny fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));
    fluent.Value(UnavailableInputChunkIds);
}

void TOperationControllerBase::CheckTentativeTreeEligibility()
{
    THashSet<TString> treeIds;
    for (const auto& task : Tasks) {
        task->LogTentativeTreeStatistics();
        for (const auto& treeId : task->FindAndBanSlowTentativeTrees()) {
            treeIds.insert(treeId);
        }
    }
    for (const auto& treeId : treeIds) {
        MaybeBanInTentativeTree(treeId);
    }
}

TSharedRef TOperationControllerBase::SafeBuildJobSpecProto(const TJobletPtr& joblet, const NScheduler::NProto::TScheduleAllocationSpec& scheduleAllocationSpec)
{
    VERIFY_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    if (auto buildJobSpecProtoDelay = Spec_->TestingOperationOptions->BuildJobSpecProtoDelay) {
        Sleep(*buildJobSpecProtoDelay);
    }

    return joblet->Task->BuildJobSpecProto(joblet, scheduleAllocationSpec);
}

TJobStartInfo TOperationControllerBase::SettleJob(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::GetJobSpec));

    if (auto getJobSpecDelay = Spec_->TestingOperationOptions->GetJobSpecDelay) {
        Sleep(*getJobSpecDelay);
    }

    if (Spec_->TestingOperationOptions->FailGetJobSpec) {
        THROW_ERROR_EXCEPTION("Testing failure");
    }

    auto joblet = FindJoblet(allocationId);

    if (!joblet) {
        YT_LOG_DEBUG(
            "Stale settle job request, no such allocation; send error instead of spec "
            "(AllocationId: %v)",
            allocationId);
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::NoSuchAllocation,
            "No such allocation %v",
            allocationId);
    }

    if (!joblet->JobSpecProtoFuture) {
        THROW_ERROR_EXCEPTION("Job for allocation %v is missing", allocationId);
    }

    auto specBlob = WaitFor(joblet->JobSpecProtoFuture)
        .ValueOrThrow();
    joblet->JobSpecProtoFuture.Reset();

    auto operationState = State.load();
    if (operationState != EControllerState::Running) {
        YT_LOG_DEBUG(
            "Stale settle job request, operation is already not running; send error instead of spec "
            "(AllocationId: %v, OperationState: %v)",
            allocationId,
            operationState);
        THROW_ERROR_EXCEPTION("Operation is not running");
    }

    //! NB(arkady-e1ppa): Concurrent OnJobAborted(Failed/Completed)
    //! can unregister joblet without changing the operation state yet.
    //! In such cases we might start job which was already finished.
    if (!FindJoblet(joblet->JobId)) {
        YT_LOG_DEBUG(
            "Stale settle job request, job is already finished; send error instead of spec "
            "(AllocationId: %v)",
            allocationId);
        THROW_ERROR_EXCEPTION("Job is already finished");
    }

    OnJobStarted(joblet);

    return TJobStartInfo{
        .JobId = joblet->JobId,
        .JobSpecBlob = std::move(specBlob),
    };
}

TYsonString TOperationControllerBase::GetSuspiciousJobsYson() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(CachedSuspiciousJobsYsonLock_);
    return CachedSuspiciousJobsYson_;
}

void TOperationControllerBase::UpdateSuspiciousJobsYson()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    // We sort suspicious jobs by their last activity time and then
    // leave top `MaxOrchidEntryCountPerType` for each job type.

    std::vector<TJobletPtr> suspiciousJoblets;
    for (const auto& [_, joblet] : JobletMap) {
        if (joblet->Suspicious) {
            suspiciousJoblets.emplace_back(joblet);
        }
    }

    std::sort(suspiciousJoblets.begin(), suspiciousJoblets.end(), [] (const TJobletPtr& lhs, const TJobletPtr& rhs) {
        return lhs->LastActivityTime < rhs->LastActivityTime;
    });

    THashMap<EJobType, int> suspiciousJobCountPerType;

    auto yson = BuildYsonStringFluently<EYsonType::MapFragment>()
        .DoFor(
            suspiciousJoblets,
            [&] (TFluentMap fluent, const TJobletPtr& joblet) {
                auto& count = suspiciousJobCountPerType[joblet->JobType];
                if (count < Config->SuspiciousJobs->MaxOrchidEntryCountPerType) {
                    ++count;
                    fluent.Item(ToString(joblet->JobId))
                        .BeginMap()
                            .Item("operation_id").Value(ToString(OperationId))
                            .Item("type").Value(joblet->JobType)
                            .Item("brief_statistics").Value(joblet->BriefStatistics)
                            .Item("node").Value(joblet->NodeDescriptor.Address)
                            .Item("last_activity_time").Value(joblet->LastActivityTime)
                        .EndMap();
                }
            })
        .Finish();

    {
        auto guard = WriterGuard(CachedSuspiciousJobsYsonLock_);
        CachedSuspiciousJobsYson_ = yson;
    }
}

void TOperationControllerBase::UpdateAggregatedRunningJobStatistics()
{
    auto statisticsLimit = Options->CustomStatisticsCountLimit;

    YT_LOG_DEBUG(
        "Updating aggregated running job statistics (StatisticsLimit: %v, RunningJobCount: %v)",
        statisticsLimit,
        JobletMap.size());

    // A lightweight structure that represents a snapshot of a joblet that is safe to be used
    // in a separate invoker. Note that job statistics and controller statistics are const-qualified,
    // thus they are immutable.
    struct TJobletSnapshot
    {
        std::shared_ptr<const TStatistics> JobStatistics;
        std::shared_ptr<const TStatistics> ControllerStatistics;
        TJobStatisticsTags Tags;
    };

    std::vector<TJobletSnapshot> snapshots;
    snapshots.reserve(JobletMap.size());
    for (const auto& joblet : GetValues(JobletMap)) {
        snapshots.emplace_back(TJobletSnapshot{
            joblet->ControllerStatistics,
            joblet->JobStatistics,
            joblet->GetAggregationTags(EJobState::Running),
        });
    }

    // NB: this routine will be done in a separate thread pool.
    auto buildAggregatedStatisticsHeavy = [snapshots = std::move(snapshots), statisticsLimit, Logger = this->Logger] {
        TAggregatedJobStatistics runningJobStatistics;
        bool isLimitExceeded = false;

        YT_LOG_DEBUG("Starting aggregated job statistics update heavy routine");

        static const auto AggregationYieldPeriod = TDuration::MilliSeconds(10);

        TPeriodicYielder yielder(AggregationYieldPeriod);

        for (const auto& [jobStatistics, controllerStatistics, tags] : snapshots) {
            UpdateAggregatedJobStatistics(
                runningJobStatistics,
                tags,
                *jobStatistics,
                *controllerStatistics,
                statisticsLimit,
                &isLimitExceeded);
            yielder.TryYield();
        }

        YT_LOG_DEBUG("Aggregated job statistics update heavy routine finished");

        return std::pair(std::move(runningJobStatistics), isLimitExceeded);
    };

    YT_LOG_DEBUG("Scheduling aggregated job statistics update heavy routine");

    TWallTimer wallTimer;
    auto [runningJobStatistics, isLimitExceeded] = WaitFor(
        BIND(buildAggregatedStatisticsHeavy)
            .AsyncVia(Host->GetStatisticsOffloadInvoker())
            .Run())
        .Value();

    YT_LOG_DEBUG("New aggregated job statistics are ready (HeavyWallTime: %v)", wallTimer.GetElapsedTime());

    if (isLimitExceeded) {
        SetOperationAlert(EOperationAlertType::CustomStatisticsLimitExceeded,
            TError("Limit for number of custom statistics exceeded for operation, so they are truncated")
                << TErrorAttribute("limit", statisticsLimit));
    }

    // Old aggregated statistics will be destroyed in controller invoker but I am too lazy to fix that now.
    AggregatedRunningJobStatistics_ = std::move(runningJobStatistics);
}

void TOperationControllerBase::ReleaseJobs(const std::vector<TJobId>& jobIds)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    YT_LOG_DEBUG(
        "Releasing jobs (JobCount: %v)",
        std::size(jobIds));

    if (std::empty(jobIds)) {
        return;
    }

    std::vector<TJobToRelease> jobsToRelease;
    jobsToRelease.reserve(jobIds.size());
    for (auto jobId : jobIds) {
        if (auto it = JobIdToReleaseFlags_.find(jobId); it != JobIdToReleaseFlags_.end()) {
            jobsToRelease.emplace_back(TJobToRelease{jobId, it->second});
            JobIdToReleaseFlags_.erase(it);
        }
    }

    Host->ReleaseJobs(jobsToRelease);
}

// TODO(max42): rename job -> joblet.
void TOperationControllerBase::AnalyzeBriefStatistics(
    const TJobletPtr& joblet,
    const TSuspiciousJobsOptionsPtr& options,
    const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError)
{
    if (!briefStatisticsOrError.IsOK()) {
        if (joblet->BriefStatistics) {
            // Failures in brief statistics building are normal during job startup,
            // when readers and writers are not built yet. After we successfully built
            // brief statistics once, we shouldn't fail anymore.

            YT_LOG_WARNING(
                briefStatisticsOrError,
                "Failed to build brief job statistics (JobId: %v)",
                joblet->JobId);
        }

        return;
    }

    const auto& briefStatistics = briefStatisticsOrError.Value();

    bool wasActive = !joblet->BriefStatistics ||
        CheckJobActivity(
            joblet->BriefStatistics,
            briefStatistics,
            options,
            joblet->JobType);

    bool wasSuspicious = joblet->Suspicious;
    joblet->Suspicious = (!wasActive && briefStatistics->Timestamp - joblet->LastActivityTime > options->InactivityTimeout);
    if (!wasSuspicious && joblet->Suspicious) {
        YT_LOG_DEBUG("Found a suspicious job (JobId: %v, JobType: %v, LastActivityTime: %v, SuspiciousInactivityTimeout: %v, "
            "OldBriefStatistics: %v, NewBriefStatistics: %v)",
            joblet->JobId,
            joblet->JobType,
            joblet->LastActivityTime,
            options->InactivityTimeout,
            joblet->BriefStatistics,
            briefStatistics);
    }

    joblet->BriefStatistics = briefStatistics;

    if (wasActive) {
        joblet->LastActivityTime = joblet->BriefStatistics->Timestamp;
    }
}

void TOperationControllerBase::UpdateAggregatedFinishedJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    i64 statisticsLimit = Options->CustomStatisticsCountLimit;
    bool isLimitExceeded = false;

    UpdateAggregatedJobStatistics(
        AggregatedFinishedJobStatistics_,
        joblet->GetAggregationTags(jobSummary.State),
        *joblet->JobStatistics,
        *joblet->ControllerStatistics,
        statisticsLimit,
        &isLimitExceeded);

    if (isLimitExceeded) {
        SetOperationAlert(EOperationAlertType::CustomStatisticsLimitExceeded,
            TError("Limit for number of custom statistics exceeded for operation, so they are truncated")
                << TErrorAttribute("limit", statisticsLimit));
    }

    joblet->Task->UpdateAggregatedFinishedJobStatistics(joblet, jobSummary);
}

void TOperationControllerBase::UpdateJobMetrics(const TJobletPtr& joblet, const TJobSummary& jobSummary, bool isJobFinished)
{
    YT_LOG_TRACE("Updating job metrics (JobId: %v)", joblet->JobId);

    auto delta = joblet->UpdateJobMetrics(jobSummary, isJobFinished);
    {
        auto guard = Guard(JobMetricsDeltaPerTreeLock_);

        auto it = JobMetricsDeltaPerTree_.find(joblet->TreeId);
        if (it == JobMetricsDeltaPerTree_.end()) {
            YT_VERIFY(JobMetricsDeltaPerTree_.emplace(joblet->TreeId, delta).second);
        } else {
            it->second += delta;
        }

        TotalTimePerTree_[joblet->TreeId] += delta.Values()[EJobMetricName::TotalTime];
        MainResourceConsumptionPerTree_[joblet->TreeId] += delta.Values()[EJobMetricName::TotalTime] *
            GetResource(joblet->ResourceLimits, PoolTreeControllerSettingsMap_[joblet->TreeId].MainResource);
    }
}

void TOperationControllerBase::LogProgress(bool force)
{
    if (!HasProgress()) {
        return;
    }

    auto now = GetCpuInstant();
    if (force || now > NextLogProgressDeadline) {
        NextLogProgressDeadline = now + LogProgressBackoff;
        YT_LOG_DEBUG("Operation progress (Progress: %v)", GetLoggingProgress());
    }
}

ui64 TOperationControllerBase::NextJobIndex()
{
    return JobIndexGenerator.Next();
}

TOperationId TOperationControllerBase::GetOperationId() const
{
    return OperationId;
}

EOperationType TOperationControllerBase::GetOperationType() const
{
    return OperationType;
}

TInstant TOperationControllerBase::GetStartTime() const
{
    return StartTime_;
}

const TString& TOperationControllerBase::GetAuthenticatedUser() const
{
    return AuthenticatedUser;
}

const TChunkListPoolPtr& TOperationControllerBase::GetOutputChunkListPool() const
{
    return OutputChunkListPool_;
}

const TControllerAgentConfigPtr& TOperationControllerBase::GetConfig() const
{
    return Config;
}

const TOperationSpecBasePtr& TOperationControllerBase::GetSpec() const
{
    return Spec_;
}

const TOperationOptionsPtr& TOperationControllerBase::GetOptions() const
{
    return Options;
}

const TOutputTablePtr& TOperationControllerBase::StderrTable() const
{
    return StderrTable_;
}

const TOutputTablePtr& TOperationControllerBase::CoreTable() const
{
    return CoreTable_;
}

const std::optional<TJobResources>& TOperationControllerBase::CachedMaxAvailableExecNodeResources() const
{
    return CachedMaxAvailableExecNodeResources_;
}

const TNodeDirectoryPtr& TOperationControllerBase::InputNodeDirectory() const
{
    return InputNodeDirectory_;
}

bool TOperationControllerBase::IsRowCountPreserved() const
{
    return false;
}

i64 TOperationControllerBase::GetUnavailableInputChunkCount() const
{
    if (!DataSliceFetcherChunkScrapers.empty() && State == EControllerState::Preparing) {
        i64 result = 0;
        for (const auto& fetcher : DataSliceFetcherChunkScrapers) {
            result += fetcher->GetUnavailableChunkCount();
        }
        return result;
    }
    return std::ssize(UnavailableInputChunkIds);
}

int TOperationControllerBase::GetTotalJobCount() const
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    // Avoid accessing the state while not prepared.
    if (!IsPrepared()) {
        return 0;
    }

    return GetTotalJobCounter()->GetTotal();
}

i64 TOperationControllerBase::GetDataSliceCount() const
{
    i64 result = 0;
    for (const auto& task : Tasks) {
        result += task->GetInputDataSliceCount();
    }

    return result;
}

void TOperationControllerBase::InitUserJobSpecTemplate(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const TUserJobSpecPtr& jobSpecConfig,
    const std::vector<TUserFile>& files,
    const TString& debugArtifactsAccount)
{
    const auto& userJobOptions = Options->UserJobOptions;

    jobSpec->set_shell_command(jobSpecConfig->Command);
    if (jobSpecConfig->JobTimeLimit) {
        jobSpec->set_job_time_limit(ToProto<i64>(*jobSpecConfig->JobTimeLimit));
    }
    jobSpec->set_prepare_time_limit(ToProto<i64>(jobSpecConfig->PrepareTimeLimit));
    jobSpec->set_memory_limit(jobSpecConfig->MemoryLimit);
    jobSpec->set_include_memory_mapped_files(jobSpecConfig->IncludeMemoryMappedFiles);
    jobSpec->set_use_yamr_descriptors(jobSpecConfig->UseYamrDescriptors);
    jobSpec->set_check_input_fully_consumed(jobSpecConfig->CheckInputFullyConsumed);
    jobSpec->set_max_stderr_size(jobSpecConfig->MaxStderrSize);
    jobSpec->set_custom_statistics_count_limit(jobSpecConfig->CustomStatisticsCountLimit);
    jobSpec->set_copy_files(jobSpecConfig->CopyFiles);
    jobSpec->set_debug_artifacts_account(debugArtifactsAccount);
    jobSpec->set_set_container_cpu_limit(jobSpecConfig->SetContainerCpuLimit || Options->SetContainerCpuLimit);
    jobSpec->set_redirect_stdout_to_stderr(jobSpecConfig->RedirectStdoutToStderr);

    // This is common policy for all operations of given type.
    if (Options->SetContainerCpuLimit) {
        jobSpec->set_container_cpu_limit(Options->CpuLimitOvercommitMultiplier * jobSpecConfig->CpuLimit + Options->InitialCpuLimitOvercommit);
    }

    // This is common policy for all operations of given type.
    i64 threadLimit = ceil(userJobOptions->InitialThreadLimit + userJobOptions->ThreadLimitMultiplier * jobSpecConfig->CpuLimit);
    jobSpec->set_thread_limit(threadLimit);

    // Option in task spec overrides value in operation options.
    if (jobSpecConfig->SetContainerCpuLimit) {
        jobSpec->set_container_cpu_limit(jobSpecConfig->CpuLimit);
    }

    jobSpec->set_force_core_dump(jobSpecConfig->ForceCoreDump);

    jobSpec->set_port_count(jobSpecConfig->PortCount);
    jobSpec->set_use_porto_memory_tracking(jobSpecConfig->UsePortoMemoryTracking);

    if (Config->EnableTmpfs) {
        for (const auto& volume : jobSpecConfig->TmpfsVolumes) {
            ToProto(jobSpec->add_tmpfs_volumes(), *volume);
        }
    }

    if (auto& diskRequest = jobSpecConfig->DiskRequest) {
        ToProto(jobSpec->mutable_disk_request(), *diskRequest);
        if (diskRequest->InodeCount) {
            jobSpec->set_inode_limit(*diskRequest->InodeCount);
        }
    }
    if (jobSpecConfig->InterruptionSignal) {
        jobSpec->set_interruption_signal(*jobSpecConfig->InterruptionSignal);
        jobSpec->set_signal_root_process_only(jobSpecConfig->SignalRootProcessOnly);
    }
    if (jobSpecConfig->RestartExitCode) {
        jobSpec->set_restart_exit_code(*jobSpecConfig->RestartExitCode);
    }

    if (Config->IopsThreshold) {
        jobSpec->set_iops_threshold(*Config->IopsThreshold);
        if (Config->IopsThrottlerLimit) {
            jobSpec->set_iops_throttler_limit(*Config->IopsThrottlerLimit);
        }
    }

    {
        // Set input and output format.
        TFormat inputFormat(EFormatType::Yson);
        TFormat outputFormat(EFormatType::Yson);

        if (jobSpecConfig->Format) {
            inputFormat = outputFormat = *jobSpecConfig->Format;
        }

        if (jobSpecConfig->InputFormat) {
            inputFormat = *jobSpecConfig->InputFormat;
        }

        if (jobSpecConfig->OutputFormat) {
            outputFormat = *jobSpecConfig->OutputFormat;
        }

        jobSpec->set_input_format(ConvertToYsonString(inputFormat).ToString());
        jobSpec->set_output_format(ConvertToYsonString(outputFormat).ToString());
    }

    jobSpec->set_enable_gpu_layers(jobSpecConfig->EnableGpuLayers);

    if (jobSpecConfig->CudaToolkitVersion) {
        jobSpec->set_cuda_toolkit_version(*jobSpecConfig->CudaToolkitVersion);
    }

    if (Config->GpuCheckLayerDirectoryPath &&
        jobSpecConfig->GpuCheckBinaryPath &&
        jobSpecConfig->GpuCheckLayerName &&
        jobSpecConfig->EnableGpuLayers)
    {
        jobSpec->set_gpu_check_binary_path(*jobSpecConfig->GpuCheckBinaryPath);
        if (auto gpuCheckBinaryArgs = jobSpecConfig->GpuCheckBinaryArgs) {
            for (const auto& argument : *gpuCheckBinaryArgs) {
                ToProto(jobSpec->add_gpu_check_binary_args(), argument);
            }
        }
    }

    if (jobSpecConfig->NetworkProject) {
        const auto& client = Host->GetClient();
        auto networkProjectAttributes = GetNetworkProject(client, AuthenticatedUser, *jobSpecConfig->NetworkProject);
        jobSpec->set_network_project_id(networkProjectAttributes->Get<ui32>("project_id"));

        jobSpec->set_enable_nat64(networkProjectAttributes->Get<bool>("enable_nat64", false));
        jobSpec->set_disable_network(networkProjectAttributes->Get<bool>("disable_network", false));
    }

    jobSpec->set_enable_porto(ToProto<int>(jobSpecConfig->EnablePorto.value_or(Config->DefaultEnablePorto)));
    jobSpec->set_fail_job_on_core_dump(jobSpecConfig->FailJobOnCoreDump);
    jobSpec->set_enable_cuda_gpu_core_dump(GetEnableCudaGpuCoreDump());

    bool makeRootFSWritable = jobSpecConfig->MakeRootFSWritable;
    if (!Config->TestingOptions->RootfsTestLayers.empty()) {
        makeRootFSWritable = true;
    }
    jobSpec->set_make_rootfs_writable(makeRootFSWritable);

    jobSpec->set_use_smaps_memory_tracker(jobSpecConfig->UseSMapsMemoryTracker);

    auto fillEnvironment = [&] (THashMap<TString, TString>& env) {
        for (const auto& [key, value] : env) {
            jobSpec->add_environment(Format("%v=%v", key, value));
        }
    };

    // Global environment.
    fillEnvironment(Config->Environment);

    // Local environment.
    fillEnvironment(jobSpecConfig->Environment);

    jobSpec->add_environment(Format("YT_OPERATION_ID=%v", OperationId));

    BuildFileSpecs(jobSpec, files, jobSpecConfig, Config->EnableBypassArtifactCache);

    if (jobSpecConfig->Monitoring->Enable) {
        ToProto(jobSpec->mutable_monitoring_config()->mutable_sensor_names(), jobSpecConfig->Monitoring->SensorNames);
    }

    jobSpec->set_enable_rpc_proxy_in_job_proxy(jobSpecConfig->EnableRpcProxyInJobProxy);
    jobSpec->set_rpc_proxy_worker_thread_pool_size(jobSpecConfig->RpcProxyWorkerThreadPoolSize);

    // Pass external docker image into job spec as is.
    if (jobSpecConfig->DockerImage &&
        !TDockerImageSpec(*jobSpecConfig->DockerImage, Config->DockerRegistry).IsInternal())
    {
        jobSpec->set_docker_image(*jobSpecConfig->DockerImage);
    }
}

const std::vector<TUserFile>& TOperationControllerBase::GetUserFiles(const TUserJobSpecPtr& userJobSpec) const
{
    return GetOrCrash(UserJobFiles_, userJobSpec);
}

void TOperationControllerBase::InitUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    VERIFY_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    ToProto(
        jobSpec->mutable_debug_transaction_id(),
        DebugTransaction ? DebugTransaction->GetId() : NullTransactionId);

    ToProto(
        jobSpec->mutable_input_transaction_id(),
        InputTransaction ? InputTransaction->GetId() : NullTransactionId);

    jobSpec->set_memory_reserve(joblet->UserJobMemoryReserve);
    jobSpec->set_job_proxy_memory_reserve(
        joblet->EstimatedResourceUsage.GetFootprintMemory() +
        joblet->EstimatedResourceUsage.GetJobProxyMemory() * joblet->JobProxyMemoryReserveFactor.value());

    if (Options->SetSlotContainerMemoryLimit) {
        jobSpec->set_slot_container_memory_limit(
            jobSpec->memory_limit() +
            joblet->EstimatedResourceUsage.GetJobProxyMemory() +
            joblet->EstimatedResourceUsage.GetFootprintMemory() +
            Options->SlotContainerMemoryOverhead);
    }

    jobSpec->add_environment(Format("YT_JOB_INDEX=%v", joblet->JobIndex));
    jobSpec->add_environment(Format("YT_TASK_JOB_INDEX=%v", joblet->TaskJobIndex));
    jobSpec->add_environment(Format("YT_JOB_ID=%v", joblet->JobId));
    jobSpec->add_environment(Format("YT_JOB_COOKIE=%v", joblet->OutputCookie));
    if (joblet->StartRowIndex >= 0) {
        jobSpec->add_environment(Format("YT_START_ROW_INDEX=%v", joblet->StartRowIndex));
    }

    if (joblet->EnabledJobProfiler && joblet->EnabledJobProfiler->Type == EProfilerType::Cuda) {
        auto cudaProfilerEnvironment = Spec_->CudaProfilerEnvironment
            ? Spec_->CudaProfilerEnvironment
            : Config->CudaProfilerEnvironment;

        if (cudaProfilerEnvironment) {
            jobSpec->add_environment(Format("%v=%v",
                cudaProfilerEnvironment->PathEnvironmentVariableName,
                cudaProfilerEnvironment->PathEnvironmentVariableValue));
        }
    }

    if (SecureVault) {
        // NB: These environment variables should be added to user job spec, not to the user job spec template.
        // They may contain sensitive information that should not be persisted with a controller.

        // We add a single variable storing the whole secure vault and all top-level scalar values.
        jobSpec->add_environment(Format("%v=%v",
            SecureVaultEnvPrefix,
            ConvertToYsonString(SecureVault, EYsonFormat::Text)));

        for (const auto& [key, node] : SecureVault->GetChildren()) {
            std::optional<TString> value;
            switch (node->GetType()) {
                #define XX(type, cppType) \
                case ENodeType::type: \
                    value = ToString(node->As ## type()->GetValue()); \
                    break;
                ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
                #undef XX
                case ENodeType::Entity:
                    break;
                default:
                    value = ConvertToYsonString(node, EYsonFormat::Text).ToString();
                    break;
            }
            if (value) {
                jobSpec->add_environment(Format("%v_%v=%v", SecureVaultEnvPrefix, key, *value));
            }
        }

        jobSpec->set_enable_secure_vault_variables_in_job_shell(Spec_->EnableSecureVaultVariablesInJobShell);
    }

    if (RetainedJobWithStderrCount_ >= Spec_->MaxStderrCount) {
        jobSpec->set_upload_stderr_if_completed(false);
    }

    if (joblet->StderrTableChunkListId) {
        AddStderrOutputSpecs(jobSpec, joblet);
    }
    if (joblet->CoreTableChunkListId) {
        AddCoreOutputSpecs(jobSpec, joblet);
    }

    if (joblet->UserJobMonitoringDescriptor) {
        auto* monitoringConfig = jobSpec->mutable_monitoring_config();
        monitoringConfig->set_enable(true);
        monitoringConfig->set_job_descriptor(ToString(*joblet->UserJobMonitoringDescriptor));
    }

    joblet->Task->PatchUserJobSpec(jobSpec, joblet);
}

void TOperationControllerBase::AddStderrOutputSpecs(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    VERIFY_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    auto* stderrTableSpec = jobSpec->mutable_stderr_table_spec();
    auto* outputSpec = stderrTableSpec->mutable_output_table_spec();
    outputSpec->set_table_writer_options(ConvertToYsonString(StderrTable_->TableWriterOptions).ToString());
    outputSpec->set_table_schema(SerializeToWireProto(StderrTable_->TableUploadOptions.TableSchema.Get()));
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->StderrTableChunkListId);

    auto writerConfig = GetStderrTableWriterConfig();
    YT_VERIFY(writerConfig);
    stderrTableSpec->set_blob_table_writer_config(ConvertToYsonString(writerConfig).ToString());
}

void TOperationControllerBase::AddCoreOutputSpecs(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    VERIFY_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    auto* coreTableSpec = jobSpec->mutable_core_table_spec();
    auto* outputSpec = coreTableSpec->mutable_output_table_spec();
    outputSpec->set_table_writer_options(ConvertToYsonString(CoreTable_->TableWriterOptions).ToString());
    outputSpec->set_table_schema(SerializeToWireProto(CoreTable_->TableUploadOptions.TableSchema.Get()));
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->CoreTableChunkListId);

    auto writerConfig = GetCoreTableWriterConfig();
    YT_VERIFY(writerConfig);
    coreTableSpec->set_blob_table_writer_config(ConvertToYsonString(writerConfig).ToString());
}

i64 TOperationControllerBase::GetFinalOutputIOMemorySize(TJobIOConfigPtr ioConfig) const
{
    i64 result = 0;
    for (const auto& outputTable : OutputTables_) {
        if (outputTable->TableWriterOptions->ErasureCodec == NErasure::ECodec::None) {
            i64 maxBufferSize = std::max(
                ioConfig->TableWriter->MaxRowWeight,
                ioConfig->TableWriter->MaxBufferSize);
            result += GetOutputWindowMemorySize(ioConfig) + maxBufferSize;
        } else {
            auto* codec = NErasure::GetCodec(outputTable->TableWriterOptions->ErasureCodec);

            if (outputTable->TableWriterOptions->EnableStripedErasure) {
                // Table writer buffers.
                result += std::max(
                    ioConfig->TableWriter->MaxRowWeight,
                    ioConfig->TableWriter->MaxBufferSize);
                // Erasure writer buffer.
                result += ioConfig->TableWriter->ErasureWindowSize;
                // Encoding writer buffer.
                result += ioConfig->TableWriter->EncodeWindowSize;
                // Part writer buffers.
                result += ioConfig->TableWriter->SendWindowSize * codec->GetTotalPartCount();
            } else {
                double replicationFactor = (double) codec->GetTotalPartCount() / codec->GetDataPartCount();
                result += static_cast<i64>(ioConfig->TableWriter->DesiredChunkSize * replicationFactor);
            }
        }
    }
    return result;
}

i64 TOperationControllerBase::GetFinalIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatisticsVector& stripeStatistics) const
{
    i64 result = 0;
    for (const auto& stat : stripeStatistics) {
        result += GetInputIOMemorySize(ioConfig, stat);
    }
    result += GetFinalOutputIOMemorySize(ioConfig);
    return result;
}

void TOperationControllerBase::ValidateUserFileCount(TUserJobSpecPtr spec, const TString& operation)
{
    if (std::ssize(spec->FilePaths) > Config->MaxUserFileCount) {
        THROW_ERROR_EXCEPTION("Too many user files in %v: maximum allowed %v, actual %v",
            operation,
            Config->MaxUserFileCount,
            spec->FilePaths.size());
    }
}

void TOperationControllerBase::OnExecNodesUpdated()
{ }

int TOperationControllerBase::GetAvailableExecNodeCount()
{
    return AvailableExecNodeCount_;
}

const TExecNodeDescriptorMap& TOperationControllerBase::GetOnlineExecNodeDescriptors()
{
    return *OnlineExecNodesDescriptors_;
}

const TExecNodeDescriptorMap& TOperationControllerBase::GetExecNodeDescriptors()
{
    return *ExecNodesDescriptors_;
}

void TOperationControllerBase::UpdateExecNodes()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TSchedulingTagFilter filter(Spec_->SchedulingTagFilter);

    auto onlineExecNodeCount = Host->GetAvailableExecNodeCount();
    auto execNodeDescriptors = Host->GetExecNodeDescriptors(filter, /*onlineOnly*/ false);
    auto onlineExecNodeDescriptors = Host->GetExecNodeDescriptors(filter, /*onlineOnly*/ true);
    auto maxAvailableResources = Host->GetMaxAvailableResources(filter);

    const auto& controllerInvoker = CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default);
    controllerInvoker->Invoke(
        BIND([=, this, this_ = MakeWeak(this)] {
            auto strongThis = this_.Lock();
            if (!strongThis) {
                return;
            }

            AvailableExecNodeCount_ = onlineExecNodeCount;
            CachedMaxAvailableExecNodeResources_ = maxAvailableResources;

            auto assign = []<class T, class U>(T* variable, U value) {
                auto oldValue = *variable;
                *variable = std::move(value);

                // Offload old value destruction to a large thread pool.
                NRpc::TDispatcher::Get()->GetCompressionPoolInvoker()->Invoke(
                    BIND([value = std::move(oldValue)] { Y_UNUSED(value); }));
            };

            assign(&ExecNodesDescriptors_, std::move(execNodeDescriptors));
            assign(&OnlineExecNodesDescriptors_, std::move(onlineExecNodeDescriptors));

            OnExecNodesUpdated();

            YT_LOG_DEBUG("Exec nodes information updated (SuitableExecNodeCount: %v, OnlineExecNodeCount: %v)",
                ExecNodesDescriptors_->size(),
                AvailableExecNodeCount_);
        }));
}

bool TOperationControllerBase::ShouldSkipSanityCheck()
{
    if (GetAvailableExecNodeCount() < Config->SafeOnlineNodeCount) {
        return true;
    }

    if (TInstant::Now() < Host->GetConnectionTime() + Config->SafeSchedulerOnlineTime) {
        return true;
    }

    if (!CachedMaxAvailableExecNodeResources_) {
        return true;
    }

    if (TInstant::Now() < StartTime_ + Spec_->SanityCheckDelay) {
        return true;
    }

    return false;
}

void TOperationControllerBase::InferSchemaFromInput(const TSortColumns& sortColumns)
{
    // We infer schema only for operations with one output table.
    YT_VERIFY(OutputTables_.size() == 1);
    YT_VERIFY(InputTables_.size() >= 1);

    OutputTables_[0]->TableUploadOptions.SchemaMode = InputTables_[0]->SchemaMode;
    for (const auto& table : InputTables_) {
        if (table->SchemaMode != OutputTables_[0]->TableUploadOptions.SchemaMode) {
            THROW_ERROR_EXCEPTION("Cannot infer output schema from input, tables have different schema modes")
                << TErrorAttribute("input_table1_path", table->GetPath())
                << TErrorAttribute("input_table1_schema_mode", table->SchemaMode)
                << TErrorAttribute("input_table2_path", InputTables_[0]->GetPath())
                << TErrorAttribute("input_table2_schema_mode", InputTables_[0]->SchemaMode);
        }
    }

    auto replaceStableNamesWithNames = [] (const TTableSchemaPtr& schema) {
        auto newColumns = schema->Columns();
        for (auto& newColumn : newColumns) {
            newColumn.SetStableName(TColumnStableName(newColumn.Name()));
        }
        return New<TTableSchema>(std::move(newColumns), schema->GetStrict(), schema->IsUniqueKeys());
    };

    if (OutputTables_[0]->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
        OutputTables_[0]->TableUploadOptions.TableSchema = TTableSchema::FromSortColumns(sortColumns);
    } else {
        auto schema = replaceStableNamesWithNames(InputTables_[0]->Schema)
            ->ToStrippedColumnAttributes()
            ->ToCanonical();

        for (const auto& table : InputTables_) {
            auto canonizedSchema = replaceStableNamesWithNames(table->Schema)
                ->ToStrippedColumnAttributes()
                ->ToCanonical();
            if (*canonizedSchema != *schema) {
                THROW_ERROR_EXCEPTION(
                    "Cannot infer output schema from input in strong schema mode, "
                    "tables have incompatible schemas")
                    << TErrorAttribute("lhs_schema", InputTables_[0]->Schema)
                    << TErrorAttribute("rhs_schema", table->Schema);
            }
        }

        OutputTables_[0]->TableUploadOptions.TableSchema = replaceStableNamesWithNames(InputTables_[0]->Schema)
            ->ToSorted(sortColumns)
            ->ToSortedStrippedColumnAttributes()
            ->ToCanonical();

        if (InputTables_[0]->Schema->HasNontrivialSchemaModification()) {
            OutputTables_[0]->TableUploadOptions.TableSchema =
                OutputTables_[0]->TableUploadOptions.TableSchema->SetSchemaModification(
                    InputTables_[0]->Schema->GetSchemaModification());
        }
    }

    FilterOutputSchemaByInputColumnSelectors(sortColumns);
}

void TOperationControllerBase::InferSchemaFromInputOrdered()
{
    // We infer schema only for operations with one output table.
    YT_VERIFY(OutputTables_.size() == 1);
    YT_VERIFY(InputTables_.size() >= 1);

    auto& outputUploadOptions = OutputTables_[0]->TableUploadOptions;

    if (InputTables_.size() == 1 && outputUploadOptions.UpdateMode == EUpdateMode::Overwrite) {
        // If only only one input table given, we inherit the whole schema including column attributes.
        outputUploadOptions.SchemaMode = InputTables_[0]->SchemaMode;
        outputUploadOptions.TableSchema = InputTables_[0]->Schema;
        FilterOutputSchemaByInputColumnSelectors(/*sortColumns*/{});
        return;
    }

    InferSchemaFromInput();
}

void TOperationControllerBase::FilterOutputSchemaByInputColumnSelectors(const TSortColumns& sortColumns)
{
    THashSet<TString> selectedColumns;
    for (const auto& table : InputTables_) {
        if (auto selectors = table->Path.GetColumns()) {
            for (const auto& column : *selectors) {
                selectedColumns.insert(column);
            }
        } else {
            return;
        }
    }

    auto& outputSchema = OutputTables_[0]->TableUploadOptions.TableSchema;

    for (const auto& sortColumn : sortColumns) {
        if (!selectedColumns.contains(sortColumn.Name)) {
            THROW_ERROR_EXCEPTION("Sort column %Qv is discarded by input column selectors", sortColumn.Name)
                << TErrorAttribute("sort_columns", sortColumns)
                << TErrorAttribute("selected_columns", selectedColumns);
        }
    }

    outputSchema = outputSchema->Filter(selectedColumns);
}

void TOperationControllerBase::ValidateOutputSchemaOrdered() const
{
    YT_VERIFY(OutputTables_.size() == 1);
    YT_VERIFY(InputTables_.size() >= 1);

    if (InputTables_.size() > 1 && OutputTables_[0]->TableUploadOptions.TableSchema->IsSorted()) {
        THROW_ERROR_EXCEPTION("Cannot generate sorted output for ordered operation with multiple input tables")
            << TErrorAttribute("output_schema", *OutputTables_[0]->TableUploadOptions.TableSchema);
    }
}

void TOperationControllerBase::ValidateOutputSchemaCompatibility(bool ignoreSortOrder, bool validateComputedColumns) const
{
    YT_VERIFY(OutputTables_.size() == 1);

    auto hasComputedColumn = OutputTables_[0]->TableUploadOptions.TableSchema->HasComputedColumns();

    for (const auto& inputTable : InputTables_) {
        if (inputTable->SchemaMode == ETableSchemaMode::Strong) {
            const auto& [compatibility, error] = CheckTableSchemaCompatibility(
                *inputTable->Schema->Filter(inputTable->Path.GetColumns()),
                *OutputTables_[0]->TableUploadOptions.GetUploadSchema(),
                ignoreSortOrder);
            if (compatibility < ESchemaCompatibility::RequireValidation) {
                // NB for historical reasons we consider optional<T> to be compatible with T when T is simple
                // check is performed during operation.
                THROW_ERROR_EXCEPTION(error);
            }
        } else if (hasComputedColumn && validateComputedColumns) {
            // Input table has weak schema, so we cannot check if all
            // computed columns were already computed. At least this is weird.
            THROW_ERROR_EXCEPTION("Output table cannot have computed "
                "columns, which are not present in all input tables");
        }
    }
}

void TOperationControllerBase::ValidateSchemaInferenceMode(ESchemaInferenceMode schemaInferenceMode) const
{
    YT_VERIFY(OutputTables_.size() == 1);
    if (OutputTables_[0]->Dynamic && schemaInferenceMode != ESchemaInferenceMode::Auto) {
        THROW_ERROR_EXCEPTION("Only schema inference mode %Qv is allowed for dynamic table in output",
            ESchemaInferenceMode::Auto);
    }
}

void TOperationControllerBase::ValidateOutputSchemaComputedColumnsCompatibility() const
{
    YT_VERIFY(OutputTables_.size() == 1);

    if (!OutputTables_[0]->TableUploadOptions.TableSchema->HasComputedColumns()) {
        return;
    }

    for (const auto& inputTable : InputTables_) {
        if (inputTable->SchemaMode == ETableSchemaMode::Strong) {
            auto filteredInputTableSchema = inputTable->Schema->Filter(inputTable->Path.GetColumns());
            ValidateComputedColumnsCompatibility(
                *filteredInputTableSchema,
                *OutputTables_[0]->TableUploadOptions.TableSchema.Get())
                .ThrowOnError();
            ValidateComputedColumns(*filteredInputTableSchema, /*isTableDynamic*/ false);
        } else {
            THROW_ERROR_EXCEPTION("Schemas of input tables must be strict "
                "if output table has computed columns");
        }
    }
}

void TOperationControllerBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SnapshotIndex_);
    Persist(context, TotalEstimatedInputChunkCount);
    Persist(context, TotalEstimatedInputUncompressedDataSize);
    Persist(context, TotalEstimatedInputRowCount);
    Persist(context, TotalEstimatedInputValueCount);
    Persist(context, TotalEstimatedInputCompressedDataSize);
    Persist(context, TotalEstimatedInputDataWeight);
    Persist(context, UnavailableIntermediateChunkCount);
    Persist(context, InputNodeDirectory_);
    Persist(context, InputTables_);
    Persist(context, InputStreamDirectory_);
    Persist(context, OutputTables_);
    Persist(context, StderrTable_);
    Persist(context, CoreTable_);
    Persist(context, IntermediateTable);
    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, UserJobFiles_);
    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, LivePreviewChunks_);
    Persist(context, Tasks);
    Persist(context, InputChunkMap);
    Persist(context, IntermediateOutputCellTagList);
    Persist(context, CellTagToRequiredOutputChunkListCount_);
    Persist(context, CellTagToRequiredDebugChunkListCount_);
    if (context.IsSave()) {
        auto pendingJobCount = CachedPendingJobCount.Load();
        Persist(context, pendingJobCount);
    } else {
        TCompositePendingJobCount pendingJobCount;
        Persist(context, pendingJobCount);
        CachedPendingJobCount.Store(pendingJobCount);
    }
    Persist(context, CachedNeededResources);
    Persist(context, ChunkOriginMap);
    Persist(context, JobletMap);

    Persist(context, JobIndexGenerator);
    Persist(context, AggregatedFinishedJobStatistics_);
    Persist(context, ScheduleAllocationStatistics_);
    Persist(context, RowCountLimitTableIndex);
    Persist(context, RowCountLimit);
    Persist(context, EstimatedInputDataSizeHistogram_);
    Persist(context, InputDataSizeHistogram_);
    Persist(context, RetainedJobWithStderrCount_);
    Persist(context, RetainedJobsCoreInfoCount_);
    Persist(context, RetainedJobCount_);

    // COMPAT(pogorelov)
    if (context.GetVersion() < ESnapshotVersion::DoNotPersistJobReleaseFlags) {
        THashMap<TJobId, TReleaseJobFlags> jobIdToReleaseFlags;
        Persist(context, jobIdToReleaseFlags);
    }

    Persist(context, JobSpecCompletedArchiveCount_);
    Persist(context, FailedJobCount_);
    Persist(context, Sink_);
    Persist(context, AutoMergeTask_);
    Persist<TUniquePtrSerializer<>>(context, AutoMergeDirector_);
    Persist(context, DataFlowGraph_);
    // COMPAT(galtsev)
    if (context.GetVersion() >= ESnapshotVersion::NewLivePreview) {
        Persist(context, *LivePreviews_);
    }
    Persist(context, AvailableExecNodesObserved_);
    Persist(context, BannedNodeIds_);
    Persist(context, PathToOutputTable_);
    Persist(context, Acl);
    Persist(context, BannedTreeIds_);
    Persist(context, PathToInputTables_);
    Persist(context, JobMetricsDeltaPerTree_);
    Persist(context, TotalTimePerTree_);
    Persist(context, CompletedRowCount_);
    Persist(context, AutoMergeEnabled_);
    Persist(context, InputHasOrderedDynamicStores_);
    Persist(context, StandardStreamDescriptors_);
    Persist(context, MainResourceConsumptionPerTree_);
    Persist(context, EnableMasterResourceUsageAccounting_);
    Persist(context, AccountResourceUsageLeaseMap_);
    Persist(context, TotalJobCounter_);

    // NB: Keep this at the end of persist as it requires some of the previous
    // fields to be already initialized.
    if (context.IsLoad()) {
        InitUpdatingTables();
    }

    // COMPAT(gepardo)
    if (context.IsSave() && AutoMergeEnabled_.empty()) {
        AutoMergeEnabled_.resize(OutputTables_.size(), false);
    }

    Persist(context, AlertManager_);

    // COMPAT(galtsev)
    if (context.GetVersion() >= ESnapshotVersion::SwitchIntermediateMedium) {
        Persist(context, FastIntermediateMediumLimit_);
    }

    YT_VERIFY(context.GetVersion() >= ESnapshotVersion::JobExperiment);
    Persist(context, BaseLayer_);
    Persist(context, JobExperiment_);

    // COMPAT(eshcherbin)
    if (context.GetVersion() >= ESnapshotVersion::InitialMinNeededResources) {
        Persist(context, InitialMinNeededResources_);
    }

    if (context.IsLoad()) {
        ScheduleAllocationStatistics_->SetMovingAverageWindowSize(Config->ScheduleAllocationStatisticsMovingAverageWindowSize);
    }

    if (context.GetVersion() >= ESnapshotVersion::JobAbortsUntilOperationFailure) {
        Persist(context, JobAbortsUntilOperationFailure_);
    }
}

void TOperationControllerBase::ValidateRevivalAllowed() const
{
    if (Spec_->FailOnJobRestart) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Cannot revive operation when spec option \"fail_on_job_restart\" is set")
                << TErrorAttribute("operation_type", OperationType)
                << TErrorAttribute("reason", EFailOnJobRestartReason::RevivalIsForbidden);
    }
}

void TOperationControllerBase::ValidateSnapshot() const
{ }

std::vector<TUserJobSpecPtr> TOperationControllerBase::GetUserJobSpecs() const
{
    return {};
}

EIntermediateChunkUnstageMode TOperationControllerBase::GetIntermediateChunkUnstageMode() const
{
    return EIntermediateChunkUnstageMode::OnSnapshotCompleted;
}

TBlobTableWriterConfigPtr TOperationControllerBase::GetStderrTableWriterConfig() const
{
    return nullptr;
}

std::optional<TRichYPath> TOperationControllerBase::GetStderrTablePath() const
{
    return std::nullopt;
}

TBlobTableWriterConfigPtr TOperationControllerBase::GetCoreTableWriterConfig() const
{
    return nullptr;
}

std::optional<TRichYPath> TOperationControllerBase::GetCoreTablePath() const
{
    return std::nullopt;
}

bool TOperationControllerBase::GetEnableCudaGpuCoreDump() const
{
    return false;
}

void TOperationControllerBase::OnChunksReleased(int /*chunkCount*/)
{ }

TTableWriterOptionsPtr TOperationControllerBase::GetIntermediateTableWriterOptions() const
{
    auto options = New<NTableClient::TTableWriterOptions>();
    options->Account = Spec_->IntermediateDataAccount;
    options->ChunksVital = false;
    options->ChunksMovable = false;
    options->ReplicationFactor = Spec_->IntermediateDataReplicationFactor;
    options->MediumName = Spec_->IntermediateDataMediumName;
    options->CompressionCodec = Spec_->IntermediateCompressionCodec;
    // Distribute intermediate chunks uniformly across storage locations.
    options->PlacementId = GetOperationId().Underlying();
    // NB(levysotsky): Don't set table_index for intermediate streams
    // as we store table indices directly in rows of intermediate chunk.
    return options;
}

TOutputStreamDescriptorPtr TOperationControllerBase::GetIntermediateStreamDescriptorTemplate() const
{
    auto descriptor = New<TOutputStreamDescriptor>();
    descriptor->CellTags = IntermediateOutputCellTagList;

    descriptor->TableWriterOptions = GetIntermediateTableWriterOptions();
    if (Spec_->IntermediateDataAccount == NSecurityClient::IntermediateAccountName &&
        GetFastIntermediateMediumLimit() > 0)
    {
        descriptor->SlowMedium = descriptor->TableWriterOptions->MediumName;
        descriptor->TableWriterOptions->MediumName = Config->FastIntermediateMedium;
        YT_LOG_INFO("Fast intermediate medium enabled (FastMedium: %v, SlowMedium: %v, FastMediumLimit: %v)",
            descriptor->TableWriterOptions->MediumName,
            descriptor->SlowMedium,
            GetFastIntermediateMediumLimit());
    }

    descriptor->TableWriterConfig = BuildYsonStringFluently()
        .BeginMap()
            .Item("upload_replication_factor").Value(Spec_->IntermediateDataReplicationFactor)
            .Item("min_upload_replication_factor").Value(Spec_->IntermediateMinDataReplicationFactor)
            .Item("populate_cache").Value(true)
            .Item("sync_on_close").Value(Spec_->IntermediateDataSyncOnClose)
            .DoIf(Spec_->IntermediateDataReplicationFactor > 1, [&] (TFluentMap fluent) {
                // Set reduced rpc_timeout if replication_factor is greater than one.
                fluent.Item("node_rpc_timeout").Value(TDuration::Seconds(120));
            })
        .EndMap();

    descriptor->RequiresRecoveryInfo = true;
    return descriptor;
}

void TOperationControllerBase::ReleaseIntermediateStripeList(const NChunkPools::TChunkStripeListPtr& stripeList)
{
    switch (GetIntermediateChunkUnstageMode()) {
        case EIntermediateChunkUnstageMode::OnJobCompleted: {
            auto chunks = GetStripeListChunks(stripeList);
            AddChunksToUnstageList(std::move(chunks));
            OnChunksReleased(stripeList->TotalChunkCount);
            break;
        }
        case EIntermediateChunkUnstageMode::OnSnapshotCompleted: {
            IntermediateStripeListReleaseQueue_.Push(stripeList);
            break;
        }
        default:
            YT_ABORT();
    }
}

const TDataFlowGraphPtr& TOperationControllerBase::GetDataFlowGraph() const
{
    return DataFlowGraph_;
}

void TOperationControllerBase::TLivePreviewChunkDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, VertexDescriptor);
    Persist(context, LivePreviewIndex);
}

void TOperationControllerBase::TResourceUsageLeaseInfo::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, LeaseId);
    Persist(context, DiskQuota);
}

void TOperationControllerBase::RegisterLivePreviewChunk(
    const TDataFlowGraph::TVertexDescriptor& vertexDescriptor,
    int index,
    const TInputChunkPtr& chunk)
{
    YT_VERIFY(LivePreviewChunks_.emplace(
        chunk,
        TLivePreviewChunkDescriptor{vertexDescriptor, index}).second);

    DataFlowGraph_->RegisterLivePreviewChunk(vertexDescriptor, index, chunk);
}

const IThroughputThrottlerPtr& TOperationControllerBase::GetJobSpecSliceThrottler() const
{
    return Host->GetJobSpecSliceThrottler();
}

// TODO(gritukan): Should this method exist?
void TOperationControllerBase::FinishTaskInput(const TTaskPtr& task)
{
    task->FinishInput();
    task->RegisterInGraph(TDataFlowGraph::SourceDescriptor);
}

const std::vector<TTaskPtr>& TOperationControllerBase::GetTasks() const
{
    return Tasks;
}

void TOperationControllerBase::SetOperationAlert(EOperationAlertType alertType, const TError& alert)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(AlertsLock_);

    auto& existingAlert = Alerts_[alertType];
    if (alert.IsOK() && !existingAlert.IsOK()) {
        YT_LOG_DEBUG("Alert reset (Type: %v)",
            alertType);
    } else if (!alert.IsOK() && existingAlert.IsOK()) {
        YT_LOG_DEBUG(alert, "Alert set (Type: %v)",
            alertType);
    } else if (!alert.IsOK() && !existingAlert.IsOK()) {
        YT_LOG_DEBUG(alert, "Alert updated (Type: %v)",
            alertType);
    }

    Alerts_[alertType] = alert;
}

bool TOperationControllerBase::IsCompleted() const
{
    if (AutoMergeTask_ && !AutoMergeTask_->IsCompleted()) {
        return false;
    }
    return true;
}

TString TOperationControllerBase::WriteCoreDump() const
{
    // Save `this` explicitly to simplify debugging a core dump in GDB.
    auto this_ = this;
    DoNotOptimizeAway(this_);

    const auto& coreDumper = Host->GetCoreDumper();
    if (!coreDumper) {
        THROW_ERROR_EXCEPTION("Core dumper is not set up");
    }
    return coreDumper->WriteCoreDump(CoreNotes_, "rpc_call").Path;
}

void TOperationControllerBase::RegisterOutputRows(i64 count, int tableIndex)
{
    if (RowCountLimitTableIndex && *RowCountLimitTableIndex == tableIndex && !IsFinished()) {
        CompletedRowCount_ += count;
        if (CompletedRowCount_ >= RowCountLimit) {
            YT_LOG_INFO("Row count limit is reached (CompletedRowCount: %v, RowCountLimit: %v).",
                CompletedRowCount_,
                RowCountLimit);
            OnOperationCompleted(/*interrupted*/ true);
        }
    }
}

std::optional<int> TOperationControllerBase::GetRowCountLimitTableIndex()
{
    return RowCountLimitTableIndex;
}

void TOperationControllerBase::LoadSnapshot(const NYT::NControllerAgent::TOperationSnapshot& snapshot)
{
    DoLoadSnapshot(snapshot);
}

void TOperationControllerBase::RegisterOutputTables(const std::vector<TRichYPath>& outputTablePaths)
{
    std::vector<IPersistentChunkPoolInputPtr> sinks;
    sinks.reserve(outputTablePaths.size());
    for (const auto& outputTablePath : outputTablePaths) {
        auto it = PathToOutputTable_.find(outputTablePath.GetPath());
        if (it != PathToOutputTable_.end()) {
            const auto& lhsAttributes = it->second->Path.Attributes();
            const auto& rhsAttributes = outputTablePath.Attributes();
            if (lhsAttributes != rhsAttributes) {
                THROW_ERROR_EXCEPTION("Output table %v appears twice with different attributes", outputTablePath.GetPath())
                    << TErrorAttribute("lhs_attributes", lhsAttributes)
                    << TErrorAttribute("rhs_attributes", rhsAttributes);
            }
            continue;
        }
        auto table = New<TOutputTable>(outputTablePath, EOutputTableType::Output);
        table->TableIndex = OutputTables_.size();
        auto rowCountLimit = table->Path.GetRowCountLimit();
        if (rowCountLimit) {
            if (RowCountLimitTableIndex) {
                THROW_ERROR_EXCEPTION("Only one output table with row_count_limit is supported");
            }
            RowCountLimitTableIndex = table->TableIndex;
            RowCountLimit = *rowCountLimit;
        }

        sinks.emplace_back(New<TSink>(this, table->TableIndex));
        OutputTables_.emplace_back(table);
        PathToOutputTable_[outputTablePath.GetPath()] = table;
    }

    Sink_ = CreateMultiChunkPoolInput(std::move(sinks));
}

void TOperationControllerBase::DoAbortJob(
    TJobId jobId,
    EAbortReason abortReason,
    bool requestJobTrackerJobAbortion)
{
    // NB(renadeen): there must be no context switches before call OnJobAborted.

    if (!ShouldProcessJobEvents()) {
        YT_LOG_DEBUG(
            "Job events processing disabled, abort skipped (JobId: %v, OperationState: %v)",
            jobId,
            State.load());
        return;
    }

    if (requestJobTrackerJobAbortion) {
        Host->AbortJob(jobId, abortReason);
    }

    OnJobAborted(std::make_unique<TAbortedJobSummary>(jobId, abortReason));
}

void TOperationControllerBase::AbortJob(TJobId jobId, EAbortReason abortReason)
{
    if (!FindJoblet(jobId)) {
        YT_LOG_DEBUG(
            "Ignore stale job abort request (JobId: %v, AbortReason: %v)",
            jobId,
            abortReason);

        return;
    }

    YT_LOG_DEBUG(
        "Aborting job by controller request (JobId: %v, AbortReason: %v)",
        jobId,
        abortReason);

    DoAbortJob(jobId, abortReason, /*requestJobTrackerJobAbortion*/ true);
}

bool TOperationControllerBase::CanInterruptJobs() const
{
    return Config->EnableJobInterrupts && !InputHasOrderedDynamicStores_ && !InputHasStaticTableWithHunks_;
}

void TOperationControllerBase::InterruptJob(TJobId jobId, EInterruptReason reason)
{
    InterruptJob(
        jobId,
        reason,
        /*timeout*/ TDuration::Zero());
}

void TOperationControllerBase::HandleJobReport(const TJobletPtr& joblet, TControllerJobReport&& jobReport)
{
    Host->GetJobReporter()->HandleJobReport(
        jobReport
            .OperationId(OperationId)
            .JobId(joblet->JobId)
            .Address(joblet->NodeDescriptor.Address));
}

void TOperationControllerBase::OnCompetitiveJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitionType)
{
    ReportJobHasCompetitors(joblet, competitionType);
    // Original job could be finished and another speculative still running.
    if (auto originalJob = FindJoblet(joblet->CompetitionIds[competitionType])) {
        ReportJobHasCompetitors(originalJob, competitionType);
    }
}

void TOperationControllerBase::ReportJobHasCompetitors(const TJobletPtr& joblet, EJobCompetitionType competitionType)
{
    if (!joblet->HasCompetitors[competitionType]) {
        joblet->HasCompetitors[competitionType] = true;

        HandleJobReport(joblet, TControllerJobReport()
            .HasCompetitors(/*hasCompetitors*/ true, competitionType));
    }
}

void TOperationControllerBase::RegisterTestingSpeculativeJobIfNeeded(const TTaskPtr& task, TAllocationId allocationId)
{
    //! NB(arkady-e1ppa): we always have one joblet per allocation.
    auto joblet = GetOrCrash(JobletMap, allocationId);
    bool needLaunchSpeculativeJob;
    switch (Spec_->TestingOperationOptions->TestingSpeculativeLaunchMode) {
        case ETestingSpeculativeLaunchMode::None:
            needLaunchSpeculativeJob = false;
            break;
        case ETestingSpeculativeLaunchMode::Once:
            needLaunchSpeculativeJob = joblet->JobIndex == 0;
            break;
        case ETestingSpeculativeLaunchMode::Always:
            needLaunchSpeculativeJob = !joblet->CompetitionType;
            break;
        default:
            YT_ABORT();
    }
    if (needLaunchSpeculativeJob) {
        task->TryRegisterSpeculativeJob(joblet);
    }
}

std::vector<TRichYPath> TOperationControllerBase::GetLayerPaths(
    const NYT::NScheduler::TUserJobSpecPtr& userJobSpec) const
{
    if (!Config->TestingOptions->RootfsTestLayers.empty()) {
        return Config->TestingOptions->RootfsTestLayers;
    }
    std::vector<TRichYPath> layerPaths;
    if (userJobSpec->DockerImage) {
        TDockerImageSpec dockerImage(*userJobSpec->DockerImage, Config->DockerRegistry);

        // External docker images are not compatible with any additional layers.
        if (!dockerImage.IsInternal()) {
            return {};
        }

        // Resolve internal docker image into base layers.
        layerPaths = GetLayerPathsFromDockerImage(Host->GetClient(), dockerImage);
    }
    std::copy(userJobSpec->LayerPaths.begin(), userJobSpec->LayerPaths.end(), std::back_inserter(layerPaths));
    if (layerPaths.empty() && Spec_->DefaultBaseLayerPath) {
        layerPaths.insert(layerPaths.begin(), *Spec_->DefaultBaseLayerPath);
    }
    if (Config->DefaultLayerPath && layerPaths.empty()) {
        // If no layers were specified, we insert the default one.
        layerPaths.insert(layerPaths.begin(), *Config->DefaultLayerPath);
    }
    if (Config->CudaToolkitLayerDirectoryPath &&
        !layerPaths.empty() &&
        userJobSpec->CudaToolkitVersion &&
        userJobSpec->EnableGpuLayers)
    {
        // If cuda toolkit is requested, add the layer as the topmost user layer.
        auto path = *Config->CudaToolkitLayerDirectoryPath + "/" + *userJobSpec->CudaToolkitVersion;
        layerPaths.insert(layerPaths.begin(), path);
    }
    if (Config->GpuCheckLayerDirectoryPath &&
        userJobSpec->GpuCheckLayerName &&
        userJobSpec->GpuCheckBinaryPath &&
        !layerPaths.empty() &&
        userJobSpec->EnableGpuLayers)
    {
        // If cuda toolkit is requested, add the layer as the topmost user layer.
        auto path = *Config->GpuCheckLayerDirectoryPath + "/" + *userJobSpec->GpuCheckLayerName;
        layerPaths.insert(layerPaths.begin(), path);
    }
    if (userJobSpec->Profilers) {
        for (const auto& profilerSpec : *userJobSpec->Profilers) {
            auto cudaProfilerLayerPath = Spec_->CudaProfilerLayerPath
                ? Spec_->CudaProfilerLayerPath
                : Config->CudaProfilerLayerPath;

            if (cudaProfilerLayerPath && profilerSpec->Type == EProfilerType::Cuda) {
                layerPaths.insert(layerPaths.begin(), *cudaProfilerLayerPath);
                break;
            }
        }
    }
    if (!layerPaths.empty()) {
        auto systemLayerPath = userJobSpec->SystemLayerPath
            ? userJobSpec->SystemLayerPath
            : Config->SystemLayerPath;
        if (systemLayerPath) {
            // This must be the top layer, so insert in the beginning.
            layerPaths.insert(layerPaths.begin(), *systemLayerPath);
        }
    }
    return layerPaths;
}

void TOperationControllerBase::MaybeCancel(ECancelationStage cancelationStage)
{
    if (Spec_->TestingOperationOptions->CancelationStage &&
        cancelationStage == *Spec_->TestingOperationOptions->CancelationStage)
    {
        YT_LOG_INFO("Making test operation failure (CancelationStage: %v)", cancelationStage);
        GetInvoker()->Invoke(BIND(
            &TOperationControllerBase::DoFailOperation,
            MakeWeak(this),
            TError("Test operation failure"),
            /*flush*/ false,
            /*abortAllJoblets*/ false));
        YT_LOG_INFO("Making test cancelation (CancelationStage: %v)", cancelationStage);
        Cancel();
    }
}

const NChunkClient::TMediumDirectoryPtr& TOperationControllerBase::GetMediumDirectory() const
{
    return MediumDirectory_;
}

TJobSplitterConfigPtr TOperationControllerBase::GetJobSplitterConfigTemplate() const
{
    auto config = CloneYsonStruct(Options->JobSplitter);

    if (!Spec_->EnableJobSplitting || !Config->EnableJobSplitting) {
        config->EnableJobSplitting = false;
    }

    if (!Spec_->JobSplitter->EnableJobSplitting) {
        config->EnableJobSplitting = false;
    }
    if (!Spec_->JobSplitter->EnableJobSpeculation) {
        config->EnableJobSpeculation = false;
    }

    return config;
}

const TInputTablePtr& TOperationControllerBase::GetInputTable(int tableIndex) const
{
    return InputTables_[tableIndex];
}

const TOutputTablePtr& TOperationControllerBase::GetOutputTable(int tableIndex) const
{
    return OutputTables_[tableIndex];
}

int TOperationControllerBase::GetOutputTableCount() const
{
    return std::ssize(OutputTables_);
}

std::vector<TTaskPtr> TOperationControllerBase::GetTopologicallyOrderedTasks() const
{
    THashMap<TDataFlowGraph::TVertexDescriptor, int> vertexDescriptorToIndex;
    auto topologicalOrdering = DataFlowGraph_->GetTopologicalOrdering();
    for (int index = 0; index < std::ssize(topologicalOrdering); ++index) {
        YT_VERIFY(vertexDescriptorToIndex.emplace(topologicalOrdering[index], index).second);
    }

    std::vector<std::pair<int, TTaskPtr>> tasksWithIndices;
    tasksWithIndices.reserve(Tasks.size());
    for (const auto& task : Tasks) {
        for (const auto& vertex : task->GetAllVertexDescriptors()) {
            auto iterator = vertexDescriptorToIndex.find(vertex);
            if (iterator != vertexDescriptorToIndex.end()) {
                tasksWithIndices.emplace_back(iterator->second, task);
                break;
            }
        }
    }
    std::sort(tasksWithIndices.begin(), tasksWithIndices.end());

    std::vector<TTaskPtr> tasks;
    tasks.reserve(tasksWithIndices.size());
    for (auto& [index, task] : tasksWithIndices) {
        Y_UNUSED(index);
        tasks.push_back(std::move(task));
    }
    return tasks;
}

void TOperationControllerBase::AccountExternalScheduleAllocationFailures() const
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    for (const auto& reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
        auto count = ExternalScheduleAllocationFailureCounts_[reason].exchange(0);
        ScheduleAllocationStatistics_->Failed()[reason] += count;
    }
}

void TOperationControllerBase::UpdatePeakMemoryUsage()
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    auto memoryUsage = GetMemoryUsage();

    PeakMemoryUsage_ = std::max(memoryUsage, PeakMemoryUsage_);
}

void TOperationControllerBase::OnMemoryLimitExceeded(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    MemoryLimitExceeded_ = true;

    GetInvoker()->Invoke(BIND(
        &TOperationControllerBase::DoFailOperation,
        MakeWeak(this),
        error,
        /*flush*/ true,
        /*abortAllJoblets*/ true));
}

bool TOperationControllerBase::IsMemoryLimitExceeded() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MemoryLimitExceeded_;
}

void TOperationControllerBase::RegisterUnavailableInputChunks()
{
    UnavailableInputChunkIds.clear();
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap) {
        if (chunkDescriptor.State == EInputChunkState::Waiting) {
            RegisterUnavailableInputChunk(chunkId);
        }
    }
}

void TOperationControllerBase::RegisterUnavailableInputChunk(TChunkId chunkId)
{
    InsertOrCrash(UnavailableInputChunkIds, chunkId);

    YT_LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", chunkId);
}

void TOperationControllerBase::UnregisterUnavailableInputChunk(TChunkId chunkId)
{
    EraseOrCrash(UnavailableInputChunkIds, chunkId);

    YT_LOG_TRACE("Input chunk is no longer unavailable (ChunkId: %v)", chunkId);
}

void TOperationControllerBase::ReportJobCookieToArchive(const TJobletPtr& joblet)
{
    HandleJobReport(joblet, TControllerJobReport()
        .JobCookie(joblet->OutputCookie));
}

void TOperationControllerBase::ReportControllerStateToArchive(const TJobletPtr& joblet, EJobState state)
{
    HandleJobReport(joblet, TControllerJobReport()
        .ControllerState(state));
}

void TOperationControllerBase::SendRunningAllocationTimeStatisticsUpdates()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::JobEvents));

    std::vector<TAgentToSchedulerRunningAllocationStatistics> runningAllocationTimeStatisticsUpdates;
    runningAllocationTimeStatisticsUpdates.reserve(std::size(RunningAllocationPreemptibleProgressStartTimes_));

    for (auto [allocationId, preemptibleProgressStartTime] : RunningAllocationPreemptibleProgressStartTimes_) {
        runningAllocationTimeStatisticsUpdates.push_back({
            .AllocationId = allocationId,
            .PreemptibleProgressStartTime = preemptibleProgressStartTime});
    }

    if (std::empty(runningAllocationTimeStatisticsUpdates)) {
        YT_LOG_DEBUG("No running allocation statistics received since last sending");
        return;
    }

    YT_LOG_DEBUG("Send running allocation statistics updates (UpdateCount: %v)", std::size(runningAllocationTimeStatisticsUpdates));

    Host->UpdateRunningAllocationsStatistics(std::move(runningAllocationTimeStatisticsUpdates));
    RunningAllocationPreemptibleProgressStartTimes_.clear();
}

void TOperationControllerBase::RemoveRemainingJobsOnOperationFinished()
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    EAbortReason jobAbortReason = [this] {
        auto operationControllerState = State.load();

        switch (operationControllerState) {
            case EControllerState::Aborted:
                return EAbortReason::OperationAborted;
            case EControllerState::Completed:
                return EAbortReason::OperationCompleted;
            case EControllerState::Failed:
                return EAbortReason::OperationFailed;
            default:
                YT_LOG_DEBUG(
                    "Operation controller is not in finished state (State: %v)",
                    operationControllerState);
                return EAbortReason::OperationFailed;
        }
    }();

    // NB(pogorelov): We should not abort jobs honestly when operation is failed because invariants may be violated.
    // Also there is no meaning to abort jobs honestly when operation is finished.
    AbortAllJoblets(jobAbortReason, /*honestly*/ false);

    auto headCookie = CompletedJobIdsReleaseQueue_.Checkpoint();
    YT_LOG_INFO(
        "Releasing jobs on controller finish (HeadCookie: %v)",
        headCookie);
    auto jobIdsToRelease = CompletedJobIdsReleaseQueue_.Release();
    ReleaseJobs(jobIdsToRelease);
}

void TOperationControllerBase::OnOperationReady() const
{
    std::vector<TStartedJobInfo> revivedJobs;
    revivedJobs.reserve(std::size(JobletMap));

    for (const auto& [_, joblet] : JobletMap) {
        revivedJobs.push_back({joblet->JobId, joblet->NodeDescriptor.Address});
    }

    YT_LOG_DEBUG("Register operation in job controller (JobCount: %v)", std::size(revivedJobs));

    Host->ReviveJobs(std::move(revivedJobs));

    for (const auto& [_, joblet] : JobletMap) {
        Host->GetJobProfiler()->ProfileRevivedJob(*joblet);
    }
}

bool TOperationControllerBase::ShouldProcessJobEvents() const
{
    return State == EControllerState::Running || State == EControllerState::Failing;
}

void TOperationControllerBase::InterruptJob(TJobId jobId, EInterruptReason interruptionReason, TDuration timeout)
{
    Host->InterruptJob(jobId, interruptionReason, timeout);
}

std::unique_ptr<TAbortedJobSummary> TOperationControllerBase::RegisterOutputChunkReplicas(
    const TJobSummary& jobSummary,
    const NChunkClient::NProto::TChunkSpec& chunkSpec)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    const auto& globalNodeDirectory = Host->GetNodeDirectory();

    auto replicas = GetReplicasFromChunkSpec(chunkSpec);
    for (auto replica : replicas) {
        auto nodeId = replica.GetNodeId();
        if (InputNodeDirectory_->FindDescriptor(nodeId)) {
            continue;
        }

        const auto* descriptor = globalNodeDirectory->FindDescriptor(nodeId);
        if (!descriptor) {
            YT_LOG_DEBUG("Job is considered aborted since its output contains unresolved node id "
                "(JobId: %v, NodeId: %v)",
                jobSummary.Id,
                nodeId);
            return std::make_unique<TAbortedJobSummary>(jobSummary, EAbortReason::UnresolvedNodeId);
        }

        InputNodeDirectory_->AddDescriptor(nodeId, *descriptor);
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TOperationControllerBase::TCachedYsonCallback::TCachedYsonCallback(TDuration period, TCallback callback)
    : UpdatePeriod_(period)
    , Callback_(std::move(callback))
{ }

const NYson::TYsonString& TOperationControllerBase::TCachedYsonCallback::GetValue()
{
    auto now = GetInstant();
    if (UpdateTime_ + UpdatePeriod_ < now) {
        Value_ = Callback_();
        UpdateTime_ = now;
    }
    return Value_;
}

////////////////////////////////////////////////////////////////////////////////

int TOperationControllerBase::GetYsonNestingLevelLimit() const
{
    return Host
        ->GetClient()
        ->GetNativeConnection()
        ->GetConfig()
        ->CypressWriteYsonNestingLevelLimit;
}

template <typename T>
TYsonString TOperationControllerBase::ConvertToYsonStringNestingLimited(const T& value) const
{
    return NYson::ConvertToYsonStringNestingLimited(value, GetYsonNestingLevelLimit());
}

i64 TOperationControllerBase::GetFastIntermediateMediumLimit() const
{
    return FastIntermediateMediumLimit_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
