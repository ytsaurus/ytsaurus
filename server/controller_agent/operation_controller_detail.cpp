#include "operation_controller_detail.h"
#include "auto_merge_task.h"
#include "intermediate_chunk_scraper.h"
#include "job_info.h"
#include "job_helpers.h"
#include "counter_manager.h"
#include "task.h"
#include "operation.h"
#include "scheduling_context.h"
#include "config.h"

#include <yt/server/lib/job_agent/job_report.h>
#include <yt/server/lib/job_agent/job_reporter.h>

#include <yt/server/lib/misc/job_table_schema.h>

#include <yt/server/lib/scheduler/helpers.h>

#include <yt/server/lib/core_dump/helpers.h>

#include <yt/server/lib/chunk_pools/helpers.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_node/public.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/ytlib/chunk_client/chunk_teleporter.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/core_dump/proto/core_info.pb.h>

#include <yt/ytlib/event_log/event_log.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/range_inferrer.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/client/security_client/acl.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/action.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/column_rename_descriptor.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/table_consumer.h>

#include <yt/client/tablet_client/public.h>
#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/transaction_client/public.h>
#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/api/transaction.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/misc/algorithm_helpers.h>
#include <yt/core/misc/chunked_input_stream.h>
#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/numeric_helpers.h>

#include <yt/library/re2/re2.h>

#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/logging/log.h>

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_resolver.h>

#include <util/generic/cast.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYT::NControllerAgent {

using namespace NChunkPools;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NFormats;
using namespace NJobProxy;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NCoreDump::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NEventLog;
using namespace NLogging;
using namespace NYTAlloc;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

using NJobTrackerClient::NProto::TJobSpec;
using NNodeTrackerClient::TNodeId;
using NProfiling::CpuInstantToInstant;
using NProfiling::TCpuInstant;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::TTableReaderOptions;
using NScheduler::TExecNodeDescriptor;
using NScheduler::NProto::TSchedulerJobResultExt;
using NScheduler::NProto::TSchedulerJobSpecExt;
using NTabletNode::DefaultMaxOverlappingStoreCount;

using std::placeholders::_1;

////////////////////////////////////////////////////////////////////////////////

static class TJobHelper
{
public:
    TJobHelper()
    {
        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                StatisticsSuffixes_[state][type] = Format("/$/%lv/%lv", state, type);
            }
        }
    }

    const TString& GetStatisticsSuffix(EJobState state, EJobType type) const
    {
        return StatisticsSuffixes_[state][type];
    }

private:
    TEnumIndexedVector<EJobState, TEnumIndexedVector<EJobType, TString>> StatisticsSuffixes_;

} JobHelper;

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
    , StartTime(operation->GetStartTime())
    , AuthenticatedUser(operation->GetAuthenticatedUser())
    , SecureVault(operation->GetSecureVault())
    , UserTransactionId(operation->GetUserTransactionId())
    , Logger(TLogger(ControllerLogger)
        .AddTag("OperationId: %v", OperationId))
    , CoreNotes_({
        Format("OperationId: %v", OperationId)
    })
    , Acl(operation->GetAcl())
    , CancelableContext(New<TCancelableContext>())
    , DiagnosableInvokerPool(CreateFairShareInvokerPool(
        CreateMemoryTaggingInvoker(CreateSerializedInvoker(Host->GetControllerThreadPoolInvoker()), operation->GetMemoryTag()),
        TEnumTraits<EOperationControllerQueue>::DomainSize))
    , InvokerPool(DiagnosableInvokerPool)
    , SuspendableInvokerPool(TransformInvokerPool(InvokerPool, CreateSuspendableInvoker))
    , CancelableInvokerPool(TransformInvokerPool(
        SuspendableInvokerPool,
        BIND(&TCancelableContext::CreateInvoker, CancelableContext)))
    , RowBuffer(New<TRowBuffer>(TRowBufferTag(), Config->ControllerRowBufferChunkSize))
    , MemoryTag_(operation->GetMemoryTag())
    , PoolTreeControllerSettingsMap_(operation->PoolTreeControllerSettingsMap())
    , Spec_(std::move(spec))
    , Options(std::move(options))
    , SuspiciousJobsYsonUpdater_(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdateSuspiciousJobsYson, MakeWeak(this)),
        Config->SuspiciousJobs->UpdatePeriod))
    , ScheduleJobStatistics_(New<TScheduleJobStatistics>())
    , CheckTimeLimitExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckTimeLimit, MakeWeak(this)),
        Config->OperationTimeLimitCheckPeriod))
    , ExecNodesCheckExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckAvailableExecNodes, MakeWeak(this)),
        Config->AvailableExecNodesCheckPeriod))
    , AnalyzeOperationProgressExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::AnalyzeOperationProgress, MakeWeak(this)),
        Config->OperationProgressAnalysisPeriod))
    , MinNeededResourcesSanityCheckExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::CheckMinNeededResourcesSanity, MakeWeak(this)),
        Config->ResourceDemandSanityCheckPeriod))
    , MaxAvailableExecNodeResourcesUpdateExecutor(New<TPeriodicExecutor>(
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        BIND(&TThis::UpdateCachedMaxAvailableExecNodeResources, MakeWeak(this)),
        Config->MaxAvailableExecNodeResourcesUpdatePeriod))
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
{
    // Attach user transaction if any. Don't ping it.
    TTransactionAttachOptions userAttachOptions;
    userAttachOptions.Ping = false;
    userAttachOptions.PingAncestors = false;
    UserTransaction = UserTransactionId
        ? Host->GetClient()->AttachTransaction(UserTransactionId, userAttachOptions)
        : nullptr;

    YT_LOG_INFO("Operation controller instantiated (OperationType: %v, Address: %v)",
        OperationType,
        static_cast<void*>(this));
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

const TJobSpec& TOperationControllerBase::GetAutoMergeJobSpecTemplate(int tableIndex) const
{
    return AutoMergeJobSpecTemplates_[tableIndex];
}

void TOperationControllerBase::SleepInInitialize()
{
    if (auto delay = Spec_->TestingOperationOptions->DelayInsideInitialize) {
        TDelayedExecutor::WaitForDuration(*delay);
    }
}

void TOperationControllerBase::InitializeClients()
{
    TClientOptions options;
    options.PinnedUser = AuthenticatedUser;
    Client = Host
        ->GetClient()
        ->GetNativeConnection()
        ->CreateNativeClient(options);
    InputClient = Client;
    OutputClient = Client;
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
    for (auto transactionId : transactions.NestedInputIds) {
        nestedInputTransactions.push_back(attachTransaction(transactionId, InputClient, true));
    }

    bool cleanStart = false;

    // Check transactions.
    {
        std::vector<std::pair<ITransactionPtr, TFuture<void>>> asyncCheckResults;

        auto checkTransaction = [&] (
            const ITransactionPtr& transaction,
            ETransactionType transactionType,
            TTransactionId transactionId)
        {
            if (cleanStart) {
                return;
            }

            if (!transaction) {
                cleanStart = true;
                YT_LOG_INFO("Operation transaction is missing, will use clean start "
                    "(TransactionType: %v, TransactionId: %v)",
                    transactionType,
                    transactionId);
                return;
            }

            asyncCheckResults.emplace_back(transaction, transaction->Ping());
        };

        // NB: Async transaction is not checked.
        if (IsTransactionNeeded(ETransactionType::Input)) {
            checkTransaction(inputTransaction, ETransactionType::Input, transactions.InputId);
            for (int index = 0; index < nestedInputTransactions.size(); ++index) {
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
                cleanStart = true;
                YT_LOG_INFO(error,
                    "Error renewing operation transaction, will use clean start (TransactionId: %v)",
                    transaction->GetId());
            }
        }
    }

    // Downloading snapshot.
    if (!cleanStart) {
        auto snapshotOrError = WaitFor(Host->DownloadSnapshot());
        if (!snapshotOrError.IsOK()) {
            YT_LOG_INFO(snapshotOrError, "Failed to download snapshot, will use clean start");
            cleanStart = true;
        } else {
            YT_LOG_INFO("Snapshot successfully downloaded");
            Snapshot = snapshotOrError.Value();
        }
    }

    // Abort transactions if needed.
    {
        std::vector<TFuture<void>> asyncResults;

        auto scheduleAbort = [&] (const ITransactionPtr& transaction, const NNative::IClientPtr& client) {
            if (transaction) {
                // Transaction object may be in incorrect state, we need to abort using only transaction id.
                asyncResults.push_back(AttachTransaction(transaction->GetId(), client)->Abort());
            }
        };

        scheduleAbort(asyncTransaction, Client);
        scheduleAbort(outputCompletionTransaction, OutputClient);
        scheduleAbort(debugCompletionTransaction, Client);

        if (cleanStart) {
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

        WaitFor(Combine(asyncResults))
            .ThrowOnError();
    }


    if (cleanStart) {
        if (Spec_->FailOnJobRestart) {
            THROW_ERROR_EXCEPTION("Cannot use clean restart when spec option fail_on_job_restart is set");
        }

        YT_LOG_INFO("Using clean start instead of revive");

        Snapshot = TOperationSnapshot();
        Y_UNUSED(WaitFor(Host->RemoveSnapshot()));

        StartTransactions();
        InitializeStructures();

        LockInputs();
    }

    InitUnrecognizedSpec();

    WaitFor(Host->UpdateInitializedOperationNode())
        .ThrowOnError();

    SleepInInitialize();

    YT_LOG_INFO("Operation initialized");

    TOperationControllerInitializeResult result;
    FillInitializeResult(&result);
    return result;
}

TOperationControllerInitializeResult TOperationControllerBase::InitializeClean()
{
    YT_LOG_INFO("Initializing operation for clean start (Title: %v)",
        Spec_->Title);

    auto initializeAction = BIND([this_ = MakeStrong(this), this] () {
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

    WaitFor(Host->UpdateInitializedOperationNode())
        .ThrowOnError();

    YT_LOG_INFO("Operation initialized");

    TOperationControllerInitializeResult result;
    FillInitializeResult(&result);
    return result;
}

bool TOperationControllerBase::HasUserJobFiles() const
{
    for (const auto& userJobSpec : GetUserJobSpecs()) {
        if (!userJobSpec->FilePaths.empty() || !userJobSpec->LayerPaths.empty()) {
            return true;
        }
    }
    return false;
}

void TOperationControllerBase::InitOutputTables()
{
    for (const auto& path : GetOutputTablePaths()) {
        RegisterOutputTable(path);
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
    if (Spec_->TestingOperationOptions && Spec_->TestingOperationOptions->AllocationSize) {
        TestingAllocationVector_.resize(*Spec_->TestingOperationOptions->AllocationSize, 'a');
    }

    InputNodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();
    DataFlowGraph_ = New<TDataFlowGraph>(InputNodeDirectory_);
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
    }

    if (auto coreTablePath = GetCoreTablePath()) {
        CoreTable_ = New<TOutputTable>(*coreTablePath, EOutputTableType::Core);
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

    auto maxInputTableCount = std::min(Config->MaxInputTableCount, Options->MaxInputTableCount);
    if (InputTables_.size() > maxInputTableCount) {
        THROW_ERROR_EXCEPTION(
            "Too many input tables: maximum allowed %v, actual %v",
            Config->MaxInputTableCount,
            InputTables_.size());
    }

    DoInitialize();
}

void TOperationControllerBase::InitUnrecognizedSpec()
{
    UnrecognizedSpec_ = GetTypedSpec()->GetUnrecognizedRecursively();
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
    auto createService = [=] (auto fluentMethod) -> IYPathServicePtr {
        return IYPathService::FromProducer(BIND([fluentMethod = std::move(fluentMethod), weakThis = MakeWeak(this)] (IYsonConsumer* consumer) {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError, "Operation controller was destroyed");
                }
                BuildYsonFluently(consumer)
                    .Do(fluentMethod);
            }),
            Config->ControllerStaticOrchidUpdatePeriod
        );
    };

    // Methods like BuildProgress, BuildBriefProgress, buildJobsYson and BuildJobSplitterInfo build map fragment,
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

    auto createCachedMapService = [=] (auto fluentMethod) -> IYPathServicePtr {
        return createService(wrapWithMap(std::move(fluentMethod)))
            ->Via(InvokerPool->GetInvoker(EOperationControllerQueue::Default));
    };

    // NB: we may safely pass unretained this below as all the callbacks are wrapped with a createService helper
    // that takes care on checking the controller presence and properly replying in case it is already destroyed.
    auto service = New<TCompositeMapService>()
        ->AddChild("progress", createCachedMapService(BIND(&TOperationControllerBase::BuildProgress, Unretained(this))))
        ->AddChild("brief_progress", createCachedMapService(BIND(&TOperationControllerBase::BuildBriefProgress, Unretained(this))))
        ->AddChild("running_jobs", createCachedMapService(BIND(&TOperationControllerBase::BuildJobsYson, Unretained(this))))
        ->AddChild("retained_finished_jobs", createCachedMapService(BIND(&TOperationControllerBase::BuildRetainedFinishedJobsYson, Unretained(this))))
        ->AddChild("job_splitter", createCachedMapService(BIND(&TOperationControllerBase::BuildJobSplitterInfo, Unretained(this))))
        ->AddChild("memory_usage", createService(BIND(&TOperationControllerBase::BuildMemoryUsageYson, Unretained(this))))
        ->AddChild("state", createService(BIND(&TOperationControllerBase::BuildStateYson, Unretained(this))))
        ->AddChild("data_flow_graph", DataFlowGraph_->GetService()
            ->WithPermissionValidator(BIND(&TOperationControllerBase::ValidateIntermediateDataAccess, MakeWeak(this))))
        ->AddChild("testing", createService(BIND(&TOperationControllerBase::BuildTestingState, Unretained(this))));
    service->SetOpaque(false);
    Orchid_ = service
        ->Via(InvokerPool->GetInvoker(EOperationControllerQueue::Default));
}

void TOperationControllerBase::DoInitialize()
{ }

void TOperationControllerBase::LockInputs()
{
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

TOperationControllerPrepareResult TOperationControllerBase::SafePrepare()
{
    SleepInPrepare();

    // Testing purpose code.
    if (Config->EnableControllerFailureSpecOption &&
        Spec_->TestingOperationOptions &&
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
        GetUserObjectBasicAttributes(
            OutputClient,
            MakeUserObjectList(OutputTables_),
            OutputTransaction->GetId(),
            Logger,
            EPermission::Write);
    } else {
        YT_LOG_INFO("Operation has no output tables");
    }

    if (StderrTable_) {
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
        THashSet<TObjectId> updatingTableIds;
        for (const auto& table : UpdatingTables_) {
            const auto& path = table->GetPath();
            if (table->Type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    path,
                    EObjectType::Table,
                    table->Type);
            }
            const bool insertedNew = updatingTableIds.insert(table->ObjectId).second;
            if (!insertedNew) {
                THROW_ERROR_EXCEPTION("Output table %v is specified multiple times",
                    path);
            }
        }

        GetOutputTablesSchema();
        PrepareOutputTables();

        LockOutputTablesAndGetAttributes();
    }

    InitializeStandardEdgeDescriptors();

    YT_LOG_INFO("Operation prepared");

    TOperationControllerPrepareResult result;
    FillPrepareResult(&result);
    return result;
}

TOperationControllerMaterializeResult TOperationControllerBase::SafeMaterialize()
{
    TOperationControllerMaterializeResult result;

    try {
        FetchInputTables();
        FetchUserFiles();
        ValidateUserFileSizes();

        PickIntermediateDataCell();
        InitChunkListPools();

        SuppressLivePreviewIfNeeded();
        CreateLivePreviewTables();

        CollectTotals();

        CustomPrepare();

        InitializeHistograms();

        InitializeSecurityTags();

        YT_LOG_INFO("Tasks prepared (RowBufferCapacity: %v)", RowBuffer->GetCapacity());

        if (IsCompleted()) {
            // Possible reasons:
            // - All input chunks are unavailable && Strategy == Skip
            // - Merge decided to teleport all input chunks
            // - Anything else?
            YT_LOG_INFO("No jobs needed");
            OnOperationCompleted(false /* interrupted */);
            return result;
        } else {
            YT_VERIFY(UnavailableInputChunkCount == 0);
            for (const auto& [chunkId, chunkDescriptor] : InputChunkMap) {
                if (chunkDescriptor.State == EInputChunkState::Waiting) {
                    ++UnavailableInputChunkCount;
                }
            }

            if (UnavailableInputChunkCount > 0) {
                YT_LOG_INFO("Found unavailable input chunks during materialization (UnavailableInputChunkCount: %v)",
                    UnavailableInputChunkCount);
            }
        }

        AddAllTaskPendingHints();

        if (Config->TestingOptions->EnableSnapshotCycleAfterMaterialization) {
            TStringStream stringStream;
            SaveSnapshot(&stringStream);
            TOperationSnapshot snapshot;
            snapshot.Version = GetCurrentSnapshotVersion();
            snapshot.Blocks = {TSharedRef::FromString(stringStream.Str())};
            DoLoadSnapshot(snapshot);
        }

        // Input chunk scraper initialization should be the last step to avoid races,
        // because input chunk scraper works in control thread.
        InitInputChunkScraper();
        InitIntermediateChunkScraper();

        UpdateMinNeededJobResources();
        // NB(eshcherbin): This update is done to ensure that needed resources amount is computed.
        UpdateAllTasks();

        CheckTimeLimitExecutor->Start();
        ProgressBuildExecutor_->Start();
        ExecNodesCheckExecutor->Start();
        SuspiciousJobsYsonUpdater_->Start();
        AnalyzeOperationProgressExecutor->Start();
        MinNeededResourcesSanityCheckExecutor->Start();
        MaxAvailableExecNodeResourcesUpdateExecutor->Start();
        CheckTentativeTreeEligibilityExecutor_->Start();

        auto jobSplitterConfig = GetJobSplitterConfig();
        if (jobSplitterConfig) {
            JobSplitter_ = CreateJobSplitter(std::move(jobSplitterConfig), OperationId);
            YT_LOG_DEBUG("Job splitter created");
        }

        if (auto maybeDelay = Spec_->TestingOperationOptions->DelayInsideMaterialize) {
            TDelayedExecutor::WaitForDuration(*maybeDelay);
        }

        if (State != EControllerState::Preparing) {
            return result;
        }
        State = EControllerState::Running;

        LogProgress(/* force */ true);
    } catch (const std::exception& ex) {
        auto wrappedError = TError(EErrorCode::MaterializationFailed, "Materialization failed")
            << ex;
        YT_LOG_INFO(wrappedError);
        OnOperationFailed(wrappedError);
        return result;
    }

    result.Suspend = Spec_->SuspendOperationAfterMaterialization;
    result.InitialNeededResources = GetNeededResources();

    YT_LOG_INFO("Materialization finished");

    return result;
}

void TOperationControllerBase::SaveSnapshot(IOutputStream* output)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TSaveContext context;
    context.SetVersion(GetCurrentSnapshotVersion());
    context.SetOutput(output);

    Save(context, this);
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

    if (Snapshot.Blocks.empty()) {
        YT_LOG_INFO("Snapshot data is missing, preparing operation from scratch");
        TOperationControllerReviveResult result;
        result.RevivedFromSnapshot = false;
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
    result.RevivedBannedTreeIds = BannedTreeIds_;
    FillPrepareResult(&result);

    InitChunkListPools();

    SuppressLivePreviewIfNeeded();
    CreateLivePreviewTables();

    if (IsCompleted()) {
        OnOperationCompleted(/* interrupted */ false);
        return result;
    }

    AddAllTaskPendingHints();

    // Input chunk scraper initialization should be the last step to avoid races.
    InitInputChunkScraper();
    InitIntermediateChunkScraper();

    if (UnavailableIntermediateChunkCount > 0) {
        IntermediateChunkScraper->Start();
    }

    UpdateMinNeededJobResources();
    // NB(eshcherbin): This update is done to ensure that needed resources amount is computed.
    UpdateAllTasks();

    result.NeededResources = GetNeededResources();

    ReinstallLivePreview();

    if (!Config->EnableJobRevival) {
        AbortAllJoblets();
    }

    CheckTimeLimitExecutor->Start();
    ProgressBuildExecutor_->Start();
    ExecNodesCheckExecutor->Start();
    SuspiciousJobsYsonUpdater_->Start();
    AnalyzeOperationProgressExecutor->Start();
    MinNeededResourcesSanityCheckExecutor->Start();
    MaxAvailableExecNodeResourcesUpdateExecutor->Start();
    CheckTentativeTreeEligibilityExecutor_->Start();

    for (const auto& [jobId, joblet] : JobletMap) {
        result.RevivedJobs.push_back({
            joblet->JobId,
            joblet->JobType,
            joblet->StartTime,
            joblet->ResourceLimits,
            joblet->Task->IsJobInterruptible(),
            joblet->TreeId,
            joblet->NodeDescriptor.Id,
            joblet->NodeDescriptor.Address
        });
    }

    YT_LOG_INFO("Operation revived");

    State = EControllerState::Running;

    return result;
}

void TOperationControllerBase::AbortAllJoblets()
{
    for (const auto& [jobId, joblet] : JobletMap) {
        auto jobSummary = TAbortedJobSummary(jobId, EAbortReason::Scheduler);
        joblet->Task->OnJobAborted(joblet, jobSummary);
        if (JobSplitter_) {
            JobSplitter_->OnJobAborted(jobSummary);
        }
    }
    JobletMap.clear();
}

bool TOperationControllerBase::IsTransactionNeeded(ETransactionType type) const
{
    switch (type) {
        case ETransactionType::Async:
            return IsIntermediateLivePreviewSupported() || IsOutputLivePreviewSupported() || GetStderrTablePath();
        case ETransactionType::Input:
            return !GetInputTablePaths().empty() || HasUserJobFiles();
        case ETransactionType::Output:
        case ETransactionType::OutputCompletion:
            // NB: cannot replace with OutputTables_.empty() here because output tables are not ready yet.
            return !GetOutputTablePaths().empty();
        case ETransactionType::Debug:
        case ETransactionType::DebugCompletion:
            // TODO(max42): Re-think about this transaction when YT-8270 is done.
            return true;
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
    std::vector<TFuture<ITransactionPtr>> asyncResults = {
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

    auto results = WaitFor(CombineAll(asyncResults))
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

TInputStreamDirectory TOperationControllerBase::GetInputStreamDirectory() const
{
    std::vector<TInputStreamDescriptor> inputStreams;
    inputStreams.reserve(InputTables_.size());
    for (const auto& inputTable : InputTables_) {
        inputStreams.emplace_back(inputTable->Teleportable, inputTable->IsPrimary(), inputTable->Dynamic /* isVersioned */);
    }
    return TInputStreamDirectory(std::move(inputStreams));
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

TTaskGroupPtr TOperationControllerBase::GetAutoMergeTaskGroup() const
{
    return AutoMergeTaskGroup;
}

TAutoMergeDirector* TOperationControllerBase::GetAutoMergeDirector()
{
    return AutoMergeDirector_.get();
}

TFuture<ITransactionPtr> TOperationControllerBase::StartTransaction(
    ETransactionType type,
    const NNative::IClientPtr& client,
    TTransactionId parentTransactionId,
    TTransactionId prerequisiteTransactionId)
{
    if (!IsTransactionNeeded(type)) {
        YT_LOG_INFO("Skipping transaction as it is not needed (Type: %v)", type);
        return MakeFuture(ITransactionPtr());
    }

    YT_LOG_INFO("Starting transaction (Type: %v, ParentId: %v, PrerequisiteTransactionId: %v)",
        type,
        parentTransactionId,
        prerequisiteTransactionId);

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
    options.Attributes = std::move(attributes);
    options.ParentId = parentTransactionId;
    if (prerequisiteTransactionId) {
        options.PrerequisiteTransactionIds.push_back(prerequisiteTransactionId);
    }
    options.Timeout = Config->OperationTransactionTimeout;
    options.PingPeriod = Config->OperationTransactionPingPeriod;

    auto transactionFuture = client->StartTransaction(NTransactionClient::ETransactionType::Master, options);

    return transactionFuture.Apply(BIND([=] (const TErrorOr<ITransactionPtr>& transactionOrError){
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

void TOperationControllerBase::PickIntermediateDataCell()
{
    IntermediateOutputCellTag = PickChunkHostingCell(
        OutputClient->GetNativeConnection(),
        Logger);

    YT_LOG_DEBUG("Intermediate data cell picked (CellTag: %v)",
        IntermediateOutputCellTag);
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
        for (const auto& table : UpdatingTables_) {
            ++CellTagToRequiredOutputChunkListCount_[table->ExternalCellTag];
        }

        ++CellTagToRequiredOutputChunkListCount_[IntermediateOutputCellTag];
    }

    DebugChunkListPool_ = New<TChunkListPool>(
        Config,
        OutputClient,
        CancelableInvokerPool,
        OperationId,
        DebugTransaction->GetId());

    CellTagToRequiredDebugChunkListCount_.clear();
    if (StderrTable_) {
        ++CellTagToRequiredDebugChunkListCount_[StderrTable_->ExternalCellTag];
    }
    if (CoreTable_) {
        ++CellTagToRequiredDebugChunkListCount_[CoreTable_->ExternalCellTag];
    }
}

void TOperationControllerBase::InitInputChunkScraper()
{
    THashSet<TChunkId> chunkIds;
    for (const auto& [chunkId, chunkDescriptor] : InputChunkMap) {
        chunkIds.insert(chunkId);
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

    if (UnavailableInputChunkCount > 0) {
        YT_LOG_INFO("Waiting for unavailable input chunks (Count: %v)",
            UnavailableInputChunkCount);
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

bool TOperationControllerBase::TryInitAutoMerge(int outputChunkCountEstimate, double dataWeightRatio)
{
    InitAutoMergeJobSpecTemplates();

    AutoMergeTaskGroup = New<TTaskGroup>();
    AutoMergeTaskGroup->MinNeededResources.SetCpu(1);

    RegisterTaskGroup(AutoMergeTaskGroup);

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

    AutoMergeTasks.reserve(OutputTables_.size());
    i64 maxIntermediateChunkCount;
    i64 chunkCountPerMergeJob;
    switch (mode) {
        case EAutoMergeMode::Relaxed:
            maxIntermediateChunkCount = std::numeric_limits<int>::max();
            chunkCountPerMergeJob = 500;
            break;
        case EAutoMergeMode::Economy:
            maxIntermediateChunkCount = std::max(500, static_cast<int>(2.5 * sqrt(outputChunkCountEstimate)));
            chunkCountPerMergeJob = maxIntermediateChunkCount / 10;
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
    for (int index = 0; index < OutputTables_.size(); ++index) {
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
        OperationId);

    bool autoMergeEnabled = false;
    bool sortedOutputAutoMergeRequired = false;

    auto standardEdgeDescriptors = GetStandardEdgeDescriptors();
    for (int index = 0; index < OutputTables_.size(); ++index) {
        const auto& outputTable = OutputTables_[index];
        if (outputTable->Path.GetAutoMerge()) {
            if (outputTable->TableUploadOptions.TableSchema.IsSorted()) {
                sortedOutputAutoMergeRequired = true;
                AutoMergeTasks.emplace_back(nullptr);
            } else {
                auto edgeDescriptor = standardEdgeDescriptors[index];
                // Auto-merge jobs produce single output, so we override the table
                // index in writer options with 0.
                edgeDescriptor.TableWriterOptions = CloneYsonSerializable(edgeDescriptor.TableWriterOptions);
                edgeDescriptor.TableWriterOptions->TableIndex = 0;
                auto task = New<TAutoMergeTask>(
                    this /* taskHost */,
                    index,
                    chunkCountPerMergeJob,
                    autoMergeSpec->ChunkSizeThreshold,
                    dataWeightPerJob,
                    Spec_->MaxDataWeightPerJob,
                    edgeDescriptor);
                RegisterTask(task);
                AutoMergeTasks.emplace_back(std::move(task));
                autoMergeEnabled = true;
            }
        } else {
            AutoMergeTasks.emplace_back(nullptr);
        }
    }

    if (sortedOutputAutoMergeRequired && !autoMergeEnabled) {
        auto error = TError("Sorted output with auto merge is not supported for now, it will be done in YT-8024");
        SetOperationAlert(EOperationAlertType::AutoMergeDisabled, error);
    }

    return autoMergeEnabled;
}

std::vector<TEdgeDescriptor> TOperationControllerBase::GetAutoMergeEdgeDescriptors()
{
    auto edgeDescriptors = GetStandardEdgeDescriptors();
    YT_VERIFY(GetAutoMergeDirector());
    YT_VERIFY(AutoMergeTasks.size() == edgeDescriptors.size());
    for (int index = 0; index < edgeDescriptors.size(); ++index) {
        if (AutoMergeTasks[index]) {
            edgeDescriptors[index].DestinationPool = AutoMergeTasks[index]->GetChunkPoolInput();
            edgeDescriptors[index].ChunkMapping = AutoMergeTasks[index]->GetChunkMapping();
            edgeDescriptors[index].ImmediatelyUnstageChunkLists = true;
            edgeDescriptors[index].RequiresRecoveryInfo = true;
            edgeDescriptors[index].IsFinalOutput = false;
        }
    }
    return edgeDescriptors;
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
    if (IsOutputLivePreviewSupported()) {
        for (const auto& table : OutputTables_) {
            std::vector<TChunkTreeId> childIds;
            childIds.reserve(table->OutputChunkTreeIds.size());
            for (const auto& pair : table->OutputChunkTreeIds) {
                childIds.push_back(pair.second);
            }
            Host->AttachChunkTreesToLivePreview(
                AsyncTransaction->GetId(),
                table->LivePreviewTableId,
                childIds);
        }
    }

    if (IsIntermediateLivePreviewSupported()) {
        std::vector<TChunkTreeId> childIds;
        childIds.reserve(ChunkOriginMap.size());
        for (const auto& [chunkId, job] : ChunkOriginMap) {
            if (!job->Suspended) {
                childIds.push_back(chunkId);
            }
        }
        Host->AttachChunkTreesToLivePreview(
            AsyncTransaction->GetId(),
            IntermediateTable->LivePreviewTableId,
            childIds);
    }
}

void TOperationControllerBase::DoLoadSnapshot(const TOperationSnapshot& snapshot)
{
    YT_LOG_INFO("Started loading snapshot (Size: %v, BlockCount: %v, Version: %v)",
        GetByteSize(snapshot.Blocks),
        snapshot.Blocks.size(),
        snapshot.Version);

    // Snapshot loading must be synchronous.
    TOneShotContextSwitchGuard guard(
        BIND([this, this_ = MakeStrong(this)] {
            TStringBuilder stackTrace;
            DumpStackTrace([&stackTrace] (const char* buffer, int length) {
                stackTrace.AppendString(TStringBuf(buffer, length));
            });
            YT_LOG_WARNING("Context switch while loading snapshot (StackTrace: %v)",
                stackTrace.Flush());
        })
    );

    TChunkedInputStream input(snapshot.Blocks);

    TLoadContext context;
    context.SetInput(&input);
    context.SetRowBuffer(RowBuffer);
    context.SetVersion(snapshot.Version);

    NPhoenix::TSerializer::InplaceLoad(context, this);

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
        OutputTransaction->GetId(),
        Host->GetIncarnationId()))
        .ValueOrThrow();

    // Set transaction id to Cypress.
    {
        const auto& client = Host->GetClient();
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto path = GetOperationPath(OperationId) + "/@output_completion_transaction_id";
        auto req = TYPathProxy::Set(path);
        req->set_value(ConvertToYsonString(OutputCompletionTransaction->GetId()).GetData());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }
}

void TOperationControllerBase::CommitOutputCompletionTransaction()
{
    // Set committed flag.
    {
        const auto& client = Host->GetClient();
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto path = GetOperationPath(OperationId) + "/@committed";
        auto req = TYPathProxy::Set(path);
        SetTransactionId(req, OutputCompletionTransaction ? OutputCompletionTransaction->GetId() : NullTransactionId);
        req->set_value(ConvertToYsonString(true).GetData());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }

    if (OutputCompletionTransaction) {
        WaitFor(OutputCompletionTransaction->Commit())
            .ThrowOnError();
        OutputCompletionTransaction.Reset();
    }

    CommitFinished = true;
}

void TOperationControllerBase::StartDebugCompletionTransaction()
{
    if (!DebugTransaction) {
        return;
    }

    DebugCompletionTransaction = WaitFor(StartTransaction(
        ETransactionType::DebugCompletion,
        OutputClient,
        DebugTransaction->GetId(),
        Host->GetIncarnationId()))
        .ValueOrThrow();

    // Set transaction id to Cypress.
    {
        const auto& client = Host->GetClient();
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto path = GetOperationPath(OperationId) + "/@debug_completion_transaction_id";
        auto req = TYPathProxy::Set(path);
        req->set_value(ConvertToYsonString(DebugCompletionTransaction->GetId()).GetData());
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }
}

void TOperationControllerBase::CommitDebugCompletionTransaction()
{
    if (!DebugTransaction) {
        return;
    }

    WaitFor(DebugCompletionTransaction->Commit())
        .ThrowOnError();
    DebugCompletionTransaction.Reset();
}

void TOperationControllerBase::SleepInCommitStage(EDelayInsideOperationCommitStage desiredStage)
{
    auto delay = Spec_->TestingOperationOptions->DelayInsideOperationCommit;
    auto stage = Spec_->TestingOperationOptions->DelayInsideOperationCommitStage;
    auto skipOnSecondEntrance = Spec_->TestingOperationOptions->NoDelayOnSecondEntranceToCommit;

    {
        const auto& client = Host->GetClient();
        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto path = GetOperationPath(OperationId) + "/@testing";
        auto req = TYPathProxy::Get(path);
        auto rspOrError = WaitFor(proxy.Execute(req));
        if (rspOrError.IsOK()) {
            auto rspNode = ConvertToNode(NYson::TYsonString(rspOrError.ValueOrThrow()->value()));
            CommitSleepStarted_ = rspNode->AsMap()->GetChild("commit_sleep_started")->GetValue<bool>();
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

void TOperationControllerBase::SafeCommit()
{
    SleepInCommitStage(EDelayInsideOperationCommitStage::Start);

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
    if (Spec_->Atomicity == EAtomicity::None) {
        return;
    }

    THashMap<TCellTag, std::vector<TOutputTablePtr>> externalCellTagToTables;
    for (const auto& table : UpdatingTables_) {
        if (table->Dynamic) {
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
        auto channel = OutputClient->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            externalCellTag);
        TObjectServiceProxy proxy(channel);
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

    auto combinedResultOrError = WaitFor(CombineAll(asyncResults));
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

        for (const auto& [externalCellTag, tables]  : externalCellTagToTables) {
            auto channel = OutputClient->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                externalCellTag);
            TObjectServiceProxy proxy(channel);
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

        auto combinedResultOrError = WaitFor(CombineAll(asyncResults));
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

    WaitFor(Combine(commitFutures))
        .ThrowOnError();

    YT_LOG_INFO("Scheduler transactions committed");

    // Fire-and-forget.
    if (InputTransaction) {
        InputTransaction->Abort();
    }
    if (AsyncTransaction) {
        AsyncTransaction->Abort();
    }
    for (const auto& transaction : NestedInputTransactions) {
        transaction->Abort();
    }
}

void TOperationControllerBase::TeleportOutputChunks()
{
    if (OutputTables_.empty()) {
        return;
    }

    auto teleporter = New<TChunkTeleporter>(
        Config,
        OutputClient,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        OutputCompletionTransaction->GetId(),
        Logger);

    for (auto& table : OutputTables_) {
        for (const auto& pair : table->OutputChunkTreeIds) {
            const auto& id = pair.second;
            if (TypeFromId(id) == EObjectType::ChunkList)
                continue;
            teleporter->RegisterChunk(id, table->ExternalCellTag);
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

    std::stable_sort(
        table->OutputChunkTreeIds.begin(),
        table->OutputChunkTreeIds.end(),
        [&] (const auto& lhs, const auto& rhs) -> bool {
            auto lhsBoundaryKeys = lhs.first.AsBoundaryKeys();
            auto rhsBoundaryKeys = rhs.first.AsBoundaryKeys();
            auto minKeyResult = CompareRows(lhsBoundaryKeys.MinKey, rhsBoundaryKeys.MinKey);
            if (minKeyResult != 0) {
                return minKeyResult < 0;
            }
            return lhsBoundaryKeys.MaxKey < rhsBoundaryKeys.MaxKey;
        });

    if (!table->OutputChunkTreeIds.empty() && table->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
        int cmp = CompareRows(
            table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey,
            table->LastKey,
            table->TableUploadOptions.TableSchema.GetKeyColumnCount());

        if (cmp < 0) {
            THROW_ERROR_EXCEPTION("Output table %v is not sorted: job outputs overlap with original table",
                table->GetPath())
                << TErrorAttribute("table_max_key", table->LastKey)
                << TErrorAttribute("job_output_min_key", table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey);
        }

        if (cmp == 0 && table->TableWriterOptions->ValidateUniqueKeys) {
            THROW_ERROR_EXCEPTION("Output table %v contains duplicate keys: job outputs overlap with original table",
                table->GetPath())
                << TErrorAttribute("table_max_key", table->LastKey)
                << TErrorAttribute("job_output_min_key", table->OutputChunkTreeIds.begin()->first.AsBoundaryKeys().MinKey);
        }
    }

    for (auto current = table->OutputChunkTreeIds.begin(); current != table->OutputChunkTreeIds.end(); ++current) {
        auto next = current + 1;
        if (next != table->OutputChunkTreeIds.end()) {
            int cmp = CompareRows(next->first.AsBoundaryKeys().MinKey, current->first.AsBoundaryKeys().MaxKey);

            if (cmp < 0) {
                THROW_ERROR_EXCEPTION("Output table %v is not sorted: job outputs have overlapping key ranges",
                    table->GetPath())
                    << TErrorAttribute("current_range_max_key", current->first.AsBoundaryKeys().MaxKey)
                    << TErrorAttribute("next_range_min_key", next->first.AsBoundaryKeys().MinKey);
            }

            if (cmp == 0 && table->TableWriterOptions->ValidateUniqueKeys) {
                THROW_ERROR_EXCEPTION("Output table %v contains duplicate keys: job outputs have overlapping key ranges",
                    table->GetPath())
                    << TErrorAttribute("current_range_max_key", current->first.AsBoundaryKeys().MaxKey)
                    << TErrorAttribute("next_range_min_key", next->first.AsBoundaryKeys().MinKey);
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
        TChunkServiceProxy::TReqExecuteBatch::TAttachChunkTreesSubrequest* req = nullptr;
        TChunkServiceProxy::TReqExecuteBatchPtr batchReq;

        auto flushCurrentReq = [&] (bool requestStatistics) {
            if (req) {
                req->set_request_statistics(requestStatistics);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to output table %v",
                    path);

                const auto& batchRsp = batchRspOrError.Value();
                const auto& rsp = batchRsp->attach_chunk_trees_subresponses(0);
                if (requestStatistics) {
                    // NB: For a static table statistics are requested only once at the end.
                    // For a dynamic table statistics are requested for each tablet separately.
                    table->DataStatistics += rsp.statistics();
                }
            }

            req = nullptr;
            batchReq.Reset();
        };

        auto addChunkTree = [&] (TChunkTreeId chunkTreeId) {
            if (req && req->child_ids_size() >= Config->MaxChildrenPerAttachRequest) {
                // NB: No need for a statistics for an intermediate request.
                flushCurrentReq(false);
            }

            if (!req) {
                batchReq = proxy.ExecuteBatch();
                GenerateMutationId(batchReq);
                batchReq->set_suppress_upstream_sync(true);
                req = batchReq->add_attach_chunk_trees_subrequests();
                ToProto(req->mutable_parent_id(), table->OutputChunkListId);
                if (table->Dynamic) {
                    ToProto(req->mutable_transaction_id(), OutputTransaction->GetId());
                }
            }

            ToProto(req->add_child_ids(), chunkTreeId);
        };

        if (table->TableUploadOptions.TableSchema.IsSorted() && ShouldVerifySortedOutput()) {
            // Sorted output generated by user operation requires rearranging.

            if (!table->TableUploadOptions.PartiallySorted) {
                VerifySortedOutput(table);
            } else {
                YT_VERIFY(table->Dynamic);
            }

            if (!table->Dynamic) {
                for (auto& pair : table->OutputChunkTreeIds) {
                    addChunkTree(pair.second);
                }
            } else {
                std::vector<std::vector<TChunkTreeId>> tabletChunks(table->PivotKeys.size());
                std::vector<std::vector<NChunkClient::NProto::TChunkSpec>> tabletChunkSpecs(table->PivotKeys.size());
                for (const auto& chunk : table->OutputChunks) {
                    auto chunkId  = chunk->ChunkId();
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

                    for (int index = start; index < end; ++index) {
                        tabletChunks[index].push_back(chunkId);
                    }
                }

                for (int index = 0; index < tabletChunks.size(); ++index) {
                    table->OutputChunkListId = table->TabletChunkListIds[index];
                    for (auto& chunkTree : tabletChunks[index]) {
                        addChunkTree(chunkTree);
                    }
                    flushCurrentReq(true);
                }
            }
        } else if (auto outputOrder = GetOutputOrder()) {
            YT_LOG_DEBUG("Sorting output chunk tree ids according to a given output order (ChunkTreeCount: %v, Table: %v)",
                table->OutputChunkTreeIds.size(),
                path);
            std::vector<std::pair<TOutputOrder::TEntry, TChunkTreeId>> chunkTreeIds;
            for (auto& pair : table->OutputChunkTreeIds) {
                chunkTreeIds.emplace_back(std::move(pair.first.AsOutputOrderEntry()), pair.second);
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
            for (const auto& pair : table->OutputChunkTreeIds) {
                addChunkTree(pair.second);
            }
        }

        // NB: Don't forget to ask for the statistics in the last request.
        flushCurrentReq(true);

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
            table->TableUploadOptions.TableSchema);
    }

    {
        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [nativeCellTag, tables] : nativeCellTagToTables) {
            auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader, nativeCellTag);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                {
                    auto req = TTableYPathProxy::EndUpload(table->GetObjectIdPath());
                    SetTransactionId(req, table->UploadTransactionId);
                    GenerateMutationId(req);
                    *req->mutable_statistics() = table->DataStatistics;
                    ToProto(req->mutable_table_schema(), table->TableUploadOptions.TableSchema);
                    req->set_schema_mode(static_cast<int>(table->TableUploadOptions.SchemaMode));
                    req->set_optimize_for(static_cast<int>(table->TableUploadOptions.OptimizeFor));
                    req->set_compression_codec(static_cast<int>(table->TableUploadOptions.CompressionCodec));
                    req->set_erasure_codec(static_cast<int>(table->TableUploadOptions.ErasureCodec));
                    if (table->TableUploadOptions.SecurityTags) {
                        ToProto(req->mutable_security_tags()->mutable_items(), *table->TableUploadOptions.SecurityTags);
                    }
                    batchReq->AddRequest(req);
                }
                if (table->OutputType == EOutputTableType::Stderr || table->OutputType == EOutputTableType::Core) {
                    auto req = TYPathProxy::Set(table->GetObjectIdPath() + "/@part_size");
                    SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                    req->set_value(ConvertToYsonString(GetPartSize(table->OutputType)).GetData());
                    batchReq->AddRequest(req);
                }
                if (table->OutputType == EOutputTableType::Core) {
                    auto req = TYPathProxy::Set(table->GetObjectIdPath() + "/@sparse");
                    SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                    req->set_value(ConvertToYsonString(true).GetData());
                    batchReq->AddRequest(req);
                }
            }

            asyncResults.push_back(batchReq->Invoke());
        }

        auto checkError = [] (const auto& error) {
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error finishing upload to output tables");
        };

        auto result = WaitFor(Combine(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
        }
    }
}

void TOperationControllerBase::SafeOnJobStarted(std::unique_ptr<TStartedJobSummary> jobSummary)
{
    auto jobId = jobSummary->Id;

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job started, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Job started (JobId: %v)", jobId);

    auto joblet = GetJoblet(jobId);
    joblet->LastActivityTime = jobSummary->StartTime;

    LogEventFluently(ELogEventType::JobStarted)
        .Item("job_id").Value(jobId)
        .Item("operation_id").Value(OperationId)
        .Item("resource_limits").Value(joblet->ResourceLimits)
        .Item("node_address").Value(joblet->NodeDescriptor.Address)
        .Item("job_type").Value(joblet->JobType);

    LogProgress();
}

void TOperationControllerBase::UpdateMemoryDigests(const TJobletPtr& joblet, const TStatistics& statistics, bool resourceOverdraft)
{
    bool taskUpdateNeeded = false;

    auto userJobMaxMemoryUsage = FindNumericValue(statistics, "/user_job/max_memory");
    if (userJobMaxMemoryUsage) {
        auto* digest = joblet->Task->GetUserJobMemoryDigest();
        YT_VERIFY(digest);
        double actualFactor = static_cast<double>(*userJobMaxMemoryUsage) / joblet->EstimatedResourceUsage.GetUserJobMemory();
        if (resourceOverdraft) {
            // During resource overdraft actual max memory values may be outdated,
            // since statistics are updated periodically. To ensure that digest converge to large enough
            // values we introduce additional factor.
            actualFactor = std::max(actualFactor, *joblet->UserJobMemoryReserveFactor * Config->ResourceOverdraftFactor);
        }
        YT_LOG_TRACE("Adding sample to the job proxy memory digest (JobType: %v, Sample: %v, JobId: %v)",
            joblet->JobType,
            actualFactor,
            joblet->JobId);
        digest->AddSample(actualFactor);
        taskUpdateNeeded = true;
    }

    auto jobProxyMaxMemoryUsage = FindNumericValue(statistics, "/job_proxy/max_memory");
    if (jobProxyMaxMemoryUsage) {
        auto* digest = joblet->Task->GetJobProxyMemoryDigest();
        YT_VERIFY(digest);
        double actualFactor = static_cast<double>(*jobProxyMaxMemoryUsage) /
            (joblet->EstimatedResourceUsage.GetJobProxyMemory() + joblet->EstimatedResourceUsage.GetFootprintMemory());
        if (resourceOverdraft) {
            actualFactor = std::max(actualFactor, *joblet->JobProxyMemoryReserveFactor * Config->ResourceOverdraftFactor);
        }
        YT_LOG_TRACE("Adding sample to the user job memory digest (JobType: %v, Sample: %v, JobId: %v)",
            joblet->JobType,
            actualFactor,
            joblet->JobId);
        digest->AddSample(actualFactor);
        taskUpdateNeeded = true;
    }

    if (taskUpdateNeeded) {
        UpdateAllTasksIfNeeded();
    }
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

void TOperationControllerBase::UpdateActualHistogram(const TStatistics& statistics)
{
    if (InputDataSizeHistogram_) {
        auto dataWeight = FindNumericValue(statistics, "/data/input/data_weight");
        if (dataWeight && *dataWeight > 0) {
            InputDataSizeHistogram_->AddValue(*dataWeight);
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

void TOperationControllerBase::SafeOnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->JobEventsControllerQueue));

    auto jobId = jobSummary->Id;
    auto abandoned = jobSummary->Abandoned;

    // Testing purpose code.
    if (Config->EnableControllerFailureSpecOption && Spec_->TestingOperationOptions &&
        Spec_->TestingOperationOptions->ControllerFailure &&
        *Spec_->TestingOperationOptions->ControllerFailure == EControllerFailureType::ExceptionThrownInOnJobCompleted)
    {
        THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::TestingError, "Testing exception");
    }

    const auto& result = jobSummary->Result;

    const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    // Validate all node ids of the output chunks and populate the local node directory.
    // In case any id is not known, abort the job.
    const auto& globalNodeDirectory = Host->GetNodeDirectory();
    for (const auto& chunkSpec : schedulerResultExt.output_chunk_specs()) {
        auto replicas = FromProto<TChunkReplicaList>(chunkSpec.replicas());
        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            if (InputNodeDirectory_->FindDescriptor(nodeId)) {
                continue;
            }

            const auto* descriptor = globalNodeDirectory->FindDescriptor(nodeId);
            if (!descriptor) {
                YT_LOG_DEBUG("Job is considered aborted since its output contains unresolved node id "
                    "(JobId: %v, NodeId: %v)",
                    jobId,
                    nodeId);
                auto abortedJobSummary = std::make_unique<TAbortedJobSummary>(*jobSummary, EAbortReason::Other);
                OnJobAborted(std::move(abortedJobSummary), false /* byScheduler */);
                return;
            }

            InputNodeDirectory_->AddDescriptor(nodeId, *descriptor);
        }
    }

    auto joblet = GetJoblet(jobId);

    // Controller should abort job if its competitor has already completed.
    auto maybeAbortReason = joblet->Task->ShouldAbortJob(joblet);
    if (maybeAbortReason) {
        YT_LOG_DEBUG("Job is considered aborted since its competitor has already completed (JobId: %v)", jobId);
        OnJobAborted(std::make_unique<TAbortedJobSummary>(*jobSummary, *maybeAbortReason), /* byScheduler */ false);
        return;
    }

    // NB: We should not explicitly tell node to remove abandoned job because it may be still
    // running at the node.
    if (!abandoned) {
        CompletedJobIdsReleaseQueue_.Push(jobId);
    }

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job completed, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Job completed (JobId: %v)", jobId);

    if (jobSummary->InterruptReason != EInterruptReason::None) {
        ExtractInterruptDescriptor(*jobSummary);
    }

    ParseStatistics(jobSummary.get(), joblet->StartTime, joblet->StatisticsYson);

    const auto& statistics = *jobSummary->Statistics;

    UpdateMemoryDigests(joblet, statistics);
    UpdateActualHistogram(statistics);

    FinalizeJoblet(joblet, jobSummary.get());
    LogFinishedJobFluently(ELogEventType::JobCompleted, joblet, *jobSummary);

    UpdateJobStatistics(joblet, *jobSummary);
    UpdateJobMetrics(joblet, *jobSummary);

    if (jobSummary->InterruptReason != EInterruptReason::None) {
        jobSummary->SplitJobCount = EstimateSplitJobCount(*jobSummary, joblet);
        if (jobSummary->InterruptReason == EInterruptReason::JobSplit) {
            // If we interrupted job on our own decision, (from JobSplitter), we should at least try to split it into 2 pieces.
            // Otherwise, the whole splitting thing makes to sense.
            jobSummary->SplitJobCount = std::max(2, jobSummary->SplitJobCount);
        }
        YT_LOG_DEBUG("Job interrupted (JobId: %v, InterruptReason: %v, UnreadDataSliceCount: %v, SplitJobCount: %v)",
            jobSummary->Id,
            jobSummary->InterruptReason,
            jobSummary->UnreadInputDataSlices.size(),
            jobSummary->SplitJobCount);
    }
    auto taskResult = joblet->Task->OnJobCompleted(joblet, *jobSummary);
    for (const auto& treeId : taskResult.NewlyBannedTrees) {
        MaybeBanInTentativeTree(treeId);
    }

    if (JobSplitter_) {
        JobSplitter_->OnJobCompleted(*jobSummary);
    }

    if (!abandoned) {
        if ((JobSpecCompletedArchiveCount_ < Config->GuaranteedArchivedJobSpecCountPerOperation || jobSummary->ExecDuration.value_or(TDuration()) > Config->MinJobDurationToArchiveJobSpec) &&
           JobSpecCompletedArchiveCount_ < Config->MaxArchivedJobSpecCountPerOperation)
        {
            ++JobSpecCompletedArchiveCount_;
            jobSummary->ReleaseFlags.ArchiveJobSpec = true;
        }
    }

    // We want to know row count before moving jobSummary to ProcessFinishedJobResult.
    std::optional<i64> optionalRowCount;
    if (RowCountLimitTableIndex) {
        optionalRowCount = FindNumericValue(statistics, Format("/data/output/%v/row_count", *RowCountLimitTableIndex));
    }

    ProcessFinishedJobResult(std::move(jobSummary), /* requestJobNodeCreation */ false);

    UnregisterJoblet(joblet);

    UpdateTask(joblet->Task);

    LogProgress();

    if (IsCompleted()) {
        OnOperationCompleted(/* interrupted */ false);
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

void TOperationControllerBase::SafeOnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary)
{
    auto jobId = jobSummary->Id;
    const auto& result = jobSummary->Result;

    auto joblet = GetJoblet(jobId);
    if (Spec_->IgnoreJobFailuresAtBannedNodes && BannedNodeIds_.find(joblet->NodeDescriptor.Id) != BannedNodeIds_.end()) {
        YT_LOG_DEBUG("Job is considered aborted since it has failed at a banned node "
            "(JobId: %v, Address: %v)",
            jobId,
            joblet->NodeDescriptor.Address);
        auto abortedJobSummary = std::make_unique<TAbortedJobSummary>(*jobSummary, EAbortReason::NodeBanned);
        OnJobAborted(std::move(abortedJobSummary), false /* byScheduler */);
        return;
    }

    auto error = FromProto<TError>(result.error());

    ParseStatistics(jobSummary.get(), joblet->StartTime, joblet->StatisticsYson);

    FinalizeJoblet(joblet, jobSummary.get());
    LogFinishedJobFluently(ELogEventType::JobFailed, joblet, *jobSummary)
        .Item("error").Value(error);

    UpdateJobMetrics(joblet, *jobSummary);
    UpdateJobStatistics(joblet, *jobSummary);

    auto taskResult = joblet->Task->OnJobFailed(joblet, *jobSummary);
    for (const auto& treeId : taskResult.NewlyBannedTrees) {
        MaybeBanInTentativeTree(treeId);
    }

    if (JobSplitter_) {
        JobSplitter_->OnJobFailed(*jobSummary);
    }

    jobSummary->ReleaseFlags.ArchiveJobSpec = true;

    ProcessFinishedJobResult(std::move(jobSummary), /* requestJobNodeCreation */ true);

    UnregisterJoblet(joblet);

    auto finally = Finally(
        [&] () {
            ReleaseJobs({jobId});
        },
        /* noUncaughtExceptions */ true
    );

    // This failure case has highest priority for users. Therefore check must be performed as early as possible.
    if (Spec_->FailOnJobRestart) {
        OnOperationFailed(TError(NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Job failed; failing operation since \"fail_on_job_restart\" spec option is set")
            << TErrorAttribute("job_id", joblet->JobId)
            << error);
        return;
    }

    if (error.Attributes().Get<bool>("fatal", false)) {
        auto wrappedError = TError("Job failed with fatal error") << error;
        OnOperationFailed(wrappedError);
        return;
    }

    int failedJobCount = GetDataFlowGraph()->GetTotalJobCounter()->GetFailed();
    int maxFailedJobCount = Spec_->MaxFailedJobCount;
    if (failedJobCount >= maxFailedJobCount) {
        OnOperationFailed(
            TError(NScheduler::EErrorCode::MaxFailedJobsLimitExceeded, "Failed jobs limit exceeded")
                << TErrorAttribute("max_failed_job_count", maxFailedJobCount)
                << error
        );
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

    UpdateTask(joblet->Task);
    LogProgress();

    if (IsCompleted()) {
        OnOperationCompleted(/* interrupted */ false);
    }
}

void TOperationControllerBase::SafeOnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary, bool byScheduler)
{
    auto jobId = jobSummary->Id;
    auto abortReason = jobSummary->AbortReason;

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job aborted, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Job aborted (JobId: %v)", jobId);

    auto joblet = GetJoblet(jobId);

    ParseStatistics(jobSummary.get(), joblet->StartTime, joblet->StatisticsYson);
    const auto& statistics = *jobSummary->Statistics;

    if (abortReason == EAbortReason::ResourceOverdraft) {
        UpdateMemoryDigests(joblet, statistics, true /* resourceOverdraft */);
    }

    if (jobSummary->LogAndProfile) {
        FinalizeJoblet(joblet, jobSummary.get());
        auto fluent = LogFinishedJobFluently(ELogEventType::JobAborted, joblet, *jobSummary)
            .Item("reason").Value(abortReason);
        if (jobSummary->PreemptedFor) {
            fluent
                .Item("preempted_for").Value(jobSummary->PreemptedFor);
        }
        UpdateJobStatistics(joblet, *jobSummary);
    }

    UpdateJobMetrics(joblet, *jobSummary);

    if (abortReason == EAbortReason::FailedChunks) {
        const auto& result = jobSummary->Result;
        const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        for (auto chunkId : schedulerResultExt.failed_chunk_ids()) {
            OnChunkFailed(FromProto<TChunkId>(chunkId));
        }
    }

    auto taskResult = joblet->Task->OnJobAborted(joblet, *jobSummary);
    for (const auto& treeId : taskResult.NewlyBannedTrees) {
        MaybeBanInTentativeTree(treeId);
    }

    if (JobSplitter_) {
        JobSplitter_->OnJobAborted(*jobSummary);
    }

    bool requestJobNodeCreation = (abortReason == EAbortReason::UserRequest);
    ProcessFinishedJobResult(std::move(jobSummary), requestJobNodeCreation);

    UnregisterJoblet(joblet);

    // This failure case has highest priority for users. Therefore check must be performed as early as possible.
    if (Spec_->FailOnJobRestart &&
        !(abortReason > EAbortReason::SchedulingFirst && abortReason < EAbortReason::SchedulingLast))
    {
        OnOperationFailed(TError(
            NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Job aborted; failing operation since \"fail_on_job_restart\" spec option is set")
            << TErrorAttribute("job_id", joblet->JobId)
            << TErrorAttribute("abort_reason", abortReason));
    }

    if (abortReason == EAbortReason::AccountLimitExceeded) {
        Host->OnOperationSuspended(TError("Account limit exceeded"));
    }

    CheckFailedJobsStatusReceived();
    UpdateTask(joblet->Task);
    LogProgress();

    if (!byScheduler) {
        ReleaseJobs({jobId});
    }

    if (IsCompleted()) {
        OnOperationCompleted(/* interrupted */ false);
    }
}

void TOperationControllerBase::SafeOnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary)
{
    auto jobId = jobSummary->Id;

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale job running, ignored (JobId: %v)", jobId);
        return;
    }

    auto joblet = GetJoblet(jobSummary->Id);

    joblet->Progress = jobSummary->Progress;
    joblet->StderrSize = jobSummary->StderrSize;

    if (joblet->JobSpeculationTimeout &&
        jobSummary->PrepareDuration.value_or(TDuration()) + jobSummary->ExecDuration.value_or(TDuration()) >= joblet->JobSpeculationTimeout)
    {
        YT_LOG_DEBUG("Speculation timeout expired; trying to launch speculative job (ExpiredJobId: %v)", jobId);
        if (joblet->Task->TryRegisterSpeculativeJob(joblet)) {
            UpdateTask(joblet->Task);
        }
    }

    if (jobSummary->StatisticsYson) {
        joblet->StatisticsYson = jobSummary->StatisticsYson;
        ParseStatistics(jobSummary.get(), joblet->StartTime);

        UpdateJobMetrics(joblet, *jobSummary);

        if (JobSplitter_) {
            JobSplitter_->OnJobRunning(*jobSummary);
            if (GetPendingJobCount() == 0) {
                auto verdict = JobSplitter_->ExamineJob(jobId);
                if (verdict == EJobSplitterVerdict::Split) {
                    YT_LOG_DEBUG("Job is going to be split (JobId: %v)", jobId);
                    Host->InterruptJob(jobId, EInterruptReason::JobSplit);
                } else if (verdict == EJobSplitterVerdict::LaunchSpeculative) {
                    YT_LOG_DEBUG("Job can be speculated (JobId: %v)", jobId);
                    if (joblet->Task->TryRegisterSpeculativeJob(joblet)) {
                        UpdateTask(joblet->Task);
                    }
                }
            }
        }

        auto asyncResult = BIND(&BuildBriefStatistics, Passed(std::move(jobSummary)))
            .AsyncVia(Host->GetControllerThreadPoolInvoker())
            .Run();

        asyncResult.Subscribe(BIND(
            &TOperationControllerBase::AnalyzeBriefStatistics,
            MakeStrong(this),
            joblet,
            Config->SuspiciousJobs)
            .Via(GetCancelableInvoker()));
    }
}

void TOperationControllerBase::FinalizeJoblet(
    const TJobletPtr& joblet,
    TJobSummary* jobSummary)
{
    YT_VERIFY(jobSummary->Statistics);
    YT_VERIFY(jobSummary->FinishTime);

    auto& statistics = *jobSummary->Statistics;
    joblet->FinishTime = *jobSummary->FinishTime;

    if (joblet->JobProxyMemoryReserveFactor) {
        statistics.AddSample("/job_proxy/memory_reserve_factor_x10000", static_cast<int>(1e4 * *joblet->JobProxyMemoryReserveFactor));
    }
}

void TOperationControllerBase::BuildJobAttributes(
    const TJobInfoPtr& job,
    EJobState state,
    bool outputStatistics,
    i64 stderrSize,
    TFluentMap fluent) const
{
    static const auto EmptyMapYson = TYsonString("{}");

    fluent
        .Item("job_type").Value(job->JobType)
        .Item("state").Value(state)
        .Item("address").Value(job->NodeDescriptor.Address)
        .Item("start_time").Value(job->StartTime)
        .Item("account").Value(job->Account)
        .Item("progress").Value(job->Progress)

        // We use Int64 for `stderr_size' to be consistent with
        // compressed_data_size / uncompressed_data_size attributes.
        .Item("stderr_size").Value(stderrSize)
        .Item("brief_statistics")
            .Value(job->BriefStatistics)
        .DoIf(outputStatistics, [&] (TFluentMap fluent) {
            fluent.Item("statistics")
                .Value(job->StatisticsYson ? job->StatisticsYson : EmptyMapYson);
        })
        .Item("suspicious").Value(job->Suspicious)
        .Item("job_competition_id").Value(job->JobCompetitionId)
        .Item("has_competitors").Value(job->HasCompetitors);
}

void TOperationControllerBase::BuildFinishedJobAttributes(
    const TFinishedJobInfoPtr& job,
    bool outputStatistics,
    bool hasStderr,
    bool hasFailContext,
    TFluentMap fluent) const
{
    auto stderrSize = hasStderr
        // Report nonzero stderr size as we are sure it is saved.
        ? std::max(job->StderrSize, static_cast<i64>(1))
        : 0;

    i64 failContextSize = hasFailContext ? 1 : 0;

    BuildJobAttributes(job, job->Summary.State, outputStatistics, stderrSize, fluent);

    const auto& summary = job->Summary;
    fluent
        .Item("finish_time").Value(job->FinishTime)
        .DoIf(summary.State == EJobState::Failed, [&] (TFluentMap fluent) {
            auto error = FromProto<TError>(summary.Result.error());
            fluent.Item("error").Value(error);
        })
        .DoIf(summary.Result.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext),
            [&] (TFluentMap fluent)
        {
            const auto& schedulerResultExt = summary.Result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            fluent.Item("core_infos").Value(schedulerResultExt.core_infos());
        })
        .Item("fail_context_size").Value(failContextSize);
}

TFluentLogEvent TOperationControllerBase::LogFinishedJobFluently(
    ELogEventType eventType,
    const TJobletPtr& joblet,
    const TJobSummary& jobSummary)
{
    return LogEventFluently(eventType)
        .Item("job_id").Value(joblet->JobId)
        .Item("operation_id").Value(OperationId)
        .Item("start_time").Value(joblet->StartTime)
        .Item("finish_time").Value(joblet->FinishTime)
        .Item("resource_limits").Value(joblet->ResourceLimits)
        .Item("statistics").Value(jobSummary.Statistics)
        .Item("node_address").Value(joblet->NodeDescriptor.Address)
        .Item("job_type").Value(joblet->JobType)
        .Item("job_competition_id").Value(joblet->JobCompetitionId)
        .Item("has_competitors").Value(joblet->HasCompetitors);
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


void TOperationControllerBase::OnChunkFailed(TChunkId chunkId)
{
    if (chunkId == NullChunkId) {
        YT_LOG_WARNING("Incompatible unavailable chunk found; deprecated node version");
        return;
    }

    auto it = InputChunkMap.find(chunkId);
    if (it == InputChunkMap.end()) {
        YT_LOG_DEBUG("Intermediate chunk has failed (ChunkId: %v)", chunkId);
        if (!OnIntermediateChunkUnavailable(chunkId)) {
            return;
        }

        IntermediateChunkScraper->Start();
    } else {
        YT_LOG_DEBUG("Input chunk has failed (ChunkId: %v)", chunkId);
        OnInputChunkUnavailable(chunkId, &it->second);
    }
}

void TOperationControllerBase::SafeOnIntermediateChunkLocated(TChunkId chunkId, const TChunkReplicaList& replicas, bool missing)
{
    if (missing) {
        // We can unstage intermediate chunks (e.g. in automerge) - just skip them.
        return;
    }

    // Intermediate chunks are always replicated.
    if (IsUnavailable(replicas, NErasure::ECodec::None)) {
        OnIntermediateChunkUnavailable(chunkId);
    } else {
        OnIntermediateChunkAvailable(chunkId, replicas);
    }
}

void TOperationControllerBase::SafeOnInputChunkLocated(TChunkId chunkId, const TChunkReplicaList& replicas, bool missing)
{
    if (missing) {
        // We must have locked all the relevant input chunks, but when user transaction is aborted
        // there can be a race between operation completion and chunk scraper.
        OnOperationFailed(TError("Input chunk %v is missing", chunkId));
        return;
    }

    ++ChunkLocatedCallCount;
    if (ChunkLocatedCallCount >= Config->ChunkScraper->MaxChunksPerRequest) {
        ChunkLocatedCallCount = 0;
        YT_LOG_DEBUG("Located another batch of chunks (Count: %v, UnavailableInputChunkCount: %v)",
            Config->ChunkScraper->MaxChunksPerRequest,
            UnavailableInputChunkCount);
    }

    auto& descriptor = GetOrCrash(InputChunkMap, chunkId);
    YT_VERIFY(!descriptor.InputChunks.empty());
    auto& chunkSpec = descriptor.InputChunks.front();
    auto codecId = NErasure::ECodec(chunkSpec->GetErasureCodec());

    if (IsUnavailable(replicas, codecId, CheckParityReplicas())) {
        OnInputChunkUnavailable(chunkId, &descriptor);
    } else {
        OnInputChunkAvailable(chunkId, replicas, &descriptor);
    }
}

void TOperationControllerBase::OnInputChunkAvailable(
    TChunkId chunkId,
    const TChunkReplicaList& replicas,
    TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Waiting) {
        return;
    }

    YT_LOG_TRACE("Input chunk is available (ChunkId: %v)", chunkId);

    --UnavailableInputChunkCount;
    YT_VERIFY(UnavailableInputChunkCount >= 0);

    if (UnavailableInputChunkCount == 0) {
        InputChunkScraper->Stop();
    }

    // Update replicas in place for all input chunks with current chunkId.
    for (auto& chunkSpec : descriptor->InputChunks) {
        chunkSpec->SetReplicaList(replicas);
    }

    descriptor->State = EInputChunkState::Active;

    for (const auto& inputStripe : descriptor->InputStripes) {
        --inputStripe.Stripe->WaitingChunkCount;
        if (inputStripe.Stripe->WaitingChunkCount > 0)
            continue;

        auto task = inputStripe.Task;
        task->GetChunkPoolInput()->Resume(inputStripe.Cookie);
        if (task->HasInputLocality()) {
            AddTaskLocalityHint(inputStripe.Stripe, task);
        }
        AddTaskPendingHint(task);
    }
}

void TOperationControllerBase::OnInputChunkUnavailable(TChunkId chunkId, TInputChunkDescriptor* descriptor)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (descriptor->State != EInputChunkState::Active) {
        return;
    }

    YT_LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", chunkId);

    ++UnavailableInputChunkCount;

    switch (Spec_->UnavailableChunkTactics) {
        case EUnavailableChunkAction::Fail:
            OnOperationFailed(TError("Input chunk %v is unavailable",
                chunkId));
            break;

        case EUnavailableChunkAction::Skip: {
            descriptor->State = EInputChunkState::Skipped;
            for (const auto& inputStripe : descriptor->InputStripes) {
                inputStripe.Stripe->DataSlices.erase(
                    std::remove_if(
                        inputStripe.Stripe->DataSlices.begin(),
                        inputStripe.Stripe->DataSlices.end(),
                        [&] (TInputDataSlicePtr slice) {
                            try {
                                return chunkId == slice->GetSingleUnversionedChunkOrThrow()->ChunkId();
                            } catch (const std::exception& ex) {
                                //FIXME(savrus) allow data slices to be unavailable.
                                THROW_ERROR_EXCEPTION("Dynamic table chunk became unavailable") << ex;
                            }
                        }),
                    inputStripe.Stripe->DataSlices.end());

                // Store information that chunk disappeared in the chunk mapping.
                for (const auto& chunk : descriptor->InputChunks) {
                    inputStripe.Task->GetChunkMapping()->OnChunkDisappeared(chunk);
                }

                AddTaskPendingHint(inputStripe.Task);
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

    if (completedJob->Suspended)
        return false;

    YT_LOG_DEBUG("Job is lost (Address: %v, JobId: %v, SourceTask: %v, OutputCookie: %v, InputCookie: %v, UnavailableIntermediateChunkCount: %v)",
        completedJob->NodeDescriptor.Address,
        completedJob->JobId,
        completedJob->SourceTask->GetTitle(),
        completedJob->OutputCookie,
        completedJob->InputCookie,
        UnavailableIntermediateChunkCount);

    completedJob->Suspended = true;
    completedJob->DestinationPool->Suspend(completedJob->InputCookie);

    if (completedJob->Restartable) {
        completedJob->SourceTask->GetChunkPoolOutput()->Lost(completedJob->OutputCookie);
        completedJob->SourceTask->OnJobLost(completedJob);
        AddTaskPendingHint(completedJob->SourceTask);
    }

    return true;
}

void TOperationControllerBase::OnIntermediateChunkAvailable(TChunkId chunkId, const TChunkReplicaList& replicas)
{
    auto& completedJob = GetOrCrash(ChunkOriginMap, chunkId);

    if (completedJob->Restartable || !completedJob->Suspended) {
        // Job will either be restarted or all chunks are fine.
        return;
    }

    if (completedJob->UnavailableChunks.erase(chunkId) == 1) {
        for (auto& dataSlice : completedJob->InputStripe->DataSlices) {
            // Intermediate chunks are always unversioned.
            auto inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
            if (inputChunk->ChunkId() == chunkId) {
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

            // TODO (psushin).
            // Unfortunately we don't know what task we are resuming, so
            // add pending hints for all.
            AddAllTaskPendingHints();
        }
    }
}

bool TOperationControllerBase::AreForeignTablesSupported() const
{
    return false;
}

bool TOperationControllerBase::IsOutputLivePreviewSupported() const
{
    return !IsLegacyLivePreviewSuppressed &&
        (GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare ||
        GetLegacyOutputLivePreviewMode() == ELegacyLivePreviewMode::ExplicitlyEnabled);
}

bool TOperationControllerBase::IsIntermediateLivePreviewSupported() const
{
    return !IsLegacyLivePreviewSuppressed &&
        (GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::DoNotCare ||
        GetLegacyIntermediateLivePreviewMode() == ELegacyLivePreviewMode::ExplicitlyEnabled);
}

ELegacyLivePreviewMode TOperationControllerBase::GetLegacyOutputLivePreviewMode() const
{
    return ELegacyLivePreviewMode::NotSupported;
}

ELegacyLivePreviewMode TOperationControllerBase::GetLegacyIntermediateLivePreviewMode() const
{
    return ELegacyLivePreviewMode::NotSupported;
}

void TOperationControllerBase::OnTransactionsAborted(const std::vector<TTransactionId>& transactionIds)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    // Check if the user transaction is still alive to determine the exact abort reason.
    bool userTransactionAborted = false;
    if (UserTransaction) {
        auto result = WaitFor(UserTransaction->Ping());
        if (result.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
            userTransactionAborted = true;
        }
    }

    if (userTransactionAborted) {
        OnOperationAborted(
            GetUserTransactionAbortedError(UserTransaction->GetId()));
    } else {
        OnOperationFailed(
            GetSchedulerTransactionsAbortedError(transactionIds),
            /* flush */ false);
    }
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
    YT_LOG_INFO("Terminating operation controller");

    // NB: Errors ignored since we cannot do anything with it.
    Y_UNUSED(WaitFor(Host->FlushOperationNode()));

    // Skip committing anything if operation controller already tried to commit results.
    if (!CommitFinished) {
        std::vector<TOutputTablePtr> tables;
        if (StderrTable_ && StderrTable_->IsPrepared()) {
            tables.push_back(StderrTable_);
        }
        if (CoreTable_ && CoreTable_->IsPrepared()) {
            tables.push_back(CoreTable_);
        }

        if (!tables.empty()) {
            try {
                StartDebugCompletionTransaction();
                BeginUploadOutputTables(tables);
                AttachOutputChunks(tables);
                EndUploadOutputTables(tables);
                CommitDebugCompletionTransaction();

                if (DebugTransaction) {
                    WaitFor(DebugTransaction->Commit())
                        .ThrowOnError();
                }
            } catch (const std::exception& ex) {
                // Bad luck we can't commit transaction.
                // Such a pity can happen for example if somebody aborted our transaction manually.
                YT_LOG_ERROR(ex, "Failed to commit debug transaction");
                // Intentionally do not wait for abort.
                // Transaction object may be in incorrect state, we need to abort using only transaction id.
                AttachTransaction(DebugTransaction->GetId(), Client)->Abort();
            }
        }
    }

    std::vector<TFuture<void>> abortTransactionFutures;
    auto abortTransaction = [&] (const ITransactionPtr& transaction, const NNative::IClientPtr& client, bool sync = true) {
        if (transaction) {
            // Transaction object may be in incorrect state, we need to abort using only transaction id.
            auto asyncResult = AttachTransaction(transaction->GetId(), client)->Abort();
            if (sync) {
                abortTransactionFutures.push_back(asyncResult);
            }
        }
    };

    // NB: We do not abort input transaction synchronously since
    // it can belong to an unavailable remote cluster.
    // Moreover if input transaction abort failed it does not harm anything.
    abortTransaction(InputTransaction, InputClient, /* sync */ false);
    abortTransaction(OutputTransaction, OutputClient);
    abortTransaction(AsyncTransaction, Client, /* sync */ false);
    for (const auto& transaction : NestedInputTransactions) {
        abortTransaction(transaction, InputClient, /* sync */ false);
    }

    WaitFor(Combine(abortTransactionFutures))
        .ThrowOnError();

    YT_VERIFY(finalState == EControllerState::Aborted || finalState == EControllerState::Failed);
    State = finalState;

    LogProgress(/* force */ true);

    YT_LOG_INFO("Operation controller terminated");
}

void TOperationControllerBase::SafeComplete()
{
    OnOperationCompleted(true);
}

void TOperationControllerBase::CheckTimeLimit()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    auto timeLimit = GetTimeLimit();
    if (timeLimit) {
        if (TInstant::Now() - StartTime > *timeLimit) {
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

    TExecNodeDescriptor observedExecNode;
    bool foundMatching = false;
    bool foundMatchingNotBanned = false;
    int nonMatchingFilterNodeCount = 0;
    int matchingButInsufficientResourcesNodeCount = 0;
    for (const auto& nodePair : GetExecNodeDescriptors()) {
        const auto& descriptor = nodePair.second;

        bool hasSuitableTree = false;
        for (const auto& [treeName, settings] : PoolTreeControllerSettingsMap_) {
            if (descriptor.CanSchedule(settings.SchedulingTagFilter)) {
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
            if (task->GetPendingJobCount() == 0) {
                continue;
            }
            hasNonTrivialTasks = true;

            const auto& neededResources = task->GetMinNeededResources();
            if (Dominates(descriptor.ResourceLimits, neededResources.ToJobResources())) {
                hasEnoughResources = true;
                break;
            }
        }
        if (hasNonTrivialTasks && !hasEnoughResources) {
            ++matchingButInsufficientResourcesNodeCount;
            continue;
        }

        observedExecNode = descriptor;
        foundMatching = true;
        if (BannedNodeIds_.find(descriptor.Id) == BannedNodeIds_.end()) {
            foundMatchingNotBanned = true;
            // foundMatchingNotBanned also implies foundMatching, hence we interrupt.
            break;
        }
    }

    if (foundMatching) {
        AvailableExecNodesObserved_ = true;
    }

    if (!AvailableExecNodesObserved_) {
        OnOperationFailed(TError(
            EErrorCode::NoOnlineNodeToScheduleJob,
            "No online nodes that match operation scheduling tag filter %Qv "
            "and have sufficient resources to schedule a job found in trees %v",
            Spec_->SchedulingTagFilter.GetFormula(),
            GetKeys(PoolTreeControllerSettingsMap_))
            << TErrorAttribute("non_matching_filter_node_count", nonMatchingFilterNodeCount)
            << TErrorAttribute("matching_but_insufficient_resources_node_count", matchingButInsufficientResourcesNodeCount));
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
        OnOperationFailed(TError(errorMessage));
        return;
    }

    YT_LOG_DEBUG("Available exec nodes check succeeded (ObservedNodeAddress: %v)",
        observedExecNode.Address);
}

void TOperationControllerBase::AnalyzeTmpfsUsage()
{
    if (!Config->EnableTmpfs) {
        return;
    }

    THashMap<TString, std::vector<std::optional<i64>>> maximumUsedTmpfsSizesPerJobType;
    THashMap<TString, TUserJobSpecPtr> userJobSpecPerJobType;

    for (const auto& task : Tasks) {
        if (!task->IsSimpleTask()) {
            continue;
        }

        auto jobType = task->GetVertexDescriptor();
        const auto& userJobSpecPtr = task->GetUserJobSpec();
        if (!userJobSpecPtr) {
            continue;
        }

        std::optional<i64> maxMemoryUsage;
        for (const auto& jobState : { EJobState::Completed, EJobState::Failed }) {
            auto statistic = "/user_job/max_memory" + JobHelper.GetStatisticsSuffix(jobState, task->GetJobType());
            auto summary = FindSummary(JobStatistics, statistic);
            if (summary) {
                if (!maxMemoryUsage) {
                    maxMemoryUsage = 0;
                }
                *maxMemoryUsage = std::max(*maxMemoryUsage, summary->GetMax());
            }
        }

        if (maxMemoryUsage) {
            auto memoryUsageRatio = static_cast<double>(*maxMemoryUsage) / userJobSpecPtr->MemoryLimit;
            if (memoryUsageRatio > Config->OperationAlerts->TmpfsAlertMemoryUsageMuteRatio) {
                continue;
            }
        }

        userJobSpecPerJobType.insert(std::make_pair(jobType, userJobSpecPtr));

        auto maxUsedTmpfsSizes = task->GetMaximumUsedTmpfsSizes();

        YT_VERIFY(userJobSpecPtr->TmpfsVolumes.size() == maxUsedTmpfsSizes.size());

        auto it = maximumUsedTmpfsSizesPerJobType.find(jobType);
        if (it == maximumUsedTmpfsSizesPerJobType.end()) {
            it = maximumUsedTmpfsSizesPerJobType.emplace(jobType, std::vector<std::optional<i64>>(maxUsedTmpfsSizes.size())).first;
        }
        auto& knownMaxUsedTmpfsSizes = it->second;

        YT_VERIFY(knownMaxUsedTmpfsSizes.size() == maxUsedTmpfsSizes.size());

        for (int index = 0; index < maxUsedTmpfsSizes.size(); ++index) {
            auto tmpfsSize = maxUsedTmpfsSizes[index];
            if (tmpfsSize) {
                if (!knownMaxUsedTmpfsSizes[index]) {
                    knownMaxUsedTmpfsSizes[index] = 0;
                }
                knownMaxUsedTmpfsSizes[index] = std::max(*knownMaxUsedTmpfsSizes[index], *tmpfsSize);
            }
        }
    }

    std::vector<TError> innerErrors;

    double minUnusedSpaceRatio = 1.0 - Config->OperationAlerts->TmpfsAlertMaxUnusedSpaceRatio;

    for (const auto& [jobType, maxUsedTmpfsSizes] : maximumUsedTmpfsSizesPerJobType) {
        const auto& userJobSpecPtr = userJobSpecPerJobType[jobType];

        YT_VERIFY(userJobSpecPtr->TmpfsVolumes.size() == maxUsedTmpfsSizes.size());

        const auto& tmpfsVolumes = userJobSpecPtr->TmpfsVolumes;
        for (int index = 0; index < tmpfsVolumes.size(); ++index) {
            auto maxUsedTmpfsSize = maxUsedTmpfsSizes[index];
            if (!maxUsedTmpfsSize) {
                continue;
            }

            auto orderedTmpfsSize = tmpfsVolumes[index]->Size;
            bool minUnusedSpaceThresholdOvercome = orderedTmpfsSize - *maxUsedTmpfsSize >
                Config->OperationAlerts->TmpfsAlertMinUnusedSpaceThreshold;
            bool minUnusedSpaceRatioViolated = *maxUsedTmpfsSize <
                minUnusedSpaceRatio * orderedTmpfsSize;

            if (minUnusedSpaceThresholdOvercome && minUnusedSpaceRatioViolated) {
                auto error = TError(
                    "Jobs of type %Qlv use less than %.1f%% of requested tmpfs size in volume %Qv",
                    jobType,
                    minUnusedSpaceRatio * 100.0,
                    tmpfsVolumes[index]->Path)
                    << TErrorAttribute("max_used_tmpfs_size", *maxUsedTmpfsSize)
                    << TErrorAttribute("tmpfs_size", orderedTmpfsSize);
                innerErrors.push_back(error);
            }
        }
    }

    TError error;
    if (!innerErrors.empty()) {
        error = TError(
            "Operation has jobs that use less than %.1f%% of requested tmpfs size; "
            "consider specifying tmpfs size closer to actual usage",
            minUnusedSpaceRatio * 100.0) << innerErrors;
    }

    SetOperationAlert(EOperationAlertType::UnusedTmpfsSpace, error);
}

void TOperationControllerBase::AnalyzeInputStatistics()
{
    TError error;
    if (GetUnavailableInputChunkCount() > 0) {
        error = TError(
            "Some input chunks are not available; "
            "the relevant parts of computation will be suspended");
    }

    SetOperationAlert(EOperationAlertType::LostInputChunks, error);
}

void TOperationControllerBase::AnalyzeIntermediateJobsStatistics()
{
    TError error;
    if (GetDataFlowGraph()->GetTotalJobCounter()->GetLost() > 0) {
        error = TError(
            "Some intermediate outputs were lost and will be regenerated; "
            "operation will take longer than usual");
    }

    SetOperationAlert(EOperationAlertType::LostIntermediateChunks, error);
}

void TOperationControllerBase::AnalyzePartitionHistogram()
{ }

void TOperationControllerBase::AnalyzeAbortedJobs()
{
    if (OperationType == EOperationType::Vanilla) {
        return;
    }

    auto aggregateTimeForJobState = [&] (EJobState state) {
        i64 sum = 0;
        for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
            auto value = FindNumericValue(
                JobStatistics,
                Format("/time/total/$/%lv/%lv", state, type));

            if (value) {
                sum += *value;
            }
        }

        return sum;
    };

    i64 completedJobsTime = aggregateTimeForJobState(EJobState::Completed);
    i64 abortedJobsTime = aggregateTimeForJobState(EJobState::Aborted);
    double abortedJobsTimeRatio = 1.0;
    if (completedJobsTime > 0) {
        abortedJobsTimeRatio = 1.0 * abortedJobsTime / completedJobsTime;
    }

    TError error;
    if (abortedJobsTime > Config->OperationAlerts->AbortedJobsAlertMaxAbortedTime &&
        abortedJobsTimeRatio > Config->OperationAlerts->AbortedJobsAlertMaxAbortedTimeRatio)
    {
        error = TError(
            "Aborted jobs time ratio is too high, scheduling is likely to be inefficient; "
            "consider increasing job count to make individual jobs smaller")
                << TErrorAttribute("aborted_jobs_time_ratio", abortedJobsTimeRatio);
    }

    SetOperationAlert(EOperationAlertType::LongAbortedJobs, error);
}

void TOperationControllerBase::AnalyzeJobsIOUsage()
{
    std::vector<TError> innerErrors;

    for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
        auto value = FindNumericValue(
            JobStatistics,
            "/user_job/woodpecker/$/completed/" + FormatEnum(jobType));

        if (value && *value > 0) {
            innerErrors.emplace_back("Detected excessive disk IO in %Qlv jobs", jobType);
        }
    }

    TError error;
    if (!innerErrors.empty()) {
        error = TError("Detected excessive disk IO in jobs; consider optimizing disk usage")
            << innerErrors;
    }

    SetOperationAlert(EOperationAlertType::ExcessiveDiskUsage, error);
}

void TOperationControllerBase::AnalyzeJobsCpuUsage()
{
    auto getCpuLimit = [] (const TUserJobSpecPtr& jobSpec) {
        return jobSpec->CpuLimit;
    };

    auto needSetAlert = [&] (i64 totalExecutionTime, i64 jobCount, double ratio) {
        TDuration averageJobDuration = TDuration::MilliSeconds(totalExecutionTime / jobCount);
        TDuration totalExecutionDuration = TDuration::MilliSeconds(totalExecutionTime);

        return totalExecutionDuration > Config->OperationAlerts->LowCpuUsageAlertMinExecTime &&
               averageJobDuration > Config->OperationAlerts->LowCpuUsageAlertMinAverageJobTime &&
               ratio < Config->OperationAlerts->LowCpuUsageAlertCpuUsageThreshold;
    };

    const TString alertMessage =
        "Average CPU usage of some of your job types is significantly lower than requested 'cpu_limit'. "
        "Consider decreasing cpu_limit in spec of your operation";

    AnalyzeProcessingUnitUsage(
        Config->OperationAlerts->LowCpuUsageAlertStatistics,
        Config->OperationAlerts->LowCpuUsageAlertJobStates,
        getCpuLimit,
        needSetAlert,
        "cpu",
        EOperationAlertType::LowCpuUsage,
        alertMessage);
}

void TOperationControllerBase::AnalyzeJobsGpuUsage()
{
    if (TInstant::Now() - StartTime < Config->OperationAlerts->LowGpuUsageAlertMinDuration && !IsCompleted()) {
        return;
    }

    auto getGpuLimit = [] (const TUserJobSpecPtr& jobSpec) {
        return jobSpec->GpuLimit;
    };

    auto needSetAlert = [&] (i64 /*totalExecutionTime*/, i64 /*jobCount*/, double ratio) {
        return ratio < Config->OperationAlerts->LowGpuUsageAlertGpuUsageThreshold;
    };

    static const TString alertMessage =
        "Average gpu usage of some of your job types is significantly lower than requested 'gpu_limit'. "
        "Consider optimizing your GPU utilization";

    AnalyzeProcessingUnitUsage(
        Config->OperationAlerts->LowGpuUsageAlertStatistics,
        Config->OperationAlerts->LowGpuUsageAlertJobStates,
        getGpuLimit,
        needSetAlert,
        "gpu",
        EOperationAlertType::LowGpuUsage,
        alertMessage);
}

void TOperationControllerBase::AnalyzeJobsDuration()
{
    if (OperationType == EOperationType::RemoteCopy || OperationType == EOperationType::Erase) {
        return;
    }

    auto operationDuration = TInstant::Now() - StartTime;

    std::vector<TError> innerErrors;

    for (auto jobType : GetSupportedJobTypesForJobsDurationAnalyzer()) {
        auto completedJobsSummary = FindSummary(
            JobStatistics,
            "/time/total/$/completed/" + FormatEnum(jobType));

        if (!completedJobsSummary) {
            continue;
        }

        auto maxJobDuration = TDuration::MilliSeconds(completedJobsSummary->GetMax());
        auto completedJobCount = completedJobsSummary->GetCount();
        auto avgJobDuration = TDuration::MilliSeconds(completedJobsSummary->GetSum() / completedJobCount);

        if (completedJobCount > Config->OperationAlerts->ShortJobsAlertMinJobCount &&
            operationDuration > maxJobDuration * 2 &&
            avgJobDuration < Config->OperationAlerts->ShortJobsAlertMinJobDuration &&
            GetDataWeightParameterNameForJob(jobType))
        {
            auto error = TError(
                "Average duration of %Qlv jobs is less than %v seconds, try increasing %v in operation spec",
                jobType,
                Config->OperationAlerts->ShortJobsAlertMinJobDuration.Seconds(),
                GetDataWeightParameterNameForJob(jobType))
                    << TErrorAttribute("average_job_duration", avgJobDuration);

            innerErrors.push_back(error);
        }
    }

    TError error;
    if (!innerErrors.empty()) {
        error = TError(
            "Operation has jobs with duration is less than %v seconds, "
            "that leads to large overhead costs for scheduling",
            Config->OperationAlerts->ShortJobsAlertMinJobDuration.Seconds())
            << innerErrors;
    }

    SetOperationAlert(EOperationAlertType::ShortJobsDuration, error);
}

void TOperationControllerBase::AnalyzeOperationDuration()
{
    TError error;
    const auto& jobCounter = GetDataFlowGraph()->GetTotalJobCounter();
    for (const auto& task : Tasks) {
        if (!task->GetUserJobSpec()) {
            continue;
        }
        i64 completedAndRunning = jobCounter->GetCompletedTotal() + jobCounter->GetRunning();
        if (completedAndRunning == 0) {
            continue;
        }
        i64 pending = jobCounter->GetPending();
        TDuration wallTime = GetInstant() - StartTime;
        TDuration estimatedDuration = (wallTime / completedAndRunning) * pending;

        if (wallTime > Config->OperationAlerts->OperationTooLongAlertMinWallTime &&
            estimatedDuration > Config->OperationAlerts->OperationTooLongAlertEstimateDurationThreshold)
        {
            error = TError(
                "Estimated duration of this operation is about %v days; "
                "consider breaking operation into smaller ones",
                estimatedDuration.Days()) << TErrorAttribute("estimated_duration", estimatedDuration);
            break;
        }
    }

    SetOperationAlert(EOperationAlertType::OperationTooLong, error);
}

void TOperationControllerBase::AnalyzeScheduleJobStatistics()
{
    auto jobSpecThrottlerActivationCount = ScheduleJobStatistics_->Failed[EScheduleJobFailReason::JobSpecThrottling];
    auto activationCountThreshold = Config->OperationAlerts->JobSpecThrottlingAlertActivationCountThreshold;

    TError error;
    if (jobSpecThrottlerActivationCount > activationCountThreshold) {
        error = TError(
            "Excessive job spec throttling is detected. Usage ratio of operation can be "
            "significantly less than fair share ratio")
             << TErrorAttribute("job_spec_throttler_activation_count", jobSpecThrottlerActivationCount);
    }

    SetOperationAlert(EOperationAlertType::ExcessiveJobSpecThrottling, error);
}

void TOperationControllerBase::AnalyzeControllerQueues()
{
    TControllerQueueStatistics currentControllerQueueStatistics;
    THashMap<EOperationControllerQueue, TDuration> queueToAverageWaitTime;
    for (auto queue : TEnumTraits<EOperationControllerQueue>::GetDomainValues()) {
        auto statistics = DiagnosableInvokerPool->GetInvokerStatistics(queue);
        const auto& lastStatistics = LastControllerQueueStatistics_[queue];

        auto deltaEnqueuedActionCount = statistics.EnqueuedActionCount - lastStatistics.EnqueuedActionCount;
        auto deltaDequeuedActionCount = statistics.DequeuedActionCount - lastStatistics.DequeuedActionCount;

        YT_LOG_DEBUG(
            "Operation controller queue statistics (ControllerQueue: %v, DeltaEnqueuedActionCount: %v, "
            "DeltaDequeuedActionCount: %v, WaitingActionCount: %v, AverageWaitTime: %v)",
            queue,
            deltaEnqueuedActionCount,
            deltaDequeuedActionCount,
            statistics.WaitingActionCount,
            statistics.AverageWaitTime);

        if (statistics.AverageWaitTime > Config->OperationAlerts->QueueAverageWaitTimeThreshold) {
            queueToAverageWaitTime.emplace(queue, statistics.AverageWaitTime);
        }

        currentControllerQueueStatistics[queue] = std::move(statistics);
    }
    std::swap(LastControllerQueueStatistics_, currentControllerQueueStatistics);

    TError highQueueAverageWaitTimeError;
    if (!queueToAverageWaitTime.empty()) {
        highQueueAverageWaitTimeError = TError("Found action queues with high average wait time: %v",
            MakeFormattableView(queueToAverageWaitTime, [] (auto* builder, const auto& pair) {
                const auto& [queue, averageWaitTime] = pair;
                builder->AppendFormat("%Qlv", queue);
            }))
            << TErrorAttribute("queues_with_high_average_wait_time", queueToAverageWaitTime);
    }
    SetOperationAlert(EOperationAlertType::HighQueueAverageWaitTime, highQueueAverageWaitTimeError);
}

void TOperationControllerBase::AnalyzeOperationProgress()
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    AnalyzeTmpfsUsage();
    AnalyzeInputStatistics();
    AnalyzeIntermediateJobsStatistics();
    AnalyzePartitionHistogram();
    AnalyzeAbortedJobs();
    AnalyzeJobsIOUsage();
    AnalyzeJobsCpuUsage();
    AnalyzeJobsGpuUsage();
    AnalyzeJobsDuration();
    AnalyzeOperationDuration();
    AnalyzeScheduleJobStatistics();
    AnalyzeControllerQueues();
}

void TOperationControllerBase::UpdateCachedMaxAvailableExecNodeResources()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    const auto& nodeDescriptors = GetExecNodeDescriptors();

    TJobResources maxAvailableResources;
    for (const auto& [nodeId, descriptor] : nodeDescriptors) {
        maxAvailableResources = Max(maxAvailableResources, descriptor.ResourceLimits);
    }

    CachedMaxAvailableExecNodeResources_ = maxAvailableResources;
}

void TOperationControllerBase::CheckMinNeededResourcesSanity()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (ShouldSkipSanityCheck()) {
        return;
    }

    for (const auto& task : Tasks) {
        if (task->GetPendingJobCount() == 0) {
            continue;
        }

        const auto& neededResources = task->GetMinNeededResources();
        if (!Dominates(*CachedMaxAvailableExecNodeResources_, neededResources.ToJobResources())) {
            OnOperationFailed(
                TError(
                    EErrorCode::NoOnlineNodeToScheduleJob,
                    "No online node can satisfy the resource demand")
                    << TErrorAttribute("task_name", task->GetTitle())
                    << TErrorAttribute("needed_resources", neededResources.ToJobResources())
                    << TErrorAttribute("max_available_resources", *CachedMaxAvailableExecNodeResources_));
        }
    }
}

TControllerScheduleJobResultPtr TOperationControllerBase::SafeScheduleJob(
    ISchedulingContext* context,
    const TJobResourcesWithQuota& jobLimits,
    const TString& treeId)
{
    if (Spec_->TestingOperationOptions->SchedulingDelay) {
        if (Spec_->TestingOperationOptions->SchedulingDelayType == ESchedulingDelayType::Async) {
            TDelayedExecutor::WaitForDuration(*Spec_->TestingOperationOptions->SchedulingDelay);
        } else {
            Sleep(*Spec_->TestingOperationOptions->SchedulingDelay);
        }
    }

    if (State != EControllerState::Running) {
        YT_LOG_DEBUG("Stale schedule job attempt");
        return nullptr;
    }

    // SafeScheduleJob must be synchronous; context switches are prohibited.
    TForbidContextSwitchGuard contextSwitchGuard;

    TWallTimer timer;
    auto scheduleJobResult = New<TControllerScheduleJobResult>();
    DoScheduleJob(context, jobLimits, treeId, scheduleJobResult.Get());
    if (scheduleJobResult->StartDescriptor) {
        AvailableExecNodesObserved_ = true;
    }
    scheduleJobResult->Duration = timer.GetElapsedTime();

    ScheduleJobStatistics_->RecordJobResult(*scheduleJobResult);

    auto now = NProfiling::GetCpuInstant();
    if (now > ScheduleJobStatisticsLogDeadline_) {
        YT_LOG_DEBUG("Schedule job statistics (Count: %v, TotalDuration: %v, FailureReasons: %v)",
            ScheduleJobStatistics_->Count,
            ScheduleJobStatistics_->Duration,
            ScheduleJobStatistics_->Failed);
        ScheduleJobStatisticsLogDeadline_ = now + NProfiling::DurationToCpuDuration(Config->ScheduleJobStatisticsLogBackoff);
    }

    return scheduleJobResult;
}

void TOperationControllerBase::UpdateConfig(const TControllerAgentConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    Config = config;
}

void TOperationControllerBase::CustomizeJoblet(const TJobletPtr& /* joblet */)
{ }

void TOperationControllerBase::CustomizeJobSpec(const TJobletPtr& joblet, TJobSpec* jobSpec) const
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

    schedulerJobSpecExt->set_yt_alloc_large_unreclaimable_bytes(GetYTAllocLargeUnreclaimableBytes());
    if (OutputTransaction) {
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
    }

    if (joblet->Task->GetUserJobSpec()) {
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet);
    }

    schedulerJobSpecExt->set_acl(ConvertToYsonString(Acl).GetData());
}

void TOperationControllerBase::RegisterTask(TTaskPtr task)
{
    task->Initialize();
    Tasks.emplace_back(std::move(task));
}

void TOperationControllerBase::RegisterTaskGroup(TTaskGroupPtr group)
{
    TaskGroups.push_back(std::move(group));
}

void TOperationControllerBase::UpdateTask(const TTaskPtr& task)
{
    int oldPendingJobCount = CachedPendingJobCount;
    int newPendingJobCount = CachedPendingJobCount + task->GetPendingJobCountDelta();
    CachedPendingJobCount = newPendingJobCount;

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
        FormatResources(CachedNeededResources));

    task->CheckCompleted();
}

void TOperationControllerBase::UpdateAllTasks()
{
    for (const auto& task: Tasks) {
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

void TOperationControllerBase::MoveTaskToCandidates(
    const TTaskPtr& task,
    std::multimap<i64, TTaskPtr>& candidateTasks)
{
    const auto& neededResources = task->GetMinNeededResources();
    i64 minMemory = neededResources.GetMemory();
    candidateTasks.insert(std::make_pair(minMemory, task));
    YT_LOG_DEBUG("Task moved to candidates (Task: %v, MinMemory: %v)",
        task->GetTitle(),
        minMemory / 1_MB);

}

void TOperationControllerBase::AddTaskPendingHint(const TTaskPtr& task)
{
    auto pendingJobCount = task->GetPendingJobCount();
    const auto& taskId = task->GetTitle();
    YT_LOG_TRACE("Adding task pending hint (Task: %v, PendingJobCount: %v)", taskId, pendingJobCount);
    if (pendingJobCount > 0) {
        auto group = task->GetGroup();
        if (group->NonLocalTasks.insert(task).second) {
            YT_LOG_TRACE("Task pending hint added (Task: %v)", taskId);
            MoveTaskToCandidates(task, group->CandidateTasks);
        }
    }
    UpdateTask(task);
}

void TOperationControllerBase::AddAllTaskPendingHints()
{
    for (const auto& task : Tasks) {
        AddTaskPendingHint(task);
    }
}

void TOperationControllerBase::DoAddTaskLocalityHint(const TTaskPtr& task, TNodeId nodeId)
{
    auto group = task->GetGroup();
    if (group->NodeIdToTasks[nodeId].insert(task).second) {
        YT_LOG_TRACE("Task locality hint added (Task: %v, Address: %v)",
            task->GetTitle(),
            InputNodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress());
    }
}

void TOperationControllerBase::AddTaskLocalityHint(TNodeId nodeId, const TTaskPtr& task)
{
    DoAddTaskLocalityHint(task, nodeId);
    UpdateTask(task);
}

void TOperationControllerBase::AddTaskLocalityHint(const TChunkStripePtr& stripe, const TTaskPtr& task)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                if (locality > 0) {
                    DoAddTaskLocalityHint(task, replica.GetNodeId());
                }
            }
        }
    }
    UpdateTask(task);
}

void TOperationControllerBase::ResetTaskLocalityDelays()
{
    YT_LOG_DEBUG("Task locality delays are reset");
    for (const auto& group : TaskGroups) {
        for (const auto& [time, task] : group->DelayedTasks) {
            if (task->GetPendingJobCount() > 0) {
                MoveTaskToCandidates(task, group->CandidateTasks);
            } else {
                YT_LOG_DEBUG("Task pending hint removed (Task: %v)",
                    task->GetTitle());
                YT_VERIFY(group->NonLocalTasks.erase(task) == 1);
            }
        }
        group->DelayedTasks.clear();
    }
}

bool TOperationControllerBase::CheckJobLimits(
    const TTaskPtr& task,
    const TJobResourcesWithQuota& jobLimits,
    const TJobResourcesWithQuota& nodeResourceLimits)
{
    auto neededResources = task->GetMinNeededResources();
    if (Dominates(jobLimits, neededResources)) {
        return true;
    }
    task->CheckResourceDemandSanity(nodeResourceLimits, neededResources);
    return false;
}

void TOperationControllerBase::DoScheduleJob(
    ISchedulingContext* context,
    const TJobResourcesWithQuota& jobLimits,
    const TString& treeId,
    TControllerScheduleJobResult* scheduleJobResult)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(Config->ScheduleJobControllerQueue));

    if (!IsRunning()) {
        YT_LOG_TRACE("Operation is not running, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationNotRunning);
        return;
    }

    if (GetPendingJobCount() == 0) {
        YT_LOG_TRACE("No pending jobs left, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NoPendingJobs);
        return;
    }

    if (BannedNodeIds_.find(context->GetNodeDescriptor().Id) != BannedNodeIds_.end()) {
        YT_LOG_TRACE("Node is banned, scheduling request ignored");
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NodeBanned);
        return;
    }

    DoScheduleLocalJob(context, jobLimits, treeId, scheduleJobResult);
    if (!scheduleJobResult->StartDescriptor) {
        DoScheduleNonLocalJob(context, jobLimits, treeId, scheduleJobResult);
    }
}

void TOperationControllerBase::DoScheduleLocalJob(
    ISchedulingContext* context,
    const TJobResourcesWithQuota& jobLimits,
    const TString& treeId,
    TControllerScheduleJobResult* scheduleJobResult)
{
    const auto& nodeResourceLimits = context->ResourceLimits();
    const auto& address = context->GetNodeDescriptor().Address;
    auto nodeId = context->GetNodeDescriptor().Id;

    for (const auto& group : TaskGroups) {
        if (scheduleJobResult->IsScheduleStopNeeded()) {
            return;
        }
        if (!Dominates(jobLimits, group->MinNeededResources))
        {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
            continue;
        }

        auto localTasksIt = group->NodeIdToTasks.find(nodeId);
        if (localTasksIt == group->NodeIdToTasks.end()) {
            continue;
        }

        i64 bestLocality = 0;
        TTaskPtr bestTask;

        auto& localTasks = localTasksIt->second;
        auto it = localTasks.begin();
        while (it != localTasks.end()) {
            auto jt = it++;
            auto task = *jt;

            // Make sure that the task has positive locality.
            // Remove pending hint if not.
            auto locality = task->GetLocality(nodeId);
            if (locality <= 0) {
                localTasks.erase(jt);
                YT_LOG_TRACE("Task locality hint removed (Task: %v, Address: %v)",
                    task->GetTitle(),
                    address);
                continue;
            }

            if (locality <= bestLocality) {
                continue;
            }

            if (task->GetPendingJobCount() == 0) {
                UpdateTask(task);
                continue;
            }

            if (!CheckJobLimits(task, jobLimits, nodeResourceLimits)) {
                scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
                continue;
            }

            bestLocality = locality;
            bestTask = task;
        }

        if (!IsRunning()) {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationNotRunning);
            break;
        }

        if (bestTask) {
            YT_LOG_DEBUG(
                "Attempting to schedule a local job (Task: %v, Address: %v, Locality: %v, JobLimits: %v, "
                "PendingDataWeight: %v, PendingJobCount: %v)",
                bestTask->GetTitle(),
                address,
                bestLocality,
                FormatResources(jobLimits, GetMediumDirectory()),
                bestTask->GetPendingDataWeight(),
                bestTask->GetPendingJobCount());

            if (!HasEnoughChunkLists(bestTask->IsStderrTableEnabled(), bestTask->IsCoreTableEnabled())) {
                YT_LOG_DEBUG("Job chunk list demand is not met");
                scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughChunkLists);
                break;
            }

            bestTask->ScheduleJob(context, jobLimits, treeId, IsTreeTentative(treeId), scheduleJobResult);
            if (scheduleJobResult->StartDescriptor) {
                RegisterTestingSpeculativeJobIfNeeded(bestTask, scheduleJobResult->StartDescriptor->Id);
                UpdateTask(bestTask);
                break;
            }
            if (scheduleJobResult->IsScheduleStopNeeded()) {
                return;
            }
        } else {
            // NB: This is one of the possible reasons, hopefully the most probable.
            scheduleJobResult->RecordFail(EScheduleJobFailReason::NoLocalJobs);
        }
    }
}

void TOperationControllerBase::DoScheduleNonLocalJob(
    ISchedulingContext* context,
    const TJobResourcesWithQuota& jobLimits,
    const TString& treeId,
    TControllerScheduleJobResult* scheduleJobResult)
{
    auto now = NProfiling::CpuInstantToInstant(context->GetNow());
    const auto& nodeResourceLimits = context->ResourceLimits();
    const auto& address = context->GetNodeDescriptor().Address;

    for (const auto& group : TaskGroups) {
        if (scheduleJobResult->IsScheduleStopNeeded()) {
            return;
        }
        if (!Dominates(jobLimits, group->MinNeededResources))
        {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
            continue;
        }

        auto& nonLocalTasks = group->NonLocalTasks;
        auto& candidateTasks = group->CandidateTasks;
        auto& delayedTasks = group->DelayedTasks;

        // Move tasks from delayed to candidates.
        while (!delayedTasks.empty()) {
            auto it = delayedTasks.begin();
            auto deadline = it->first;
            if (now < deadline) {
                break;
            }
            auto task = it->second;
            delayedTasks.erase(it);
            if (task->GetPendingJobCount() == 0) {
                YT_LOG_DEBUG("Task pending hint removed (Task: %v)",
                    task->GetTitle());
                YT_VERIFY(nonLocalTasks.erase(task) == 1);
                UpdateTask(task);
            } else {
                YT_LOG_DEBUG("Task delay deadline reached (Task: %v)", task->GetTitle());
                MoveTaskToCandidates(task, candidateTasks);
            }
        }

        // Consider candidates in the order of increasing memory demand.
        {
            int processedTaskCount = 0;
            int noPendingJobsTaskCount = 0;
            auto it = candidateTasks.begin();
            while (it != candidateTasks.end()) {
                ++processedTaskCount;
                auto task = it->second;

                // Make sure that the task is ready to launch jobs.
                // Remove pending hint if not.
                if (task->GetPendingJobCount() == 0) {
                    YT_LOG_DEBUG("Task pending hint removed (Task: %v)", task->GetTitle());
                    candidateTasks.erase(it++);
                    YT_VERIFY(nonLocalTasks.erase(task) == 1);
                    UpdateTask(task);
                    ++noPendingJobsTaskCount;
                    continue;
                }

                // Check min memory demand for early exit.
                if (task->GetMinNeededResources().GetMemory() > jobLimits.GetMemory()) {
                    scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
                    break;
                }

                if (!CheckJobLimits(task, jobLimits, nodeResourceLimits)) {
                    ++it;
                    scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
                    continue;
                }

                if (!task->GetDelayedTime()) {
                    task->SetDelayedTime(now);
                }

                auto deadline = *task->GetDelayedTime() + task->GetLocalityTimeout();
                if (deadline > now) {
                    YT_LOG_DEBUG("Task delayed (Task: %v, Deadline: %v)",
                        task->GetTitle(),
                        deadline);
                    delayedTasks.insert(std::make_pair(deadline, task));
                    candidateTasks.erase(it++);
                    scheduleJobResult->RecordFail(EScheduleJobFailReason::TaskDelayed);
                    continue;
                }

                if (!IsRunning()) {
                    scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationNotRunning);
                    break;
                }

                YT_LOG_DEBUG(
                    "Attempting to schedule a non-local job (Task: %v, Address: %v, JobLimits: %v, "
                    "PendingDataWeight: %v, PendingJobCount: %v)",
                    task->GetTitle(),
                    address,
                    FormatResources(jobLimits, GetMediumDirectory()),
                    task->GetPendingDataWeight(),
                    task->GetPendingJobCount());

                if (!HasEnoughChunkLists(task->IsStderrTableEnabled(), task->IsCoreTableEnabled())) {
                    YT_LOG_DEBUG("Job chunk list demand is not met");
                    scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughChunkLists);
                    break;
                }

                task->ScheduleJob(context, jobLimits, treeId, IsTreeTentative(treeId), scheduleJobResult);
                if (scheduleJobResult->StartDescriptor) {
                    RegisterTestingSpeculativeJobIfNeeded(task, scheduleJobResult->StartDescriptor->Id);
                    UpdateTask(task);
                    return;
                }
                if (scheduleJobResult->IsScheduleStopNeeded()) {
                    return;
                }

                // If task failed to schedule job, its min resources might have been updated.
                auto minMemory = task->GetMinNeededResources().GetMemory();
                if (it->first == minMemory) {
                    ++it;
                } else {
                    it = candidateTasks.erase(it);
                    candidateTasks.insert(std::make_pair(minMemory, task));
                }
            }

            if (processedTaskCount == noPendingJobsTaskCount) {
                scheduleJobResult->RecordFail(EScheduleJobFailReason::NoCandidateTasks);
            }

            YT_LOG_DEBUG("Non-local tasks processed (TotalCount: %v, NoPendingJobsCount: %v)",
                processedTaskCount,
                noPendingJobsTaskCount);
        }
    }
}

bool TOperationControllerBase::IsTreeTentative(const TString& treeId) const
{
    return GetOrCrash(PoolTreeControllerSettingsMap_, treeId).Tentative;
}

void TOperationControllerBase::MaybeBanInTentativeTree(const TString& treeId)
{
    if (!BannedTreeIds_.insert(treeId).second) {
        return;
    }

    Host->OnOperationBannedInTentativeTree(
        treeId,
        GetJobIdsByTreeId(treeId));

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

IDiagnosableInvokerPool::TInvokerStatistics TOperationControllerBase::GetInvokerStatistics(EOperationControllerQueue queue) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return DiagnosableInvokerPool->GetInvokerStatistics(queue);
}

TFuture<void> TOperationControllerBase::Suspend()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (Spec_->TestingOperationOptions->DelayInsideSuspend) {
        return Combine(std::vector<TFuture<void>> {
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

int TOperationControllerBase::GetPendingJobCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    // Avoid accessing the state while not prepared.
    if (!IsPrepared()) {
        return 0;
    }

    // NB: For suspended operations we still report proper pending job count
    // but zero demand.
    if (!IsRunning()) {
        return 0;
    }

    return CachedPendingJobCount;
}

void TOperationControllerBase::IncreaseNeededResources(const TJobResources& resourcesDelta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(CachedNeededResourcesLock);
    CachedNeededResources += resourcesDelta;
}

TJobResources TOperationControllerBase::GetNeededResources() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(CachedNeededResourcesLock);
    return CachedNeededResources;
}

TJobResourcesWithQuotaList TOperationControllerBase::GetMinNeededJobResources() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NConcurrency::TReaderGuard guard(CachedMinNeededResourcesJobLock);
    return CachedMinNeededJobResources;
}

void TOperationControllerBase::UpdateMinNeededJobResources()
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default)->Invoke(BIND([=, this_ = MakeStrong(this)] {
        THashMap<EJobType, TJobResourcesWithQuota> minNeededJobResources;

        for (const auto& task : Tasks) {
            if (task->GetPendingJobCount() == 0) {
                continue;
            }

            auto jobType = task->GetJobType();
            TJobResourcesWithQuota resources;
            try {
                resources = task->GetMinNeededResources();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to update min nedeeded resources")
                    << ex;
                OnOperationFailed(error);
                return;
            }

            auto resIt = minNeededJobResources.find(jobType);
            if (resIt == minNeededJobResources.end()) {
                minNeededJobResources[jobType] = resources;
            } else {
                resIt->second = Min(resIt->second, resources);
            }
        }

        TJobResourcesWithQuotaList result;
        for (const auto& [jobType, resources] : minNeededJobResources) {
            result.push_back(resources);
            YT_LOG_DEBUG("Aggregated minimal needed resources for jobs (JobType: %v, MinNeededResources: %v)",
                jobType,
                FormatResources(resources, GetMediumDirectory()));
        }

        {
            NConcurrency::TWriterGuard guard(CachedMinNeededResourcesJobLock);
            CachedMinNeededJobResources.swap(result);
        }
    }));
}

void TOperationControllerBase::FlushOperationNode(bool checkFlushResult)
{
    YT_LOG_DEBUG("Flushing operation node");
    // Some statistics are reported only on operation end so
    // we need to synchronously check everything and set
    // appropriate alerts before flushing operation node.
    // Flush of newly calculated statistics is guaranteed by OnOperationFailed.
    AnalyzeOperationProgress();

    auto flushResult = WaitFor(Host->FlushOperationNode());
    if (checkFlushResult && !flushResult.IsOK()) {
        // We do not want to complete operation if progress flush has failed.
        OnOperationFailed(flushResult, /* flush */ false);
    }
}

void TOperationControllerBase::OnOperationCompleted(bool interrupted)
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    Y_UNUSED(interrupted);

    // This can happen if operation failed during completion in derived class (e.g. SortController).
    if (IsFinished()) {
        return;
    }
    State = EControllerState::Completed;

    BuildAndSaveProgress();
    FlushOperationNode(/* checkFlushResult */ true);

    LogProgress(/* force */ true);

    Host->OnOperationCompleted();
}

void TOperationControllerBase::OnOperationFailed(const TError& error, bool flush)
{
    VERIFY_INVOKER_POOL_AFFINITY(InvokerPool);

    YT_LOG_DEBUG("Operation controller failed (Error: %v, Flush: %v)", error, flush);

    // During operation failing job aborting can lead to another operation fail, we don't want to invoke it twice.
    if (IsFinished()) {
        return;
    }
    State = EControllerState::Failed;

    BuildAndSaveProgress();
    LogProgress(/* force */ true);

    if (flush) {
        // NB: Error ignored since we cannot do anything with it.
        FlushOperationNode(/* checkFlushResult */ false);
    }

    Error_ = error;

    YT_LOG_DEBUG("Notifying host about operation controller failure");
    Host->OnOperationFailed(error);
    YT_LOG_DEBUG("Host notified about operation controller failure");
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

    if (State == EControllerState::Running) {
        State = EControllerState::Failing;
    }

    auto error = GetTimeLimitError();

    bool hasJobsToFail = false;
    for (const auto& [jobId, joblet] : JobletMap) {
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
                Host->FailJob(jobId);
                break;
            default:
                Host->AbortJob(jobId, error);
        }
    }

    if (hasJobsToFail) {
        TDelayedExecutor::MakeDelayed(Spec_->TimeLimitJobFailTimeout)
            .Apply(BIND(&TOperationControllerBase::OnOperationFailed, MakeWeak(this), error, /* flush */ true)
            .Via(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default)));
    } else {
        OnOperationFailed(error, /* flush */ true);
    }
}

void TOperationControllerBase::CheckFailedJobsStatusReceived()
{
    if (IsFailing() && JobletMap.empty()) {
        auto error = GetTimeLimitError();
        OnOperationFailed(error, /* flush */ true);
    }
}

const std::vector<TEdgeDescriptor>& TOperationControllerBase::GetStandardEdgeDescriptors() const
{
    return StandardEdgeDescriptors_;
}

void TOperationControllerBase::InitializeStandardEdgeDescriptors()
{
    StandardEdgeDescriptors_.resize(Sinks_.size());
    for (int index = 0; index < Sinks_.size(); ++index) {
        StandardEdgeDescriptors_[index] = OutputTables_[index]->GetEdgeDescriptorTemplate(index);
        StandardEdgeDescriptors_[index].DestinationPool = Sinks_[index].get();
        StandardEdgeDescriptors_[index].IsFinalOutput = true;
        StandardEdgeDescriptors_[index].LivePreviewIndex = index;
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
        chunkIds.emplace_back(chunk->ChunkId());
        YT_LOG_DEBUG("Releasing intermediate chunk (ChunkId: %v, VertexDescriptor: %v, LivePreviewIndex: %v)",
            chunk->ChunkId(),
            livePreviewDescriptor.VertexDescriptor,
            livePreviewDescriptor.LivePreviewIndex);
        LivePreviewChunks_.erase(it);
    }
    Host->AddChunkTreesToUnstageList(std::move(chunkIds), false /* recursive */);
}

void TOperationControllerBase::ProcessSafeException(const std::exception& ex)
{
    OnOperationFailed(TError("Exception thrown in operation controller that led to operation failure")
        << ex);
}

void TOperationControllerBase::ProcessSafeException(const TAssertionFailedException& ex)
{
    TControllerAgentCounterManager::Get()->IncrementAssertionsFailed(OperationType);

    OnOperationFailed(
        TError(
            NScheduler::EErrorCode::OperationControllerCrashed,
            "Operation controller crashed; please file a ticket at YTADMINREQ and attach a link to this operation")
            << TErrorAttribute("failed_condition", ex.GetExpression())
            << TErrorAttribute("stack_trace", ex.GetStackTrace())
            << TErrorAttribute("core_path", ex.GetCorePath())
            << TErrorAttribute("operation_id", OperationId));
}

EJobState TOperationControllerBase::GetStatisticsJobState(const TJobletPtr& joblet, const EJobState& state)
{
    // NB: Completed restarted job is considered as lost in statistics.
    // Actually we have lost previous incarnation of this job, but it was already considered as completed in statistics.
    return (joblet->Restarted && state == EJobState::Completed)
        ? EJobState::Lost
        : state;
}

void TOperationControllerBase::ProcessFinishedJobResult(std::unique_ptr<TJobSummary> summary, bool requestJobNodeCreation)
{
    auto jobId = summary->Id;

    const auto& schedulerResultExt = summary->Result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    auto stderrChunkId = FromProto<TChunkId>(schedulerResultExt.stderr_chunk_id());
    auto failContextChunkId = FromProto<TChunkId>(schedulerResultExt.fail_context_chunk_id());

    auto hasStderr = static_cast<bool>(stderrChunkId);
    auto hasFailContext = static_cast<bool>(failContextChunkId);
    auto coreInfoCount = schedulerResultExt.core_infos().size();

    auto joblet = GetJoblet(jobId);
    // Job is not actually started.
    if (!joblet->StartTime) {
        return;
    }

    bool shouldRetainJob =
        (requestJobNodeCreation && RetainedJobCount_ < Config->MaxJobNodesPerOperation) ||
        (hasStderr && RetainedJobWithStderrCount_ < Spec_->MaxStderrCount) ||
        (coreInfoCount > 0 && RetainedJobsCoreInfoCount_ + coreInfoCount <= Spec_->MaxCoreInfoCount);

    if (hasStderr && shouldRetainJob) {
        summary->ReleaseFlags.ArchiveStderr = true;
        // Job spec is necessary for ACL checks for stderr.
        summary->ReleaseFlags.ArchiveJobSpec = true;
    }
    if (hasFailContext && shouldRetainJob) {
        summary->ReleaseFlags.ArchiveFailContext = true;
        // Job spec is necessary for ACL checks for fail context.
        summary->ReleaseFlags.ArchiveJobSpec = true;
    }

    summary->ReleaseFlags.ArchiveProfile = true;

    auto finishedJob = New<TFinishedJobInfo>(joblet, std::move(*summary));

    // NB: we do not want these values to get into the snapshot as they may be pretty large.
    finishedJob->Summary.StatisticsYson = TYsonString();
    finishedJob->Summary.Statistics.reset();

    if (finishedJob->Summary.ReleaseFlags.IsNonTrivial()) {
        FinishedJobs_.emplace(jobId, finishedJob);
    }

    if (!shouldRetainJob) {
        if (hasStderr) {
            Host->AddChunkTreesToUnstageList({stderrChunkId}, false /* recursive */);
        }
        return;
    }

    auto attributesFragment = BuildYsonStringFluently<EYsonType::MapFragment>()
        .Do([&] (TFluentMap fluent) {
            BuildFinishedJobAttributes(
                finishedJob,
                /* outputStatistics */ true,
                hasStderr,
                hasFailContext,
                fluent);
        })
        .Finish();

    if (Config->EnableRetainedFinishedJobs) {
        auto attributes = BuildYsonStringFluently()
            .DoMap([&] (TFluentMap fluent) {
                fluent.GetConsumer()->OnRaw(attributesFragment);
            });
        RetainedFinishedJobs_.emplace_back(finishedJob->JobId, std::move(attributes));
    }

    {
        TCreateJobNodeRequest request;
        request.JobId = jobId;
        request.Attributes = std::move(attributesFragment);
        request.StderrChunkId = stderrChunkId;
        request.FailContextChunkId = failContextChunkId;

        Host->CreateJobNode(std::move(request));
    }

    if (hasStderr) {
        ++RetainedJobWithStderrCount_;
    }
    RetainedJobsCoreInfoCount_ += coreInfoCount;
    ++RetainedJobCount_;
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

bool TOperationControllerBase::IsFinished() const
{
    return State == EControllerState::Completed ||
        State == EControllerState::Failed ||
        State == EControllerState::Aborted;
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
        if (table->Schema.HasNontrivialSchemaModification()) {
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
                "User %v belongs to legacy live preview suppression blacklist; in order "
                "to overcome this suppression reason, explicitly specify enable_legacy_live_preview = %%true "
                "in operation spec", AuthenticatedUser)
                    << TErrorAttribute(
                        "legacy_live_preview_blacklist_regex",
                        Config->LegacyLivePreviewUserBlacklist->pattern()));
        }
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
    auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    auto addRequest = [&] (
        const TString& path,
        TCellTag cellTag,
        int replicationFactor,
        NCompression::ECodec compressionCodec,
        const std::optional<TString>& account,
        const TString& key,
        const TYsonString& acl,
        const std::optional<TTableSchema>& schema)
    {
        auto req = TCypressYPathProxy::Create(path);
        req->set_type(static_cast<int>(EObjectType::Table));
        req->set_ignore_existing(true);

        auto attributes = CreateEphemeralAttributes();
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

    if (IsOutputLivePreviewSupported()) {
        YT_LOG_INFO("Creating live preview for output tables");

        for (int index = 0; index < OutputTables_.size(); ++index) {
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
                table->TableUploadOptions.TableSchema);
        }
    }

    if (StderrTable_) {
        YT_LOG_INFO("Creating live preview for stderr table");

        auto path = GetOperationPath(OperationId) + "/stderr";

        addRequest(
            path,
            StderrTable_->ExternalCellTag,
            StderrTable_->TableWriterOptions->ReplicationFactor,
            StderrTable_->TableWriterOptions->CompressionCodec,
            std::nullopt /* account */,
            "create_stderr",
            StderrTable_->EffectiveAcl,
            StderrTable_->TableUploadOptions.TableSchema);
    }

    if (IsIntermediateLivePreviewSupported()) {
        YT_LOG_INFO("Creating live preview for intermediate table");

        auto path = GetOperationPath(OperationId) + "/intermediate";

        auto intermediateDataAcl = MakeOperationArtifactAcl(Acl);
        if (Config->AllowUsersGroupReadIntermediateData) {
            intermediateDataAcl.Entries.emplace_back(
                ESecurityAction::Allow,
                std::vector<TString>{UsersGroupName},
                EPermissionSet(EPermission::Read));
        }
        addRequest(
            path,
            IntermediateOutputCellTag,
            1,
            Spec_->IntermediateCompressionCodec,
            Spec_->IntermediateDataAccount,
            "create_intermediate",
            ConvertToYsonString(intermediateDataAcl),
            std::nullopt);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error creating live preview tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto handleResponse = [&] (TLivePreviewTableBase& table, TCypressYPathProxy::TRspCreatePtr rsp) {
        table.LivePreviewTableId = FromProto<NCypressClient::TNodeId>(rsp->node_id());
    };

    if (IsOutputLivePreviewSupported()) {
        auto rspsOrError = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_output");
        YT_VERIFY(rspsOrError.size() == OutputTables_.size());

        for (int index = 0; index < OutputTables_.size(); ++index) {
            handleResponse(*OutputTables_[index], rspsOrError[index].Value());
        }

        YT_LOG_INFO("Live preview for output tables created");
    }

    if (StderrTable_) {
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>("create_stderr");
        handleResponse(*StderrTable_, rsp.Value());

        YT_LOG_INFO("Live preview for stderr table created");
    }

    if (IsIntermediateLivePreviewSupported()) {
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>("create_intermediate");
        handleResponse(*IntermediateTable, rsp.Value());

        YT_LOG_INFO("Live preview for intermediate table created");
    }
}

void TOperationControllerBase::FetchInputTables()
{
    i64 totalChunkCount = 0;
    i64 totalExtensionSize = 0;

    YT_LOG_INFO("Started fetching input tables");

    TQueryOptions queryOptions;
    queryOptions.VerboseLogging = true;
    queryOptions.RangeExpansionLimit = Config->MaxRangesOnTable;

    auto columnarStatisticsFetcher = New<TColumnarStatisticsFetcher>(
        Config->Fetcher,
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        CreateFetcherChunkScraper(),
        InputClient,
        Logger,
        /*storeChunkStatistics=*/false);

    auto chunkSpecFetcher = New<TChunkSpecFetcher>(
        InputClient,
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        Config->MaxChunksPerFetch,
        Config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int tableIndex) {
            const auto& table = InputTables_[tableIndex];
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            if (table->Dynamic || IsBoundaryKeysFetchEnabled()) {
                req->add_extension_tags(TProtoExtensionTag<TBoundaryKeysExt>::Value);
            }
            // NB: we always fetch parity replicas since
            // erasure reader can repair data on flight.
            req->set_fetch_parity_replicas(true);
            AddCellTagToSyncWith(req, table->ObjectId);
            SetTransactionId(req, table->ExternalTransactionId);
        },
        Logger);

    // We fetch columnar statistics only for the tables that have column selectors specified.
    for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables_.size()); ++tableIndex) {
        auto& table = InputTables_[tableIndex];
        auto ranges = table->Path.GetRanges();
        int originalRangeCount = ranges.size();

        // XXX(max42): does this ever happen?
        if (ranges.empty()) {
            continue;
        }

        bool hasColumnSelectors = table->Path.GetColumns().operator bool();

        if (InputQuery && table->Schema.IsSorted()) {
            auto rangeInferrer = CreateRangeInferrer(
                InputQuery->Query->WhereClause,
                table->Schema,
                table->Schema.GetKeyColumns(),
                Host->GetClient()->GetNativeConnection()->GetColumnEvaluatorCache(),
                BuiltinRangeExtractorMap,
                queryOptions);

            std::vector<TReadRange> inferredRanges;
            for (const auto& range : ranges) {
                auto lower = range.LowerLimit().HasKey() ? range.LowerLimit().GetKey() : MinKey();
                auto upper = range.UpperLimit().HasKey() ? range.UpperLimit().GetKey() : MaxKey();
                auto result = rangeInferrer(TRowRange(lower.Get(), upper.Get()), RowBuffer);
                for (const auto& inferred : result) {
                    auto inferredRange = range;
                    inferredRange.LowerLimit().SetKey(TOwningKey(inferred.first));
                    inferredRange.UpperLimit().SetKey(TOwningKey(inferred.second));
                    inferredRanges.push_back(inferredRange);
                }
            }
            ranges = std::move(inferredRanges);
        }

        if (ranges.size() > Config->MaxRangesOnTable) {
            THROW_ERROR_EXCEPTION(
                "Too many ranges on table: maximum allowed %v, actual %v",
                Config->MaxRangesOnTable,
                ranges.size())
                << TErrorAttribute("table_path", table->Path);
        }

        YT_LOG_INFO("Adding input table for fetch (Path: %v, RangeCount: %v, InferredRangeCount: %v, HasColumnSelectors: %v)",
            table->GetPath(),
            originalRangeCount,
            ranges.size(),
            hasColumnSelectors);

        chunkSpecFetcher->Add(
            table->ObjectId,
            table->ExternalCellTag,
            table->Dynamic && !table->Schema.IsSorted() ? -1 : table->ChunkCount,
            tableIndex,
            ranges);
    }

    YT_LOG_INFO("Fetching input tables");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("Input tables fetched");

    for (const auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
        int tableIndex = chunkSpec.table_index();
        auto& table = InputTables_[tableIndex];

        auto inputChunk = New<TInputChunk>(chunkSpec);
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetChunkIndex(totalChunkCount++);

        if (inputChunk->GetRowCount() > 0) {
            // Input chunks may have zero row count in case of unsensible read range with coinciding
            // lower and upper row index. We skip such chunks.
            table->Chunks.emplace_back(inputChunk);
            for (const auto& extension : chunkSpec.chunk_meta().extensions().extensions()) {
                totalExtensionSize += extension.data().size();
            }
            RegisterInputChunk(table->Chunks.back());

            auto hasColumnSelectors = table->Path.GetColumns().operator bool();
            if (hasColumnSelectors && Spec_->UseColumnarStatistics) {
                columnarStatisticsFetcher->AddChunk(inputChunk, *table->Path.GetColumns());
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
}

void TOperationControllerBase::RegisterInputChunk(const TInputChunkPtr& inputChunk)
{
    auto chunkId = inputChunk->ChunkId();

    // Insert an empty TInputChunkDescriptor if a new chunkId is encountered.
    auto& chunkDescriptor = InputChunkMap[chunkId];
    chunkDescriptor.InputChunks.push_back(inputChunk);

    if (IsUnavailable(inputChunk, CheckParityReplicas())) {
        chunkDescriptor.State = EInputChunkState::Waiting;
    }
}

void TOperationControllerBase::LockInputTables()
{
    //! TODO(ignat): Merge in with lock input files method.
    YT_LOG_INFO("Locking input tables");

    auto channel = InputClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    YT_VERIFY(Config->LockInputTablesRetries);
    auto batchReq = proxy.ExecuteBatchWithRetries(Config->LockInputTablesRetries);

    for (const auto& table : InputTables_) {
        auto req = TTableYPathProxy::Lock(table->GetPath());
        req->Tag() = table;
        req->set_mode(static_cast<int>(ELockMode::Snapshot));
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
        table->ExternalCellTag = rsp->external_cell_tag();
        table->ExternalTransactionId = rsp->has_external_transaction_id()
            ? FromProto<TTransactionId>(rsp->external_transaction_id())
            : *table->TransactionId;
        PathToInputTables_[table->GetPath()].push_back(table);
    }
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

    for (const auto& table : InputTables_) {
        if (table->Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                table->GetPath(),
                EObjectType::Table,
                table->Type);
        }
    }

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
        SetOperationAlert(EOperationAlertType::OmittedInaccesibleColumnsInInputTables, error);
    }

    THashMap<TCellTag, std::vector<TInputTablePtr>> externalCellTagToTables;
    for (const auto& table : InputTables_) {
        externalCellTagToTables[table->ExternalCellTag].push_back(table);
    }

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
        auto channel = InputClient->GetMasterChannelOrThrow(EMasterChannelKind::Follower, externalCellTag);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        for (const auto& table : tables) {
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "dynamic",
                "chunk_count",
                "retained_timestamp",
                "schema_mode",
                "schema",
                "unflushed_timestamp",
                "content_revision",
                "enable_dynamic_store_read",
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

    auto result = WaitFor(Combine(asyncResults));
    checkError(result);

    for (const auto& batchRsp : result.Value()) {
        checkError(GetCumulativeError(batchRsp));
        for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGet>()) {
            const auto& rsp = rspOrError.Value();
            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

            auto table = std::any_cast<TInputTablePtr>(rsp->Tag());
            table->Dynamic = attributes->Get<bool>("dynamic");
            table->Schema = attributes->Get<TTableSchema>("schema");
            table->SchemaMode = attributes->Get<ETableSchemaMode>("schema_mode");
            table->ChunkCount = attributes->Get<int>("chunk_count");
            table->ContentRevision = attributes->Get<NHydra::TRevision>("content_revision");

            // Validate that timestamp is correct.
            ValidateDynamicTableTimestamp(table->Path, table->Dynamic, table->Schema, *attributes);

            YT_LOG_INFO("Input table locked (Path: %v, ObjectId: %v, Schema: %v, Dynamic: %v, ChunkCount: %v, SecurityTags: %v, "
                "Revision: %llx, ContentRevision: %llx)",
                table->GetPath(),
                table->ObjectId,
                table->Schema,
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
                YT_LOG_DEBUG("Start renaming columns");
                try {
                    THashMap<TString, TString> columnMapping;
                    for (const auto& descriptor : table->ColumnRenameDescriptors) {
                        auto it = columnMapping.insert({descriptor.OriginalName, descriptor.NewName});
                        YT_VERIFY(it.second);
                    }
                    auto newColumns = table->Schema.Columns();
                    for (auto& column : newColumns) {
                        auto it = columnMapping.find(column.Name());
                        if (it != columnMapping.end()) {
                            column.SetName(it->second);
                            ValidateColumnSchema(column, table->Schema.IsSorted(), table->Dynamic);
                            columnMapping.erase(it);
                        }
                    }
                    if (!columnMapping.empty()) {
                        THROW_ERROR_EXCEPTION("Rename is supported only for columns in schema")
                            << TErrorAttribute("failed_rename_descriptors", columnMapping);
                    }
                    table->Schema = TTableSchema(newColumns, table->Schema.GetStrict(), table->Schema.GetUniqueKeys());
                    ValidateColumnUniqueness(table->Schema);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error renaming columns")
                        << TErrorAttribute("table_path", table->Path)
                        << TErrorAttribute("column_rename_descriptors", table->ColumnRenameDescriptors)
                        << ex;
                }
                YT_LOG_DEBUG("Columns are renamed (Path: %v, NewSchema: %v)",
                    table->GetPath(),
                    table->Schema);
            }
        }
    }
}

void TOperationControllerBase::GetOutputTablesSchema()
{
    YT_LOG_INFO("Getting output tables schema");

    // XXX(babenko): fetch from external cells
    auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
    TObjectServiceProxy proxy(channel);
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : UpdatingTables_) {
        auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "schema_mode",
            "schema",
            "optimize_for",
            "compression_codec",
            "erasure_codec",
            "dynamic"
        });
        req->Tag() = table;
        SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error getting attributes of output tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>();
    for (const auto& rspOrError : rspsOrError) {
        const auto& rsp = rspOrError.Value();

        auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
        const auto& path = table->Path;

        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        table->Dynamic = attributes->Get<bool>("dynamic");
        table->TableUploadOptions = GetTableUploadOptions(
            path,
            *attributes,
            0); // Here we assume zero row count, we will do additional check later.

        if (table->Dynamic) {
            if (!table->TableUploadOptions.TableSchema.IsSorted()) {
                THROW_ERROR_EXCEPTION("Only sorted dynamic table can be updated")
                    << TErrorAttribute("table_path", path);
            }

            // Check if bulk insert is enabled for a certain user.
            if (!Config->EnableBulkInsertForEveryone) {
                auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Cache);
                TObjectServiceProxy proxy(channel);
                auto batchReq = proxy.ExecuteBatch();

                auto req = TYPathProxy::Get("//sys/users/" + ToYPathLiteral(AuthenticatedUser) + "/@");
                ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                    "enable_bulk_insert"
                });
                batchReq->AddRequest(req);

                auto batchRspOrError = WaitFor(batchReq->Invoke());
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    GetCumulativeError(batchRspOrError),
                    "Failed to check if bulk insert is enabled");
                const auto& batchRsp = batchRspOrError.Value();

                const auto& rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                const auto& rsp = rspOrError.Value();
                auto attributes = ConvertToAttributes(TYsonString(rsp->value()));
                if (!attributes->Get<bool>("enable_bulk_insert", false)) {
                    THROW_ERROR_EXCEPTION("Bulk insert is disabled for user %Qv, contact yt-admin@ for enabling",
                        AuthenticatedUser);
                }
            }
        }

        // TODO(savrus): I would like to see commit ts here. But as for now, start ts suffices.
        table->Timestamp = GetTransactionForOutputTable(table)->GetStartTimestamp();

        // NB(psushin): This option must be set before PrepareOutputTables call.
        table->TableWriterOptions->EvaluateComputedColumns = table->TableUploadOptions.TableSchema.HasComputedColumns();

        table->TableWriterOptions->SchemaModification = table->TableUploadOptions.SchemaModification;

        YT_LOG_DEBUG("Received output table schema (Path: %v, Schema: %v, SchemaMode: %v, LockMode: %v)",
            path,
            table->TableUploadOptions.TableSchema,
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
        auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        {
            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : UpdatingTables_) {
                auto req = TTableYPathProxy::Lock(table->GetObjectIdPath());
                SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                GenerateMutationId(req);
                req->set_mode(static_cast<int>(table->TableUploadOptions.LockMode));
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

                YT_LOG_INFO("Output table locked (Path: %v, ObjectId: %v, Schema: %v, ExternalTransactionId: %v, Revision: %llx)",
                    table->GetPath(),
                    objectId,
                    table->TableUploadOptions.TableSchema,
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
    }

    YT_LOG_INFO("Getting output tables attributes");

    {
        auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& table : UpdatingTables_) {
            auto req = TTableYPathProxy::Get(table->GetObjectIdPath() + "/@");
            req->Tag() = table;
            ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
                "account",
                "chunk_writer",
                "effective_acl",
                "primary_medium",
                "replication_factor",
                "row_count",
                "vital",
                "enable_skynet_sharing",
                "tablet_state",
                "atomicity",
                "tablet_statistics",
                "max_overlapping_store_count",
            });
            SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
            batchReq->AddRequest(req);
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error getting attributes of output tables");
        const auto& batchRsp = batchRspOrError.Value();

        auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>();
        for (const auto& rspOrError : rspsOrError) {
            const auto& rsp = rspOrError.Value();

            auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
            const auto& path = table->GetPath();

            auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

            if (table->Dynamic) {
                auto tabletState = attributes->Get<ETabletState>("tablet_state");
                if (tabletState != ETabletState::Mounted && tabletState != ETabletState::Frozen) {
                    THROW_ERROR_EXCEPTION("Output table %v tablet state %Qv does not allow to write into it",
                        path,
                        tabletState);
                }

                if (UserTransactionId) {
                    THROW_ERROR_EXCEPTION(
                        "Operations with output to dynamic tables cannot be run under user transaction")
                        << TErrorAttribute("user_transaction_id", UserTransactionId);
                }

                auto atomicity = attributes->Get<EAtomicity>("atomicity");
                if (atomicity != Spec_->Atomicity) {
                    THROW_ERROR_EXCEPTION("Output table %v atomicity %Qv does not match spec atomicity %Qv",
                        path,
                        atomicity,
                        Spec_->Atomicity);
                }

                if (table->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
                    auto overlappingStoreCount = TryGetInt64(
                        attributes->GetYson("tablet_statistics").GetData(),
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

            if (table->TableUploadOptions.TableSchema.IsSorted()) {
                table->TableWriterOptions->ValidateSorted = true;
                table->TableWriterOptions->ValidateUniqueKeys = table->TableUploadOptions.TableSchema.GetUniqueKeys();
            } else {
                table->TableWriterOptions->ValidateSorted = false;
            }

            table->TableWriterOptions->CompressionCodec = table->TableUploadOptions.CompressionCodec;
            table->TableWriterOptions->ErasureCodec = table->TableUploadOptions.ErasureCodec;
            table->TableWriterOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
            table->TableWriterOptions->MediumName = attributes->Get<TString>("primary_medium");
            table->TableWriterOptions->Account = attributes->Get<TString>("account");
            table->TableWriterOptions->ChunksVital = attributes->Get<bool>("vital");
            table->TableWriterOptions->OptimizeFor = table->TableUploadOptions.OptimizeFor;
            table->TableWriterOptions->EnableSkynetSharing = attributes->Get<bool>("enable_skynet_sharing", false);

            // Workaround for YT-5827.
            if (table->TableUploadOptions.TableSchema.Columns().empty() &&
                table->TableUploadOptions.TableSchema.GetStrict())
            {
                table->TableWriterOptions->OptimizeFor = EOptimizeFor::Lookup;
            }

            table->EffectiveAcl = attributes->GetYson("effective_acl");
            table->WriterConfig = attributes->FindYson("chunk_writer");

            YT_LOG_INFO("Output table attributes fetched (Path: %v, Options: %v, UploadTransactionId: %v)",
                path,
                ConvertToYsonString(table->TableWriterOptions, EYsonFormat::Text).GetData(),
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
            auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader, nativeCellTag);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                auto req = TTableYPathProxy::BeginUpload(table->GetObjectIdPath());
                SetTransactionId(req, GetTransactionForOutputTable(table)->GetId());
                GenerateMutationId(req);
                req->Tag() = table;
                req->set_update_mode(static_cast<int>(table->TableUploadOptions.UpdateMode));
                req->set_lock_mode(static_cast<int>(table->TableUploadOptions.LockMode));
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

        auto result = WaitFor(Combine(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
            for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspBeginUpload>()) {
                const auto& rsp = rspOrError.Value();

                auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());
                table->UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
            }
        }
    }

    {
        YT_LOG_INFO("Getting output tables upload parameters");

        std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
        for (const auto& [externalCellTag, tables] : externalCellTagToTables) {
            auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Follower, externalCellTag);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                auto req = TTableYPathProxy::GetUploadParams(table->GetObjectIdPath());
                SetTransactionId(req, table->UploadTransactionId);
                req->Tag() = table;
                if (table->TableUploadOptions.TableSchema.IsSorted() &&
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

        auto result = WaitFor(Combine(asyncResults));
        checkError(result);

        for (const auto& batchRsp : result.Value()) {
            checkError(GetCumulativeError(batchRsp));
            for (const auto& rspOrError : batchRsp->GetResponses<TTableYPathProxy::TRspGetUploadParams>()) {
                const auto& rsp = rspOrError.Value();
                auto table = std::any_cast<TOutputTablePtr>(rsp->Tag());

                if (table->Dynamic) {
                    table->PivotKeys = FromProto<std::vector<TOwningKey>>(rsp->pivot_keys());
                    table->TabletChunkListIds = FromProto<std::vector<TChunkListId>>(rsp->tablet_chunk_list_ids());
                } else {
                    table->OutputChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
                    if (table->TableUploadOptions.TableSchema.IsSorted() && table->TableUploadOptions.UpdateMode == EUpdateMode::Append) {
                        table->LastKey = FromProto<TOwningKey>(rsp->last_key());
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

    auto chunkSpecFetcher = New<TChunkSpecFetcher>(
        InputClient,
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        Config->MaxChunksPerFetch,
        Config->MaxChunksPerLocateRequest,
        [&] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int fileIndex) {
            const auto& file = *userFiles[fileIndex];
            req->set_fetch_all_meta_extensions(false);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            if (file.Dynamic || IsBoundaryKeysFetchEnabled()) {
                req->add_extension_tags(TProtoExtensionTag<TBoundaryKeysExt>::Value);
            }
            // NB: we always fetch parity replicas since
            // erasure reader can repair data on flight.
            req->set_fetch_parity_replicas(true);
            AddCellTagToSyncWith(req, file.ObjectId);
            SetTransactionId(req, file.ExternalTransactionId);
        },
        Logger);

    for (auto& [userJobSpec, files] : UserJobFiles_) {
        for (auto& file : files) {
            int fileIndex = userFiles.size();
            userFiles.push_back(&file);

            YT_LOG_INFO("Adding user file for fetch (Path: %v, TaskTitle: %v)",
                file.Path,
                userJobSpec->TaskTitle);

            std::vector<TReadRange> readRanges;
            if (file.Type == EObjectType::Table) {
                readRanges = file.Path.GetRanges();
            } else if (file.Type == EObjectType::File) {
                readRanges = {TReadRange()};
            } else {
                YT_ABORT();
            }

            chunkSpecFetcher->Add(
                file.ObjectId,
                file.ExternalCellTag,
                file.ChunkCount,
                fileIndex,
                readRanges);
        }
    }

    YT_LOG_INFO("Fetching user files");

    WaitFor(chunkSpecFetcher->Fetch())
        .ThrowOnError();

    YT_LOG_INFO("User files fetched (ChunkCount: %v)",
        chunkSpecFetcher->ChunkSpecs().size());

    for (auto& chunkSpec : chunkSpecFetcher->ChunkSpecs()) {
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
        Config->Fetcher,
        InputNodeDirectory_,
        CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default),
        CreateFetcherChunkScraper(),
        InputClient,
        Logger,
        /*storeChunkStatistics=*/false);

    // Collect columnar statistics for table files with column selectors.
    for (auto& pair : UserJobFiles_) {
        auto& files = pair.second;
        for (auto& file : files) {
            if (file.Type == EObjectType::Table) {
                for (const auto& chunkSpec : file.ChunkSpecs) {
                    auto chunk = New<TInputChunk>(chunkSpec);
                    file.Chunks.emplace_back(chunk);
                    if (file.Path.GetColumns() && Spec_->UseColumnarStatistics) {
                        columnarStatisticsFetcher->AddChunk(chunk, *file.Path.GetColumns());
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

    for (auto& pair : UserJobFiles_) {
        auto& files = pair.second;
        for (const auto& file : files) {
            YT_LOG_DEBUG("Validating user file (FileName: %v, Path: %v, Type: %v, HasColumns: %v)",
                file.FileName,
                file.Path,
                file.Type,
                file.Path.GetColumns().operator bool());
            auto chunkCount = file.Type == NObjectClient::EObjectType::File ? file.ChunkCount : file.Chunks.size();
            if (chunkCount > Config->MaxUserFileChunkCount) {
                THROW_ERROR_EXCEPTION(
                    "User file %v exceeds chunk count limit: %v > %v",
                    file.Path,
                    chunkCount,
                    Config->MaxUserFileChunkCount);
            }
            if (file.Type == NObjectClient::EObjectType::Table) {
                i64 dataWeight = 0;
                for (const auto& chunk : file.Chunks) {
                    dataWeight += chunk->GetDataWeight();
                }
                if (dataWeight > Config->MaxUserFileTableDataWeight) {
                    THROW_ERROR_EXCEPTION(
                        "User file table %v exceeds data weight limit: %v > %v",
                        file.Path,
                        dataWeight,
                        Config->MaxUserFileTableDataWeight);
                }
            } else {
                i64 uncompressedSize = 0;
                for (const auto& chunkSpec : file.ChunkSpecs) {
                    uncompressedSize += GetChunkUncompressedDataSize(chunkSpec);
                }
                if (uncompressedSize > Config->MaxUserFileSize) {
                    THROW_ERROR_EXCEPTION(
                        "User file %v exceeds size limit: %v > %v",
                        file.Path,
                        uncompressedSize,
                        Config->MaxUserFileSize);
                }
            }
        }
    }

}

void TOperationControllerBase::LockUserFiles()
{
    YT_LOG_INFO("Locking user files");

    auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);
    auto batchReq = proxy.ExecuteBatch();

    for (auto& [userJobSpec, files] : UserJobFiles_) {
        for (auto& file : files) {
            auto req = TFileYPathProxy::Lock(file.Path.GetPath());
            req->set_mode(static_cast<int>(ELockMode::Snapshot));
            GenerateMutationId(req);
            SetTransactionId(req, *file.TransactionId);
            req->Tag() = &file;
            batchReq->AddRequest(req);
        }
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
            TLogger(Logger)
                .AddTag("TaskTitle: %v", userJobSpec->TaskTitle),
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
                THROW_ERROR_EXCEPTION("User layer %v has invalid type: expected %Qlv , actual %Qlv",
                    path,
                    EObjectType::File,
                    file.Type);
            }
        }
    }


    auto channel = OutputClient->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
    TObjectServiceProxy proxy(channel);
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& files : GetValues(UserJobFiles_)) {
        for (const auto& file : files) {
            {
                auto req = TYPathProxy::Get(file.GetObjectIdPath() + "/@");
                SetTransactionId(req, *file.TransactionId);
                std::vector<TString> attributeKeys;
                attributeKeys.push_back("file_name");
                switch (file.Type) {
                    case EObjectType::File:
                        attributeKeys.push_back("executable");
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
    for (auto& pair : UserJobFiles_) {
        const auto& userJobSpec = pair.first;
        auto& files = pair.second;
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
                            std::unique_ptr<IAttributeDictionary> linkAttributes;
                            if (linkRsp.IsOK()) {
                                linkAttributes = ConvertToAttributes(TYsonString(linkRsp.Value()->value()));
                                actualAttributes = linkAttributes.get();
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
                            break;

                        case EObjectType::Table:
                            file.Dynamic = attributes.Get<bool>("dynamic");
                            file.Schema = attributes.Get<TTableSchema>("schema");
                            file.Format = attributes.FindYson("format");
                            if (!file.Format) {
                                file.Format = file.Path.GetFormat();
                            }
                            // Validate that format is correct.
                            try {
                                if (!file.Format) {
                                    THROW_ERROR_EXCEPTION("Format is missing");
                                }
                                ConvertTo<TFormat>(file.Format);
                            } catch (const std::exception& ex) {
                                THROW_ERROR_EXCEPTION("Failed to parse format of table file %v",
                                    file.Path) << ex;
                            }
                            // Validate that timestamp is correct.
                            ValidateDynamicTableTimestamp(file.Path, file.Dynamic, file.Schema, attributes);

                            break;

                        default:
                            YT_ABORT();
                    }

                    i64 chunkCount = attributes.Get<i64>("chunk_count");
                    if (file.Type == EObjectType::File && chunkCount > Config->MaxUserFileChunkCount) {
                        THROW_ERROR_EXCEPTION(
                            "User file %v exceeds chunk count limit: %v > %v",
                            path,
                            chunkCount,
                            Config->MaxUserFileChunkCount);
                    }
                    file.ChunkCount = chunkCount;
                    file.ContentRevision = attributes.Get<NHydra::TRevision>("content_revision");

                    YT_LOG_INFO("User file locked (Path: %v, TaskTitle: %v, FileName: %v, SecurityTags: %v, ContentRevision: %v)",
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
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto fetchFunctions = [&] (const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers) {
        MergeFrom(typeInferrers.Get(), *BuiltinTypeInferrersMap);

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
                << TErrorAttribute("extenal_names", externalNames);
        }

        std::vector<std::pair<TString, TString>> keys;
        for (const auto& name : externalNames) {
            keys.emplace_back(*Config->UdfRegistryPath, name);
        }

        auto descriptors = LookupAllUdfDescriptors(keys, Host->GetClient());

        AppendUdfDescriptors(typeInferrers, externalCGInfo, externalNames, descriptors);
    };

    auto inferSchema = [&] () {
        std::vector<TTableSchema> schemas;
        for (const auto& table : InputTables_) {
            schemas.push_back(table->Schema);
        }
        return InferInputSchema(schemas, false);
    };

    auto query = PrepareJobQuery(
        queryString,
        schema ? *schema : inferSchema(),
        fetchFunctions);

    auto getColumns = [] (const TTableSchema& desiredSchema, const TTableSchema& tableSchema) {
        std::vector<TString> columns;
        for (const auto& column : desiredSchema.Columns()) {
            if (tableSchema.FindColumn(column.Name())) {
                columns.push_back(column.Name());
            }
        }

        return columns.size() == tableSchema.GetColumnCount()
            ? std::optional<std::vector<TString>>()
            : std::make_optional(std::move(columns));
    };

    // Use query column filter for input tables.
    for (auto table : InputTables_) {
        auto columns = getColumns(query->GetReadSchema(), table->Schema);
        if (columns) {
            table->Path.SetColumns(*columns);
        }
    }

    InputQuery.emplace();
    InputQuery->Query = std::move(query);
    InputQuery->ExternalCGInfo = std::move(externalCGInfo);
}

void TOperationControllerBase::WriteInputQueryToJobSpec(TSchedulerJobSpecExt* schedulerJobSpecExt)
{
    auto* querySpec = schedulerJobSpecExt->mutable_input_query_spec();
    ToProto(querySpec->mutable_query(), InputQuery->Query);
    querySpec->mutable_query()->set_input_row_limit(std::numeric_limits<i64>::max());
    querySpec->mutable_query()->set_output_row_limit(std::numeric_limits<i64>::max());
    ToProto(querySpec->mutable_external_functions(), InputQuery->ExternalCGInfo->Functions);
}

void TOperationControllerBase::CollectTotals()
{
    // This is the sum across all input chunks not accounting lower/upper read limits.
    // Used to calculate compression ratio.
    i64 totalInputDataWeight = 0;
    for (const auto& table : InputTables_) {
        for (const auto& inputChunk : table->Chunks) {
            if (IsUnavailable(inputChunk, CheckParityReplicas())) {
                auto chunkId = inputChunk->ChunkId();

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
            TotalEstimatedInputCompressedDataSize += inputChunk->GetCompressedDataSize();
            TotalEstimatedInputDataWeight += inputChunk->GetDataWeight();
            ++TotalEstimatedInputChunkCount;
        }
    }

    InputCompressionRatio = static_cast<double>(TotalEstimatedInputCompressedDataSize) / totalInputDataWeight;
    DataWeightRatio = static_cast<double>(totalInputDataWeight) / TotalEstimatedInputUncompressedDataSize;

    YT_LOG_INFO("Estimated input totals collected (ChunkCount: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v, DataWeight: %v, TotalDataWeight: %v)",
        TotalEstimatedInputChunkCount,
        TotalEstimatedInputRowCount,
        TotalEstimatedInputUncompressedDataSize,
        TotalEstimatedInputCompressedDataSize,
        TotalEstimatedInputDataWeight,
        totalInputDataWeight);
}

void TOperationControllerBase::CustomPrepare()
{ }

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
        if (!table->IsForeign() && ((table->Dynamic && table->Schema.IsSorted()) == versioned)) {
            for (const auto& chunk : table->Chunks) {
                if (IsUnavailable(chunk, CheckParityReplicas())) {
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

std::pair<i64, i64> TOperationControllerBase::CalculatePrimaryVersionedChunksStatistics() const
{
    i64 dataWeight = 0;
    i64 rowCount = 0;
    for (const auto& table : InputTables_) {
        if (!table->IsForeign() && table->Dynamic && table->Schema.IsSorted()) {
            for (const auto& chunk : table->Chunks) {
                dataWeight += chunk->GetDataWeight();
                rowCount += chunk->GetRowCount();
            }
        }
    }
    return std::make_pair(dataWeight, rowCount);
}

std::vector<TInputDataSlicePtr> TOperationControllerBase::CollectPrimaryVersionedDataSlices(i64 sliceSize)
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

    std::vector<TFuture<void>> asyncResults;
    std::vector<IChunkSliceFetcherPtr> fetchers;

    for (const auto& table : InputTables_) {
        if (!table->IsForeign() && table->Dynamic && table->Schema.IsSorted()) {
            auto fetcher = CreateChunkSliceFetcher(
                Config->Fetcher,
                sliceSize,
                InputNodeDirectory_,
                GetCancelableInvoker(),
                createScraperForFetcher(),
                Host->GetClient(),
                RowBuffer,
                Logger);

            auto keyColumnCount = table->Schema.GetKeyColumns().size();

            for (const auto& chunk : table->Chunks) {
                if (IsUnavailable(chunk, CheckParityReplicas()) &&
                    Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Skip)
                {
                    continue;
                }

                fetcher->AddChunkForSlicing(chunk, keyColumnCount, true);
            }

            fetcher->SetCancelableContext(GetCancelableContext());
            asyncResults.emplace_back(fetcher->Fetch());
            fetchers.emplace_back(std::move(fetcher));
        }
    }

    WaitFor(Combine(asyncResults))
        .ThrowOnError();

    std::vector<TInputDataSlicePtr> result;
    for (const auto& fetcher : fetchers) {
        auto dataSlices = CombineVersionedChunkSlices(fetcher->GetChunkSlices());
        for (auto& dataSlice : dataSlices) {
            YT_LOG_TRACE("Added dynamic table slice (TablePath: %v, Range: %v..%v, ChunkIds: %v)",
                InputTables_[dataSlice->GetTableIndex()]->GetPath(),
                dataSlice->LowerLimit(),
                dataSlice->UpperLimit(),
                dataSlice->ChunkSlices);
            result.emplace_back(std::move(dataSlice));
        }
    }

    DataSliceFetcherChunkScrapers.clear();

    return result;
}

std::vector<TInputDataSlicePtr> TOperationControllerBase::CollectPrimaryInputDataSlices(i64 versionedSliceSize)
{
    std::vector<std::vector<TInputDataSlicePtr>> dataSlicesByTableIndex(InputTables_.size());
    for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlicesByTableIndex[dataSlice->GetTableIndex()].emplace_back(std::move(dataSlice));
    }
    for (auto& dataSlice : CollectPrimaryVersionedDataSlices(versionedSliceSize)) {
        dataSlicesByTableIndex[dataSlice->GetTableIndex()].emplace_back(std::move(dataSlice));
    }
    std::vector<TInputDataSlicePtr> dataSlices;
    for (auto& tableDataSlices : dataSlicesByTableIndex) {
        std::move(tableDataSlices.begin(), tableDataSlices.end(), std::back_inserter(dataSlices));
    }
    return dataSlices;
}

std::vector<std::deque<TInputDataSlicePtr>> TOperationControllerBase::CollectForeignInputDataSlices(int foreignKeyColumnCount) const
{
    std::vector<std::deque<TInputDataSlicePtr>> result;
    for (const auto& table : InputTables_) {
        if (table->IsForeign()) {
            result.push_back(std::deque<TInputDataSlicePtr>());

            if (table->Dynamic && table->Schema.IsSorted()) {
                std::vector<TInputChunkSlicePtr> chunkSlices;
                chunkSlices.reserve(table->Chunks.size());
                for (const auto& chunkSpec : table->Chunks) {
                    chunkSlices.push_back(CreateInputChunkSlice(
                        chunkSpec,
                        RowBuffer->Capture(chunkSpec->BoundaryKeys()->MinKey.Get()),
                        GetKeySuccessor(chunkSpec->BoundaryKeys()->MaxKey.Get(), RowBuffer)));
                }

                auto dataSlices = CombineVersionedChunkSlices(chunkSlices);
                for (const auto& dataSlice : dataSlices) {
                    if (IsUnavailable(dataSlice, CheckParityReplicas())) {
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
                    if (IsUnavailable(inputChunk, CheckParityReplicas())) {
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
                    result.back().push_back(CreateUnversionedInputDataSlice(CreateInputChunkSlice(
                        inputChunk,
                        GetKeyPrefix(inputChunk->BoundaryKeys()->MinKey.Get(), foreignKeyColumnCount, RowBuffer),
                        GetKeyPrefixSuccessor(inputChunk->BoundaryKeys()->MaxKey.Get(), foreignKeyColumnCount, RowBuffer))));
                }
            }
        }
    }
    return result;
}

bool TOperationControllerBase::InputHasVersionedTables() const
{
    for (const auto& table : InputTables_) {
        if (table->Dynamic && table->Schema.IsSorted()) {
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

bool TOperationControllerBase::IsLocalityEnabled() const
{
    return Config->EnableLocality && TotalEstimatedInputDataWeight > Spec_->MinLocalityInputDataWeight;
}

TString TOperationControllerBase::GetLoggingProgress() const
{
    if (!DataFlowGraph_) {
        return "Cannot obtain progress: dataflow graph is not initialized.";
    }

    const auto& jobCounter = DataFlowGraph_->GetTotalJobCounter();
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

bool TOperationControllerBase::IsJobInterruptible() const
{
    return true;
}

void TOperationControllerBase::ExtractInterruptDescriptor(TCompletedJobSummary& jobSummary) const
{
    std::vector<TInputDataSlicePtr> dataSliceList;

    const auto& result = jobSummary.Result;
    const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    std::vector<TDataSliceDescriptor> unreadDataSliceDescriptors;
    std::vector<TDataSliceDescriptor> readDataSliceDescriptors;
    if (schedulerResultExt.unread_chunk_specs_size() > 0) {
        FromProto(
            &unreadDataSliceDescriptors,
            schedulerResultExt.unread_chunk_specs(),
            schedulerResultExt.chunk_spec_count_per_unread_data_slice());
    }
    if (schedulerResultExt.read_chunk_specs_size() > 0) {
        FromProto(
            &readDataSliceDescriptors,
            schedulerResultExt.read_chunk_specs(),
            schedulerResultExt.chunk_spec_count_per_read_data_slice());
    }

    auto extractDataSlice = [&] (const TDataSliceDescriptor& dataSliceDescriptor) {
        std::vector<TInputChunkSlicePtr> chunkSliceList;
        chunkSliceList.reserve(dataSliceDescriptor.ChunkSpecs.size());
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
            chunkSliceList.emplace_back(std::move(chunkSlice));
        }
        TInputDataSlicePtr dataSlice;
        if (InputTables_[dataSliceDescriptor.GetDataSourceIndex()]->Dynamic) {
            dataSlice = CreateVersionedInputDataSlice(chunkSliceList);
        } else {
            YT_VERIFY(chunkSliceList.size() == 1);
            dataSlice = CreateUnversionedInputDataSlice(chunkSliceList[0]);
        }
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

int TOperationControllerBase::EstimateSplitJobCount(const TCompletedJobSummary& jobSummary, const TJobletPtr& joblet)
{
    if (!JobSplitter_ || GetPendingJobCount() > 0) {
        return 1;
    }

    auto inputDataStatistics = GetTotalInputDataStatistics(*jobSummary.Statistics);

    // We don't estimate unread row count based on unread slices,
    // because foreign slices are not passed back to scheduler.
    // Instead, we take the difference between estimated row count and actual read row count.
    i64 unreadRowCount = joblet->InputStripeList->TotalRowCount - inputDataStatistics.row_count();

    if (unreadRowCount <= 0) {
        // This is almost impossible, still we don't want to fail operation in this case.
        YT_LOG_WARNING("Estimated unread row count is negative (JobId: %v, UnreadRowCount: %v)", jobSummary.Id, unreadRowCount);
        unreadRowCount = 1;
    }

    return JobSplitter_->EstimateJobCount(jobSummary, unreadRowCount);
}

TKeyColumns TOperationControllerBase::CheckInputTablesSorted(
    const TKeyColumns& keyColumns,
    std::function<bool(const TInputTablePtr& table)> inputTableFilter)
{
    YT_VERIFY(!InputTables_.empty());

    for (const auto& table : InputTables_) {
        if (inputTableFilter(table) && !table->Schema.IsSorted()) {
            THROW_ERROR_EXCEPTION("Input table %v is not sorted",
                table->GetPath());
        }
    }

    auto validateColumnFilter = [] (const TInputTablePtr& table, const TKeyColumns& keyColumns) {
        auto columns = table->Path.GetColumns();
        if (!columns) {
            return;
        }

        auto columnSet = THashSet<TString>(columns->begin(), columns->end());
        for (const auto& keyColumn : keyColumns) {
            if (columnSet.find(keyColumn) == columnSet.end()) {
                THROW_ERROR_EXCEPTION("Column filter for input table %v must include key column %Qv",
                    table->GetPath(),
                    keyColumn);
            }
        }
    };

    if (!keyColumns.empty()) {
        for (const auto& table : InputTables_) {
            if (!inputTableFilter(table)) {
                continue;
            }

            if (!CheckKeyColumnsCompatible(table->Schema.GetKeyColumns(), keyColumns)) {
                THROW_ERROR_EXCEPTION("Input table %v is sorted by columns %v that are not compatible "
                    "with the requested columns %v",
                    table->GetPath(),
                    table->Schema.GetKeyColumns(),
                    keyColumns);
            }
            validateColumnFilter(table, keyColumns);
        }
        return keyColumns;
    } else {
        for (const auto& referenceTable : InputTables_) {
            if (inputTableFilter(referenceTable)) {
                for (const auto& table : InputTables_) {
                    if (!inputTableFilter(table)) {
                        continue;
                    }

                    if (table->Schema.GetKeyColumns() != referenceTable->Schema.GetKeyColumns()) {
                        THROW_ERROR_EXCEPTION("Key columns do not match: input table %v is sorted by columns %v "
                            "while input table %v is sorted by columns %v",
                            table->GetPath(),
                            table->Schema.GetKeyColumns(),
                            referenceTable->GetPath(),
                            referenceTable->Schema.GetKeyColumns());
                    }
                    validateColumnFilter(table, referenceTable->Schema.GetKeyColumns());
                }
                return referenceTable->Schema.GetKeyColumns();
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

    for (int index = 0; index < prefixColumns.size(); ++index) {
        if (fullColumns[index] != prefixColumns[index]) {
            return false;
        }
    }

    return true;
}

bool TOperationControllerBase::ShouldVerifySortedOutput() const
{
    return true;
}

TOutputOrderPtr TOperationControllerBase::GetOutputOrder() const
{
    return nullptr;
}

bool TOperationControllerBase::CheckParityReplicas() const
{
    return false;
}

bool TOperationControllerBase::IsBoundaryKeysFetchEnabled() const
{
    return false;
}

void TOperationControllerBase::AttachToIntermediateLivePreview(TChunkId chunkId)
{
    if (IsIntermediateLivePreviewSupported()) {
        AttachToLivePreview(chunkId, IntermediateTable->LivePreviewTableId);
    }
}

void TOperationControllerBase::AttachToLivePreview(
    TChunkTreeId chunkTreeId,
    NCypressClient::TNodeId tableId)
{
    Host->AttachChunkTreesToLivePreview(
        AsyncTransaction->GetId(),
        tableId,
        {chunkTreeId});
}

void TOperationControllerBase::RegisterStderr(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    if (!joblet->StderrTableChunkListId) {
        return;
    }

    YT_VERIFY(StderrTable_);

    const auto& chunkListId = joblet->StderrTableChunkListId;
    const auto& result = jobSummary.Result;

    if (!result.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext)) {
        return;
    }
    const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    YT_VERIFY(schedulerResultExt.has_stderr_table_boundary_keys());

    const auto& boundaryKeys = schedulerResultExt.stderr_table_boundary_keys();
    if (boundaryKeys.empty()) {
        return;
    }
    auto key = BuildBoundaryKeysFromOutputResult(boundaryKeys, StderrTable_->GetEdgeDescriptorTemplate(), RowBuffer);
    StderrTable_->OutputChunkTreeIds.emplace_back(key, chunkListId);

    YT_LOG_DEBUG("Stderr chunk tree registered (ChunkListId: %v)",
        chunkListId);
}

void TOperationControllerBase::RegisterCores(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    if (!joblet->CoreTableChunkListId) {
        return;
    }

    YT_VERIFY(CoreTable_);

    const auto& chunkListId = joblet->CoreTableChunkListId;
    const auto& result = jobSummary.Result;

    if (!result.HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext)) {
        return;
    }
    const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    for (const auto& coreInfo : schedulerResultExt.core_infos()) {
        YT_LOG_DEBUG("Core file (JobId: %v, ProcessId: %v, ExecutableName: %v, Size: %v, Error: %v)",
            joblet->JobId,
            coreInfo.process_id(),
            coreInfo.executable_name(),
            coreInfo.size(),
            coreInfo.has_error() ? FromProto<TError>(coreInfo.error()) : TError());
    }

    if (!schedulerResultExt.has_core_table_boundary_keys()) {
        return;
    }
    const auto& boundaryKeys = schedulerResultExt.core_table_boundary_keys();
    if (boundaryKeys.empty()) {
        return;
    }
    auto key = BuildBoundaryKeysFromOutputResult(boundaryKeys, CoreTable_->GetEdgeDescriptorTemplate(), RowBuffer);
    CoreTable_->OutputChunkTreeIds.emplace_back(key, chunkListId);
}

const ITransactionPtr& TOperationControllerBase::GetTransactionForOutputTable(const TOutputTablePtr& table) const
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

    if (table->TableUploadOptions.TableSchema.IsSorted() && ShouldVerifySortedOutput()) {
        YT_VERIFY(chunk->BoundaryKeys());
        YT_VERIFY(chunk->GetRowCount() > 0);
        YT_VERIFY(chunk->GetUniqueKeys() || !table->TableWriterOptions->ValidateUniqueKeys);

        NScheduler::NProto::TOutputResult resultBoundaryKeys;
        resultBoundaryKeys.set_empty(false);
        resultBoundaryKeys.set_sorted(true);
        resultBoundaryKeys.set_unique_keys(chunk->GetUniqueKeys());
        ToProto(resultBoundaryKeys.mutable_min(), chunk->BoundaryKeys()->MinKey);
        ToProto(resultBoundaryKeys.mutable_max(), chunk->BoundaryKeys()->MaxKey);

        key = BuildBoundaryKeysFromOutputResult(resultBoundaryKeys, StandardEdgeDescriptors_[tableIndex], RowBuffer);
    }

    table->OutputChunkTreeIds.emplace_back(key, chunk->ChunkId());

    if (table->Dynamic) {
        table->OutputChunks.push_back(chunk);
    }

    if (IsOutputLivePreviewSupported()) {
        AttachToLivePreview(chunk->ChunkId(), table->LivePreviewTableId);
    }

    RegisterOutputRows(chunk->GetRowCount(), tableIndex);

    YT_LOG_DEBUG("Teleport chunk registered (Table: %v, ChunkId: %v, Key: %v)",
        tableIndex,
        chunk->ChunkId(),
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
            auto chunkId = inputChunk->ChunkId();

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
        auto chunkId = dataSlice->GetSingleUnversionedChunkOrThrow()->ChunkId();
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

    if (RecentSnapshotIndex_) {
        YT_LOG_WARNING("Starting next snapshot without completing previous one (SnapshotIndex: %v)",
            SnapshotIndex_);
    }
    RecentSnapshotIndex_ = SnapshotIndex_++;

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

        Host->AddChunkTreesToUnstageList(chunkTreeIdsToRelease, true /* recursive */);
    }

    RecentSnapshotIndex_    .reset();
    LastSuccessfulSnapshotTime_ = TInstant::Now();
}

void TOperationControllerBase::Dispose()
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    YT_VERIFY(IsFinished());

    {
        TGuard<TSpinLock> guard(JobMetricsDeltaPerTreeLock_);

        for (auto& [treeId, metrics] : JobMetricsDeltaPerTree_) {
            auto totalTime = TotalTimePerTree_.at(treeId);

            switch (State) {
                case EControllerState::Completed:
                    metrics.Values()[EJobMetricName::TotalTimeOperationCompleted] = totalTime;
                    break;

                case EControllerState::Aborted:
                    metrics.Values()[EJobMetricName::TotalTimeOperationAborted] = totalTime;
                    break;

                case EControllerState::Failed:
                    metrics.Values()[EJobMetricName::TotalTimeOperationFailed] = totalTime;
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

    auto headCookie = CompletedJobIdsReleaseQueue_.Checkpoint();
    YT_LOG_INFO("Releasing jobs on controller disposal (HeadCookie: %v)",
        headCookie);
    auto jobIdsToRelease = CompletedJobIdsReleaseQueue_.Release();
    ReleaseJobs(jobIdsToRelease);
}

void TOperationControllerBase::UpdateRuntimeParameters(const TOperationRuntimeParametersUpdatePtr& update)
{
    if (update->Acl) {
        Acl = *update->Acl;
    }
}

TOperationJobMetrics TOperationControllerBase::PullJobMetricsDelta(bool force)
{
    TGuard<TSpinLock> guard(JobMetricsDeltaPerTreeLock_);

    auto now = NProfiling::GetCpuInstant();
    if (!force && LastJobMetricsDeltaReportTime_ + DurationToCpuDuration(Config->JobMetricsReportPeriod) > now) {
        return {};
    }

    TOperationJobMetrics result;
    for (auto& pair : JobMetricsDeltaPerTree_) {
        const auto& treeId = pair.first;
        auto& delta = pair.second;
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
    TGuard<TSpinLock> guard(AlertsLock_);
    return Alerts_;
}

TOperationInfo TOperationControllerBase::BuildOperationInfo()
{
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

    result.JobSplitter =
        BuildYsonStringFluently<EYsonType::MapFragment>()
            .Do(std::bind(&TOperationControllerBase::BuildJobSplitterInfo, this, _1))
        .Finish();

    result.MemoryUsage = GetMemoryUsage();

    result.ControllerState = State;

    return result;
}

ssize_t TOperationControllerBase::GetMemoryUsage() const
{
    return GetMemoryUsageForTag(MemoryTag_);
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
    YT_VERIFY(JobletMap.insert(std::make_pair(joblet->JobId, joblet)).second);
}

TJobletPtr TOperationControllerBase::FindJoblet(TJobId jobId) const
{
    auto it = JobletMap.find(jobId);
    return it == JobletMap.end() ? nullptr : it->second;
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
            NScheduler::EErrorCode::NoSuchJob,
            "No such job %v",
            jobId);
    }
    return joblet;
}

void TOperationControllerBase::UnregisterJoblet(const TJobletPtr& joblet)
{
    YT_VERIFY(JobletMap.erase(joblet->JobId) == 1);
}

std::vector<TJobId> TOperationControllerBase::GetJobIdsByTreeId(const TString& treeId)
{
    std::vector<TJobId> jobIds;
    for (const auto& [jobId, joblet] : JobletMap) {
        if (joblet->TreeId == treeId) {
            jobIds.push_back(jobId);
        }
    }
    return jobIds;
}

void TOperationControllerBase::SetProgressUpdated()
{
    ShouldUpdateProgressInCypress_.store(false);
}

bool TOperationControllerBase::ShouldUpdateProgress() const
{
    return HasProgress() && ShouldUpdateProgressInCypress_;
}

bool TOperationControllerBase::HasProgress() const
{
    if (!IsPrepared()) {
        return false;
    }

    {
        TGuard<TSpinLock> guard(ProgressLock_);
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
        .Item("input_table_paths").ListLimited(inputPaths, 1)
        .Item("output_table_paths").ListLimited(outputPaths, 1);
}

void TOperationControllerBase::BuildProgress(TFluentMap fluent) const
{
    if (!IsPrepared()) {
        return;
    }

    fluent
        .Item("state").Value(State.load())
        .Item("build_time").Value(TInstant::Now())
        .Item("ready_job_count").Value(GetPendingJobCount())
        .Item("job_statistics").Value(JobStatistics)
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
            .Item("output_supported").Value(IsOutputLivePreviewSupported())
            .Item("intermediate_supported").Value(IsIntermediateLivePreviewSupported())
            .Item("stderr_supported").Value(static_cast<bool>(StderrTable_))
        .EndMap()
        .Item("schedule_job_statistics").BeginMap()
            .Item("count").Value(ScheduleJobStatistics_->Count)
            .Item("duration").Value(ScheduleJobStatistics_->Duration)
            .Item("failed").Value(ScheduleJobStatistics_->Failed)
        .EndMap()
        .DoIf(DataFlowGraph_.operator bool(), [=] (TFluentMap fluent) {
            fluent
                .Item("jobs").Value(DataFlowGraph_->GetTotalJobCounter())
                .Item("data_flow_graph").BeginMap()
                    .Do(BIND(&TDataFlowGraph::BuildLegacyYson, DataFlowGraph_))
                .EndMap();
        })
        .DoIf(EstimatedInputDataSizeHistogram_.operator bool(), [=] (TFluentMap fluent) {
            EstimatedInputDataSizeHistogram_->BuildHistogramView();
            fluent
                .Item("estimated_input_data_size_histogram").Value(*EstimatedInputDataSizeHistogram_);
        })
        .DoIf(InputDataSizeHistogram_.operator bool(), [=] (TFluentMap fluent) {
            InputDataSizeHistogram_->BuildHistogramView();
            fluent
                .Item("input_data_size_histogram").Value(*InputDataSizeHistogram_);
        })
        .Item("snapshot_index").Value(SnapshotIndex_)
        .Item("recent_snapshot_index").Value(RecentSnapshotIndex_)
        .Item("last_successful_snapshot_time").Value(LastSuccessfulSnapshotTime_);
}

void TOperationControllerBase::BuildBriefProgress(TFluentMap fluent) const
{
    if (IsPrepared() && DataFlowGraph_) {
        fluent
            .Item("state").Value(State.load())
            .Item("jobs").Do(BIND([&] (TFluentAny fluent) {
                SerializeBriefVersion(DataFlowGraph_->GetTotalJobCounter(), fluent.GetConsumer());
            }))
            .Item("build_time").Value(TInstant::Now());
    }
}

void TOperationControllerBase::BuildAndSaveProgress()
{
    YT_LOG_DEBUG("Building and saving progress");

    auto progressString = BuildYsonStringFluently()
        .BeginMap()
        .Do([=] (TFluentMap fluent) {
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
            .Do([=] (TFluentMap fluent) {
                auto asyncResult = WaitFor(
                    BIND(&TOperationControllerBase::BuildBriefProgress, MakeStrong(this))
                        .AsyncVia(GetInvoker())
                        .Run(fluent));
                asyncResult
                    .ThrowOnError();
            })
        .EndMap();

    {
        TGuard<TSpinLock> guard(ProgressLock_);
        if (!ProgressString_ || ProgressString_ != progressString ||
            !BriefProgressString_ || BriefProgressString_ != briefProgressString)
        {
            ShouldUpdateProgressInCypress_.store(true);
            YT_LOG_DEBUG("New progress is different from previous one, should update progress in Cypress");
        }
        ProgressString_ = progressString;
        BriefProgressString_ = briefProgressString;
    }
    YT_LOG_DEBUG("Progress built and saved");
}

TYsonString TOperationControllerBase::GetProgress() const
{
    TGuard<TSpinLock> guard(ProgressLock_);
    return ProgressString_;
}

TYsonString TOperationControllerBase::GetBriefProgress() const
{
    TGuard<TSpinLock> guard(ProgressLock_);
    return BriefProgressString_;
}

TYsonString TOperationControllerBase::BuildJobYson(TJobId id, bool outputStatistics) const
{
    TCallback<void(TFluentMap)> attributesBuilder;

    // Case of running job.
    {
        auto joblet = FindJoblet(id);
        if (joblet) {
            attributesBuilder = BIND(
                &TOperationControllerBase::BuildJobAttributes,
                MakeStrong(this),
                joblet,
                EJobState::Running,
                outputStatistics,
                joblet->StderrSize);
        } else {
            attributesBuilder = BIND([] (TFluentMap) {});
        }
    }

    YT_VERIFY(attributesBuilder);

    return BuildYsonStringFluently()
        .BeginMap()
            .Do(attributesBuilder)
        .EndMap();
}

IYPathServicePtr TOperationControllerBase::GetOrchid() const
{
    if (CancelableContext->IsCanceled()) {
        return nullptr;
    }
    return Orchid_;
}

void TOperationControllerBase::BuildJobsYson(TFluentMap fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    auto now = GetInstant();
    if (CachedRunningJobsUpdateTime_ + Config->CachedRunningJobsUpdatePeriod < now) {
        CachedRunningJobsYson_ = BuildYsonStringFluently<EYsonType::MapFragment>()
            .DoFor(JobletMap, [&] (TFluentMap fluent, const std::pair<TJobId, TJobletPtr>& pair) {
                auto jobId = pair.first;
                const auto& joblet = pair.second;
                if (joblet->StartTime) {
                    fluent.Item(ToString(jobId)).BeginMap()
                        .Do([&] (TFluentMap fluent) {
                            BuildJobAttributes(
                                joblet,
                                EJobState::Running,
                                /* outputStatistics */ false,
                                joblet->StderrSize,
                                fluent);
                        })
                    .EndMap();
                }
            })
            .Finish();
        CachedRunningJobsUpdateTime_ = now;
    }

    fluent.GetConsumer()->OnRaw(CachedRunningJobsYson_);
}

void TOperationControllerBase::BuildRetainedFinishedJobsYson(TFluentMap fluent) const
{
    for (const auto& [jobId, attributes] : RetainedFinishedJobs_) {
        fluent
            .Item(ToString(jobId)).Value(attributes);
    }
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

TSharedRef TOperationControllerBase::SafeBuildJobSpecProto(const TJobletPtr& joblet)
{
    return joblet->Task->BuildJobSpecProto(joblet);
}

TSharedRef TOperationControllerBase::ExtractJobSpec(TJobId jobId) const
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::GetJobSpec));

    if (auto getJobSpecDelay = Spec_->TestingOperationOptions->GetJobSpecDelay) {
        Sleep(*getJobSpecDelay);
    }

    if (Spec_->TestingOperationOptions->FailGetJobSpec) {
        THROW_ERROR_EXCEPTION("Testing failure");
    }

    auto joblet = GetJobletOrThrow(jobId);
    if (!joblet->JobSpecProtoFuture) {
        THROW_ERROR_EXCEPTION("Spec of job %v is missing", jobId);
    }

    auto result = WaitFor(joblet->JobSpecProtoFuture)
        .ValueOrThrow();
    joblet->JobSpecProtoFuture.Reset();

    return result;
}

TYsonString TOperationControllerBase::GetSuspiciousJobsYson() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(CachedSuspiciousJobsYsonLock_);
    return CachedSuspiciousJobsYson_;
}

void TOperationControllerBase::UpdateSuspiciousJobsYson()
{
    VERIFY_INVOKER_AFFINITY(CancelableInvokerPool->GetInvoker(EOperationControllerQueue::Default));

    // We sort suspicious jobs by their last activity time and then
    // leave top `MaxOrchidEntryCountPerType` for each job type.

    std::vector<TJobletPtr> suspiciousJoblets;
    for (const auto& [jobId, joblet] : JobletMap) {
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
        TWriterGuard guard(CachedSuspiciousJobsYsonLock_);
        CachedSuspiciousJobsYson_ = yson;
    }
}

void TOperationControllerBase::ReleaseJobs(const std::vector<TJobId>& jobIds)
{
    std::vector<TJobToRelease> jobsToRelease;
    jobsToRelease.reserve(jobIds.size());

    for (auto jobId : jobIds) {
        TReleaseJobFlags releaseFlags;
        auto it = FinishedJobs_.find(jobId);
        if (it != FinishedJobs_.end()) {
            const auto& jobSummary = it->second->Summary;
            releaseFlags = jobSummary.ReleaseFlags;
            FinishedJobs_.erase(it);
        }
        jobsToRelease.emplace_back(TJobToRelease{jobId, releaseFlags});
    }
    Host->ReleaseJobs(jobsToRelease);
}

void TOperationControllerBase::AnalyzeBriefStatistics(
    const TJobletPtr& job,
    const TSuspiciousJobsOptionsPtr& options,
    const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError)
{
    if (!briefStatisticsOrError.IsOK()) {
        if (job->BriefStatistics) {
            // Failures in brief statistics building are normal during job startup,
            // when readers and writers are not built yet. After we successfully built
            // brief statistics once, we shouldn't fail anymore.

            YT_LOG_WARNING(
                briefStatisticsOrError,
                "Failed to build brief job statistics (JobId: %v)",
                job->JobId);
        }

        return;
    }

    const auto& briefStatistics = briefStatisticsOrError.Value();

    bool wasActive = !job->BriefStatistics ||
        CheckJobActivity(
            job->BriefStatistics,
            briefStatistics,
            options,
            job->JobType);

    bool wasSuspicious = job->Suspicious;
    job->Suspicious = (!wasActive && briefStatistics->Timestamp - job->LastActivityTime > options->InactivityTimeout);
    if (!wasSuspicious && job->Suspicious) {
        YT_LOG_DEBUG("Found a suspicious job (JobId: %v, JobType: %v, LastActivityTime: %v, SuspiciousInactivityTimeout: %v, "
            "OldBriefStatistics: %v, NewBriefStatistics: %v)",
            job->JobId,
            job->JobType,
            job->LastActivityTime,
            options->InactivityTimeout,
            job->BriefStatistics,
            briefStatistics);
    }

    job->BriefStatistics = briefStatistics;

    if (wasActive) {
        job->LastActivityTime = job->BriefStatistics->Timestamp;
    }
}

void TOperationControllerBase::UpdateJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    YT_VERIFY(jobSummary.Statistics);

    // NB: There is a copy happening here that can be eliminated.
    auto statistics = *jobSummary.Statistics;
    YT_LOG_TRACE("Job data statistics (JobId: %v, Input: %v, Output: %v)",
        jobSummary.Id,
        GetTotalInputDataStatistics(statistics),
        GetTotalOutputDataStatistics(statistics));

    auto statisticsState = GetStatisticsJobState(joblet, jobSummary.State);
    auto statisticsSuffix = JobHelper.GetStatisticsSuffix(statisticsState, joblet->JobType);
    statistics.AddSuffixToNames(statisticsSuffix);
    JobStatistics.Update(statistics);
}

void TOperationControllerBase::UpdateJobMetrics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    YT_LOG_TRACE("Updating job metrics (JobId: %v)", joblet->JobId);

    auto delta = joblet->UpdateJobMetrics(jobSummary);
    {
        TGuard<TSpinLock> guard(JobMetricsDeltaPerTreeLock_);

        auto it = JobMetricsDeltaPerTree_.find(joblet->TreeId);
        if (it == JobMetricsDeltaPerTree_.end()) {
            YT_VERIFY(JobMetricsDeltaPerTree_.insert(std::make_pair(joblet->TreeId, delta)).second);
        } else {
            it->second += delta;
        }

        TotalTimePerTree_[joblet->TreeId] += delta.Values()[EJobMetricName::TotalTime];
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
        YT_LOG_DEBUG("Progress: %v", GetLoggingProgress());
    }
}

void TOperationControllerBase::BuildJobSplitterInfo(TFluentMap fluent) const
{
    VERIFY_INVOKER_AFFINITY(InvokerPool->GetInvoker(EOperationControllerQueue::Default));

    if (IsPrepared() && JobSplitter_) {
        JobSplitter_->BuildJobSplitterInfo(fluent);
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

TCellTag TOperationControllerBase::GetIntermediateOutputCellTag() const
{
    return IntermediateOutputCellTag;
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

const TOutputTablePtr& TOperationControllerBase::StderrTable() const
{
    return StderrTable_;
}

const TOutputTablePtr& TOperationControllerBase::CoreTable() const
{
    return CoreTable_;
}

IJobSplitter* TOperationControllerBase::GetJobSplitter()
{
    return JobSplitter_.get();
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
    return UnavailableInputChunkCount;
}

int TOperationControllerBase::GetTotalJobCount() const
{
    VERIFY_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    // Avoid accessing the state while not prepared.
    if (!IsPrepared()) {
        return 0;
    }

    return GetDataFlowGraph()->GetTotalJobCounter()->GetTotal();
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
    NScheduler::NProto::TUserJobSpec* jobSpec,
    const TUserJobSpecPtr& config,
    const std::vector<TUserFile>& files,
    const TString& fileAccount)
{
    jobSpec->set_shell_command(config->Command);
    if (config->JobTimeLimit) {
        jobSpec->set_job_time_limit(ToProto<i64>(*config->JobTimeLimit));
    }
    jobSpec->set_prepare_time_limit(ToProto<i64>(config->PrepareTimeLimit));
    jobSpec->set_memory_limit(config->MemoryLimit);
    jobSpec->set_include_memory_mapped_files(config->IncludeMemoryMappedFiles);
    jobSpec->set_use_yamr_descriptors(config->UseYamrDescriptors);
    jobSpec->set_check_input_fully_consumed(config->CheckInputFullyConsumed);
    jobSpec->set_max_stderr_size(config->MaxStderrSize);
    jobSpec->set_max_profile_size(config->MaxProfileSize);
    jobSpec->set_custom_statistics_count_limit(config->CustomStatisticsCountLimit);
    jobSpec->set_copy_files(config->CopyFiles);
    jobSpec->set_file_account(fileAccount);
    jobSpec->set_set_container_cpu_limit(config->SetContainerCpuLimit);
    jobSpec->set_force_core_dump(config->ForceCoreDump);

    jobSpec->set_port_count(config->PortCount);
    jobSpec->set_use_porto_memory_tracking(config->UsePortoMemoryTracking);

    if (Config->EnableTmpfs) {
        for (const auto& volume : config->TmpfsVolumes) {
            ToProto(jobSpec->add_tmpfs_volumes(), *volume);
        }
    }

    if (config->DiskRequest) {
        auto mediumDirectory = GetMediumDirectory();
        auto* mediumDescriptor = mediumDirectory->FindByName(config->DiskRequest->MediumName);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION("Unknown medium %Qv", config->DiskRequest->MediumName);
        }

        config->DiskRequest->MediumIndex = mediumDescriptor->Index;

        ToProto(jobSpec->mutable_disk_request(), *config->DiskRequest);

        // COMPAT(ignat): remove after nodes update.
        jobSpec->set_disk_space_limit(config->DiskRequest->DiskSpace);
        if (config->DiskRequest->InodeCount) {
            jobSpec->set_inode_limit(*config->DiskRequest->InodeCount);
        }
    }
    if (config->InterruptionSignal) {
        jobSpec->set_interruption_signal(*config->InterruptionSignal);
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

        if (config->Format) {
            inputFormat = outputFormat = *config->Format;
        }

        if (config->InputFormat) {
            inputFormat = *config->InputFormat;
        }

        if (config->OutputFormat) {
            outputFormat = *config->OutputFormat;
        }

        jobSpec->set_input_format(ConvertToYsonString(inputFormat).GetData());
        jobSpec->set_output_format(ConvertToYsonString(outputFormat).GetData());
    }

    jobSpec->set_enable_setup_commands(config->EnableSetupCommands);
    jobSpec->set_enable_gpu_layers(config->EnableGpuLayers);

    if (config->CudaToolkitVersion) {
        jobSpec->set_cuda_toolkit_version(*config->CudaToolkitVersion);
    }

    if (config->NetworkProject) {
        const auto& client = Host->GetClient();
        const auto networkProjectPath = "//sys/network_projects/" + ToYPathLiteral(*config->NetworkProject);
        auto checkPermissionRspOrError = WaitFor(client->CheckPermission(AuthenticatedUser,
            networkProjectPath,
            EPermission::Use));
        if (checkPermissionRspOrError.ValueOrThrow().Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("User %Qv is not allowed to use network project %Qv",
                AuthenticatedUser,
                *config->NetworkProject);
        }

        auto getRspOrError = WaitFor(client->GetNode(networkProjectPath + "/@project_id"));
        jobSpec->set_network_project_id(ConvertTo<ui32>(getRspOrError.ValueOrThrow()));
    }

    // COMPAT(gritukan): Drop it when nodes will be fresh enough.
    jobSpec->set_write_sparse_core_dumps(true);

    jobSpec->set_enable_porto(static_cast<int>(config->EnablePorto.value_or(Config->DefaultEnablePorto)));
    jobSpec->set_fail_job_on_core_dump(config->FailJobOnCoreDump);
    jobSpec->set_enable_cuda_gpu_core_dump(GetEnableCudaGpuCoreDump());

    auto fillEnvironment = [&] (THashMap<TString, TString>& env) {
        for (const auto& [key, value] : env) {
            jobSpec->add_environment(Format("%v=%v", key, value));
        }
    };

    // Global environment.
    fillEnvironment(Config->Environment);

    // Local environment.
    fillEnvironment(config->Environment);

    jobSpec->add_environment(Format("YT_OPERATION_ID=%v", OperationId));

    if (config->EnableProfiling) {
        jobSpec->add_environment(Format("YT_PROFILE_JOB=1"));
    }

    BuildFileSpecs(jobSpec, files);
}

const std::vector<TUserFile>& TOperationControllerBase::GetUserFiles(const TUserJobSpecPtr& userJobSpec) const
{
    return GetOrCrash(UserJobFiles_, userJobSpec);
}

void TOperationControllerBase::InitUserJobSpec(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    ToProto(jobSpec->mutable_debug_output_transaction_id(), DebugTransaction->GetId());

    i64 memoryReserve = joblet->EstimatedResourceUsage.GetUserJobMemory() * *joblet->UserJobMemoryReserveFactor;
    // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
    // This is ensured by adjusting memory reserve factor in user job config as initialization,
    // but just in case we also limit the actual memory_reserve value here.
    if (jobSpec->has_tmpfs_size()) {
        memoryReserve = std::max(memoryReserve, jobSpec->tmpfs_size());
    }
    jobSpec->set_memory_reserve(memoryReserve);

    jobSpec->add_environment(Format("YT_JOB_INDEX=%v", joblet->JobIndex));
    jobSpec->add_environment(Format("YT_TASK_JOB_INDEX=%v", joblet->TaskJobIndex));
    jobSpec->add_environment(Format("YT_JOB_ID=%v", joblet->JobId));
    jobSpec->add_environment(Format("YT_JOB_COOKIE=%v", joblet->OutputCookie));
    if (joblet->StartRowIndex >= 0) {
        jobSpec->add_environment(Format("YT_START_ROW_INDEX=%v", joblet->StartRowIndex));
    }

    if (SecureVault) {
        // NB: These environment variables should be added to user job spec, not to the user job spec template.
        // They may contain sensitive information that should not be persisted with a controller.

        // We add a single variable storing the whole secure vault and all top-level scalar values.
        jobSpec->add_environment(Format("YT_SECURE_VAULT=%v",
            ConvertToYsonString(SecureVault, EYsonFormat::Text)));

        for (const auto& [key, node] : SecureVault->GetChildren()) {
            TString value;
            if (node->GetType() == ENodeType::Int64) {
                value = ToString(node->GetValue<i64>());
            } else if (node->GetType() == ENodeType::Uint64) {
                value = ToString(node->GetValue<ui64>());
            } else if (node->GetType() == ENodeType::Boolean) {
                value = ToString(node->GetValue<bool>());
            } else if (node->GetType() == ENodeType::Double) {
                value = ToString(node->GetValue<double>());
            } else if (node->GetType() == ENodeType::String) {
                value = node->GetValue<TString>();
            } else {
                // We do not export composite values as a separate environment variables.
                continue;
            }
            jobSpec->add_environment(Format("YT_SECURE_VAULT_%v=%v", key, value));
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
}

void TOperationControllerBase::AddStderrOutputSpecs(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    auto* stderrTableSpec = jobSpec->mutable_stderr_table_spec();
    auto* outputSpec = stderrTableSpec->mutable_output_table_spec();
    outputSpec->set_table_writer_options(ConvertToYsonString(StderrTable_->TableWriterOptions).GetData());
    ToProto(outputSpec->mutable_table_schema(), StderrTable_->TableUploadOptions.TableSchema);
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->StderrTableChunkListId);

    auto writerConfig = GetStderrTableWriterConfig();
    YT_VERIFY(writerConfig);
    stderrTableSpec->set_blob_table_writer_config(ConvertToYsonString(writerConfig).GetData());
}

void TOperationControllerBase::AddCoreOutputSpecs(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet) const
{
    auto* coreTableSpec = jobSpec->mutable_core_table_spec();
    auto* outputSpec = coreTableSpec->mutable_output_table_spec();
    outputSpec->set_table_writer_options(ConvertToYsonString(CoreTable_->TableWriterOptions).GetData());
    ToProto(outputSpec->mutable_table_schema(), CoreTable_->TableUploadOptions.TableSchema);
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->CoreTableChunkListId);

    auto writerConfig = GetCoreTableWriterConfig();
    YT_VERIFY(writerConfig);
    coreTableSpec->set_blob_table_writer_config(ConvertToYsonString(writerConfig).GetData());
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
            double replicationFactor = (double) codec->GetTotalPartCount() / codec->GetDataPartCount();
            result += static_cast<i64>(ioConfig->TableWriter->DesiredChunkSize * replicationFactor);
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

NTableClient::TTableReaderOptionsPtr TOperationControllerBase::CreateTableReaderOptions(TJobIOConfigPtr ioConfig)
{
    auto options = New<TTableReaderOptions>();
    options->EnableRowIndex = ioConfig->ControlAttributes->EnableRowIndex;
    options->EnableTableIndex = ioConfig->ControlAttributes->EnableTableIndex;
    options->EnableRangeIndex = ioConfig->ControlAttributes->EnableRangeIndex;
    options->EnableTabletIndex = ioConfig->ControlAttributes->EnableTabletIndex;
    return options;
}

void TOperationControllerBase::ValidateUserFileCount(TUserJobSpecPtr spec, const TString& operation)
{
    if (spec->FilePaths.size() > Config->MaxUserFileCount) {
        THROW_ERROR_EXCEPTION("Too many user files in %v: maximum allowed %v, actual %v",
            operation,
            Config->MaxUserFileCount,
            spec->FilePaths.size());
    }
}

void TOperationControllerBase::OnExecNodesUpdated()
{ }

void TOperationControllerBase::GetExecNodesInformation()
{
    auto now = NProfiling::GetCpuInstant();
    if (now < GetExecNodesInformationDeadline_) {
        return;
    }

    OnlineExecNodeCount_ = Host->GetOnlineExecNodeCount();
    ExecNodesDescriptors_ = Host->GetExecNodeDescriptors(NScheduler::TSchedulingTagFilter(Spec_->SchedulingTagFilter));
    OnlineExecNodesDescriptors_ = Host->GetExecNodeDescriptors(NScheduler::TSchedulingTagFilter(Spec_->SchedulingTagFilter), /* onlineOnly */ true);

    GetExecNodesInformationDeadline_ = now + NProfiling::DurationToCpuDuration(Config->ControllerExecNodeInfoUpdatePeriod);

    OnExecNodesUpdated();
    YT_LOG_DEBUG("Exec nodes information updated (SuitableExecNodeCount: %v, OnlineExecNodeCount: %v)", ExecNodesDescriptors_->size(), OnlineExecNodeCount_);
}

int TOperationControllerBase::GetOnlineExecNodeCount()
{
    GetExecNodesInformation();
    return OnlineExecNodeCount_;
}

const TExecNodeDescriptorMap& TOperationControllerBase::GetOnlineExecNodeDescriptors()
{
    GetExecNodesInformation();
    return *OnlineExecNodesDescriptors_;
}

const TExecNodeDescriptorMap& TOperationControllerBase::GetExecNodeDescriptors()
{
    GetExecNodesInformation();
    return *ExecNodesDescriptors_;
}

bool TOperationControllerBase::ShouldSkipSanityCheck()
{
    if (GetOnlineExecNodeCount() < Config->SafeOnlineNodeCount) {
        return true;
    }

    if (TInstant::Now() < Host->GetConnectionTime() + Config->SafeSchedulerOnlineTime) {
        return true;
    }

    if (!CachedMaxAvailableExecNodeResources_) {
        return true;
    }

    return false;
}

void TOperationControllerBase::InferSchemaFromInput(const TKeyColumns& keyColumns)
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

    if (OutputTables_[0]->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
        OutputTables_[0]->TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(keyColumns);
    } else {
        auto schema = InputTables_[0]->Schema
            .ToStrippedColumnAttributes()
            .ToCanonical();

        for (const auto& table : InputTables_) {
            if (table->Schema.ToStrippedColumnAttributes().ToCanonical() != schema) {
                THROW_ERROR_EXCEPTION("Cannot infer output schema from input in strong schema mode, tables have incompatible schemas");
            }
        }

        OutputTables_[0]->TableUploadOptions.TableSchema = InputTables_[0]->Schema
            .ToSorted(keyColumns)
            .ToSortedStrippedColumnAttributes()
            .ToCanonical();

        if (InputTables_[0]->Schema.HasNontrivialSchemaModification()) {
            OutputTables_[0]->TableUploadOptions.TableSchema.SetSchemaModification(
                InputTables_[0]->Schema.GetSchemaModification());
        }
    }

    FilterOutputSchemaByInputColumnSelectors();
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
        FilterOutputSchemaByInputColumnSelectors();
        return;
    }

    InferSchemaFromInput();
}

void TOperationControllerBase::FilterOutputSchemaByInputColumnSelectors()
{
    THashSet<TString> columns;
    for (const auto& table : InputTables_) {
        if (auto selectors = table->Path.GetColumns()) {
            for (const auto& column : *selectors) {
                columns.insert(column);
            }
        } else {
            return;
        }
    }

    OutputTables_[0]->TableUploadOptions.TableSchema =
        OutputTables_[0]->TableUploadOptions.TableSchema.Filter(columns);
}

void TOperationControllerBase::ValidateOutputSchemaOrdered() const
{
    YT_VERIFY(OutputTables_.size() == 1);
    YT_VERIFY(InputTables_.size() >= 1);

    if (InputTables_.size() > 1 && OutputTables_[0]->TableUploadOptions.TableSchema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Cannot generate sorted output for ordered operation with multiple input tables")
            << TErrorAttribute("output_schema", OutputTables_[0]->TableUploadOptions.TableSchema);
    }
}

void TOperationControllerBase::ValidateOutputSchemaCompatibility(bool ignoreSortOrder, bool validateComputedColumns) const
{
    YT_VERIFY(OutputTables_.size() == 1);

    auto hasComputedColumn = OutputTables_[0]->TableUploadOptions.TableSchema.HasComputedColumns();

    for (const auto& inputTable : InputTables_) {
        if (inputTable->SchemaMode == ETableSchemaMode::Strong) {
            // NB for historical reasons we consider optional<T> to be compatible with T when T is simple
            // check is performed during operation.
            ValidateTableSchemaCompatibility(
                inputTable->Schema.Filter(inputTable->Path.GetColumns()),
                OutputTables_[0]->TableUploadOptions.GetUploadSchema(),
                ignoreSortOrder,
                /*allowSimpleTypeDeoptionalize*/ true)
                .ThrowOnError();
        } else if (hasComputedColumn && validateComputedColumns) {
            // Input table has weak schema, so we cannot check if all
            // computed columns were already computed. At least this is weird.
            THROW_ERROR_EXCEPTION("Output table cannot have computed "
                "columns, which are not present in all input tables");
        }
    }
}

TJobSplitterConfigPtr TOperationControllerBase::GetJobSplitterConfig() const
{
    return nullptr;
}

void TOperationControllerBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SnapshotIndex_);
    Persist(context, TotalEstimatedInputChunkCount);
    Persist(context, TotalEstimatedInputUncompressedDataSize);
    Persist(context, TotalEstimatedInputRowCount);
    Persist(context, TotalEstimatedInputCompressedDataSize);
    Persist(context, TotalEstimatedInputDataWeight);
    Persist(context, UnavailableInputChunkCount);
    Persist(context, UnavailableIntermediateChunkCount);
    Persist(context, InputNodeDirectory_);
    Persist(context, InputTables_);
    Persist(context, OutputTables_);
    Persist(context, StderrTable_);
    Persist(context, CoreTable_);
    Persist(context, IntermediateTable);
    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, UserJobFiles_);
    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, LivePreviewChunks_);
    Persist(context, Tasks);
    Persist(context, TaskGroups);
    Persist(context, InputChunkMap);
    Persist(context, IntermediateOutputCellTag);
    Persist(context, CellTagToRequiredOutputChunkListCount_);
    Persist(context, CellTagToRequiredDebugChunkListCount_);
    Persist(context, CachedPendingJobCount);
    Persist(context, CachedNeededResources);
    Persist(context, ChunkOriginMap);
    Persist(context, JobletMap);
    Persist(context, JobIndexGenerator);
    Persist(context, JobStatistics);
    Persist(context, ScheduleJobStatistics_);
    Persist(context, RowCountLimitTableIndex);
    Persist(context, RowCountLimit);
    Persist(context, EstimatedInputDataSizeHistogram_);
    Persist(context, InputDataSizeHistogram_);
    Persist(context, RetainedJobWithStderrCount_);
    Persist(context, RetainedJobsCoreInfoCount_);
    Persist(context, RetainedJobCount_);
    Persist(context, FinishedJobs_);
    Persist(context, JobSpecCompletedArchiveCount_);
    Persist(context, Sinks_);
    Persist(context, AutoMergeTaskGroup);
    Persist(context, AutoMergeTasks);
    Persist(context, AutoMergeJobSpecTemplates_);
    Persist<TUniquePtrSerializer<>>(context, AutoMergeDirector_);
    Persist(context, JobSplitter_);
    Persist(context, DataFlowGraph_);
    Persist(context, AvailableExecNodesObserved_);
    Persist(context, BannedNodeIds_);
    Persist(context, PathToOutputTable_);
    Persist(context, Acl);
    Persist(context, BannedTreeIds_);
    Persist(context, PathToInputTables_);
    Persist(context, JobMetricsDeltaPerTree_);
    Persist(context, TotalTimePerTree_);
    Persist(context, CompletedRowCount_);

    // NB: Keep this at the end of persist as it requires some of the previous
    // fields to be already initialized.
    if (context.IsLoad()) {
        for (const auto& task : Tasks) {
            task->Initialize();
        }
        InitUpdatingTables();
        InitializeOrchid();
    }
}

void TOperationControllerBase::InitAutoMergeJobSpecTemplates()
{
    // TODO(max42): should this really belong to TOperationControllerBase?
    // We can possibly move it to TAutoMergeTask itself.

    AutoMergeJobSpecTemplates_.resize(OutputTables_.size());
    for (int tableIndex = 0; tableIndex < OutputTables_.size(); ++tableIndex) {
        AutoMergeJobSpecTemplates_[tableIndex].set_type(static_cast<int>(EJobType::UnorderedMerge));
        auto* schedulerJobSpecExt = AutoMergeJobSpecTemplates_[tableIndex]
            .MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(
            ConvertToYsonString(CreateTableReaderOptions(Spec_->AutoMerge->JobIO)).GetData());

        auto dataSourceDirectory = New<TDataSourceDirectory>();
        // NB: chunks read by auto-merge jobs have table index set to output table index,
        // so we need to specify several unused data sources before actual one.
        dataSourceDirectory->DataSources().resize(tableIndex);
        dataSourceDirectory->DataSources().push_back(MakeUnversionedDataSource(
            IntermediatePath,
            OutputTables_[tableIndex]->TableUploadOptions.TableSchema,
            /* columns */ std::nullopt,
            /* omittedInaccessibleColumns */ {}));

        NChunkClient::NProto::TDataSourceDirectoryExt dataSourceDirectoryExt;
        ToProto(&dataSourceDirectoryExt, dataSourceDirectory);
        SetProtoExtension(schedulerJobSpecExt->mutable_extensions(), dataSourceDirectoryExt);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(Spec_->AutoMerge->JobIO).GetData());
    }
}

void TOperationControllerBase::ValidateRevivalAllowed() const
{
    if (Spec_->FailOnJobRestart) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Cannot revive operation when spec option fail_on_job_restart is set")
                << TErrorAttribute("operation_type", OperationType);
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

void TOperationControllerBase::OnChunksReleased(int /* chunkCount */)
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
    options->PlacementId = GetOperationId();
    options->TableIndex = 0;
    return options;
}

TEdgeDescriptor TOperationControllerBase::GetIntermediateEdgeDescriptorTemplate() const
{
    TEdgeDescriptor descriptor;
    descriptor.CellTag = GetIntermediateOutputCellTag();
    descriptor.TableWriterOptions = GetIntermediateTableWriterOptions();
    descriptor.TableWriterConfig = BuildYsonStringFluently()
        .BeginMap()
            .Item("upload_replication_factor").Value(Spec_->IntermediateDataReplicationFactor)
            .Item("min_upload_replication_factor").Value(1)
            .Item("populate_cache").Value(true)
            .Item("sync_on_close").Value(false)
            .DoIf(Spec_->IntermediateDataReplicationFactor > 1, [&] (TFluentMap fluent) {
                // Set reduced rpc_timeout if replication_factor is greater than one.
                fluent.Item("node_rpc_timeout").Value(TDuration::Seconds(120));
            })
        .EndMap();

    descriptor.RequiresRecoveryInfo = true;
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

void TOperationControllerBase::RegisterLivePreviewChunk(
    const TDataFlowGraph::TVertexDescriptor& vertexDescriptor,
    int index,
    const TInputChunkPtr& chunk)
{
    YT_VERIFY(LivePreviewChunks_.insert(std::make_pair(
        chunk,
        TLivePreviewChunkDescriptor{vertexDescriptor, index})).second);

    DataFlowGraph_->RegisterLivePreviewChunk(vertexDescriptor, index, chunk);
}

const IThroughputThrottlerPtr& TOperationControllerBase::GetJobSpecSliceThrottler() const
{
    return Host->GetJobSpecSliceThrottler();
}

void TOperationControllerBase::FinishTaskInput(const TTaskPtr& task)
{
    task->FinishInput(TDataFlowGraph::SourceDescriptor);
}

void TOperationControllerBase::SetOperationAlert(EOperationAlertType alertType, const TError& alert)
{
    TGuard<TSpinLock> guard(AlertsLock_);

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
    for (const auto& task : AutoMergeTasks) {
        if (task && !task->IsCompleted()) {
            return false;
        }
    }
    return true;
}

TString TOperationControllerBase::WriteCoreDump() const
{
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
            OnOperationCompleted(true /* interrupted */);
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

TOutputTablePtr TOperationControllerBase::RegisterOutputTable(const TRichYPath& outputTablePath)
{
    auto it = PathToOutputTable_.find(outputTablePath.GetPath());
    if (it != PathToOutputTable_.end()) {
        const auto& lhsAttributes = it->second->Path.Attributes();
        const auto& rhsAttributes = outputTablePath.Attributes();
        if (lhsAttributes != rhsAttributes) {
            THROW_ERROR_EXCEPTION("Output table %v appears twice with different attributes", outputTablePath.GetPath())
                << TErrorAttribute("lhs_attributes", lhsAttributes)
                << TErrorAttribute("rhs_attributes", rhsAttributes);
        }
        return it->second;
    }
    auto table = New<TOutputTable>(outputTablePath, EOutputTableType::Output);
    auto rowCountLimit = table->Path.GetRowCountLimit();
    if (rowCountLimit) {
        if (RowCountLimitTableIndex) {
            THROW_ERROR_EXCEPTION("Only one output table with row_count_limit is supported");
        }
        RowCountLimitTableIndex = OutputTables_.size();
        RowCountLimit = *rowCountLimit;
    }

    Sinks_.emplace_back(std::make_unique<TSink>(this, OutputTables_.size()));
    table->ChunkPoolInput = Sinks_.back().get();
    OutputTables_.emplace_back(table);
    PathToOutputTable_[outputTablePath.GetPath()] = table;
    return table;
}

void TOperationControllerBase::AbortJobViaScheduler(TJobId jobId, EAbortReason abortReason)
{
    Host->AbortJob(
        jobId,
        TError("Job is aborted by controller") << TErrorAttribute("abort_reason", abortReason));
}

void TOperationControllerBase::OnSpeculativeJobScheduled(const TJobletPtr& joblet)
{
    MarkJobHasCompetitors(joblet);
    // Original job could be finished and another speculative still running.
    if (auto originalJob = FindJoblet(joblet->JobCompetitionId)) {
        MarkJobHasCompetitors(originalJob);
    }
}

void TOperationControllerBase::MarkJobHasCompetitors(const TJobletPtr& joblet)
{
    if (!joblet->HasCompetitors) {
        joblet->HasCompetitors = true;
        auto statistics = NJobAgent::TControllerJobReport()
            .OperationId(OperationId)
            .JobId(joblet->JobId)
            .HasCompetitors(true);
        Host->GetJobReporter()->ReportStatistics(std::move(statistics));
    }
}

void TOperationControllerBase::RegisterTestingSpeculativeJobIfNeeded(const TTaskPtr& task, TJobId jobId)
{
    const auto& joblet = GetOrCrash(JobletMap, jobId);
    bool needLaunchSpeculativeJob;
    switch (Spec_->TestingOperationOptions->TestingSpeculativeLaunchMode) {
        case ETestingSpeculativeLaunchMode::None:
            needLaunchSpeculativeJob = false;
            break;
        case ETestingSpeculativeLaunchMode::Once:
            needLaunchSpeculativeJob = joblet->JobIndex == 0;
            break;
        case ETestingSpeculativeLaunchMode::Always:
            needLaunchSpeculativeJob = !joblet->Speculative;
            break;
        default:
            YT_ABORT();
    }
    if (needLaunchSpeculativeJob) {
        task->TryRegisterSpeculativeJob(joblet);
    }
}

std::vector<NYPath::TRichYPath> TOperationControllerBase::GetLayerPaths(
    const NYT::NScheduler::TUserJobSpecPtr& userJobSpec)
{
    auto layerPaths = userJobSpec->LayerPaths;
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
    if (Config->SystemLayerPath && !layerPaths.empty()) {
        // This must be the top layer, so insert in the beginning.
        layerPaths.insert(layerPaths.begin(), *Config->SystemLayerPath);
    }
    return layerPaths;
}

void TOperationControllerBase::AnalyzeProcessingUnitUsage(
    const std::vector<TString>& usageStatistics,
    const std::vector<TString>& jobStates,
    const std::function<double(const TUserJobSpecPtr&)>& getLimit,
    const std::function<bool(i64, i64, double)>& needSetAlert,
    const TString& name,
    EOperationAlertType alertType,
    const TString& message)
{
    std::vector<TString> allStatistics;
    for (const auto& stat : usageStatistics) {
        for (const auto& jobState : jobStates) {
            allStatistics.push_back(Format("%s/$/%s/", stat, jobState));
        }
    }

    THashMap<EJobType, TError> jobTypeToError;
    for (const auto& task : Tasks) {
        auto jobType = task->GetJobType();
        if (jobTypeToError.find(jobType) != jobTypeToError.end()) {
            continue;
        }

        const auto& userJobSpecPtr = task->GetUserJobSpec();
        if (!userJobSpecPtr) {
            continue;
        }


        i64 totalExecutionTime = 0;
        i64 jobCount = 0;

        for (const auto& jobState : jobStates) {
            auto summary = FindSummary(JobStatistics, Format("/time/exec/$/%s/%s", jobState, FormatEnum(jobType)));
            if (summary) {
                totalExecutionTime += summary->GetSum();
                jobCount += summary->GetCount();
            }
        }

        double limit = getLimit(userJobSpecPtr);
        if (jobCount == 0 || totalExecutionTime == 0 || limit == 0) {
            continue;
        }

        i64 usage = 0;
        for (const auto& stat : allStatistics) {
            auto value = FindNumericValue(JobStatistics, stat + FormatEnum(jobType));
            usage += value.value_or(0);
        }

        TDuration totalExecutionDuration = TDuration::MilliSeconds(totalExecutionTime);
        double ratio = static_cast<double>(usage) / (totalExecutionTime * limit);

        if (needSetAlert(totalExecutionTime, jobCount, ratio))
        {
            auto error = TError("Jobs of type %Qlv use %.2f%% of requested %s limit", jobType, 100 * ratio, name)
                << TErrorAttribute(Format("%s_time", name), usage)
                << TErrorAttribute("exec_time", totalExecutionDuration)
                << TErrorAttribute(Format("%s_limit", name), limit);
            YT_VERIFY(jobTypeToError.emplace(jobType, error).second);
        }
    }

    TError error;
    if (!jobTypeToError.empty()) {
        std::vector<TError> innerErrors;
        innerErrors.reserve(jobTypeToError.size());
        for (const auto& [jobType, error] : jobTypeToError) {
            innerErrors.push_back(error);
        }
        error = TError(message) << innerErrors;
    }

    SetOperationAlert(alertType, error);
}

void TOperationControllerBase::MaybeCancel(ECancelationStage cancelationStage)
{
    if (Spec_->TestingOperationOptions && Spec_->TestingOperationOptions->CancelationStage &&
        cancelationStage == *Spec_->TestingOperationOptions->CancelationStage)
    {
        YT_LOG_INFO("Making test operation failure (CancelationStage: %v)", cancelationStage);
        GetInvoker()->Invoke(BIND(&TOperationControllerBase::OnOperationFailed, MakeWeak(this), TError("Test operation failure"), false /* flush */));
        YT_LOG_INFO("Making test cancelation (CancelationStage: %v)", cancelationStage);
        Cancel();
    }
}

const NChunkClient::TMediumDirectoryPtr& TOperationControllerBase::GetMediumDirectory() const
{
    return MediumDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

TOperationControllerBase::TSink::TSink(TOperationControllerBase* controller, int outputTableIndex)
    : Controller_(controller)
    , OutputTableIndex_(outputTableIndex)
{ }

IChunkPoolInput::TCookie TOperationControllerBase::TSink::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    YT_VERIFY(stripe->ChunkListId);
    auto& table = Controller_->OutputTables_[OutputTableIndex_];
    auto chunkListId = stripe->ChunkListId;

    if (table->TableUploadOptions.TableSchema.IsSorted() && Controller_->ShouldVerifySortedOutput()) {
        // We override the key suggested by the task with the one formed by the stripe boundary keys.
        YT_VERIFY(stripe->BoundaryKeys);
        key = stripe->BoundaryKeys;
    }

    if (Controller_->IsOutputLivePreviewSupported()) {
        Controller_->AttachToLivePreview(chunkListId, table->LivePreviewTableId);
    }
    table->OutputChunkTreeIds.emplace_back(key, chunkListId);
    table->ChunkCount += stripe->GetStatistics().ChunkCount;

    const auto& Logger = Controller_->Logger;
    YT_LOG_DEBUG("Output stripe registered (Table: %v, ChunkListId: %v, Key: %v, ChunkCount: %v)",
        OutputTableIndex_,
        chunkListId,
        key,
        stripe->GetStatistics().ChunkCount);

    if (table->Dynamic) {
        for (auto& slice : stripe->DataSlices) {
            YT_VERIFY(slice->ChunkSlices.size() == 1);
            table->OutputChunks.push_back(slice->ChunkSlices[0]->GetInputChunk());
        }
    }

    return IChunkPoolInput::NullCookie;
}

IChunkPoolInput::TCookie TOperationControllerBase::TSink::Add(TChunkStripePtr stripe)
{
    return AddWithKey(stripe, TChunkStripeKey());
}

void TOperationControllerBase::TSink::Suspend(TCookie /* cookie */)
{
    YT_ABORT();
}

void TOperationControllerBase::TSink::Resume(TCookie /* cookie */)
{
    YT_ABORT();
}

void TOperationControllerBase::TSink::Reset(
    TCookie /* cookie */,
    TChunkStripePtr /* stripe */,
    TInputChunkMappingPtr /* mapping */)
{
    YT_ABORT();
}

void TOperationControllerBase::TSink::Finish()
{
    // Mmkay. Don't know what to do here though :)
}

void TOperationControllerBase::TSink::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Controller_);
    Persist(context, OutputTableIndex_);
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TOperationControllerBase::TSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
