#include "scheduler.h"
#include "private.h"
#include "event_log.h"
#include "fair_share_strategy.h"
#include "helpers.h"
#include "job_prober_service.h"
#include "job_resources.h"
#include "map_controller.h"
#include "master_connector.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "remote_copy_controller.h"
#include "scheduler_strategy.h"
#include "snapshot_downloader.h"
#include "sort_controller.h"

#include <yt/server/exec_agent/public.h>

#include <yt/server/cell_scheduler/bootstrap.h>
#include <yt/server/cell_scheduler/config.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/ytlib/shell/config.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/ytlib/table_client/schemaless_writer.h>
#include <yt/ytlib/table_client/table_consumer.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/response_keeper.h>

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/finally.h>

#include <yt/core/profiling/scoped_timer.h>
#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NScheduler {

using namespace NProfiling;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NApi;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NHydra;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NJobProberClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NSecurityClient;
using namespace NShell;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerClient::TNodeDirectory;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////

//! Ensures that operation controllers are being destroyed in a
//! dedicated invoker.
class TOperationControllerWrapper
    : public IOperationController
{
public:
    TOperationControllerWrapper(
        const TOperationId& id,
        IOperationControllerPtr underlying,
        IInvokerPtr dtorInvoker)
        : Id_(id)
        , Underlying_(std::move(underlying))
        , DtorInvoker_(std::move(dtorInvoker))
    { }

    virtual ~TOperationControllerWrapper()
    {
        DtorInvoker_->Invoke(BIND([underlying = std::move(Underlying_), id = Id_] () mutable {
            LOG_INFO("Started destroying operation controller (OperationId: %v)",
                id);
            underlying.Reset();
            LOG_INFO("Finished destroying operation controller (OperationId: %v)",
                id);
        }));
    }

    virtual void Initialize(bool cleanStart) override
    {
        Underlying_->Initialize(cleanStart);
    }

    virtual void Prepare() override
    {
        Underlying_->Prepare();
    }

    virtual void Materialize() override
    {
        Underlying_->Materialize();
    }

    virtual void Commit() override
    {
        Underlying_->Commit();
    }

    virtual void SaveSnapshot(TOutputStream* stream) override
    {
        Underlying_->SaveSnapshot(stream);
    }

    virtual void Revive(const TSharedRef& snapshot) override
    {
        Underlying_->Revive(snapshot);
    }

    virtual void Abort() override
    {
        Underlying_->Abort();
    }

    virtual void Complete() override
    {
        Underlying_->Complete();
    }

    virtual TCancelableContextPtr GetCancelableContext() const override
    {
        return Underlying_->GetCancelableContext();
    }

    virtual IInvokerPtr GetCancelableControlInvoker() const override
    {
        return Underlying_->GetCancelableControlInvoker();
    }

    virtual IInvokerPtr GetCancelableInvoker() const override
    {
        return Underlying_->GetCancelableInvoker();
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Underlying_->GetInvoker();
    }

    virtual TFuture<void> Suspend() override
    {
        return Underlying_->Suspend();
    }

    virtual void Resume() override
    {
        Underlying_->Resume();
    }

    virtual bool GetCleanStart() const override
    {
        return Underlying_->GetCleanStart();
    }

    virtual int GetPendingJobCount() const override
    {
        return Underlying_->GetPendingJobCount();
    }

    virtual int GetTotalJobCount() const override
    {
        return Underlying_->GetTotalJobCount();
    }

    virtual TJobResources GetNeededResources() const override
    {
        return Underlying_->GetNeededResources();
    }

    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) override
    {
        Underlying_->OnJobStarted(jobId, startTime);
    }

    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override
    {
        Underlying_->OnJobCompleted(std::move(jobSummary));
    }

    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) override
    {
        Underlying_->OnJobFailed(std::move(jobSummary));
    }

    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) override
    {
        Underlying_->OnJobAborted(std::move(jobSummary));
    }

    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResources& jobLimits) override
    {
        return Underlying_->ScheduleJob(std::move(context), jobLimits);
    }

    virtual void UpdateConfig(TSchedulerConfigPtr config) override
    {
        Underlying_->UpdateConfig(std::move(config));
    }

    virtual bool HasProgress() const override
    {
        return Underlying_->HasProgress();
    }

    virtual void BuildProgress(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildProgress(consumer);
    }

    virtual void BuildBriefProgress(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildBriefProgress(consumer);
    }

    virtual Stroka GetLoggingProgress() const override
    {
        return Underlying_->GetLoggingProgress();
    }

    virtual void BuildResult(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildResult(consumer);
    }

    virtual void BuildMemoryDigestStatistics(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildMemoryDigestStatistics(consumer);
    }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildBriefSpec(consumer);
    }

    virtual TFuture<TYsonString> BuildInputPathYson(const TJobId& jobId) const override
    {
        return Underlying_->BuildInputPathYson(jobId);
    }

private:
    const TOperationId Id_;
    const IOperationControllerPtr Underlying_;
    const IInvokerPtr DtorInvoker_;

};

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
    , public IOperationHost
    , public ISchedulerStrategyHost
    , public TEventLogHostBase
{
public:
    using TEventLogHostBase::LogEventFluently;

    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , InitialConfig_(ConvertToNode(Config_))
        , Bootstrap_(bootstrap)
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
        , ControllerThreadPool_(New<TThreadPool>(Config_->ControllerThreadCount, "Controller"))
        , JobSpecBuilderThreadPool_(New<TThreadPool>(Config_->JobSpecBuilderThreadCount, "SpecBuilder"))
        , MasterConnector_(new TMasterConnector(Config_, Bootstrap_))
        , TotalResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUsageProfiler_(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter_("/total_completed_job_time")
        , TotalFailedJobTimeCounter_("/total_failed_job_time")
        , TotalAbortedJobTimeCounter_("/total_aborted_job_time")
        , TotalResourceLimits_(ZeroJobResources())
        , TotalResourceUsage_(ZeroJobResources())
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_THREAD_AFFINITY(GetControlInvoker(), ControlThread);

        auto localHostName = TAddressResolver::Get()->GetLocalHostName();
        int port = Bootstrap_->GetConfig()->RpcPort;
        ServiceAddress_ = BuildServiceAddress(localHostName, port);

        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            JobStateToTag_[state] = TProfileManager::Get()->RegisterTag("state", Format("%lv", state));
        }
        for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
            JobTypeToTag_[type] = TProfileManager::Get()->RegisterTag("type", Format("%lv", type));
        }
        for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
            JobAbortReasonToTag_[reason] = TProfileManager::Get()->RegisterTag("reason", Format("%lv", reason));
        }
    }

    void Initialize()
    {
        InitStrategy();

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestPools,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandlePools,
            Unretained(this)));

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestNodesAttributes,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandleNodesAttributes,
            Unretained(this)));

        MasterConnector_->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestConfig,
            Unretained(this)));
        MasterConnector_->AddGlobalWatcherHandler(BIND(
            &TImpl::HandleConfig,
            Unretained(this)));

        MasterConnector_->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        MasterConnector_->SubscribeUserTransactionAborted(BIND(
            &TImpl::OnUserTransactionAborted,
            Unretained(this)));
        MasterConnector_->SubscribeSchedulerTransactionAborted(BIND(
            &TImpl::OnSchedulerTransactionAborted,
            Unretained(this)));

        MasterConnector_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();

        auto nameTable = New<TNameTable>();
        auto options = New<TTableWriterOptions>();
        options->ValidateDuplicateIds = true;
        options->ValidateRowWeight = true;
        options->ValidateColumnCount = true;

        EventLogWriter_ = CreateSchemalessBufferedTableWriter(
            Config_->EventLog,
            options,
            Bootstrap_->GetMasterClient(),
            nameTable,
            Config_->EventLog->Path);

        // Open is always synchronous for buffered writer.
        YCHECK(EventLogWriter_->Open().IsSet());

        EventLogValueConsumer_.reset(new TWritingValueConsumer(EventLogWriter_, true));
        EventLogTableConsumer_.reset(new TTableConsumer(EventLogValueConsumer_.get()));

        LogEventFluently(ELogEventType::SchedulerStarted)
            .Item("address").Value(ServiceAddress_);

        LoggingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnLogging, MakeWeak(this)),
            Config_->ClusterInfoLoggingPeriod);
        LoggingExecutor_->Start();

        PendingEventLogRowsFlushExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnPendingEventLogRowsFlush, MakeWeak(this)),
            Config_->PendingEventLogRowsFlushPeriod);
        PendingEventLogRowsFlushExecutor_->Start();
    }

    ISchedulerStrategy* GetStrategy()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Strategy_.get();
    }

    IYPathServicePtr GetOrchidService()
    {
        auto producer = BIND(&TImpl::BuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    std::vector<TOperationPtr> GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TOperationPtr> operations;
        for (const auto& pair : IdToOperation_) {
            operations.push_back(pair.second);
        }
        return operations;
    }

    IInvokerPtr GetSnapshotIOInvoker()
    {
        return SnapshotIOQueue_->GetInvoker();
    }

    bool IsConnected()
    {
        return MasterConnector_->IsConnected();
    }

    void ValidateConnected()
    {
        if (!IsConnected()) {
            THROW_ERROR_EXCEPTION(GetMasterDisconnectedError());
        }
    }


    TOperationPtr FindOperation(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = IdToOperation_.find(id);
        return it == IdToOperation_.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperation(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        YCHECK(operation);
        return operation;
    }

    TOperationPtr GetOperationOrThrow(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        if (!operation) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchOperation,
                "No such operation %v",
                id);
        }
        return operation;
    }


    virtual int GetExecNodeCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeMapLock_);

        return ExecNodeCount_;
    }

    virtual int GetTotalNodeCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeMapLock_);

        return TotalNodeCount_;
    }

    virtual std::vector<TExecNodeDescriptor> GetExecNodeDescriptors(const TNullable<Stroka>& tag) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TReaderGuard guard(ExecNodeMapLock_);

        std::vector<TExecNodeDescriptor> result;
        result.reserve(IdToNode_.size());
        for (const auto& pair : IdToNode_) {
            const auto& node = pair.second;
            if (node->GetMasterState() == ENodeState::Online &&
                node->CanSchedule(tag))
            {
                result.push_back(node->BuildExecDescriptor());
            }
        }
        return result;
    }


    void ValidatePermission(
        const Stroka& user,
        const TOperationId& operationId,
        EPermission permission)
    {
        auto path = GetOperationPath(operationId);

        auto client = Bootstrap_->GetMasterClient();
        auto asyncResult = client->CheckPermission(user, path, permission);
        auto resultOrError = WaitFor(asyncResult);
        if (!resultOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error checking permission for operation %v",
                operationId)
                << resultOrError;
        }

        const auto& result = resultOrError.Value();
        if (result.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("User %Qv has been denied access to operation %v",
                user,
                operationId);
        }
    }

    TFuture<TOperationPtr> StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        IMapNodePtr spec,
        const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (static_cast<int>(IdToOperation_.size()) >= Config_->MaxOperationCount) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the total number of concurrent operations %v has been reached",
                Config_->MaxOperationCount);
        }

        // Attach user transaction if any. Don't ping it.
        TTransactionAttachOptions userAttachOptions;
        userAttachOptions.Ping = false;
        userAttachOptions.PingAncestors = false;
        auto userTransaction = transactionId
            ? GetMasterClient()->AttachTransaction(transactionId, userAttachOptions)
            : nullptr;

        // Merge operation spec with template
        auto specTemplate = GetSpecTemplate(type, spec);
        if (specTemplate) {
            spec = UpdateNode(specTemplate, spec)->AsMap();
        }

        TOperationSpecBasePtr operationSpec;
        try {
            operationSpec = ConvertTo<TOperationSpecBasePtr>(spec);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
        }

        // Create operation object.
        auto operationId = MakeRandomId(
            EObjectType::Operation,
            GetMasterClient()->GetConnection()->GetPrimaryMasterCellTag());
        auto operation = New<TOperation>(
            operationId,
            type,
            mutationId,
            userTransaction,
            spec,
            user,
            operationSpec->Owners,
            TInstant::Now());
        operation->SetState(EOperationState::Initializing);

        Strategy_->CanAddOperation(operation)
            .ThrowOnError();

        LOG_INFO("Starting operation (OperationType: %v, OperationId: %v, TransactionId: %v, User: %v)",
            type,
            operationId,
            transactionId,
            user);

        LOG_INFO("Total resource limits (OperationId: %v, ResourceLimits: %v)",
            operationId,
            FormatResources(GetTotalResourceLimits()));

        // Spawn a new fiber where all startup logic will work asynchronously.
        BIND(&TImpl::DoStartOperation, MakeStrong(this), operation)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();

        return operation->GetStarted();
    }

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidatePermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already shuting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }

        LOG_INFO(error, "Aborting operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            ELogEventType::OperationAborted,
            error);

        return operation->GetFinished();
    }

    TFuture<void> SuspendOperation(TOperationPtr operation, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidatePermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Cannot suspend operation in %Qlv state",
                operation->GetState()));
        }

        operation->SetSuspended(true);

        LOG_INFO("Operation suspended (OperationId: %v)",
            operation->GetId());

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> ResumeOperation(TOperationPtr operation, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidatePermission(user, operation->GetId(), EPermission::Write);

        if (!operation->GetSuspended()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Operation is not suspended. Its state %Qlv",
                operation->GetState()));
        }

        operation->SetSuspended(false);

        LOG_INFO("Operation resumed (OperationId: %v)",
            operation->GetId());

        return MasterConnector_->FlushOperationNode(operation);
    }

    TFuture<void> CompleteOperation(TOperationPtr operation, const TError& error, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        ValidatePermission(user, operation->GetId(), EPermission::Write);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already shuting down (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }
        if (operation->GetState() != EOperationState::Running) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Operation is not running. Its state %Qlv",
                operation->GetState()));
        }

        LOG_INFO(error, "Completing operation (OperationId: %v, State: %v)",
            operation->GetId(),
            operation->GetState());

        auto controller = operation->GetController();
        YCHECK(controller);
        controller->Complete();

        return operation->GetFinished();
    }

    TFuture<TYsonString> Strace(const TJobId& jobId, const Stroka& user)
    {
        return BIND(&TImpl::DoStrace, MakeStrong(this), jobId, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<void> DumpInputContext(const TJobId& jobId, const TYPath& path, const Stroka& user)
    {
        return BIND(&TImpl::DoDumpInputContext, MakeStrong(this), jobId, path, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<void> SignalJob(const TJobId& jobId, const Stroka& signalName, const Stroka& user)
    {
        return BIND(&TImpl::DoSignalJob, MakeStrong(this), jobId, signalName, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<void> AbandonJob(const TJobId& jobId, const Stroka& user)
    {
        return BIND(&TImpl::DoAbandonJob, MakeStrong(this), jobId, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<TYsonString> PollJobShell(const TJobId& jobId, const TYsonString& parameters, const Stroka& user)
    {
        return BIND(&TImpl::DoPollJobShell, MakeStrong(this), jobId, parameters, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    TFuture<void> AbortJob(const TJobId& jobId, const Stroka& user)
    {
        return BIND(&TImpl::DoAbortJob, MakeStrong(this), jobId, user)
            .AsyncVia(MasterConnector_->GetCancelableControlInvoker())
            .Run();
    }

    void IncreaseProfilingCounter(TJobPtr job, i64 value)
    {
        TJobCounter* counter = &JobCounter_;
        if (job->GetState() == EJobState::Aborted) {
            counter = &AbortedJobCounter_[GetAbortReason(job->Status()->result())];
        }
        (*counter)[job->GetState()][job->GetType()] += value;
    }


    void SetJobState(TJobPtr job, EJobState state)
    {
        IncreaseProfilingCounter(job, -1);
        job->SetState(state);
        IncreaseProfilingCounter(job, 1);
    }

    void ProcessHeartbeatJobs(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        std::vector<TJobPtr>* runningJobs,
        bool* hasWaitingJobs,
        yhash_set<TOperationPtr>* operationsToLog)
    {
        auto now = TInstant::Now();

        bool forceJobsLogging = false;
        auto lastJobsLogTime = node->GetLastJobsLogTime();
        if (!lastJobsLogTime || now > lastJobsLogTime.Get() + Config_->JobsLoggingPeriod) {
            forceJobsLogging = true;
            node->SetLastJobsLogTime(now);
        }

        bool updateRunningJobs = false;
        auto lastRunningJobsUpdateTime = node->GetLastRunningJobsUpdateTime();
        if (!lastRunningJobsUpdateTime || now > lastRunningJobsUpdateTime.Get() + Config_->RunningJobsUpdatePeriod) {
            updateRunningJobs = true;
            node->SetLastRunningJobsUpdateTime(now);
        }

        // Verify that all flags are in initial state.
        for (const auto& job : node->Jobs()) {
            YCHECK(!job->GetFoundOnNode());
        }

        for (auto& jobStatus : *request->mutable_jobs()) {
            auto jobType = EJobType(jobStatus.job_type());
            // Skip jobs that are not issued by the scheduler.
            if (jobType <= EJobType::SchedulerFirst || jobType >= EJobType::SchedulerLast) {
                continue;
            }

            auto job = ProcessJobHeartbeat(
                node,
                request,
                response,
                New<TRefCountedJobStatus>(std::move(jobStatus)),
                forceJobsLogging,
                updateRunningJobs);
            if (job) {
                job->SetFoundOnNode(true);
                switch (job->GetState()) {
                    case EJobState::Completed:
                    case EJobState::Failed:
                    case EJobState::Aborted: {
                        auto operation = GetOperation(job->GetOperationId());
                        operationsToLog->insert(operation);
                        break;
                    }
                    case EJobState::Running:
                        runningJobs->push_back(job);
                        break;
                    case EJobState::Waiting:
                        *hasWaitingJobs = true;
                        break;
                    default:
                        break;
                }
            }
        }

        // Check for missing jobs.
        std::vector<TJobPtr> missingJobs;
        for (const auto& job : node->Jobs()) {
            if (!job->GetFoundOnNode()) {
                missingJobs.push_back(job);
            } else {
                job->SetFoundOnNode(false);
            }
        }

        for (const auto& job : missingJobs) {
            LOG_ERROR("Job is missing (Address: %v, JobId: %v, OperationId: %v)",
                node->GetDefaultAddress(),
                job->GetId(),
                job->GetOperationId());
            OnJobAborted(job, JobStatusFromError(TError("Job vanished")));
        }
    }

    TFuture<void> ProcessScheduledJobs(
        const ISchedulingContextPtr& schedulingContext,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        yhash_set<TOperationPtr>* operationsToLog)
    {
        std::vector<TFuture<void>> asyncResults;

        for (const auto& job : schedulingContext->StartedJobs()) {
            auto operation = FindOperation(job->GetOperationId());
            if (!operation || operation->GetState() != EOperationState::Running) {
                LOG_DEBUG("Dangling started job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            RegisterJob(job);
            IncreaseProfilingCounter(job, 1);

            const auto& controller = operation->GetController();
            controller->GetCancelableInvoker()->Invoke(BIND(
                &IOperationController::OnJobStarted,
                controller,
                job->GetId(),
                job->GetStartTime()));

            auto* startInfo = response->add_jobs_to_start();
            ToProto(startInfo->mutable_job_id(), job->GetId());
            ToProto(startInfo->mutable_operation_id(), operation->GetId());
            *startInfo->mutable_resource_limits() = job->ResourceUsage().ToNodeResources();

            // Build spec asynchronously.
            asyncResults.push_back(
                BIND(job->GetSpecBuilder(), startInfo->mutable_spec())
                    .AsyncVia(JobSpecBuilderThreadPool_->GetInvoker())
                    .Run());

            // Release to avoid circular references.
            job->SetSpecBuilder(TJobSpecBuilder());
            operationsToLog->insert(operation);
        }

        for (const auto& job : schedulingContext->PreemptedJobs()) {
            if (!FindOperation(job->GetOperationId()) || job->GetHasPendingUnregistration()) {
                LOG_DEBUG("Dangling preempted job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }
            PreemptJob(job);
            ToProto(response->add_jobs_to_abort(), job->GetId());
        }

        return Combine(asyncResults);
    }

    void ProcessHeartbeat(TCtxHeartbeatPtr context)
    {
        auto* request = &context->Request();
        auto* response = &context->Response();

        auto nodeId = request->node_id();
        auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
        auto node = GetOrRegisterNode(nodeId, descriptor);
        // NB: Resource limits and usage of node should be updated even if
        // node is offline to avoid getting incorrect total limits when node becomes online.
        UpdateNodeResources(node, request->resource_limits(), request->resource_usage());
        if (node->GetMasterState() != ENodeState::Online) {
            THROW_ERROR_EXCEPTION("Node is not online");
        }

        // We should process only one heartbeat at a time from the same node.
        if (node->GetHasOngoingHeartbeat()) {
            THROW_ERROR_EXCEPTION("Node has ongoing heartbeat");
        }

        TLeaseManager::RenewLease(node->GetLease());

        bool isThrottlingActive = false;
        if (ConcurrentHeartbeatCount_ > Config_->HardConcurrentHeartbeatLimit) {
            isThrottlingActive = true;
            LOG_INFO("Hard heartbeat limit reached (NodeAddress: %v, Limit: %v)",
                node->GetDefaultAddress(),
                Config_->HardConcurrentHeartbeatLimit);
        } else if (ConcurrentHeartbeatCount_ > Config_->SoftConcurrentHeartbeatLimit &&
            node->GetLastSeenTime() + Config_->HeartbeatProcessBackoff > TInstant::Now())
        {
            isThrottlingActive = true;
            LOG_INFO("Soft heartbeat limit reached (NodeAddress: %v, Limit: %v)",
                node->GetDefaultAddress(),
                Config_->SoftConcurrentHeartbeatLimit);
        }

        yhash_set<TOperationPtr> operationsToLog;
        TFuture<void> scheduleJobsAsyncResult = VoidFuture;

        {
            BeginNodeHeartbeatProcessing(node);
            auto heartbeatGuard = Finally([&] {
                EndNodeHeartbeatProcessing(node);
            });

            // NB: No exception must leave this try/catch block.
            try {
                std::vector<TJobPtr> runningJobs;
                bool hasWaitingJobs = false;
                PROFILE_TIMING ("/analysis_time") {
                    ProcessHeartbeatJobs(
                        node,
                        request,
                        response,
                        &runningJobs,
                        &hasWaitingJobs,
                        &operationsToLog);
                }

                if (hasWaitingJobs || isThrottlingActive) {
                    if (hasWaitingJobs) {
                        LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling");
                    }
                    if (isThrottlingActive) {
                        LOG_DEBUG("Throttling is active, suppressing new jobs scheduling");
                    }
                    response->set_scheduling_skipped(true);
                } else {
                    auto schedulingContext = CreateSchedulingContext(
                        Config_,
                        node,
                        runningJobs,
                        Bootstrap_->GetMasterClient()->GetConnection()->GetPrimaryMasterCellTag());

                    PROFILE_TIMING ("/schedule_time") {
                        node->SetHasOngoingJobsScheduling(true);
                        Strategy_->ScheduleJobs(schedulingContext);
                        node->SetHasOngoingJobsScheduling(false);
                    }

                    TotalResourceUsage_ -= node->GetResourceUsage();
                    node->SetResourceUsage(schedulingContext->ResourceUsage());
                    TotalResourceUsage_ += node->GetResourceUsage();

                    scheduleJobsAsyncResult = ProcessScheduledJobs(
                        schedulingContext,
                        response,
                        &operationsToLog);

                    response->set_scheduling_skipped(false);
                }

                std::vector<TJobPtr> jobsWithPendingUnregistration;
                for (const auto& job : node->Jobs()) {
                    if (job->GetHasPendingUnregistration()) {
                        jobsWithPendingUnregistration.push_back(job);
                    }
                }

                for (const auto& job : jobsWithPendingUnregistration) {
                    DoUnregisterJob(job);
                }
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Failed to process heartbeat");
            }

        }

        context->ReplyFrom(scheduleJobsAsyncResult);

        // NB: Do heavy logging after responding to heartbeat.
        for (const auto& operation : operationsToLog) {
            if (!FindOperation(operation->GetId())) {
                continue;
            }

            LogOperationProgress(operation);
        }
    }


    // ISchedulerStrategyHost implementation
    DEFINE_SIGNAL(void(TOperationPtr), OperationRegistered);
    DEFINE_SIGNAL(void(TOperationPtr), OperationUnregistered);
    DEFINE_SIGNAL(void(TOperationPtr, INodePtr update), OperationRuntimeParamsUpdated);

    DEFINE_SIGNAL(void(const TJobPtr& job), JobFinished);
    DEFINE_SIGNAL(void(const TJobPtr&, const TJobResources& resourcesDelta), JobUpdated);

    DEFINE_SIGNAL(void(INodePtr pools), PoolsUpdated);


    virtual TMasterConnector* GetMasterConnector() override
    {
        return MasterConnector_.get();
    }

    virtual TJobResources GetTotalResourceLimits() override
    {
        return TotalResourceLimits_;
    }

    virtual TJobResources GetResourceLimits(const TNullable<Stroka>& tag) override
    {
        if (!tag || NodeTagToResources_.find(*tag) == NodeTagToResources_.end()) {
            return TotalResourceLimits_;
        } else {
            return NodeTagToResources_[*tag];
        }
    }

    virtual void ActivateOperation(const TOperationId& operationId) override
    {
        auto operation = GetOperation(operationId);

        auto codicilGuard = operation->MakeCodicilGuard();

        operation->SetActivated(true);
        if (operation->GetPrepared()) {
            MaterializeOperation(operation);
        }
    }

    void MaterializeOperation(TOperationPtr operation)
    {
        auto controller = operation->GetController();
        if (controller->GetCleanStart()) {
            operation->SetState(EOperationState::Materializing);
            BIND(&IOperationController::Materialize, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run()
                .Subscribe(BIND([operation] (const TError& error) {
                    if (error.IsOK()) {
                        if (operation->GetState() == EOperationState::Materializing) {
                            operation->SetState(EOperationState::Running);
                        }
                    }
                })
                .Via(controller->GetCancelableControlInvoker()));
        } else {
            operation->SetState(EOperationState::Running);
        }
    }


    // IOperationHost implementation
    virtual NApi::IClientPtr GetMasterClient() override
    {
        return Bootstrap_->GetMasterClient();
    }

    virtual NHive::TClusterDirectoryPtr GetClusterDirectory() override
    {
        return Bootstrap_->GetClusterDirectory();
    }

    virtual IInvokerPtr GetControlInvoker() override
    {
        return Bootstrap_->GetControlInvoker();
    }

    virtual IInvokerPtr CreateOperationControllerInvoker() override
    {
        return CreateSerializedInvoker(ControllerThreadPool_->GetInvoker());
    }

    virtual TThrottlerManagerPtr GetChunkLocationThrottlerManager() const override
    {
        return Bootstrap_->GetChunkLocationThrottlerManager();
    }

    virtual IYsonConsumer* GetEventLogConsumer() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return EventLogTableConsumer_.get();
    }

    virtual void OnOperationCompleted(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoCompleteOperation, MakeStrong(this), operation));
    }

    virtual void OnOperationFailed(TOperationPtr operation, const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        MasterConnector_->GetCancelableControlInvoker()->Invoke(
            BIND(&TImpl::DoFailOperation, MakeStrong(this), operation, error));
    }

    virtual std::unique_ptr<IValueConsumer> CreateLogConsumer() override
    {
        return std::unique_ptr<IValueConsumer>(new TEventLogValueConsumer(this));
    }

private:
    const TSchedulerConfigPtr Config_;
    const INodePtr InitialConfig_;
    TBootstrap* const Bootstrap_;

    TActionQueuePtr SnapshotIOQueue_;
    TThreadPoolPtr ControllerThreadPool_;
    TThreadPoolPtr JobSpecBuilderThreadPool_;

    std::unique_ptr<TMasterConnector> MasterConnector_;

    std::unique_ptr<ISchedulerStrategy> Strategy_;

    NConcurrency::TReaderWriterSpinLock ExecNodeMapLock_;
    typedef yhash_map<Stroka, TExecNodePtr> TExecNodeByAddressMap;
    TExecNodeByAddressMap AddressToNode_;
    typedef yhash_map<TNodeId, TExecNodePtr> TExecNodeByIdMap;
    TExecNodeByIdMap IdToNode_;

    TNodeDirectoryPtr NodeDirectory_ = New<TNodeDirectory>();

    typedef yhash_map<TOperationId, TOperationPtr> TOperationIdMap;
    TOperationIdMap IdToOperation_;

    typedef yhash_map<TJobId, TJobPtr> TJobMap;

    int ActiveJobCount_ = 0;

    NProfiling::TProfiler TotalResourceLimitsProfiler_;
    NProfiling::TProfiler TotalResourceUsageProfiler_;

    NProfiling::TAggregateCounter TotalCompletedJobTimeCounter_;
    NProfiling::TAggregateCounter TotalFailedJobTimeCounter_;
    NProfiling::TAggregateCounter TotalAbortedJobTimeCounter_;

    typedef TEnumIndexedVector<TEnumIndexedVector<i64, EJobType>, EJobState> TJobCounter;
    TJobCounter JobCounter_;
    TEnumIndexedVector<TJobCounter, EAbortReason> AbortedJobCounter_;

    TEnumIndexedVector<TTagId, EJobState> JobStateToTag_;
    TEnumIndexedVector<TTagId, EJobType> JobTypeToTag_;
    TEnumIndexedVector<TTagId, EAbortReason> JobAbortReasonToTag_;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TJobResources TotalResourceLimits_ = ZeroJobResources();
    TJobResources TotalResourceUsage_ = ZeroJobResources();
    // Exec node is then node that is online and has user slots.
    int ExecNodeCount_ = 0;
    int TotalNodeCount_ = 0;
    int ConcurrentHeartbeatCount_ = 0;

    TPeriodicExecutorPtr LoggingExecutor_;
    TPeriodicExecutorPtr PendingEventLogRowsFlushExecutor_;

    Stroka ServiceAddress_;

    class TEventLogValueConsumer
        : public IValueConsumer
    {
    public:
        explicit TEventLogValueConsumer(TScheduler::TImpl* host)
            : Host_(host)
        { }

        virtual TNameTablePtr GetNameTable() const override
        {
            return Host_->EventLogWriter_->GetNameTable();
        }

        virtual bool GetAllowUnknownColumns() const override
        {
            return true;
        }

        virtual void OnBeginRow() override
        { }

        virtual void OnValue(const TUnversionedValue& value) override
        {
            Builder_.AddValue(value);
        }

        virtual void OnEndRow() override
        {
            Host_->PendingEventLogRows_.Enqueue(Builder_.FinishRow());
        }

    private:
        TScheduler::TImpl* const Host_;
        TUnversionedOwningRowBuilder Builder_;

    };

    ISchemalessWriterPtr EventLogWriter_;
    std::unique_ptr<IValueConsumer> EventLogValueConsumer_;
    std::unique_ptr<IYsonConsumer> EventLogTableConsumer_;
    TMultipleProducerSingleConsumerLockFreeStack<TUnversionedOwningRow> PendingEventLogRows_;

    yhash_map<Stroka, TJobResources> NodeTagToResources_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                TTagIdList commonTags = {JobStateToTag_[state], JobTypeToTag_[type]};
                if (state == EJobState::Aborted) {
                    for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
                        auto tags = commonTags;
                        tags.push_back(JobAbortReasonToTag_[reason]);
                        Profiler.Enqueue("/job_count", AbortedJobCounter_[reason][state][type], tags);
                    }
                } else {
                    Profiler.Enqueue("/job_count", JobCounter_[state][type], commonTags);
                }
            }
        }

        Profiler.Enqueue("/active_job_count", ActiveJobCount_);

        Profiler.Enqueue("/operation_count", IdToOperation_.size());
        Profiler.Enqueue("/exec_node_count", GetExecNodeCount());
        Profiler.Enqueue("/total_node_count", GetTotalNodeCount());

        ProfileResources(TotalResourceLimitsProfiler_, TotalResourceLimits_);
        ProfileResources(TotalResourceUsageProfiler_, TotalResourceUsage_);
    }


    void OnLogging()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsConnected()) {
            LogEventFluently(ELogEventType::ClusterInfo)
                .Item("exec_node_count").Value(GetExecNodeCount())
                .Item("total_node_count").Value(GetTotalNodeCount())
                .Item("resource_limits").Value(TotalResourceLimits_)
                .Item("resource_usage").Value(TotalResourceUsage_);
        }
    }


    void OnPendingEventLogRowsFlush()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (IsConnected()) {
            auto owningRows = PendingEventLogRows_.DequeueAll();
            std::vector<TUnversionedRow> rows(owningRows.begin(), owningRows.end());
            EventLogWriter_->Write(rows);
        }
    }


    void OnMasterConnected(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Start();

        LogEventFluently(ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);

        AbortAbortingOperations(result.AbortingOperations);
        ReviveOperations(result.RevivingOperations);

        Strategy_->StartPeriodicActivity();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting scheduler state cleanup");

        auto responseKeeper = Bootstrap_->GetResponseKeeper();
        responseKeeper->Stop();

        LogEventFluently(ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        auto operations = IdToOperation_;
        for (const auto& pair : operations) {
            auto operation = pair.second;
            LOG_INFO("Forgetting operation (OperationId: %v)", operation->GetId());
            if (!operation->IsFinishedState()) {
                operation->GetController()->Abort();
                SetOperationFinalState(
                    operation,
                    EOperationState::Aborted,
                    TError("Master disconnected"));
            }
            FinishOperation(operation);
        }
        YCHECK(IdToOperation_.empty());

        {
            TReaderGuard guard(ExecNodeMapLock_);
            for (const auto& pair : AddressToNode_) {
                auto node = pair.second;
                node->Jobs().clear();
                node->IdToJob().clear();
            }
        }

        ActiveJobCount_ = 0;

        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                JobCounter_[state][type] = 0;
                for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
                    AbortedJobCounter_[reason][state][type] = 0;
                }
            }
        }

        Strategy_->ResetState();

        LOG_INFO("Finished scheduler state cleanup");
    }

    TError GetMasterDisconnectedError()
    {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected");
    }

    void LogOperationFinished(TOperationPtr operation, ELogEventType logEventType, TError error)
    {
        LogEventFluently(logEventType)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec())
            .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
            .Item("start_time").Value(operation->GetStartTime())
            .Item("finish_time").Value(operation->GetFinishTime())
            .Item("controller_time_statistics").Value(operation->ControllerTimeStatistics())
            .Item("error").Value(error);
    }

    void OnUserTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            ELogEventType::OperationAborted,
            TError("Operation transaction has expired or was aborted"));
    }

    void OnSchedulerTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            ELogEventType::OperationFailed,
            TError("Scheduler transaction has expired or was aborted"));
    }

    void RequestPools(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating pools");

        auto req = TYPathProxy::Get("//sys/pools");
        static auto poolConfigTemplate = New<TPoolConfig>();
        static auto poolConfigKeys = poolConfigTemplate->GetRegisteredKeys();
        ToProto(req->mutable_attributes()->mutable_keys(), poolConfigKeys);
        batchReq->AddRequest(req, "get_pools");
    }

    void HandlePools(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error getting pools configuration");
            return;
        }

        try {
            const auto& rsp = rspOrError.Value();
            auto poolsNode = ConvertToNode(TYsonString(rsp->value()));
            PoolsUpdated_.Fire(poolsNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing pools configuration");
        }
    }

    void RequestNodesAttributes(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating nodes information");

        auto req = TYPathProxy::Get("//sys/nodes");
        std::vector<Stroka> attributeKeys{
            "tags",
            "state",
            "io_weight"
        };
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "get_nodes");
    }

    void HandleNodesAttributes(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_nodes");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error updating nodes information");
            return;
        }

        try {
            TReaderGuard guard(ExecNodeMapLock_);

            const auto& rsp = rspOrError.Value();
            auto nodesMap = ConvertToNode(TYsonString(rsp->value()))->AsMap();
            for (const auto& child : nodesMap->GetChildren()) {
                auto address = child.first;
                auto node = child.second;
                const auto& attributes = node->Attributes();
                auto newState = attributes.Get<ENodeState>("state");
                auto ioWeight = attributes.Get<double>("io_weight", 0.0);

                if (AddressToNode_.find(address) == AddressToNode_.end()) {
                    if (newState == ENodeState::Online) {
                        LOG_WARNING("Node %v is not registered in scheduler but online at master", address);
                    }
                    continue;
                }

                auto execNode = AddressToNode_[address];
                auto oldState = execNode->GetMasterState();

                auto tags = node->Attributes().Get<std::vector<Stroka>>("tags");
                UpdateNodeTags(execNode, tags);

                if (oldState != newState) {
                    if (oldState == ENodeState::Online && newState != ENodeState::Online) {
                        SubtractNodeResources(execNode);
                        AbortJobsAtNode(execNode);
                    }
                    if (oldState != ENodeState::Online && newState == ENodeState::Online) {
                        AddNodeResources(execNode);
                    }
                }

                execNode->SetMasterState(newState);
                execNode->SetIOWeight(ioWeight);

                if (oldState != newState) {
                    LOG_INFO("Node %lv (Address: %v)", newState, address);
                }
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating nodes information");
        }

        LOG_INFO("Nodes information updated");
    }

    void RequestOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static auto runtimeParamsTemplate = New<TOperationRuntimeParams>();
        auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()) + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), runtimeParamsTemplate->GetRegisteredKeys());
        batchReq->AddRequest(req, "get_runtime_params");
    }

    void HandleOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params");
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error updating operation runtime parameters");
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto attributesNode = ConvertToNode(TYsonString(rsp->value()));

        OperationRuntimeParamsUpdated_.Fire(operation, attributesNode);
    }

    void RequestConfig(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating scheduler configuration");

        auto req = TYPathProxy::Get("//sys/scheduler/config");
        batchReq->AddRequest(req, "get_config");
    }

    void HandleConfig(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_config");
        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No config in Cypress, just ignore.
            return;
        }
        if (!rspOrError.IsOK()) {
            LOG_ERROR(rspOrError, "Error getting scheduler configuration");
            return;
        }

        auto oldConfig = ConvertToNode(Config_);

        try {
            const auto& rsp = rspOrError.Value();
            auto configFromCypress = ConvertToNode(TYsonString(rsp->value()));

            auto mergedConfig = UpdateNode(InitialConfig_, configFromCypress);
            try {
                Config_->Load(mergedConfig, /* validate */ true, /* setDefaults */ true);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error updating cell scheduler configuration");
                Config_->Load(oldConfig, /* validate */ true, /* setDefaults */ true);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing updated scheduler configuration");
        }

        auto newConfig = ConvertToNode(Config_);

        if (!AreNodesEqual(oldConfig, newConfig)) {
            LOG_INFO("Scheduler configuration updated");
            auto config = CloneYsonSerializable(Config_);
            for (const auto& operation : GetOperations()) {
                auto controller = operation->GetController();
                BIND(&IOperationController::UpdateConfig, controller, config)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
            }
        }
    }


    void DoStartOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        bool registered = false;
        try {
            auto controller = New<TOperationControllerWrapper>(
                operation->GetId(),
                CreateController(operation.Get()),
                ControllerThreadPool_->GetInvoker());
            operation->SetController(controller);

            RegisterOperation(operation);
            registered = true;

            controller->Initialize(/* cleanStart */ true);

            WaitFor(MasterConnector_->CreateOperationNode(operation))
                .ThrowOnError();

            if (operation->GetState() != EOperationState::Initializing) {
                throw TFiberCanceledException();
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            if (registered) {
                OnOperationFailed(operation, wrappedError);
            } else {
                operation->SetStarted(wrappedError);
            }
            THROW_ERROR(wrappedError);
        }

        LogEventFluently(ELogEventType::OperationStarted)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec());

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a separate fiber in a non-blocking
        // fashion.
        auto controller = operation->GetController();
        BIND(&TImpl::DoPrepareOperation, MakeStrong(this), operation)
            .AsyncVia(controller->GetCancelableControlInvoker())
            .Run();

        operation->SetStarted(TError());
    }

    void DoPrepareOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        const auto& operationId = operation->GetId();

        try {
            // Run async preparation.
            LOG_INFO("Preparing operation (OperationId: %v)",
                operationId);

            operation->SetState(EOperationState::Preparing);

            auto controller = operation->GetController();
            auto asyncResult = BIND(&IOperationController::Prepare, controller)
                .AsyncVia(controller->GetCancelableInvoker())
                .Run();

            NProfiling::TScopedTimer timer;
            auto result = WaitFor(asyncResult);
            auto prepareDuration = timer.GetElapsed();
            operation->UpdateControllerTimeStatistics("/prepare", prepareDuration);

            THROW_ERROR_EXCEPTION_IF_FAILED(result);

            if (operation->GetState() != EOperationState::Preparing) {
                throw TFiberCanceledException();
            }
            operation->SetState(EOperationState::Pending);
            operation->SetPrepared(true);
            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to prepare")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        LOG_INFO("Operation has been prepared (OperationId: %v)",
            operationId);

        LogEventFluently(ELogEventType::OperationPrepared)
            .Item("operation_id").Value(operationId);

        LogOperationProgress(operation);

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }

    void AbortAbortingOperations(const std::vector<TOperationPtr>& operations)
    {
        for (const auto& operation : operations) {
            AbortAbortingOperation(operation);
        }
    }

    void ReviveOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& operation : operations) {
            ReviveOperation(operation);
        }
    }

    void ReviveOperation(TOperationPtr operation)
    {
        auto codicilGuard = operation->MakeCodicilGuard();

        const auto& operationId = operation->GetId();

        LOG_INFO("Reviving operation (OperationId: %v)",
            operationId);

        if (operation->GetMutationId()) {
            TRspStartOperation response;
            ToProto(response.mutable_operation_id(), operationId);
            auto responseMessage = CreateResponseMessage(response);
            auto responseKeeper = Bootstrap_->GetResponseKeeper();
            responseKeeper->EndRequest(operation->GetMutationId(), responseMessage);
        }

        // NB: The operation is being revived, hence it already
        // has a valid node associated with it.
        // If the revival fails, we still need to update the node
        // and unregister the operation from Master Connector.

        try {
            auto controller = CreateController(operation.Get());
            operation->SetController(controller);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %v)",
                operationId);
            auto wrappedError = TError("Operation has failed to revive") << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);
            MasterConnector_->FlushOperationNode(operation);
            return;
        }

        RegisterOperation(operation);

        auto controller = operation->GetController();
        BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation)
            .AsyncVia(controller->GetCancelableControlInvoker())
            .Run();
    }

    void DoReviveOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        try {
            auto controller = operation->GetController();
            TSharedRef snapshot;

            {
                bool cleanStart = false;

                auto snapshotOrError = WaitFor(MasterConnector_->DownloadSnapshot(operation->GetId()));
                if (!snapshotOrError.IsOK()) {
                    LOG_INFO(snapshotOrError, "Failed to download snapshot, will use clean start (OperationId: %v)", operation->GetId());
                    cleanStart = true;
                    auto error = WaitFor(MasterConnector_->RemoveSnapshot(operation->GetId()));
                    if (!error.IsOK()) {
                        LOG_WARNING(error, "Failed to remove snapshot (OperationId: %v)", operation->GetId());
                    }
                } else {
                    LOG_INFO("Snapshot succesfully downloaded (OperationId: %v)", operation->GetId());
                    snapshot = snapshotOrError.Value();
                }

                controller->Initialize(cleanStart);
            }

            if (operation->GetState() != EOperationState::Reviving) {
                throw TFiberCanceledException();
            }

            {
                auto error = WaitFor(MasterConnector_->ResetRevivingOperationNode(operation));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            {
                auto asyncResult = VoidFuture;
                if (controller->GetCleanStart()) {
                    asyncResult = BIND(&IOperationController::Prepare, controller)
                        .AsyncVia(controller->GetCancelableInvoker())
                        .Run();
                } else {
                    asyncResult = BIND(&IOperationController::Revive, controller, snapshot)
                        .AsyncVia(controller->GetCancelableInvoker())
                        .Run();
                }
                auto error = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            if (operation->GetState() != EOperationState::Reviving) {
                throw TFiberCanceledException();
            }

            operation->SetState(EOperationState::Pending);
            operation->SetPrepared(true);
            if (operation->GetActivated()) {
                MaterializeOperation(operation);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %v)",
                operation->GetId());
            auto wrappedError = TError("Operation has failed to revive") << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        LOG_INFO("Operation has been revived and is now running (OperationId: %v)",
            operation->GetId());
    }


    TExecNodePtr GetOrRegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TReaderGuard guard(ExecNodeMapLock_);
        auto it = AddressToNode_.find(descriptor.GetDefaultAddress());
        if (it == AddressToNode_.end()) {
            // NB: RegisterNode will acquire the write lock.
            // This may seem racy but in fact it is not since nodes only get registered
            // in Control Thread.
            guard.Release();
            return RegisterNode(nodeId, descriptor);
        }

        auto node = it->second;
        // Update the current descriptor, just in case.
        node->UpdateNodeDescriptor(descriptor);
        return node;
    }

    TExecNodePtr RegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto node = New<TExecNode>(nodeId, descriptor);
        const auto& address = node->GetDefaultAddress();

        auto lease = TLeaseManager::CreateLease(
            Config_->NodeHeartbeatTimeout,
            BIND(&TImpl::UnregisterNode, MakeWeak(this), node)
                .Via(GetControlInvoker()));

        node->SetLease(lease);

        {
            TWriterGuard guard(ExecNodeMapLock_);
            YCHECK(AddressToNode_.insert(std::make_pair(address, node)).second);
            YCHECK(IdToNode_.insert(std::make_pair(node->GetId(), node)).second);
        }

        LOG_INFO("Node registered (Address: %v)", address);

        return node;
    }

    void AbortJobsAtNode(TExecNodePtr node)
    {
        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        const auto& address = node->GetDefaultAddress();
        for (const auto& job : jobs) {
            LOG_DEBUG("Aborting job on an offline node %v (JobId: %v, OperationId: %v)",
                address,
                job->GetId(),
                job->GetOperationId());
            OnJobAborted(job, JobStatusFromError(TError("Node offline")));
        }
    }

    void UpdateNodeTags(TExecNodePtr node, const std::vector<Stroka>& tagsList)
    {
        yhash_set<Stroka> newTags(tagsList.begin(), tagsList.end());

        for (const auto& tag : newTags) {
            if (NodeTagToResources_.find(tag) == NodeTagToResources_.end()) {
                YCHECK(NodeTagToResources_.insert(std::make_pair(tag, TJobResources())).second);
            }
        }

        if (node->GetMasterState() == ENodeState::Online) {
            auto oldTags = node->Tags();
            for (const auto& oldTag : oldTags) {
                if (newTags.find(oldTag) == newTags.end()) {
                    NodeTagToResources_[oldTag] -= node->GetResourceLimits();
                }
            }

            for (const auto& tag : newTags) {
                if (oldTags.find(tag) == oldTags.end()) {
                    NodeTagToResources_[tag] += node->GetResourceLimits();
                }
            }
        }

        node->Tags() = newTags;
    }

    void SubtractNodeResources(TExecNodePtr node)
    {
        TotalResourceLimits_ -= node->GetResourceLimits();
        TotalResourceUsage_ -= node->GetResourceUsage();
        TotalNodeCount_ -= 1;
        if (node->GetResourceLimits().GetUserSlots() > 0) {
            ExecNodeCount_ -= 1;
        }

        for (const auto& tag : node->Tags()) {
            NodeTagToResources_[tag] -= node->GetResourceLimits();
        }
    }

    void AddNodeResources(TExecNodePtr node)
    {
        TotalResourceLimits_ += node->GetResourceLimits();
        TotalResourceUsage_ += node->GetResourceUsage();
        TotalNodeCount_ += 1;

        if (node->GetResourceLimits().GetUserSlots() > 0) {
            ExecNodeCount_ += 1;
        } else {
            // Check that we succesfully reset all resource limits to zero for node with zero user slots.
            YCHECK(node->GetResourceLimits() == ZeroJobResources());
        }

        for (const auto& tag : node->Tags()) {
            NodeTagToResources_[tag] += node->GetResourceLimits();
        }
    }

    void UnregisterNode(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (node->GetHasOngoingHeartbeat()) {
            LOG_INFO("Node unregistration postponed until heartbeat is finished (Address: %v)",
                node->GetDefaultAddress());
            node->SetHasPendingUnregistration(true);
        } else {
            DoUnregisterNode(node);
        }
    }

    void DoUnregisterNode(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node unregistered (Address: %v)", node->GetDefaultAddress());

        if (node->GetMasterState() == ENodeState::Online) {
            SubtractNodeResources(node);
        }

        AbortJobsAtNode(node);

        {
            TWriterGuard guard(ExecNodeMapLock_);
            YCHECK(AddressToNode_.erase(node->GetDefaultAddress()) == 1);
            YCHECK(IdToNode_.erase(node->GetId()) == 1);
        }
    }

    void UpdateNodeResources(TExecNodePtr node, const TJobResources& limits, const TJobResources& usage)
    {
        auto oldResourceLimits = node->GetResourceLimits();
        auto oldResourceUsage = node->GetResourceUsage();

        // NB: Total limits are updated separately in heartbeat.
        if (limits.GetUserSlots() > 0) {
            if (node->GetResourceLimits().GetUserSlots() == 0 && node->GetMasterState() == ENodeState::Online) {
                ExecNodeCount_ += 1;
            }
            node->SetResourceLimits(limits);
            node->SetResourceUsage(usage);
        } else {
            if (node->GetResourceLimits().GetUserSlots() > 0 && node->GetMasterState() == ENodeState::Online) {
                ExecNodeCount_ -= 1;
            }
            node->SetResourceLimits(ZeroJobResources());
            node->SetResourceUsage(ZeroJobResources());
        }

        if (node->GetMasterState() == ENodeState::Online) {
            TotalResourceLimits_ -= oldResourceLimits;
            TotalResourceLimits_ += node->GetResourceLimits();
            for (const auto& tag : node->Tags()) {
                auto& resources = NodeTagToResources_[tag];
                resources -= oldResourceLimits;
                resources += node->GetResourceLimits();
            }

            TotalResourceUsage_ -= oldResourceUsage;
            TotalResourceUsage_ += node->GetResourceUsage();
        }
    }

    void BeginNodeHeartbeatProcessing(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        node->SetHasOngoingHeartbeat(true);

        ConcurrentHeartbeatCount_ += 1;
    }

    void EndNodeHeartbeatProcessing(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(node->GetHasOngoingHeartbeat());

        node->SetHasOngoingHeartbeat(false);

        ConcurrentHeartbeatCount_ -= 1;
        node->SetLastSeenTime(TInstant::Now());

        if (node->GetHasPendingUnregistration()) {
            DoUnregisterNode(node);
        }
    }


    void RegisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation_.insert(std::make_pair(operation->GetId(), operation)).second);

        OperationRegistered_.Fire(operation);

        GetMasterConnector()->AddOperationWatcherRequester(
            operation,
            BIND(&TImpl::RequestOperationRuntimeParams, Unretained(this), operation));
        GetMasterConnector()->AddOperationWatcherHandler(
            operation,
            BIND(&TImpl::HandleOperationRuntimeParams, Unretained(this), operation));

        LOG_DEBUG("Operation registered (OperationId: %v)",
            operation->GetId());
    }

    void AbortOperationJobs(TOperationPtr operation)
    {
        auto jobs = operation->Jobs();
        for (const auto& job : jobs) {
            OnJobAborted(
                job,
                JobStatusFromError(TError("Operation is in %Qlv state", operation->GetState())));
        }

        for (const auto& job : operation->Jobs()) {
            YCHECK(job->GetHasPendingUnregistration());
        }
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);

        OperationUnregistered_.Fire(operation);

        LOG_DEBUG("Operation unregistered (OperationId: %v)",
            operation->GetId());
    }

    void LogOperationProgress(TOperationPtr operation)
    {
        if (operation->GetState() != EOperationState::Running)
            return;

        auto controller = operation->GetController();
        auto controllerLoggingProgress = WaitFor(
            BIND(&IOperationController::GetLoggingProgress, controller)
                .AsyncVia(controller->GetInvoker())
                .Run())
            .ValueOrThrow();

        if (!FindOperation(operation->GetId())) {
            return;
        }

        LOG_DEBUG("Progress: %v, %v (OperationId: %v)",
            controllerLoggingProgress,
            Strategy_->GetOperationLoggingProgress(operation->GetId()),
            operation->GetId());
    }

    void SetOperationFinalState(TOperationPtr operation, EOperationState state, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!operation->GetStarted().IsSet()) {
            operation->SetStarted(error);
        }
        operation->SetState(state);
        operation->SetFinishTime(TInstant::Now());
        ToProto(operation->Result().mutable_error(), error);
    }


    void CommitSchedulerTransactions(TOperationPtr operation)
    {
        YCHECK(operation->GetState() == EOperationState::Completing);

        LOG_INFO("Committing scheduler transactions (OperationId: %v)",
            operation->GetId());

        auto commitTransaction = [&] (ITransactionPtr transaction) {
            if (!transaction) {
                return;
            }
            auto result = WaitFor(transaction->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Operation has failed to commit");
            if (operation->GetState() != EOperationState::Completing) {
                throw TFiberCanceledException();
            }
        };

        operation->SetHasActiveTransactions(false);
        commitTransaction(operation->GetInputTransaction());
        commitTransaction(operation->GetOutputTransaction());
        commitTransaction(operation->GetSyncSchedulerTransaction());

        LOG_INFO("Scheduler transactions committed (OperationId: %v)",
            operation->GetId());

        // NB: Never commit async transaction since it's used for writing Live Preview tables.
        operation->GetAsyncSchedulerTransaction()->Abort();
    }

    // TODO(ignat): unify with aborting transactions in controller.
    void AbortSchedulerTransactions(TOperationPtr operation)
    {
        auto abortTransaction = [&] (ITransactionPtr transaction) {
            if (transaction) {
                // Fire-and-forget.
                transaction->Abort();
            }
        };

        operation->SetHasActiveTransactions(false);
        abortTransaction(operation->GetInputTransaction());
        abortTransaction(operation->GetOutputTransaction());
        abortTransaction(operation->GetSyncSchedulerTransaction());
        abortTransaction(operation->GetAsyncSchedulerTransaction());
    }

    void FinishOperation(TOperationPtr operation)
    {
        if (!operation->GetFinished().IsSet()) {
            operation->SetFinished();
            operation->SetController(nullptr);
            operation->UpdateControllerTimeStatistics(
                Strategy_->GetOperationTimeStatistics(operation->GetId()));
            UnregisterOperation(operation);
        }
    }


    void RegisterJob(TJobPtr job)
    {
        auto operation = GetOperation(job->GetOperationId());

        auto node = job->GetNode();

        YCHECK(operation->Jobs().insert(job).second);
        YCHECK(node->Jobs().insert(job).second);
        YCHECK(node->IdToJob().insert(std::make_pair(job->GetId(), job)).second);
        ++ActiveJobCount_;

        LOG_DEBUG("Job registered (JobId: %v, JobType: %v, OperationId: %v)",
            job->GetId(),
            job->GetType(),
            operation->GetId());
    }

    void UnregisterJob(TJobPtr job)
    {
        auto node = job->GetNode();

        if (node->GetHasOngoingJobsScheduling()) {
            job->SetHasPendingUnregistration(true);
        } else {
            DoUnregisterJob(job);
        }
    }

    void DoUnregisterJob(TJobPtr job)
    {
        auto operation = FindOperation(job->GetOperationId());
        auto node = job->GetNode();

        YCHECK(!node->GetHasOngoingJobsScheduling());

        YCHECK(node->Jobs().erase(job) == 1);
        YCHECK(node->IdToJob().erase(job->GetId()) == 1);
        --ActiveJobCount_;

        if (operation) {
            YCHECK(operation->Jobs().erase(job) == 1);
            JobFinished_.Fire(job);

            LOG_DEBUG("Job unregistered (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
        } else {
            LOG_DEBUG("Dangling job unregistered (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
        }
    }

    TExecNodePtr GetNodeByJob(const TJobId& jobId)
    {
        TReaderGuard guard(ExecNodeMapLock_);
        auto nodeId = GetNodeId(jobId);
        auto it = IdToNode_.find(nodeId);
        if (it == IdToNode_.end()) {
            return nullptr;
        }
        return it->second;
    }

    TJobPtr FindJob(const TJobId& jobId, const TExecNodePtr& node)
    {
        const auto& idToJob = node->IdToJob();
        auto it = idToJob.find(jobId);
        return it == idToJob.end() ? nullptr : it->second;
    }

    TJobPtr FindJob(const TJobId& jobId)
    {
        auto node = GetNodeByJob(jobId);
        if (!node) {
            return nullptr;
        }
        return FindJob(jobId, node);
    }

    TJobPtr GetJobOrThrow(const TJobId& jobId)
    {
        auto job = FindJob(jobId);
        if (!job) {
            THROW_ERROR_EXCEPTION("No such job %v", jobId);
        }
        return job;
    }

    void PreemptJob(TJobPtr job)
    {
        YCHECK(FindOperation(job->GetOperationId()));

        LOG_DEBUG("Preempting job (JobId: %v, OperationId: %v)",
            job->GetId(),
            job->GetOperationId());

        job->SetPreempted(true);
    }


    void OnJobRunning(TJobPtr job, TRefCountedJobStatusPtr status)
    {
        auto delta = status->resource_usage() - job->ResourceUsage();
        JobUpdated_.Fire(job, delta);
        job->ResourceUsage() = status->resource_usage();
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting)
        {
            job->SetStatus(std::move(status));
        }
    }

    void OnJobWaiting(TJobPtr /*job*/)
    {
        // Do nothing.
    }

    void OnJobCompleted(TJobPtr job, TRefCountedJobStatusPtr status, bool abandoned = false)
    {
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting ||
            job->GetState() == EJobState::None)
        {
            SetJobState(job, EJobState::Completed);
            job->SetStatus(std::move(status));

            OnJobFinished(job);

            auto operation = GetOperation(job->GetOperationId());

            ProcessFinishedJobResult(job);

            if (operation->GetState() == EOperationState::Running) {
                const auto& controller = operation->GetController();
                controller->GetCancelableInvoker()->Invoke(BIND(
                    &IOperationController::OnJobCompleted,
                    controller,
                    Passed(std::make_unique<TCompletedJobSummary>(job, abandoned))));
            }
        }

        UnregisterJob(job);
    }

    void OnJobFailed(TJobPtr job, TRefCountedJobStatusPtr status)
    {
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting ||
            job->GetState() == EJobState::None)
        {
            SetJobState(job, EJobState::Failed);
            job->SetStatus(std::move(status));

            OnJobFinished(job);

            auto operation = GetOperation(job->GetOperationId());

            ProcessFinishedJobResult(job);

            if (operation->GetState() == EOperationState::Running) {
                const auto& controller = operation->GetController();
                controller->GetCancelableInvoker()->Invoke(BIND(
                    &IOperationController::OnJobFailed,
                    controller,
                    Passed(std::make_unique<TFailedJobSummary>(job))));
            }
        }

        UnregisterJob(job);
    }

    void OnJobAborted(TJobPtr job, TRefCountedJobStatusPtr status)
    {
        // Only update the status for the first time.
        // Typically the scheduler decides to abort the job on its own.
        // In this case we should ignore the status returned from the node
        // and avoid notifying the controller twice.
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting ||
            job->GetState() == EJobState::None)
        {
            job->SetStatus(std::move(status));
            // We should set status before to correctly consider AbortReason.
            SetJobState(job, EJobState::Aborted);

            OnJobFinished(job);

            auto operation = GetOperation(job->GetOperationId());

            // Check if job was aborted due to signal.
            if (GetAbortReason(job->Status()->result()) == EAbortReason::UserRequest) {
                ProcessFinishedJobResult(job);
            }

            if (operation->GetState() == EOperationState::Running) {
                const auto& controller = operation->GetController();
                controller->GetCancelableInvoker()->Invoke(BIND(
                    &IOperationController::OnJobAborted,
                    controller,
                    Passed(std::make_unique<TAbortedJobSummary>(job))));
            }
        }

        UnregisterJob(job);
    }

    void OnJobFinished(TJobPtr job)
    {
        job->SetFinishTime(TInstant::Now());
        auto duration = job->GetDuration();

        switch (job->GetState()) {
            case EJobState::Completed:
                Profiler.Increment(TotalCompletedJobTimeCounter_, duration.MicroSeconds());
                break;
            case EJobState::Failed:
                Profiler.Increment(TotalFailedJobTimeCounter_, duration.MicroSeconds());
                break;
            case EJobState::Aborted:
                Profiler.Increment(TotalAbortedJobTimeCounter_, duration.MicroSeconds());
                break;
            default:
                YUNREACHABLE();
        }
    }

    void ProcessFinishedJobResult(TJobPtr job)
    {
        auto jobFailedOrAborted = job->GetState() == EJobState::Failed || job->GetState() == EJobState::Aborted;
        const auto& schedulerResultExt = job->Status()->result().GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        auto stderrChunkId = FromProto<TChunkId>(schedulerResultExt.stderr_chunk_id());
        auto failContextChunkId = FromProto<TChunkId>(schedulerResultExt.fail_context_chunk_id());

        auto operation = GetOperation(job->GetOperationId());

        if (jobFailedOrAborted) {
            if (stderrChunkId) {
                operation->SetStderrCount(operation->GetStderrCount() + 1);
            }
            if (operation->GetJobNodeCount() < Config_->MaxJobNodesPerOperation) {
                MasterConnector_->CreateJobNode(job, stderrChunkId, failContextChunkId, operation->MakeInputPathsYson(job));
                operation->SetJobNodeCount(operation->GetJobNodeCount() + 1);
            }
            return;
        }

        YCHECK(!failContextChunkId);
        if (!stderrChunkId) {
            // Do not create job node.
            return;
        }

        // Job has not failed, but has stderr.
        if (operation->GetStderrCount() < operation->GetMaxStderrCount() &&
            operation->GetJobNodeCount() < Config_->MaxJobNodesPerOperation)
        {
            MasterConnector_->CreateJobNode(job, stderrChunkId, failContextChunkId, operation->MakeInputPathsYson(job));
            operation->SetStderrCount(operation->GetStderrCount() + 1);
            operation->SetJobNodeCount(operation->GetJobNodeCount() + 1);
        } else {
            ReleaseStderrChunk(job, stderrChunkId);
        }
    }

    void ReleaseStderrChunk(TJobPtr job, const TChunkId& chunkId)
    {
        auto operation = GetOperation(job->GetOperationId());
        auto transaction = operation->GetAsyncSchedulerTransaction();
        if (!transaction)
            return;

        auto channel = GetMasterClient()->GetMasterChannelOrThrow(NApi::EMasterChannelKind::Leader);
        TChunkServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        auto req = batchReq->add_unstage_chunk_tree_subrequests();
        ToProto(req->mutable_chunk_tree_id(), chunkId);
        req->set_recursive(false);

        // Fire-and-forget.
        // The subscriber is only needed to log the outcome.
        batchReq->Invoke().Subscribe(
            BIND(&TImpl::OnStderrChunkReleased, MakeStrong(this)));
    }

    void OnStderrChunkReleased(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
    {
        // NB: We only look at the topmost error and ignore subresponses.
        if (!batchRspOrError.IsOK()) {
            LOG_WARNING(batchRspOrError, "Error releasing stderr chunk");
        }
    }

    void InitStrategy()
    {
        Strategy_ = CreateFairShareStrategy(Config_, this);
    }

    IOperationControllerPtr CreateController(TOperation* operation)
    {
        auto config = CloneYsonSerializable(Config_);

        switch (operation->GetType()) {
            case EOperationType::Map:
                return CreateMapController(config, this, operation);
            case EOperationType::Merge:
                return CreateMergeController(config, this, operation);
            case EOperationType::Erase:
                return CreateEraseController(config, this, operation);
            case EOperationType::Sort:
                return CreateSortController(config, this, operation);
            case EOperationType::Reduce:
                return CreateReduceController(config, this, operation);
            case EOperationType::JoinReduce:
                return CreateJoinReduceController(config, this, operation);
            case EOperationType::MapReduce:
                return CreateMapReduceController(config, this, operation);
            case EOperationType::RemoteCopy:
                return CreateRemoteCopyController(config, this, operation);
            default:
                YUNREACHABLE();
        }
    }

    INodePtr GetSpecTemplate(EOperationType type, IMapNodePtr spec)
    {
        switch (type) {
            case EOperationType::Map:
                return Config_->MapOperationOptions->SpecTemplate;
            case EOperationType::Merge: {
                auto mergeSpec = ParseOperationSpec<TMergeOperationSpec>(spec);
                switch (mergeSpec->Mode) {
                    case EMergeMode::Unordered:
                        return Config_->UnorderedMergeOperationOptions->SpecTemplate;
                    case EMergeMode::Ordered:
                        return Config_->OrderedMergeOperationOptions->SpecTemplate;
                    case EMergeMode::Sorted:
                        return Config_->SortedMergeOperationOptions->SpecTemplate;
                    default:
                        YUNREACHABLE();
                }
            }
            case EOperationType::Erase:
                return Config_->EraseOperationOptions->SpecTemplate;
            case EOperationType::Sort:
                return Config_->SortOperationOptions->SpecTemplate;
            case EOperationType::Reduce:
                return Config_->ReduceOperationOptions->SpecTemplate;
            case EOperationType::JoinReduce:
                return Config_->JoinReduceOperationOptions->SpecTemplate;
            case EOperationType::MapReduce:
                return Config_->MapReduceOperationOptions->SpecTemplate;
            case EOperationType::RemoteCopy:
                return Config_->RemoteCopyOperationOptions->SpecTemplate;
            default:
                YUNREACHABLE();
        }
    }

    TJobProberServiceProxy CreateJobProberProxy(const TJobPtr& job)
    {
        const auto& address = job->GetNode()->GetInterconnectAddress();
        auto factory = Bootstrap_->GetMasterClient()->GetNodeChannelFactory();
        auto channel = factory->CreateChannel(address);

        TJobProberServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Bootstrap_->GetConfig()->Scheduler->JobProberRpcTimeout);
        return proxy;
    }

    TYsonString DoStrace(const TJobId& jobId, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        ValidatePermission(user, job->GetOperationId(), EPermission::Write);

        LOG_INFO("Getting strace dump (JobId: %v)",
            jobId);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.Strace();
        ToProto(req->mutable_job_id(), jobId);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting strace dump of job %v",
            jobId);

        const auto& rsp = rspOrError.Value();

        LOG_INFO("Strace dump received (JobId: %v)",
            jobId);

        return TYsonString(rsp->trace());
    }

    void DoDumpInputContext(const TJobId& jobId, const TYPath& path, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        ValidatePermission(user, job->GetOperationId(), EPermission::Write);

        LOG_INFO("Saving input contexts (JobId: %v, Path: %v)",
            jobId,
            path);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.DumpInputContext();
        ToProto(req->mutable_job_id(), jobId);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error saving input context of job %v into %v",
            jobId,
            path);

        const auto& rsp = rspOrError.Value();
        auto chunkIds = FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
        YCHECK(chunkIds.size() == 1);

        MasterConnector_->AttachJobContext(path, chunkIds.front(), job);

        LOG_INFO("Input contexts saved (JobId: %v)",
            jobId);
    }

    void DoSignalJob(const TJobId& jobId, const Stroka& signalName, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        ValidatePermission(user, job->GetOperationId(), EPermission::Write);

        LOG_INFO("Sending job signal (JobId: %v, Signal: %v)",
            jobId,
            signalName);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.SignalJob();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_signal_name(), signalName);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error sending signal %v to job %v",
            signalName,
            jobId);

        LOG_INFO("Job signal sent (JobId: %v)",
            jobId);
    }

    void DoAbandonJob(const TJobId& jobId, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        ValidatePermission(user, job->GetOperationId(), EPermission::Write);

        switch (job->GetType()) {
            case EJobType::Map:
            case EJobType::OrderedMap:
            case EJobType::SortedReduce:
            case EJobType::PartitionMap:
            case EJobType::ReduceCombiner:
            case EJobType::PartitionReduce:
                break;
            default:
                THROW_ERROR_EXCEPTION("Cannot abandon job %v of type %Qlv",
                    jobId,
                    job->GetType());
        }

        if (job->GetState() != EJobState::Running &&
            job->GetState() != EJobState::Waiting)
        {
            THROW_ERROR_EXCEPTION("Cannot abandon job %v since it is not running",
                jobId);
        }

        auto status = New<TRefCountedJobStatus>();
        OnJobCompleted(job, std::move(status), /* abandoned */ true);
    }

    TYsonString DoPollJobShell(const TJobId& jobId, const TYsonString& parameters, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        TShellParameters shellParameters;
        Deserialize(shellParameters, ConvertToNode(parameters));
        if (shellParameters.Operation == EShellOperation::Spawn) {
            ValidatePermission(user, job->GetOperationId(), EPermission::Write);
        }

        LOG_INFO("Polling job shell (JobId: %v, Parameters: %v)",
            jobId,
            ConvertToYsonString(parameters, EYsonFormat::Text));

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.PollJobShell();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_parameters(), parameters.Data());

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Error polling job shell for job %v", jobId)
                << rspOrError
                << TErrorAttribute("parameters", parameters);
        }

        const auto& rsp = rspOrError.Value();
        return TYsonString(rsp->result());
    }

    void DoAbortJob(const TJobId& jobId, const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto job = GetJobOrThrow(jobId);

        ValidatePermission(user, job->GetOperationId(), EPermission::Write);

        if (job->GetState() != EJobState::Running &&
            job->GetState() != EJobState::Waiting)
        {
            THROW_ERROR_EXCEPTION("Cannot abort job %v since it is not running",
                jobId);
        }

        auto status = JobStatusFromError(TError("Job aborted by user request")
            << TErrorAttribute("abort_reason", EAbortReason::UserRequest)
            << TErrorAttribute("user", user));
        OnJobAborted(job, std::move(status));
    }

    void DoCompleteOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        const auto& operationId = operation->GetId();

        LOG_INFO("Completing operation (OperationId: %v)",
            operationId);

        operation->SetState(EOperationState::Completing);

        // The operation may still have running jobs (e.g. those started speculatively).
        AbortOperationJobs(operation);

        try {
            // First flush: ensure that all stderrs are attached and the
            // state is changed to Completing.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                WaitFor(asyncResult);
                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            {
                auto controller = operation->GetController();
                auto asyncResult = BIND(&IOperationController::Commit, controller)
                    .AsyncVia(controller->GetCancelableInvoker())
                    .Run();
                WaitFor(asyncResult)
                    .ThrowOnError();

                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            CommitSchedulerTransactions(operation);

            YCHECK(operation->GetState() == EOperationState::Completing);
            SetOperationFinalState(operation, EOperationState::Completed, TError());

            // Second flush: ensure that state is changed to Completed.
            {
                auto asyncResult = MasterConnector_->FlushOperationNode(operation);
                WaitFor(asyncResult);
                YCHECK(operation->GetState() == EOperationState::Completed);
            }

            FinishOperation(operation);
        } catch (const std::exception& ex) {
            OnOperationFailed(operation, ex);
            return;
        }

        LOG_INFO("Operation completed (OperationId: %v)",
             operationId);

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void DoFailOperation(TOperationPtr operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO(error, "Operation failed (OperationId: %v)",
             operation->GetId());

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            ELogEventType::OperationFailed,
            error);
    }

    void TerminateOperation(
        TOperationPtr operation,
        EOperationState intermediateState,
        EOperationState finalState,
        ELogEventType logEventType,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto state = operation->GetState();
        if (IsOperationFinished(state) ||
            state == EOperationState::Failing ||
            state == EOperationState::Aborting)
        {
            // Safe to call multiple times, just ignore it.
            return;
        }

        operation->SetState(intermediateState);

        AbortOperationJobs(operation);

        // First flush: ensure that all stderrs are attached and the
        // state is changed to its intermediate value.
        {
            auto asyncResult = MasterConnector_->FlushOperationNode(operation);
            WaitFor(asyncResult);
            if (operation->GetState() != intermediateState)
                return;
        }

        SetOperationFinalState(operation, finalState, error);

        AbortSchedulerTransactions(operation);

        // Second flush: ensure that the state is changed to its final value.
        {
            auto asyncResult = MasterConnector_->FlushOperationNode(operation);
            WaitFor(asyncResult);
            if (operation->GetState() != finalState)
                return;
        }

        auto controller = operation->GetController();
        if (controller) {
            controller->Abort();
        }

        FinishOperation(operation);

        LogOperationFinished(operation, logEventType, error);
    }

    void AbortAbortingOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto codicilGuard = operation->MakeCodicilGuard();

        LOG_INFO("Aborting operation (OperationId: %v)",
             operation->GetId());

        YCHECK(operation->GetState() == EOperationState::Aborting);

        AbortSchedulerTransactions(operation);
        SetOperationFinalState(operation, EOperationState::Aborted, TError());

        WaitFor(MasterConnector_->FlushOperationNode(operation));

        LogOperationFinished(operation, ELogEventType::OperationCompleted, TError());
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(MasterConnector_->IsConnected())
                .Item("cell").BeginMap()
                    .Item("resource_limits").Value(TotalResourceLimits_)
                    .Item("resource_usage").Value(TotalResourceUsage_)
                    .Item("exec_node_count").Value(ExecNodeCount_)
                    .Item("total_node_count").Value(TotalNodeCount_)
                .EndMap()
                .Item("operations").DoMapFor(GetOperations(), [=] (TFluentMap fluent, const TOperationPtr& operation) {
                    if (FindOperation(operation->GetId())) {
                        BuildOperationYson(operation, fluent);
                    }
                })
                .Item("nodes").DoMapFor(IdToNode_, [=] (TFluentMap fluent, const TExecNodeByIdMap::value_type& pair) {
                    BuildNodeYson(pair.second, fluent);
                })
                .Item("clusters").DoMapFor(GetClusterDirectory()->GetClusterNames(), [=] (TFluentMap fluent, const Stroka& clusterName) {
                    BuildClusterYson(clusterName, fluent);
                })
                .Item("config").Value(Config_)
                .DoIf(Strategy_ != nullptr, BIND(&ISchedulerStrategy::BuildOrchid, Strategy_.get()))
            .EndMap();
    }

    void BuildClusterYson(const Stroka& clusterName, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(clusterName)
            .Value(GetClusterDirectory()->GetConnection(clusterName)->GetConfig());
    }

    void BuildOperationYson(TOperationPtr operation, IYsonConsumer* consumer)
    {
        auto codicilGuard = operation->MakeCodicilGuard();

        auto controller = operation->GetController();
        bool hasControllerProgress = operation->HasControllerProgress();
        BuildYsonMapFluently(consumer)
            .Item(ToString(operation->GetId())).BeginMap()
                // Include the complete list of attributes.
                .Do(BIND(&NScheduler::BuildInitializingOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .DoIf(hasControllerProgress, BIND([=] (IYsonConsumer* consumer) {
                        WaitFor(
                            BIND(&IOperationController::BuildProgress, controller)
                                .AsyncVia(controller->GetInvoker())
                                .Run(consumer));
                    }))
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgress, Strategy_.get(), operation->GetId()))
                .EndMap()
                .Item("brief_progress").BeginMap()
                    .DoIf(hasControllerProgress, BIND([=] (IYsonConsumer* consumer) {
                        WaitFor(
                            BIND(&IOperationController::BuildBriefProgress, controller)
                                .AsyncVia(controller->GetInvoker())
                                .Run(consumer));
                    }))
                    .Do(BIND(&ISchedulerStrategy::BuildBriefOperationProgress, Strategy_.get(), operation->GetId()))
                .EndMap()
                .Item("running_jobs").BeginAttributes()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .DoMapFor(operation->Jobs(), [=] (TFluentMap fluent, TJobPtr job) {
                    BuildJobYson(job, fluent);
                })
                .Do(BIND([=] (IYsonConsumer* consumer) {
                    WaitFor(
                        BIND(&IOperationController::BuildMemoryDigestStatistics, controller)
                            .AsyncVia(controller->GetInvoker())
                            .Run(consumer));
                    }))
            .EndMap();
    }

    void BuildJobYson(TJobPtr job, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(ToString(job->GetId())).BeginMap()
                .Do([=] (TFluentMap fluent) {
                    BuildJobAttributes(job, Null, fluent);
                })
            .EndMap();
    }

    void BuildNodeYson(TExecNodePtr node, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(node->GetDefaultAddress()).BeginMap()
                .Do([=] (TFluentMap fluent) {
                    BuildExecNodeAttributes(node, fluent);
                })
            .EndMap();
    }


    TJobPtr ProcessJobHeartbeat(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        TRefCountedJobStatusPtr jobStatus,
        bool forceJobsLogging,
        bool updateRunningJobs)
    {
        auto jobId = FromProto<TJobId>(jobStatus->job_id());
        auto state = EJobState(jobStatus->state());

        NLogging::TLogger Logger(SchedulerLogger);
        Logger.AddTag("Address: %v, JobId: %v",
            node->GetDefaultAddress(),
            jobId);

        auto job = FindJob(jobId, node);
        if (!job) {
            switch (state) {
                case EJobState::Completed:
                    LOG_DEBUG("Unknown job has completed, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Failed:
                    LOG_DEBUG("Unknown job has failed, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Aborted:
                    LOG_DEBUG(FromProto<TError>(jobStatus->result().error()), "Job aborted, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Running:
                    LOG_DEBUG("Unknown job is running, abort scheduled");
                    ToProto(response->add_jobs_to_abort(), jobId);
                    break;

                case EJobState::Waiting:
                    LOG_DEBUG("Unknown job is waiting, abort scheduled");
                    ToProto(response->add_jobs_to_abort(), jobId);
                    break;

                case EJobState::Aborting:
                    LOG_DEBUG("Job is aborting");
                    break;

                default:
                    YUNREACHABLE();
            }
            return nullptr;
        }

        auto operation = GetOperation(job->GetOperationId());

        auto codicilGuard = operation->MakeCodicilGuard();

        Logger.AddTag("JobType: %v, State: %v, OperationId: %v",
            job->GetType(),
            state,
            operation->GetId());

        // Check if the job is running on a proper node.
        if (node->GetId() != job->GetNode()->GetId()) {
            const auto& expectedAddress = job->GetNode()->GetDefaultAddress();
            // Job has moved from one node to another. No idea how this could happen.
            if (state == EJobState::Aborting) {
                // Do nothing, job is already terminating.
            } else if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
                ToProto(response->add_jobs_to_remove(), jobId);
                LOG_WARNING("Job status report was expected from %v, removal scheduled",
                    expectedAddress);
            } else {
                ToProto(response->add_jobs_to_abort(), jobId);
                LOG_WARNING("Job status report was expected from %v, abort scheduled",
                    expectedAddress);
            }
            return nullptr;
        }

        bool shouldLogJob = (state != job->GetState()) || forceJobsLogging;
        switch (state) {
            case EJobState::Completed: {
                LOG_DEBUG("Job completed, removal scheduled");
                OnJobCompleted(job, std::move(jobStatus));
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Failed: {
                auto error = FromProto<TError>(jobStatus->result().error());
                LOG_DEBUG(error, "Job failed, removal scheduled");
                OnJobFailed(job, std::move(jobStatus));
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Aborted: {
                auto error = FromProto<TError>(jobStatus->result().error());
                LOG_DEBUG(error, "Job aborted, removal scheduled");
                if (job->GetPreempted() && error.GetCode() == NExecAgent::EErrorCode::AbortByScheduler) {
                    auto error = TError("Job preempted")
                        << TErrorAttribute("abort_reason", EAbortReason::Preemption);
                    OnJobAborted(job, JobStatusFromError(error));
                } else {
                    OnJobAborted(job, std::move(jobStatus));
                }
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Running:
            case EJobState::Waiting:
                if (job->GetState() == EJobState::Aborted) {
                    LOG_DEBUG("Aborting job");
                    ToProto(response->add_jobs_to_abort(), jobId);
                } else {
                    LOG_DEBUG_IF(shouldLogJob, "Job is %lv", state);
                    SetJobState(job, state);
                    switch (state) {
                        case EJobState::Running:
                            job->SetProgress(jobStatus->progress());
                            if (updateRunningJobs) {
                                OnJobRunning(job, std::move(jobStatus));
                            }
                            break;

                        case EJobState::Waiting:
                            if (updateRunningJobs) {
                                OnJobWaiting(job);
                            }
                            break;

                        default:
                            YUNREACHABLE();
                    }
                }
                break;

            case EJobState::Aborting:
                LOG_DEBUG("Job is aborting");
                break;

            default:
                YUNREACHABLE();
        }

        return job;
    }

};

////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TScheduler::~TScheduler() = default;

void TScheduler::Initialize()
{
    Impl_->Initialize();
}

ISchedulerStrategy* TScheduler::GetStrategy()
{
    return Impl_->GetStrategy();
}

IYPathServicePtr TScheduler::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

std::vector<TOperationPtr> TScheduler::GetOperations()
{
    return Impl_->GetOperations();
}

IInvokerPtr TScheduler::GetSnapshotIOInvoker()
{
    return Impl_->GetSnapshotIOInvoker();
}

bool TScheduler::IsConnected()
{
    return Impl_->IsConnected();
}

void TScheduler::ValidateConnected()
{
    Impl_->ValidateConnected();
}

TOperationPtr TScheduler::FindOperation(const TOperationId& id)
{
    return Impl_->FindOperation(id);
}

TOperationPtr TScheduler::GetOperationOrThrow(const TOperationId& id)
{
    return Impl_->GetOperationOrThrow(id);
}

TFuture<TOperationPtr> TScheduler::StartOperation(
    EOperationType type,
    const TTransactionId& transactionId,
    const TMutationId& mutationId,
    IMapNodePtr spec,
    const Stroka& user)
{
    return Impl_->StartOperation(
        type,
        transactionId,
        mutationId,
        spec,
        user);
}

TFuture<void> TScheduler::AbortOperation(
    TOperationPtr operation,
    const TError& error,
    const Stroka& user)
{
    return Impl_->AbortOperation(operation, error, user);
}

TFuture<void> TScheduler::SuspendOperation(
    TOperationPtr operation,
    const Stroka& user)
{
    return Impl_->SuspendOperation(operation, user);
}

TFuture<void> TScheduler::ResumeOperation(
    TOperationPtr operation,
    const Stroka& user)
{
    return Impl_->ResumeOperation(operation, user);
}

TFuture<void> TScheduler::CompleteOperation(
    TOperationPtr operation,
    const TError& error,
    const Stroka& user)
{
    return Impl_->CompleteOperation(operation, error, user);
}

TFuture<void> TScheduler::DumpInputContext(const TJobId& jobId, const NYPath::TYPath& path, const Stroka& user)
{
    return Impl_->DumpInputContext(jobId, path, user);
}

TFuture<TYsonString> TScheduler::Strace(const TJobId& jobId, const Stroka& user)
{
    return Impl_->Strace(jobId, user);
}

TFuture<void> TScheduler::SignalJob(const TJobId& jobId, const Stroka& signalName, const Stroka& user)
{
    return Impl_->SignalJob(jobId, signalName, user);
}

TFuture<void> TScheduler::AbandonJob(const TJobId& jobId, const Stroka& user)
{
    return Impl_->AbandonJob(jobId, user);
}

TFuture<TYsonString> TScheduler::PollJobShell(const TJobId& jobId, const TYsonString& parameters, const Stroka& user)
{
    return Impl_->PollJobShell(jobId, parameters, user);
}

TFuture<void> TScheduler::AbortJob(const TJobId& jobId, const Stroka& user)
{
    return Impl_->AbortJob(jobId, user);
}

void TScheduler::ProcessHeartbeat(TCtxHeartbeatPtr context)
{
    Impl_->ProcessHeartbeat(context);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
