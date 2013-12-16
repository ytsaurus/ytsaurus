#include "stdafx.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "null_strategy.h"
#include "fair_share_strategy.h"
#include "operation_controller.h"
#include "map_controller.h"
#include "merge_controller.h"
#include "sort_controller.h"
#include "helpers.h"
#include "master_connector.h"
#include "job_resources.h"
#include "private.h"
#include "snapshot_downloader.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/string.h>

#include <core/actions/invoker_util.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/fiber.h>

#include <core/rpc/dispatcher.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_ypath_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/scheduler/helpers.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/fluent.h>

#include <ytlib/hydra/public.h>
#include <ytlib/hydra/rpc_helpers.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NYson;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NHydra;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;

using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////

static auto& Logger = SchedulerLogger;
static auto& Profiler = SchedulerProfiler;
static TDuration ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public TRefCounted
    , public IOperationHost
    , public ISchedulerStrategyHost
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : Config(config)
        , Bootstrap(bootstrap)
        , BackgroundQueue(New<TActionQueue>("Background"))
        , SnapshotIOQueue(New<TActionQueue>("SnapshotIO"))
        , MasterConnector(new TMasterConnector(Config, Bootstrap))
        , TotalResourceLimitsProfiler(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUsageProfiler(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter("/total_completed_job_time")
        , TotalFailedJobTimeCounter("/total_failed_job_time")
        , TotalAbortedJobTimeCounter("/total_aborted_job_time")
        , JobTypeCounters(static_cast<int>(EJobType::SchedulerLast))
        , TotalResourceLimits(ZeroNodeResources())
        , TotalResourceUsage(ZeroNodeResources())
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_AFFINITY(GetControlInvoker(), ControlThread);
        VERIFY_INVOKER_AFFINITY(GetSnapshotIOInvoker(), SnapshotIOThread);
    }

    void Initialize()
    {
        InitStrategy();

        MasterConnector->AddGlobalWatcherRequester(BIND(
            &TImpl::RequestConfig,
            Unretained(this)));
        MasterConnector->AddGlobalWatcherHandler(BIND(
            &TImpl::HandleConfig,
            Unretained(this)));

        MasterConnector->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        MasterConnector->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        MasterConnector->SubscribeUserTransactionAborted(BIND(
            &TImpl::OnUserTransactionAborted,
            Unretained(this)));
        MasterConnector->SubscribeSchedulerTransactionAborted(BIND(
            &TImpl::OnSchedulerTransactionAborted,
            Unretained(this)));

        MasterConnector->Start();

        ProfilingExecutor = New<TPeriodicExecutor>(
            Bootstrap->GetControlInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor->Start();
    }


    IYPathServicePtr GetOrchidService()
    {
        auto producer = BIND(&TImpl::BuildOrchidYson, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    std::vector<TOperationPtr> GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TOperationPtr> operations;
        for (const auto& pair : IdToOperation) {
            operations.push_back(pair.second);
        }
        return operations;
    }

    IInvokerPtr GetSnapshotIOInvoker()
    {
        return SnapshotIOQueue->GetInvoker();
    }


    bool IsConnected()
    {
        return MasterConnector->IsConnected();
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

        auto it = IdToOperation.find(id);
        return it == IdToOperation.end() ? nullptr : it->second;
    }

    TOperationPtr GetOperationOrThrow(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        if (!operation) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::NoSuchOperation,
                "No such operation %s",
                ~ToString(id));
        }
        return operation;
    }

    TOperationPtr FindOperationByMutationId(const TMutationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = MutationIdToOperation.find(id);
        return it == MutationIdToOperation.end() ? nullptr : it->second;
    }


    TExecNodePtr FindNode(const Stroka& address)
    {
        auto it = AddressToNode.find(address);
        return it == AddressToNode.end() ? nullptr : it->second;
    }

    TExecNodePtr GetNode(const Stroka& address)
    {
        auto node = FindNode(address);
        YCHECK(node);
        return node;
    }

    TExecNodePtr GetOrRegisterNode(const TNodeDescriptor& descriptor)
    {
        auto it = AddressToNode.find(descriptor.Address);
        if (it == AddressToNode.end()) {
            return RegisterNode(descriptor);
        }

        // Update the current descriptor, just in case.
        auto node = it->second;
        node->Descriptor() = descriptor;
        return node;
    }


    TFuture<TStartResult> StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const TMutationId& mutationId,
        IMapNodePtr spec,
        const Stroka& user)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Check for an already existing operation.
        if (mutationId != NullMutationId) {
            auto existingOperation = FindOperationByMutationId(mutationId);
            if (existingOperation) {
                return MakeFuture(TStartResult(existingOperation));
            }
        }

        // Attach user transaction if any. Don't ping it.
        TTransactionAttachOptions userAttachOptions(transactionId);
        userAttachOptions.AutoAbort = false;
        userAttachOptions.Ping = false;
        userAttachOptions.PingAncestors = false;
        auto userTransaction =
            transactionId == NullTransactionId
            ? nullptr
            : GetTransactionManager()->Attach(userAttachOptions);

        // Create operation object.
        auto operationId = TOperationId::Create();
        auto operation = New<TOperation>(
            operationId,
            type,
            mutationId,
            userTransaction,
            spec,
            user,
            TInstant::Now());
        operation->SetCleanStart(true);
        operation->SetState(EOperationState::Initializing);

        LOG_INFO("Starting operation (OperationType: %s, OperationId: %s, TransactionId: %s, MutationId: %s, User: %s)",
            ~type.ToString(),
            ~ToString(operationId),
            ~ToString(transactionId),
            ~ToString(mutationId),
            ~user);

        IOperationControllerPtr controller;
        try {
            controller = CreateController(~operation);
            operation->SetController(controller);
            controller->Initialize();
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            LOG_ERROR(wrappedError);
            return MakeFuture(TStartResult(wrappedError));
        }

        RegisterOperation(operation);

        // Spawn a new fiber where all startup logic will work asynchronously.
        return
            BIND(&TImpl::DoStartOperation, MakeStrong(this), operation)
                .AsyncVia(Bootstrap->GetControlInvoker())
                .Run()
                .Apply(BIND([=] (TError error) {
                    return error.IsOK()
                        ? TScheduler::TStartResult(operation)
                        : TScheduler::TStartResult(error);
                }));
    }

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishingState()) {
            LOG_INFO(error, "Operation is already finishing (OperationId: %s, State: %s)",
                ~ToString(operation->GetOperationId()),
                ~operation->GetState().ToString());
            return operation->GetFinished();
        }

        if (operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already finished (OperationId: %s, State: %s)",
                ~ToString(operation->GetOperationId()),
                ~operation->GetState().ToString());
            return operation->GetFinished();
        }

        LOG_INFO(error, "Aborting operation (OperationId: %s, State: %s)",
            ~ToString(operation->GetOperationId()),
            ~operation->GetState().ToString());

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            error);

        return operation->GetFinished();
    }

    TAsyncError SuspendOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Cannot suspend operation in %s state",
                ~FormatEnum(operation->GetState()).Quote()));
        }

        operation->SetSuspended(true);

        LOG_INFO("Operation suspended (OperationId: %s)",
            ~ToString(operation->GetOperationId()));
        
        return MakeFuture(TError());
    }

    TAsyncError ResumeOperation(TOperationPtr operation)
    {
        if (!operation->GetSuspended()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Operation is not suspended",
                ~FormatEnum(operation->GetState()).Quote()));
        }

        operation->SetSuspended(false);

        LOG_INFO("Operation resumed (OperationId: %s)",
            ~ToString(operation->GetOperationId()));

        return MakeFuture(TError());
    }


    void ProcessHeartbeat(TExecNodePtr node, TCtxHeartbeatPtr context)
    {
        auto* request = &context->Request();
        auto* response = &context->Response();

        TLeaseManager::RenewLease(node->GetLease());

        auto oldResourceLimits = node->ResourceLimits();
        auto oldResourceUsage = node->ResourceUsage();

        node->ResourceLimits() = request->resource_limits();
        node->ResourceUsage() = request->resource_usage();

        // Update total resource limits _before_ processing the heartbeat to
        // maintain exact values of total resource limits.
        TotalResourceLimits -= oldResourceLimits;
        TotalResourceLimits += node->ResourceLimits();

        if (MasterConnector->IsConnected()) {
            std::vector<TJobPtr> runningJobs;
            bool hasWaitingJobs = false;
            yhash_set<TOperationPtr> operationsToLog;
            PROFILE_TIMING ("/analysis_time") {
                auto missingJobs = node->Jobs();

                for (auto& jobStatus : *request->mutable_jobs()) {
                    auto jobType = EJobType(jobStatus.job_type());
                    // Skip jobs that are not issued by the scheduler.
                    if (jobType <= EJobType::SchedulerFirst || jobType >= EJobType::SchedulerLast)
                        continue;

                    auto job = ProcessJobHeartbeat(
                        node,
                        request,
                        response,
                        &jobStatus);
                    if (job) {
                        YCHECK(missingJobs.erase(job) == 1);
                        switch (job->GetState()) {
                        case EJobState::Completed:
                        case EJobState::Failed:
                        case EJobState::Aborted:
                            operationsToLog.insert(job->GetOperation());
                            break;
                        case EJobState::Running:
                            runningJobs.push_back(job);
                            break;
                        case EJobState::Waiting:
                            hasWaitingJobs = true;
                            break;
                        default:
                            break;
                        }
                    }
                }

                // Check for missing jobs.
                for (auto job : missingJobs) {
                    LOG_ERROR("Job is missing (Address: %s, JobId: %s, OperationId: %s)",
                        ~node->GetAddress(),
                        ~ToString(job->GetId()),
                        ~ToString(job->GetOperation()->GetOperationId()));
                    AbortJob(job, TError("Job vanished"));
                    UnregisterJob(job);
                }
            }

            auto schedulingContext = CreateSchedulingContext(node, runningJobs);

            if (hasWaitingJobs) {
                LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling");
            } else {
                PROFILE_TIMING ("/schedule_time") {
                    Strategy->ScheduleJobs(~schedulingContext);
                }
            }

            for (auto job : schedulingContext->PreemptedJobs()) {
                ToProto(response->add_jobs_to_abort(), job->GetId());
            }

            auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());
            auto specBuilderInvoker = NRpc::TDispatcher::Get()->GetPoolInvoker();
            for (auto job : schedulingContext->StartedJobs()) {
                auto* startInfo = response->add_jobs_to_start();
                ToProto(startInfo->mutable_job_id(), job->GetId());
                *startInfo->mutable_resource_limits() = job->ResourceUsage();

                // Build spec asynchronously.
                awaiter->Await(
                    BIND(job->GetSpecBuilder(), startInfo->mutable_spec())
                    .AsyncVia(specBuilderInvoker)
                    .Run());

                // Release to avoid circular references.
                job->SetSpecBuilder(TJobSpecBuilder());
                operationsToLog.insert(job->GetOperation());
            }

            awaiter->Complete(BIND([=] () {
                context->Reply();
            }));

            for (auto operation : operationsToLog) {
                LogOperationProgress(operation);
            }
        } else {
            context->Reply(GetMasterDisconnectedError());
        }

        // Update total resource usage _after_ processing the heartbeat to avoid
        // "unsaturated CPU" phenomenon.
        TotalResourceUsage -= oldResourceUsage;
        TotalResourceUsage += node->ResourceUsage();
    }


    // ISchedulerStrategyHost implementation
    DEFINE_SIGNAL(void(TOperationPtr), OperationRegistered);
    DEFINE_SIGNAL(void(TOperationPtr), OperationUnregistered);

    DEFINE_SIGNAL(void(TJobPtr job), JobStarted);
    DEFINE_SIGNAL(void(TJobPtr job), JobFinished);
    DEFINE_SIGNAL(void(TJobPtr, const TNodeResources& resourcesDelta), JobUpdated);


    virtual TMasterConnector* GetMasterConnector() override
    {
        return ~MasterConnector;
    }

    virtual TNodeResources GetTotalResourceLimits() override
    {
        return TotalResourceLimits;
    }


    // IOperationHost implementation
    virtual NRpc::IChannelPtr GetMasterChannel() override
    {
        return Bootstrap->GetMasterChannel();
    }

    virtual TTransactionManagerPtr GetTransactionManager() override
    {
        return Bootstrap->GetTransactionManager();
    }

    virtual IInvokerPtr GetControlInvoker() override
    {
        return Bootstrap->GetControlInvoker();
    }

    virtual IInvokerPtr GetBackgroundInvoker() override
    {
        return BackgroundQueue->GetInvoker();
    }

    virtual std::vector<TExecNodePtr> GetExecNodes() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TExecNodePtr> result;
        for (const auto& pair : AddressToNode) {
            result.push_back(pair.second);
        }
        return result;
    }

    virtual int GetExecNodeCount() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return static_cast<int>(AddressToNode.size());
    }

    virtual void OnOperationCompleted(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        LOG_INFO("Committing operation (OperationId: %s)",
            ~ToString(operationId));

        // The operation may still have running jobs (e.g. those started speculatively).
        AbortOperationJobs(operation);

        operation->SetState(EOperationState::Completing);

        auto controller = operation->GetController();
        BIND(&TImpl::DoCompleteOperation, MakeStrong(this), operation)
            .AsyncVia(controller->GetCancelableControlInvoker())
            .Run();
    }

    virtual void OnOperationFailed(TOperationPtr operation, const TError& error) override
    {
        LOG_INFO(error, "Operation failed (OperationId: %s)",
            ~ToString(operation->GetOperationId()));

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            error);
    }


private:
    friend class TSchedulingContext;

    TSchedulerConfigPtr Config;
    TBootstrap* Bootstrap;

    TActionQueuePtr BackgroundQueue;
    TActionQueuePtr SnapshotIOQueue;

    std::unique_ptr<TMasterConnector> MasterConnector;

    std::unique_ptr<ISchedulerStrategy> Strategy;

    typedef yhash_map<Stroka, TExecNodePtr> TExecNodeMap;
    TExecNodeMap AddressToNode;

    typedef yhash_map<TOperationId, TOperationPtr> TOperationIdMap;
    TOperationIdMap IdToOperation;

    typedef yhash_map<TMutationId, TOperationPtr> TOperationMutationIdMap;
    TOperationMutationIdMap MutationIdToOperation;

    typedef yhash_map<TJobId, TJobPtr> TJobMap;
    TJobMap IdToJob;

    NProfiling::TProfiler TotalResourceLimitsProfiler;
    NProfiling::TProfiler TotalResourceUsageProfiler;

    NProfiling::TAggregateCounter TotalCompletedJobTimeCounter;
    NProfiling::TAggregateCounter TotalFailedJobTimeCounter;
    NProfiling::TAggregateCounter TotalAbortedJobTimeCounter;

    std::vector<int> JobTypeCounters;
    TPeriodicExecutorPtr ProfilingExecutor;

    TNodeResources TotalResourceLimits;
    TNodeResources TotalResourceUsage;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(SnapshotIOThread);


    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto jobTypeValue : EJobType::GetDomainValues()) {
            auto jobType = EJobType(jobTypeValue);
            if (jobType > EJobType::SchedulerFirst && jobType < EJobType::SchedulerLast) {
                Profiler.Enqueue("/job_count/" + FormatEnum(jobType), JobTypeCounters[jobType]);
            }
        }

        Profiler.Enqueue("/job_count/total", IdToJob.size());
        Profiler.Enqueue("/operation_count", IdToOperation.size());
        Profiler.Enqueue("/node_count", AddressToNode.size());

        ProfileResources(TotalResourceLimitsProfiler, TotalResourceLimits);
        ProfileResources(TotalResourceUsageProfiler, TotalResourceUsage);
    }


    void OnMasterConnected(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ReviveOperations(result.Operations);
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operations = IdToOperation;
        for (const auto& pair : operations) {
            auto operation = pair.second;
            if (!operation->IsFinishedState()) {
                operation->GetController()->Abort();
                SetOperationFinalState(
                    operation,
                    EOperationState::Aborted,
                    TError("Master disconnected"));
            }
            FinishOperation(operation);
        }
        YCHECK(IdToOperation.empty());

        for (const auto& pair : AddressToNode) {
            auto node = pair.second;
            node->Jobs().clear();
        }

        IdToJob.clear();

        std::fill(JobTypeCounters.begin(), JobTypeCounters.end(), 0);
    }

    TError GetMasterDisconnectedError()
    {
        return TError(
            NRpc::EErrorCode::Unavailable,
            "Master is not connected");
    }


    void OnUserTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TerminateOperation(
            operation,
            EOperationState::Aborting,
            EOperationState::Aborted,
            TError("Operation transaction has expired or was aborted"));
    }

    void OnSchedulerTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            TError("Scheduler transaction has expired or was aborted"));
    }



    void RequestConfig(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating configuration");

        auto req = TYPathProxy::Get("//sys/scheduler/config");
        batchReq->AddRequest(req, "get_config");
    }

    void HandleConfig(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_config");
        if (rsp->GetError().FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No config in Cypress, just ignore.
            return;
        }
        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error getting scheduler configuration");
            return;
        }

        try {
            if (!ReconfigureYsonSerializable(Config, TYsonString(rsp->value())))
                return;
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing updated scheduler configuration");
        }

        LOG_INFO("Scheduler configuration updated");
    }


    TError DoStartOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        try {
            StartAsyncSchedulerTransaction(operation);
            StartSyncSchedulerTransaction(operation);
            StartIOTransactions(operation);

            {
                auto asyncResult = MasterConnector->CreateOperationNode(operation);
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                if (operation->GetState() != EOperationState::Initializing) {
                    throw TFiberCanceledException();
                }
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return wrappedError;
        }

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a separate fiber in a non-blocking
        // fashion.
        auto controller = operation->GetController();
        controller->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoPrepareOperation,
            MakeStrong(this),
            operation));

        return TError();
    }
    
    void DoPrepareOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        auto operationId = operation->GetOperationId();

        try {
            // Run async preparation.
            LOG_INFO("Preparing operation (OperationId: %s)",
                ~ToString(operationId));

            operation->SetState(EOperationState::Preparing);

            {
                auto controller = operation->GetController();
                auto asyncResult = controller->Prepare();
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to prepare")
                << ex;
            OnOperationFailed(operation, wrappedError);
            return;
        }

        if (operation->GetState() != EOperationState::Preparing) {
            throw TFiberCanceledException();
        }

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation has been prepared and is now running (OperationId: %s)",
            ~ToString(operationId));

        LogOperationProgress(operation);

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }


    void StartSyncSchedulerTransaction(TOperationPtr operation)
    {
        auto operationId = operation->GetOperationId();

        LOG_INFO("Starting sync scheduler transaction (OperationId: %s)",
            ~ToString(operationId));

        TObjectServiceProxy proxy(GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto userTransaction = operation->GetUserTransaction();
            auto req = TMasterYPathProxy::CreateObjects();
            if (userTransaction) {
                ToProto(req->mutable_transaction_id(), userTransaction->GetId());
            }
            req->set_type(EObjectType::Transaction);

            auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
            reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Sprintf("Scheduler sync for operation %s",
                ~ToString(operation->GetOperationId())));
            ToProto(reqExt->mutable_attributes(), *attributes);

            GenerateMutationId(req);
            batchReq->AddRequest(req, "start_sync_tx");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting sync scheduler transaction");
        if (operation->GetState() != EOperationState::Initializing &&
            operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        auto transactionManager = GetTransactionManager();

        {
            auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_sync_tx");
            auto transactionid = FromProto<TObjectId>(rsp->object_ids(0));
            TTransactionAttachOptions options(transactionid);
            options.AutoAbort = false;
            options.Ping = true;
            options.PingAncestors = false;
            operation->SetSyncSchedulerTransaction(transactionManager->Attach(options));
        }

        LOG_INFO("Scheduler sync transaction started (SyncTransactionId: %s, OperationId: %s)",
            ~ToString(operation->GetSyncSchedulerTransaction()->GetId()),
            ~ToString(operationId));
    }

    void StartAsyncSchedulerTransaction(TOperationPtr operation)
    {
        auto operationId = operation->GetOperationId();

        LOG_INFO("Starting async scheduler transaction (OperationId: %s)",
            ~ToString(operationId));

        TObjectServiceProxy proxy(GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TMasterYPathProxy::CreateObjects();
            req->set_type(EObjectType::Transaction);

            auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
            reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());
            reqExt->set_enable_uncommitted_accounting(false);
            reqExt->set_enable_staged_accounting(false);

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Sprintf("Scheduler async for operation %s",
                ~ToString(operationId)));
            ToProto(reqExt->mutable_attributes(), *attributes);

            GenerateMutationId(req);
            batchReq->AddRequest(req, "start_async_tx");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting async scheduler transaction");
        if (operation->GetState() != EOperationState::Initializing &&
            operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        auto transactionManager = GetTransactionManager();

        {
            auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_async_tx");
            auto transactionid = FromProto<TObjectId>(rsp->object_ids(0));
            TTransactionAttachOptions options(transactionid);
            options.AutoAbort = false;
            options.Ping = true;
            options.PingAncestors = false;
            operation->SetAsyncSchedulerTransaction(transactionManager->Attach(options));
        }

        LOG_INFO("Scheduler async transaction started (AsyncTranasctionId: %s, OperationId: %s)",
            ~ToString(operation->GetAsyncSchedulerTransaction()->GetId()),
            ~ToString(operationId));
    }

    void StartIOTransactions(TOperationPtr operation)
    {
        auto operationId = operation->GetOperationId();

        LOG_INFO("Starting IO transactions (OperationId: %s)",
            ~ToString(operationId));

        TObjectServiceProxy proxy(GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();
        auto parentTransactionId = operation->GetSyncSchedulerTransaction()->GetId();

        {
            auto req = TMasterYPathProxy::CreateObjects();
            ToProto(req->mutable_transaction_id(), parentTransactionId);
            req->set_type(EObjectType::Transaction);

            auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
            reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Sprintf("Scheduler input for operation %s",
                ~ToString(operation->GetOperationId())));
            ToProto(reqExt->mutable_attributes(), *attributes);

            NHydra::GenerateMutationId(req);
            batchReq->AddRequest(req, "start_in_tx");
        }

        {
            auto req = TMasterYPathProxy::CreateObjects();
            ToProto(req->mutable_transaction_id(), parentTransactionId);
            req->set_type(EObjectType::Transaction);

            auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
            reqExt->set_enable_uncommitted_accounting(false);
            reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("title", Sprintf("Scheduler output for operation %s",
                ~ToString(operationId)));
            ToProto(reqExt->mutable_attributes(), *attributes);

            NHydra::GenerateMutationId(req);
            batchReq->AddRequest(req, "start_out_tx");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting IO transactions");
        if (operation->GetState() != EOperationState::Initializing &&
            operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        auto transactionManager = GetTransactionManager();

        {
            auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_in_tx");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting input transaction");
            auto id = FromProto<TTransactionId>(rsp->object_ids(0));
            TTransactionAttachOptions options(id);
            options.AutoAbort = false;
            options.Ping = true;
            operation->SetInputTransaction(transactionManager->Attach(options));
        }

        {
            auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_out_tx");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting output transaction");
            auto id = FromProto<TTransactionId>(rsp->object_ids(0));
            TTransactionAttachOptions options(id);
            options.AutoAbort = false;
            options.Ping = true;
            operation->SetOutputTransaction(transactionManager->Attach(options));
        }

        LOG_INFO("IO transactions started (InputTransactionId: %s, OutputTranasctionId: %s, OperationId: %s)",
            ~ToString(operation->GetInputTransaction()->GetId()),
            ~ToString(operation->GetOutputTransaction()->GetId()),
            ~ToString(operationId));
    }



    void ReviveOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(IdToOperation.empty());
        for (auto operation : operations) {
            ReviveOperation(operation);
        }
    }

    void ReviveOperation(TOperationPtr operation)
    {
        auto operationId = operation->GetOperationId();

        LOG_INFO("Reviving operation (OperationId: %s)",
            ~ToString(operationId));

        // NB: The operation is being revived, hence it already
        // has a valid node associated with it.
        // If the revival fails, we still need to update the node
        // and unregister the operation from Master Connector.

        IOperationControllerPtr controller;
        try {
            controller = CreateController(~operation);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %s)",
                ~ToString(operationId));
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);
            MasterConnector->FlushOperationNode(operation);
            return;
        }

        operation->SetController(controller);
        RegisterOperation(operation);

        BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation)
            .AsyncVia(controller->GetCancelableControlInvoker())
            .Run();
    }

    void DoReviveOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        auto operationId = operation->GetOperationId();

        try {
            // NB: Async transaction is always restarted, even if operation state is being recovered.
            StartAsyncSchedulerTransaction(operation);

            if (operation->GetCleanStart()) {
                StartSyncSchedulerTransaction(operation);
                StartIOTransactions(operation);
            }

            {
                auto asyncResult = MasterConnector->ResetRevivingOperationNode(operation);
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                if (operation->GetState() != EOperationState::Reviving) {
                    throw TFiberCanceledException();
                }
            }

            {
                auto controller = operation->GetController();
                auto asyncResult = controller->Revive();
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                if (operation->GetState() != EOperationState::Reviving) {
                    throw TFiberCanceledException();
                }
            }

            operation->SetState(EOperationState::Running);

            // Discard the snapshot, if any.
            operation->Snapshot() = Null;

            LOG_INFO("Operation has been revived and is now running (OperationId: %s)",
                ~ToString(operationId));
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %s)",
                ~ToString(operationId));
            auto wrappedError = TError("Operation has failed to revive")
                << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);
            MasterConnector->FlushOperationNode(operation);
            AbortSchedulerTransactions(operation);
            FinishOperation(operation);
        }
    }


    TJobPtr FindJob(const TJobId& jobId)
    {
        auto it = IdToJob.find(jobId);
        return it == IdToJob.end() ? nullptr : it->second;
    }


    TExecNodePtr RegisterNode(const TNodeDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node online (Descriptor: %s)", ~ToString(descriptor));

        auto node = New<TExecNode>(descriptor);

        auto lease = TLeaseManager::CreateLease(
            Config->NodeHearbeatTimeout,
            BIND(&TImpl::UnregisterNode, MakeWeak(this), node)
                .Via(GetControlInvoker()));

        node->SetLease(lease);

        TotalResourceLimits += node->ResourceLimits();
        TotalResourceUsage += node->ResourceUsage();

        YCHECK(AddressToNode.insert(std::make_pair(node->GetAddress(), node)).second);

        return node;
    }

    void UnregisterNode(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node offline (Address: %s)", ~node->GetAddress());

        TotalResourceLimits -= node->ResourceLimits();
        TotalResourceUsage -= node->ResourceUsage();

        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        const auto& address = node->GetAddress();
        for (auto job : jobs) {
            LOG_INFO("Aborting job on an offline node %s (JobId: %s, OperationId: %s)",
                ~address,
                ~ToString(job->GetId()),
                ~ToString(job->GetOperation()->GetOperationId()));
            AbortJob(job, TError("Node offline"));
            UnregisterJob(job);
        }
        YCHECK(AddressToNode.erase(address) == 1);
    }


    void RegisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation.insert(std::make_pair(operation->GetOperationId(), operation)).second);

        auto mutationId = operation->GetMutationId();
        if (mutationId != NullMutationId) {
            YCHECK(MutationIdToOperation.insert(std::make_pair(mutationId, operation)).second);
        }

        OperationRegistered_.Fire(operation);

        LOG_DEBUG("Operation registered (OperationId: %s)",
            ~ToString(operation->GetOperationId()));
    }

    void AbortOperationJobs(TOperationPtr operation)
    {
        auto jobs = operation->Jobs();
        for (auto job : jobs) {
            AbortJob(job, TError("Operation aborted"));
            UnregisterJob(job);
        }

        YCHECK(operation->Jobs().empty());
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation.erase(operation->GetOperationId()) == 1);

        auto mutationId = operation->GetMutationId();
        if (mutationId != NullMutationId) {
            YCHECK(MutationIdToOperation.erase(operation->GetMutationId()) == 1);
        }

        OperationUnregistered_.Fire(operation);

        LOG_DEBUG("Operation unregistered (OperationId: %s)",
            ~ToString(operation->GetOperationId()));
    }

    void LogOperationProgress(TOperationPtr operation)
    {
        if (operation->GetState() != EOperationState::Running)
            return;

        LOG_DEBUG("Progress: %s, %s (OperationId: %s)",
            ~operation->GetController()->GetLoggingProgress(),
            ~Strategy->GetOperationLoggingProgress(operation),
            ~ToString(operation->GetOperationId()));
    }

    void SetOperationFinalState(TOperationPtr operation, EOperationState state, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->SetState(state);
        operation->SetFinishTime(TInstant::Now());
        ToProto(operation->Result().mutable_error(), error);
    }


    void CommitSchedulerTransactions(TOperationPtr operation)
    {
        YCHECK(operation->GetState() == EOperationState::Completing);

        LOG_INFO("Committing scheduler transactions (OperationId: %s)",
            ~ToString(operation->GetOperationId()));

        auto commitTransaction = [&] (TTransactionPtr transaction) {
            auto result = WaitFor(transaction->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Operation has failed to commit");
            if (operation->GetState() != EOperationState::Completing) {
                throw TFiberCanceledException();
            }
        };

        commitTransaction(operation->GetInputTransaction());
        commitTransaction(operation->GetOutputTransaction());
        commitTransaction(operation->GetSyncSchedulerTransaction());

        LOG_INFO("Scheduler transactions committed (OperationId: %s)",
            ~ToString(operation->GetOperationId()));

        // NB: Never commit async transaction since it's used for writing Live Preview tables.
        operation->GetAsyncSchedulerTransaction()->Abort();
    }

    void AbortSchedulerTransactions(TOperationPtr operation)
    {
        auto abortTransaction = [&] (TTransactionPtr transaction) {
            if (transaction) {
                // Fire-and-forget.
                transaction->Abort();
            }
        };

        // NB: No need to abort IO transactions since they are nested inside sync transaction.
        abortTransaction(operation->GetSyncSchedulerTransaction());
        abortTransaction(operation->GetAsyncSchedulerTransaction());
    }

    void FinishOperation(TOperationPtr operation)
    {
        operation->SetFinished();
        operation->SetController(nullptr);
        UnregisterOperation(operation);
    }


    void RegisterJob(TJobPtr job)
    {
        auto operation = job->GetOperation();
        auto node = job->GetNode();

        ++JobTypeCounters[job->GetType()];

        YCHECK(IdToJob.insert(std::make_pair(job->GetId(), job)).second);
        YCHECK(operation->Jobs().insert(job).second);
        YCHECK(node->Jobs().insert(job).second);
        
        job->GetNode()->ResourceUsage() += job->ResourceUsage();

        JobStarted_.Fire(job);

        LOG_DEBUG("Job registered (JobId: %s, JobType: %s, OperationId: %s)",
            ~ToString(job->GetId()),
            ~job->GetType().ToString(),
            ~ToString(operation->GetOperationId()));
    }

    void UnregisterJob(TJobPtr job)
    {
        auto operation = job->GetOperation();
        auto node = job->GetNode();

        --JobTypeCounters[job->GetType()];

        YCHECK(IdToJob.erase(job->GetId()) == 1);
        YCHECK(operation->Jobs().erase(job) == 1);
        YCHECK(node->Jobs().erase(job) == 1);

        JobFinished_.Fire(job);

        LOG_DEBUG("Job unregistered (JobId: %s, OperationId: %s)",
            ~ToString(job->GetId()),
            ~ToString(operation->GetOperationId()));
    }

    void AbortJob(TJobPtr job, const TError& error)
    {
        // This method must be safe to call for any job.
        if (job->GetState() != EJobState::Running &&
            job->GetState() != EJobState::Waiting)
            return;

        job->SetState(EJobState::Aborted);
        ToProto(job->Result().mutable_error(), error);

        OnJobFinished(job);

        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobAborted(job);
        }
    }

    void PreemptJob(TJobPtr job)
    {
        LOG_DEBUG("Job preempted (JobId: %s, OperationId: %s)",
            ~ToString(job->GetId()),
            ~ToString(job->GetOperation()->GetOperationId()));

        job->GetNode()->ResourceUsage() -= job->ResourceUsage();
        JobUpdated_.Fire(job, -job->ResourceUsage());
        job->ResourceUsage() = ZeroNodeResources();

        AbortJob(job, TError("Job preempted"));
    }


    void OnJobRunning(TJobPtr job, const TJobStatus& status)
    {
        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobRunning(job, status);
        }
    }

    void OnJobWaiting(TJobPtr)
    {
        // Do nothing.
    }

    void OnJobCompleted(TJobPtr job, TJobResult* result)
    {
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting)
        {
            job->SetState(EJobState::Completed);
            job->Result().Swap(result);

            OnJobFinished(job);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobCompleted(job);
            }

            ProcessFinishedJobResult(job);
        }

        UnregisterJob(job);
    }

    void OnJobFailed(TJobPtr job, TJobResult* result)
    {
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting)
        {
            job->SetState(EJobState::Failed);
            job->Result().Swap(result);

            OnJobFinished(job);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobFailed(job);
            }

            ProcessFinishedJobResult(job);
        }

        UnregisterJob(job);
    }

    void OnJobAborted(TJobPtr job, TJobResult* result)
    {
        // Only update the result for the first time.
        // Typically the scheduler decides to abort the job on its own.
        // In this case we should ignore the result returned from the node
        // and avoid notifying the controller twice.
        if (job->GetState() == EJobState::Running ||
            job->GetState() == EJobState::Waiting)
        {
            job->SetState(EJobState::Aborted);
            job->Result().Swap(result);

            OnJobFinished(job);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobAborted(job);
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
                Profiler.Increment(TotalCompletedJobTimeCounter, duration.MicroSeconds());
                break;
            case EJobState::Failed:
                Profiler.Increment(TotalFailedJobTimeCounter, duration.MicroSeconds());
                break;
            case EJobState::Aborted:
                Profiler.Increment(TotalAbortedJobTimeCounter, duration.MicroSeconds());
                break;
            default:
                YUNREACHABLE();
        }
    }

    void ProcessFinishedJobResult(TJobPtr job)
    {
        auto jobFailed = job->GetState() == EJobState::Failed;
        const auto& schedulerResultExt = job->Result().GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        if (schedulerResultExt.has_stderr_chunk_id()) {
            auto operation = job->GetOperation();
            auto stderrChunkId = FromProto<TChunkId>(schedulerResultExt.stderr_chunk_id());

            if (jobFailed || operation->GetStderrCount() < operation->GetMaxStderrCount()) {
                MasterConnector->CreateJobNode(job, stderrChunkId);
                operation->SetStderrCount(operation->GetStderrCount() + 1);
            } else {
                ReleaseStderrChunk(job, stderrChunkId);
            }
        } else if (jobFailed) {
            MasterConnector->CreateJobNode(job, NullChunkId);
        }
    }

    void ReleaseStderrChunk(TJobPtr job, const TChunkId& chunkId)
    {
        TObjectServiceProxy proxy(GetMasterChannel());
        auto transaction = job->GetOperation()->GetAsyncSchedulerTransaction();
        if (!transaction)
            return;

        auto req = TTransactionYPathProxy::UnstageObject(FromObjectId(transaction->GetId()));
        ToProto(req->mutable_object_id(), chunkId);
        req->set_recursive(false);

        // Fire-and-forget.
        // The subscriber is only needed to log the outcome.
        proxy.Execute(req).Subscribe(
            BIND(&TImpl::OnStderrChunkReleased, MakeStrong(this)));
    }

    void OnStderrChunkReleased(TTransactionYPathProxy::TRspUnstageObjectPtr rsp)
    {
        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error releasing stderr chunk");
        }
    }

    void InitStrategy()
    {
        switch (Config->Strategy) {
            case ESchedulerStrategy::Null:
                Strategy = CreateNullStrategy(this);
                break;
            case ESchedulerStrategy::FairShare:
                Strategy = CreateFairShareStrategy(Config, this);
                break;
            default:
                YUNREACHABLE();
        }
    }

    IOperationControllerPtr CreateController(TOperation* operation)
    {
        switch (operation->GetType()) {
            case EOperationType::Map:
                return CreateMapController(Config, this, operation);
            case EOperationType::Merge:
                return CreateMergeController(Config, this, operation);
            case EOperationType::Erase:
                return CreateEraseController(Config, this, operation);
            case EOperationType::Sort:
                return CreateSortController(Config, this, operation);
            case EOperationType::Reduce:
                return CreateReduceController(Config, this, operation);
            case EOperationType::MapReduce:
                return CreateMapReduceController(Config, this, operation);
            default:
                YUNREACHABLE();
        }
    }


    void DoCompleteOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetOperationId();

        if (operation->GetState() != EOperationState::Completing) {
            throw TFiberCanceledException();
        }

        try {
            // First flush: ensure that all stderrs are attached and the
            // state is changed to Completing.
            {
                auto asyncResult = MasterConnector->FlushOperationNode(operation);
                WaitFor(asyncResult);
                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            {
                auto controller = operation->GetController();
                auto asyncResult = controller->Commit();
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            CommitSchedulerTransactions(operation);

            LOG_INFO("Operation committed (OperationId: %s)",
                ~ToString(operationId));

            YCHECK(operation->GetState() == EOperationState::Completing);
            SetOperationFinalState(operation, EOperationState::Completed, TError());

            // Second flush: ensure that state is changed to Completed.
            {
                auto asyncResult = MasterConnector->FlushOperationNode(operation);
                WaitFor(asyncResult);
                YCHECK(operation->GetState() == EOperationState::Completed);
            }

            FinishOperation(operation);
        } catch (const std::exception& ex) {
            OnOperationFailed(operation, ex);
        }       
    }

    void TerminateOperation(
        TOperationPtr operation,
        EOperationState intermediateState,
        EOperationState finalState,
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

        AbortOperationJobs(operation);

        operation->SetState(intermediateState);

        // First flush: ensure that all stderrs are attached and the
        // state is changed to its intermediate value.
        {
            auto asyncResult = MasterConnector->FlushOperationNode(operation);
            WaitFor(asyncResult);
            if (operation->GetState() != intermediateState)
                return;
        }

        SetOperationFinalState(operation, finalState, error);

        AbortSchedulerTransactions(operation);
        
        // Second flush: ensure that the state is changed to its final value.
        {
            auto asyncResult = MasterConnector->FlushOperationNode(operation);
            WaitFor(asyncResult);
            if (operation->GetState() != finalState)
                return;
        }

        operation->GetController()->Abort();

        FinishOperation(operation);
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("connected").Value(MasterConnector->IsConnected())
                .Item("cell").BeginMap()
                    .Item("resource_limits").Value(TotalResourceLimits)
                    .Item("resource_usage").Value(TotalResourceUsage)
                .EndMap()
                .Item("operations").DoMapFor(IdToOperation, [=] (TFluentMap fluent, TOperationIdMap::value_type pair) {
                    BuildOperationYson(pair.second, fluent);
                })
                .Item("nodes").DoMapFor(AddressToNode, [=] (TFluentMap fluent, TExecNodeMap::value_type pair) {
                    BuildNodeYson(pair.second, fluent);
                })
                .DoIf(Strategy != nullptr, BIND(&ISchedulerStrategy::BuildOrchidYson, ~Strategy))
            .EndMap();
    }

    void BuildOperationYson(TOperationPtr operation, IYsonConsumer* consumer)
    {
        auto state = operation->GetState();
        bool hasProgress = (state == EOperationState::Running) || IsOperationFinished(state);
        BuildYsonMapFluently(consumer)
            .Item(ToString(operation->GetOperationId())).BeginMap()
                .Do(BIND(&NScheduler::BuildOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .DoIf(hasProgress, BIND(&IOperationController::BuildProgressYson, operation->GetController()))
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgressYson, ~Strategy, operation))
                .EndMap()
                .Item("running_jobs").BeginAttributes()
                    .Item("opaque").Value("true")
                .EndAttributes()
                .DoMapFor(operation->Jobs(), [=] (TFluentMap fluent, TJobPtr job) {
                    BuildJobYson(job, fluent);
                })
            .EndMap();
    }

    void BuildJobYson(TJobPtr job, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(ToString(job->GetId())).BeginMap()
                .Do([=] (TFluentMap fluent) {
                    BuildJobAttributes(job, fluent);
                })
            .EndMap();
    }

    void BuildNodeYson(TExecNodePtr node, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(node->GetAddress()).BeginMap()
                .Do([=] (TFluentMap fluent) {
                    BuildExecNodeAttributes(node, fluent);
                })
            .EndMap();
    }


    TJobPtr ProcessJobHeartbeat(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        TJobStatus* jobStatus)
    {
        auto jobId = FromProto<TJobId>(jobStatus->job_id());
        auto state = EJobState(jobStatus->state());
        const auto& jobAddress = node->GetAddress();

        NLog::TTaggedLogger Logger(SchedulerLogger);
        Logger.AddTag(Sprintf("Address: %s, JobId: %s",
            ~jobAddress,
            ~ToString(jobId)));

        auto job = FindJob(jobId);
        if (!job) {
            switch (state) {
                case EJobState::Completed:
                    LOG_WARNING("Unknown job has completed, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Failed:
                    LOG_INFO("Unknown job has failed, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Aborted:
                    LOG_INFO("Job aborted, removal scheduled");
                    ToProto(response->add_jobs_to_remove(), jobId);
                    break;

                case EJobState::Running:
                    LOG_WARNING("Unknown job is running, abort scheduled");
                    ToProto(response->add_jobs_to_abort(), jobId);
                    break;

                case EJobState::Waiting:
                    LOG_WARNING("Unknown job is waiting, abort scheduled");
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

        auto operation = job->GetOperation();

        Logger.AddTag(Sprintf("JobType: %s, State: %s, OperationId: %s",
            ~job->GetType().ToString(),
            ~state.ToString(),
            ~ToString(operation->GetOperationId())));

        // Check if the job is running on a proper node.
        const auto& expectedAddress = job->GetNode()->GetAddress();
        if (jobAddress != expectedAddress) {
            // Job has moved from one node to another. No idea how this could happen.
            if (state == EJobState::Aborting) {
                // Do nothing, job is already terminating.
            } else if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
                ToProto(response->add_jobs_to_remove(), jobId);
                LOG_WARNING("Job status report was expected from %s, removal scheduled",
                    ~expectedAddress);
            } else {
                ToProto(response->add_jobs_to_abort(), jobId);
                LOG_WARNING("Job status report was expected from %s, abort scheduled",
                    ~expectedAddress);
            }
            return nullptr;
        }

        switch (state) {
            case EJobState::Completed:
                LOG_INFO("Job completed, removal scheduled");
                OnJobCompleted(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;

            case EJobState::Failed: {
                auto error = FromProto(jobStatus->result().error());
                LOG_WARNING(error, "Job failed, removal scheduled");
                OnJobFailed(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Aborted: {
                auto error = FromProto(jobStatus->result().error());
                LOG_INFO(error, "Job aborted, removal scheduled");
                OnJobAborted(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Running:
            case EJobState::Waiting:
                if (job->GetState() == EJobState::Aborted) {
                    LOG_INFO("Aborting job (Address: %s, JobType: %s, JobId: %s, OperationId: %s)",
                        ~jobAddress,
                        ~job->GetType().ToString(),
                        ~ToString(jobId),
                        ~ToString(operation->GetOperationId()));
                    ToProto(response->add_jobs_to_abort(), jobId);
                } else {
                    switch (state) {
                        case EJobState::Running: {
                            LOG_DEBUG("Job is running");
                            job->SetState(state);
                            OnJobRunning(job, *jobStatus);
                            auto delta = jobStatus->resource_usage() - job->ResourceUsage();
                            JobUpdated_.Fire(job, delta);
                            job->ResourceUsage() = jobStatus->resource_usage();
                            break;
                        }

                        case EJobState::Waiting:
                            LOG_DEBUG("Job is waiting");
                            OnJobWaiting(job);
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

    std::unique_ptr<ISchedulingContext> CreateSchedulingContext(
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs);
};

////////////////////////////////////////////////////////////////////

class TScheduler::TSchedulingContext
    : public ISchedulingContext
{
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, StartedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, PreemptedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, RunningJobs);

public:
    TSchedulingContext(
        TImpl* owner,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs)
        : Node_(node)
        , RunningJobs_(runningJobs)
        , Owner(owner)
    { }


    virtual bool CanStartMoreJobs() const override
    {
        if (!Node_->HasSpareResources()) {
            return false;
        }

        auto maxJobStarts = Owner->Config->MaxStartedJobsPerHeartbeat;
        if (maxJobStarts && StartedJobs_.size() >= maxJobStarts.Get()) {
            return false;
        }

        return true;
    }

    virtual TJobPtr StartJob(
        TOperationPtr operation,
        EJobType type,
        const TNodeResources& resourceLimits,
        TJobSpecBuilder specBuilder) override
    {
        auto id = TJobId::Create();
        auto startTime = TInstant::Now();
        auto job = New<TJob>(
            id,
            type,
            operation,
            Node_,
            startTime,
            resourceLimits,
            specBuilder);
        StartedJobs_.push_back(job);
        Owner->RegisterJob(job);
        return job;
    }

    virtual void PreemptJob(TJobPtr job) override
    {
        YCHECK(job->GetNode() == Node_);
        PreemptedJobs_.push_back(job);
        Owner->PreemptJob(job);
    }

private:
    TImpl* Owner;

};

std::unique_ptr<ISchedulingContext> TScheduler::TImpl::CreateSchedulingContext(
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs)
{
    return std::unique_ptr<ISchedulingContext>(new TSchedulingContext(
        this,
        node,
        runningJobs));
}

////////////////////////////////////////////////////////////////////

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TScheduler::~TScheduler()
{ }

void TScheduler::Initialize()
{
    Impl->Initialize();
}

IYPathServicePtr TScheduler::GetOrchidService()
{
    return Impl->GetOrchidService();
}

std::vector<TOperationPtr> TScheduler::GetOperations()
{
    return Impl->GetOperations();
}

std::vector<TExecNodePtr> TScheduler::GetExecNodes()
{
    return Impl->GetExecNodes();
}

IInvokerPtr TScheduler::GetSnapshotIOInvoker()
{
    return Impl->GetSnapshotIOInvoker();
}

bool TScheduler::IsConnected()
{
    return Impl->IsConnected();
}

void TScheduler::ValidateConnected()
{
    Impl->ValidateConnected();
}

TOperationPtr TScheduler::FindOperation(const TOperationId& id)
{
    return Impl->FindOperation(id);
}

TOperationPtr TScheduler::GetOperationOrThrow(const TOperationId& id)
{
    return Impl->GetOperationOrThrow(id);
}

TExecNodePtr TScheduler::FindNode(const Stroka& address)
{
    return Impl->FindNode(address);
}

TExecNodePtr TScheduler::GetNode(const Stroka& address)
{
    return Impl->GetNode(address);
}

TExecNodePtr TScheduler::GetOrRegisterNode(const TNodeDescriptor& descriptor)
{
    return Impl->GetOrRegisterNode(descriptor);
}

TFuture<TScheduler::TStartResult> TScheduler::StartOperation(
    EOperationType type,
    const TTransactionId& transactionId,
    const TMutationId& mutationId,
    IMapNodePtr spec,
    const Stroka& user)
{
    return Impl->StartOperation(
        type,
        transactionId,
        mutationId,
        spec,
        user);
}

TFuture<void> TScheduler::AbortOperation(
    TOperationPtr operation,
    const TError& error)
{
    return Impl->AbortOperation(operation, error);
}

TAsyncError TScheduler::SuspendOperation(TOperationPtr operation)
{
    return Impl->SuspendOperation(operation);
}

TAsyncError TScheduler::ResumeOperation(TOperationPtr operation)
{
    return Impl->ResumeOperation(operation);
}

void TScheduler::ProcessHeartbeat(TExecNodePtr node, TCtxHeartbeatPtr context)
{
    Impl->ProcessHeartbeat(node, context);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

