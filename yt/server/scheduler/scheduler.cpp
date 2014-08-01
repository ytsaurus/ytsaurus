#include "stdafx.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "fair_share_strategy.h"
#include "operation_controller.h"
#include "map_controller.h"
#include "merge_controller.h"
#include "remote_copy_controller.h"
#include "sort_controller.h"
#include "helpers.h"
#include "master_connector.h"
#include "job_resources.h"
#include "private.h"
#include "snapshot_downloader.h"
#include "event_log.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/string.h>

#include <core/actions/invoker_util.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/scheduler.h>

#include <core/rpc/dispatcher.h>

#include <core/logging/log.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/object_ypath_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/table_client/async_writer.h>
#include <ytlib/table_client/buffered_table_writer.h>
#include <ytlib/table_client/table_consumer.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/fluent.h>

#include <ytlib/hydra/public.h>
#include <ytlib/hydra/rpc_helpers.h>

#include <ytlib/hive/cluster_directory.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NHydra;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NTableClient;

using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
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
        : Config_(config)
        , Bootstrap_(bootstrap)
        , BackgroundQueue_(New<TActionQueue>("Background"))
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
        , MasterConnector_(new TMasterConnector(Config_, Bootstrap_))
        , TotalResourceLimitsProfiler_(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUsageProfiler_(Profiler.GetPathPrefix() + "/total_resource_usage")
        , TotalCompletedJobTimeCounter_("/total_completed_job_time")
        , TotalFailedJobTimeCounter_("/total_failed_job_time")
        , TotalAbortedJobTimeCounter_("/total_aborted_job_time")
        , JobTypeCounters_(static_cast<int>(EJobType::SchedulerLast))
        , TotalResourceLimits_(ZeroNodeResources())
        , TotalResourceUsage_(ZeroNodeResources())
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_AFFINITY(GetControlInvoker(), ControlThread);
        VERIFY_INVOKER_AFFINITY(GetSnapshotIOInvoker(), SnapshotIOThread);

        auto localHostName = TAddressResolver::Get()->GetLocalHostName();
        int port = Bootstrap_->GetConfig()->RpcPort;
        ServiceAddress_ = BuildServiceAddress(localHostName, port);
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

        EventLogWriter_ = CreateBufferedTableWriter(
            Config_->EventLog,
            // TODO(ignat): pass Client instead of Channel and TransactionManager
            Bootstrap_->GetMasterClient()->GetMasterChannel(),
            Bootstrap_->GetMasterClient()->GetTransactionManager(),
            Config_->EventLog->Path);
        EventLogWriter_->Open();
        EventLogConsumer_.reset(new TTableConsumer(EventLogWriter_));

        LogEventFluently(ELogEventType::SchedulerStarted)
            .Item("address").Value(ServiceAddress_);
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

    TOperationPtr FindOperationByMutationId(const TMutationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = MutationIdToOperation_.find(id);
        return it == MutationIdToOperation_.end() ? nullptr : it->second;
    }


    TExecNodePtr FindNode(const Stroka& address)
    {
        auto it = AddressToNode_.find(address);
        return it == AddressToNode_.end() ? nullptr : it->second;
    }

    TExecNodePtr GetNode(const Stroka& address)
    {
        auto node = FindNode(address);
        YCHECK(node);
        return node;
    }

    TExecNodePtr GetOrRegisterNode(const TNodeDescriptor& descriptor)
    {
        auto it = AddressToNode_.find(descriptor.GetDefaultAddress());
        if (it == AddressToNode_.end()) {
            return RegisterNode(descriptor);
        }

        // Update the current descriptor, just in case.
        auto node = it->second;
        node->Descriptor() = descriptor;
        return node;
    }


    TFuture<TOperationStartResult> StartOperation(
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
                return existingOperation->GetStarted();
            }
        }

        if (static_cast<int>(IdToOperation_.size()) >= Config_->MaxOperationCount) {
            THROW_ERROR_EXCEPTION("Limit for the number of concurrent operations %v has been reached",
                Config_->MaxOperationCount);
        }

        // Attach user transaction if any. Don't ping it.
        auto transactionManager = GetMasterClient()->GetTransactionManager();
        TTransactionAttachOptions userAttachOptions(transactionId);
        userAttachOptions.AutoAbort = false;
        userAttachOptions.Ping = false;
        userAttachOptions.PingAncestors = false;
        auto userTransaction =
            transactionId == NullTransactionId
            ? nullptr
            : transactionManager->Attach(userAttachOptions);

        // Merge operation spec with template
        auto specTemplate = GetSpecTemplate(type, spec);
        if (specTemplate) {
            spec = NYTree::UpdateNode(specTemplate, spec)->AsMap();
        }

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

        LOG_INFO("Starting operation (OperationType: %v, OperationId: %v, TransactionId: %v, MutationId: %v, User: %v)",
            type,
            operationId,
            transactionId,
            mutationId,
            ~user);


        LOG_INFO("Total resource limits (OperationId: %v, ResourceLimits: {%v})",
            operationId,
            ~FormatResources(GetTotalResourceLimits()));

        // Spawn a new fiber where all startup logic will work asynchronously.
        BIND(&TImpl::DoStartOperation, MakeStrong(this), operation)
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();

        return operation->GetStarted();
    }

    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishingState()) {
            LOG_INFO(error, "Operation is already finishing (OperationId: %v, State: %v)",
                operation->GetId(),
                operation->GetState());
            return operation->GetFinished();
        }

        if (operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already finished (OperationId: %v, State: %v)",
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

    TAsyncError SuspendOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishingState() || operation->IsFinishedState()) {
            return MakeFuture(TError(
                EErrorCode::InvalidOperationState,
                "Cannot suspend operation in %v state",
                ~FormatEnum(operation->GetState()).Quote()));
        }

        operation->SetSuspended(true);

        LOG_INFO("Operation suspended (OperationId: %v)",
            operation->GetId());


        return OKFuture;
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

        LOG_INFO("Operation resumed (OperationId: %v)",
            operation->GetId());

        return OKFuture;
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
        TotalResourceLimits_ -= oldResourceLimits;
        TotalResourceLimits_ += node->ResourceLimits();

        if (MasterConnector_->IsConnected()) {
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
                    LOG_ERROR("Job is missing (Address: %v, JobId: %v, OperationId: %v)",
                        ~node->GetAddress(),
                        job->GetId(),
                        job->GetOperation()->GetId());
                    AbortJob(job, TError("Job vanished"));
                    UnregisterJob(job);
                }
            }

            auto schedulingContext = CreateSchedulingContext(node, runningJobs);

            if (hasWaitingJobs) {
                LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling");
            } else {
                PROFILE_TIMING ("/schedule_time") {
                    Strategy_->ScheduleJobs(schedulingContext.get());
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
        TotalResourceUsage_ -= oldResourceUsage;
        TotalResourceUsage_ += node->ResourceUsage();
    }


    // ISchedulerStrategyHost implementation
    DEFINE_SIGNAL(void(TOperationPtr), OperationRegistered);
    DEFINE_SIGNAL(void(TOperationPtr), OperationUnregistered);
    DEFINE_SIGNAL(void(TOperationPtr, INodePtr update), OperationRuntimeParamsUpdated);

    DEFINE_SIGNAL(void(TJobPtr job), JobStarted);
    DEFINE_SIGNAL(void(TJobPtr job), JobFinished);
    DEFINE_SIGNAL(void(TJobPtr, const TNodeResources& resourcesDelta), JobUpdated);
    
    DEFINE_SIGNAL(void(INodePtr pools), PoolsUpdated);


    virtual TMasterConnector* GetMasterConnector() override
    {
        return MasterConnector_.get();
    }

    virtual TNodeResources GetTotalResourceLimits() override
    {
        return TotalResourceLimits_;
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

    virtual IInvokerPtr GetBackgroundInvoker() override
    {
        return BackgroundQueue_->GetInvoker();
    }

    virtual std::vector<TExecNodePtr> GetExecNodes() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TExecNodePtr> result;
        for (const auto& pair : AddressToNode_) {
            result.push_back(pair.second);
        }
        return result;
    }

    virtual int GetExecNodeCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return static_cast<int>(AddressToNode_.size());
    }

    virtual IYsonConsumer* GetEventLogConsumer() override
    {
        return EventLogConsumer_.get();
    }

    virtual void OnOperationCompleted(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        LOG_INFO("Committing operation (OperationId: %v)",
            operationId);

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
        LOG_INFO(error, "Operation failed (OperationId: %v)",
            operation->GetId());

        TerminateOperation(
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            ELogEventType::OperationFailed,
            error);
    }


private:
    friend class TSchedulingContext;

    TSchedulerConfigPtr Config_;
    TBootstrap* Bootstrap_;

    TActionQueuePtr BackgroundQueue_;
    TActionQueuePtr SnapshotIOQueue_;

    std::unique_ptr<TMasterConnector> MasterConnector_;

    std::unique_ptr<ISchedulerStrategy> Strategy_;

    typedef yhash_map<Stroka, TExecNodePtr> TExecNodeMap;
    TExecNodeMap AddressToNode_;

    typedef yhash_map<TOperationId, TOperationPtr> TOperationIdMap;
    TOperationIdMap IdToOperation_;

    typedef yhash_map<TMutationId, TOperationPtr> TOperationMutationIdMap;
    TOperationMutationIdMap MutationIdToOperation_;

    typedef yhash_map<TJobId, TJobPtr> TJobMap;
    TJobMap IdToJob_;

    NProfiling::TProfiler TotalResourceLimitsProfiler_;
    NProfiling::TProfiler TotalResourceUsageProfiler_;

    NProfiling::TAggregateCounter TotalCompletedJobTimeCounter_;
    NProfiling::TAggregateCounter TotalFailedJobTimeCounter_;
    NProfiling::TAggregateCounter TotalAbortedJobTimeCounter_;

    std::vector<int> JobTypeCounters_;
    TPeriodicExecutorPtr ProfilingExecutor_;

    TNodeResources TotalResourceLimits_;
    TNodeResources TotalResourceUsage_;

    Stroka ServiceAddress_;

    NTableClient::IAsyncWriterPtr EventLogWriter_;
    std::unique_ptr<IYsonConsumer> EventLogConsumer_;



    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(SnapshotIOThread);


    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto jobType : EJobType::GetDomainValues()) {
            if (jobType > EJobType::SchedulerFirst && jobType < EJobType::SchedulerLast) {
                Profiler.Enqueue("/job_count/" + FormatEnum(jobType), JobTypeCounters_[jobType]);
            }
        }

        Profiler.Enqueue("/job_count/total", IdToJob_.size());
        Profiler.Enqueue("/operation_count", IdToOperation_.size());
        Profiler.Enqueue("/node_count", AddressToNode_.size());

        ProfileResources(TotalResourceLimitsProfiler_, TotalResourceLimits_);
        ProfileResources(TotalResourceUsageProfiler_, TotalResourceUsage_);
    }


    void OnMasterConnected(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LogEventFluently(ELogEventType::MasterConnected)
            .Item("address").Value(ServiceAddress_);

        ReviveOperations(result.Operations);
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LogEventFluently(ELogEventType::MasterDisconnected)
            .Item("address").Value(ServiceAddress_);

        auto operations = IdToOperation_;
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
        YCHECK(IdToOperation_.empty());

        for (const auto& pair : AddressToNode_) {
            auto node = pair.second;
            node->Jobs().clear();
        }

        IdToJob_.clear();

        std::fill(JobTypeCounters_.begin(), JobTypeCounters_.end(), 0);
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
            ELogEventType::OperationAborted,
            TError("Operation transaction has expired or was aborted"));
    }

    void OnSchedulerTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly, poolConfigKeys);
        ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_pools");
    }

    void HandlePools(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_pools");
        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error getting pools configuration");
            return;
        }

        try {
            auto poolsNode = ConvertToNode(TYsonString(rsp->value()));
            PoolsUpdated_.Fire(poolsNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing pools configuration");
        }
    }
    
    void RequestOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        static auto runtimeParamsTemplate = New<TOperationRuntimeParams>();
        auto req = TYPathProxy::Get(GetOperationPath(operation->GetId()));
        TAttributeFilter attributeFilter(
            EAttributeFilterMode::MatchingOnly,
            runtimeParamsTemplate->GetRegisteredKeys());
        ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_runtime_params");
    }

    void HandleOperationRuntimeParams(
        TOperationPtr operation,
        TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_runtime_params");
        if (!rsp->IsOK()) {
            LOG_ERROR(*rsp, "Error updating operation runtime parameters");
            return;
        }

        auto operationNode = ConvertToNode(TYsonString(rsp->value()));
        auto attributesNode = ConvertToNode(operationNode->Attributes());
        
        OperationRuntimeParamsUpdated_.Fire(operation, attributesNode);
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
            if (!ReconfigureYsonSerializable(Config_, TYsonString(rsp->value())))
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

        bool registered = false;
        try {
            auto controller = CreateController(operation.Get());
            operation->SetController(controller);

            SwitchTo(controller->GetCancelableControlInvoker());

            RegisterOperation(operation);
            registered = true;

            controller->Initialize();
            controller->Essentiate();

            auto error = WaitFor(MasterConnector_->CreateOperationNode(operation));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);

            if (operation->GetState() != EOperationState::Initializing)
                throw TFiberCanceledException();
        } catch (const std::exception& ex) {
            auto wrappedError = TError("Operation has failed to initialize")
                << ex;
            if (registered) {
                OnOperationFailed(operation, wrappedError);
            }
            operation->SetStarted(wrappedError);
            return wrappedError;
        }

        LogEventFluently(ELogEventType::OperationStarted)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec());

        // NB: Once we've registered the operation in Cypress we're free to complete
        // StartOperation request. Preparation will happen in a separate fiber in a non-blocking
        // fashion.
        operation->GetController()->GetCancelableControlInvoker()->Invoke(BIND(
            &TImpl::DoPrepareOperation,
            MakeStrong(this),
            operation));

        operation->SetStarted(TError());
        return TError();
    }

    void DoPrepareOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Initializing) {
            throw TFiberCanceledException();
        }

        auto operationId = operation->GetId();

        try {
            // Run async preparation.
            LOG_INFO("Preparing operation (OperationId: %v)",
                operationId);

            operation->SetState(EOperationState::Preparing);

            auto controller = operation->GetController();
            auto asyncResult = controller->Prepare();
            auto result = WaitFor(asyncResult);
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
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

        LOG_INFO("Operation has been prepared and is now running (OperationId: %v)",
            operationId);

        LogOperationProgress(operation);

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }

    void ReviveOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(IdToOperation_.empty());
        for (auto operation : operations) {
            ReviveOperation(operation);
        }
    }

    void ReviveOperation(TOperationPtr operation)
    {
        auto operationId = operation->GetId();

        LOG_INFO("Reviving operation (OperationId: %v)",
            operationId);

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

        BIND(&TImpl::DoReviveOperation, MakeStrong(this), operation)
            .AsyncVia(operation->GetController()->GetCancelableControlInvoker())
            .Run();
    }

    void DoReviveOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Reviving) {
            throw TFiberCanceledException();
        }

        try {
            auto controller = operation->GetController();

            if (!operation->Snapshot()) {
                operation->SetCleanStart(true);
                controller->Initialize();
            }

            controller->Essentiate();

            {
                auto error = WaitFor(MasterConnector_->ResetRevivingOperationNode(operation));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            {
                auto error = WaitFor(operation->Snapshot()
                	? controller->Revive()
                	: controller->Prepare());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            if (operation->GetState() != EOperationState::Reviving)
                throw TFiberCanceledException();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Operation has failed to revive (OperationId: %v)",
                operation->GetId());
            auto wrappedError = TError("Operation has failed to revive") << ex;
            SetOperationFinalState(operation, EOperationState::Failed, wrappedError);
            MasterConnector_->FlushOperationNode(operation);
            return;
        }

        // Discard the snapshot, if any.
        operation->Snapshot().Reset();

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation has been revived and is now running (OperationId: %v)",
            operation->GetId());
    }


    TJobPtr FindJob(const TJobId& jobId)
    {
        auto it = IdToJob_.find(jobId);
        return it == IdToJob_.end() ? nullptr : it->second;
    }


    TExecNodePtr RegisterNode(const TNodeDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node online (Descriptor: %v)", descriptor.GetDefaultAddress());

        auto node = New<TExecNode>(descriptor);

        auto lease = TLeaseManager::CreateLease(
            Config_->NodeHearbeatTimeout,
            BIND(&TImpl::UnregisterNode, MakeWeak(this), node)
                .Via(GetControlInvoker()));

        node->SetLease(lease);

        TotalResourceLimits_ += node->ResourceLimits();
        TotalResourceUsage_ += node->ResourceUsage();

        YCHECK(AddressToNode_.insert(std::make_pair(node->GetAddress(), node)).second);

        return node;
    }

    void UnregisterNode(TExecNodePtr node)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node offline (Address: %v)", ~node->GetAddress());

        TotalResourceLimits_ -= node->ResourceLimits();
        TotalResourceUsage_ -= node->ResourceUsage();

        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        const auto& address = node->GetAddress();
        for (auto job : jobs) {
            LOG_INFO("Aborting job on an offline node %v (JobId: %v, OperationId: %v)",
                ~address,
                job->GetId(),
                job->GetOperation()->GetId());
            AbortJob(job, TError("Node offline"));
            UnregisterJob(job);
        }
        YCHECK(AddressToNode_.erase(address) == 1);
    }


    void RegisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation_.insert(std::make_pair(operation->GetId(), operation)).second);

        auto mutationId = operation->GetMutationId();
        if (mutationId != NullMutationId) {
            YCHECK(MutationIdToOperation_.insert(std::make_pair(mutationId, operation)).second);
        }

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
        for (auto job : jobs) {
            AbortJob(job, TError("Operation aborted"));
            UnregisterJob(job);
        }

        YCHECK(operation->Jobs().empty());
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(IdToOperation_.erase(operation->GetId()) == 1);

        auto mutationId = operation->GetMutationId();
        if (mutationId != NullMutationId) {
            YCHECK(MutationIdToOperation_.erase(operation->GetMutationId()) == 1);
        }

        OperationUnregistered_.Fire(operation);

        LOG_DEBUG("Operation unregistered (OperationId: %v)",
            operation->GetId());
    }

    void LogOperationProgress(TOperationPtr operation)
    {
        if (operation->GetState() != EOperationState::Running)
            return;

        LOG_DEBUG("Progress: %v, %v (OperationId: %v)",
            ~operation->GetController()->GetLoggingProgress(),
            ~Strategy_->GetOperationLoggingProgress(operation),
            operation->GetId());
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

        LOG_INFO("Committing scheduler transactions (OperationId: %v)",
            operation->GetId());

        auto commitTransaction = [&] (TTransactionPtr transaction) {
            if (!transaction) {
                return;
            }
            auto result = WaitFor(transaction->Commit());
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Operation has failed to commit");
            if (operation->GetState() != EOperationState::Completing) {
                throw TFiberCanceledException();
            }
        };

        commitTransaction(operation->GetInputTransaction());
        commitTransaction(operation->GetOutputTransaction());
        commitTransaction(operation->GetSyncSchedulerTransaction());

        LOG_INFO("Scheduler transactions committed (OperationId: %v)",
            operation->GetId());

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
            UnregisterOperation(operation);
        }
    }


    void RegisterJob(TJobPtr job)
    {
        auto operation = job->GetOperation();
        auto node = job->GetNode();

        ++JobTypeCounters_[job->GetType()];

        YCHECK(IdToJob_.insert(std::make_pair(job->GetId(), job)).second);
        YCHECK(operation->Jobs().insert(job).second);
        YCHECK(node->Jobs().insert(job).second);

        job->GetNode()->ResourceUsage() += job->ResourceUsage();

        JobStarted_.Fire(job);

        LOG_DEBUG("Job registered (JobId: %v, JobType: %v, OperationId: %v)",
            job->GetId(),
            job->GetType(),
            operation->GetId());
    }

    void UnregisterJob(TJobPtr job)
    {
        auto operation = job->GetOperation();
        auto node = job->GetNode();

        --JobTypeCounters_[job->GetType()];

        YCHECK(IdToJob_.erase(job->GetId()) == 1);
        YCHECK(operation->Jobs().erase(job) == 1);
        YCHECK(node->Jobs().erase(job) == 1);

        JobFinished_.Fire(job);

        LOG_DEBUG("Job unregistered (JobId: %v, OperationId: %v)",
            job->GetId(),
            operation->GetId());
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
        LOG_DEBUG("Job preempted (JobId: %v, OperationId: %v)",
            job->GetId(),
            job->GetOperation()->GetId());

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
        auto jobFailed = job->GetState() == EJobState::Failed;
        const auto& schedulerResultExt = job->Result().GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        std::vector<TChunkId> failContexts;
        for (const auto& item : schedulerResultExt.fail_context_chunk_ids()) {
            failContexts.push_back(FromProto<TGuid>(item));
        }

        if (schedulerResultExt.has_stderr_chunk_id()) {
            auto operation = job->GetOperation();
            auto stderrChunkId = FromProto<TChunkId>(schedulerResultExt.stderr_chunk_id());

            if (jobFailed || operation->GetStderrCount() < operation->GetMaxStderrCount()) {
                if (jobFailed) {
                    MasterConnector_->CreateJobNode(job, stderrChunkId, failContexts);
                } else {
                    MasterConnector_->CreateJobNode(job, stderrChunkId, std::vector<TChunkId>());
                }
                operation->SetStderrCount(operation->GetStderrCount() + 1);
            } else {
                ReleaseStderrChunk(job, stderrChunkId);
            }
        } else if (jobFailed) {
            MasterConnector_->CreateJobNode(job, NullChunkId, failContexts);
        }
    }

    void ReleaseStderrChunk(TJobPtr job, const TChunkId& chunkId)
    {
        TObjectServiceProxy proxy(GetMasterClient()->GetMasterChannel());
        auto transaction = job->GetOperation()->GetAsyncSchedulerTransaction();
        if (!transaction)
            return;

        auto req = TMasterYPathProxy::UnstageObject();
        ToProto(req->mutable_object_id(), chunkId);
        req->set_recursive(false);

        // Fire-and-forget.
        // The subscriber is only needed to log the outcome.
        proxy.Execute(req).Subscribe(
            BIND(&TImpl::OnStderrChunkReleased, MakeStrong(this)));
    }

    void OnStderrChunkReleased(TMasterYPathProxy::TRspUnstageObjectPtr rsp)
    {
        if (!rsp->IsOK()) {
            LOG_WARNING(*rsp, "Error releasing stderr chunk");
        }
    }

    void InitStrategy()
    {
        Strategy_ = CreateFairShareStrategy(Config_, this);
    }

    IOperationControllerPtr CreateController(TOperation* operation)
    {
        switch (operation->GetType()) {
            case EOperationType::Map:
                return CreateMapController(Config_, this, operation);
            case EOperationType::Merge:
                return CreateMergeController(Config_, this, operation);
            case EOperationType::Erase:
                return CreateEraseController(Config_, this, operation);
            case EOperationType::Sort:
                return CreateSortController(Config_, this, operation);
            case EOperationType::Reduce:
                return CreateReduceController(Config_, this, operation);
            case EOperationType::MapReduce:
                return CreateMapReduceController(Config_, this, operation);
            case EOperationType::RemoteCopy:
                return CreateRemoteCopyController(Config_, this, operation);
            default:
                YUNREACHABLE();
        }
    }

    INodePtr GetSpecTemplate(EOperationType type, IMapNodePtr spec)
    {
        switch (type) {
            case EOperationType::Map:
                return Config_->MapOperationSpec;
            case EOperationType::Merge: {
                auto mergeSpec = ParseOperationSpec<TMergeOperationSpec>(spec);
                switch (mergeSpec->Mode) {
                    case EMergeMode::Unordered: {
                        return Config_->UnorderedMergeOperationSpec;
                    }
                    case EMergeMode::Ordered: {
                        return Config_->OrderedMergeOperationSpec;
                    }
                    case EMergeMode::Sorted: {
                        return Config_->SortedMergeOperationSpec;
                    }
                    default:
                        YUNREACHABLE();
                }
            }
            case EOperationType::Erase:
                return Config_->EraseOperationSpec;
            case EOperationType::Sort:
                return Config_->SortOperationSpec;
            case EOperationType::Reduce:
                return Config_->ReduceOperationSpec;
            case EOperationType::MapReduce:
                return Config_->MapReduceOperationSpec;
            case EOperationType::RemoteCopy:
                return Config_->RemoteCopyOperationSpec;
            default:
                YUNREACHABLE();
        }
    }


    void DoCompleteOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationId = operation->GetId();

        LOG_INFO("Completing operation (OperationId: %v)",
            operationId);

        if (operation->GetState() != EOperationState::Completing) {
            throw TFiberCanceledException();
        }

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
                auto asyncResult = controller->Commit();
                auto result = WaitFor(asyncResult);
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                if (operation->GetState() != EOperationState::Completing) {
                    throw TFiberCanceledException();
                }
            }

            CommitSchedulerTransactions(operation);

            LOG_INFO("Operation transactions committed (OperationId: %v)",
                operationId);

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

        LogEventFluently(ELogEventType::OperationCompleted)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec())
            .Item("start_time").Value(operation->GetStartTime())
            .Item("finish_time").Value(operation->GetFinishTime());

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

        AbortOperationJobs(operation);

        operation->SetState(intermediateState);

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

        LogEventFluently(logEventType)
            .Item("operation_id").Value(operation->GetId())
            .Item("operation_type").Value(operation->GetType())
            .Item("spec").Value(operation->GetSpec())
            .Item("start_time").Value(operation->GetStartTime())
            .Item("finish_time").Value(operation->GetFinishTime())
            .Item("error").Value(error);

        FinishOperation(operation);
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
                .EndMap()
                .Item("operations").DoMapFor(IdToOperation_, [=] (TFluentMap fluent, const TOperationIdMap::value_type& pair) {
                    BuildOperationYson(pair.second, fluent);
                })
                .Item("nodes").DoMapFor(AddressToNode_, [=] (TFluentMap fluent, const TExecNodeMap::value_type& pair) {
                    BuildNodeYson(pair.second, fluent);
                })
                .Item("clusters").DoMapFor(GetClusterDirectory()->GetClusterNames(), [=] (TFluentMap fluent, const Stroka& clusterName) {
                    BuildClusterYson(clusterName, fluent);
                })
                .DoIf(Strategy_ != nullptr, BIND(&ISchedulerStrategy::BuildOrchid, Strategy_.get()))
            .EndMap();
    }

    void BuildClusterYson(const Stroka& clusterName, IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item(clusterName)
            .Value(GetClusterDirectory()->GetConnectionConfig(clusterName));
    }

    void BuildOperationYson(TOperationPtr operation, IYsonConsumer* consumer)
    {
        auto state = operation->GetState();
        bool hasProgress = (state == EOperationState::Running) || IsOperationFinished(state);
        BuildYsonMapFluently(consumer)
            .Item(ToString(operation->GetId())).BeginMap()
                // Include the complete list of attributes.
                .Do(BIND(&NScheduler::BuildInitializingOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .DoIf(hasProgress, BIND(&IOperationController::BuildProgress, operation->GetController()))
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgress, Strategy_.get(), operation))
                .EndMap()
                .Item("brief_progress").BeginMap()
                    .DoIf(hasProgress, BIND(&IOperationController::BuildBriefProgress, operation->GetController()))
                    .Do(BIND(&ISchedulerStrategy::BuildBriefOperationProgress, Strategy_.get(), operation))
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

        NLog::TLogger Logger(SchedulerLogger);
        Logger.AddTag("Address: %v, JobId: %v",
            jobAddress,
            jobId);

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

        Logger.AddTag("JobType: %v, State: %v, OperationId: %v",
            job->GetType(),
            state,
            operation->GetId());

        // Check if the job is running on a proper node.
        const auto& expectedAddress = job->GetNode()->GetAddress();
        if (jobAddress != expectedAddress) {
            // Job has moved from one node to another. No idea how this could happen.
            if (state == EJobState::Aborting) {
                // Do nothing, job is already terminating.
            } else if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
                ToProto(response->add_jobs_to_remove(), jobId);
                LOG_WARNING("Job status report was expected from %v, removal scheduled",
                    ~expectedAddress);
            } else {
                ToProto(response->add_jobs_to_abort(), jobId);
                LOG_WARNING("Job status report was expected from %v, abort scheduled",
                    ~expectedAddress);
            }
            return nullptr;
        }

        switch (state) {
            case EJobState::Completed: {
                if (jobStatus->has_result()) {
                    const auto& statistics = jobStatus->result().statistics();
                    LOG_INFO("Job completed, removal scheduled (Input: {%v}, Output: {%v}, Time: %v" ")",
                        statistics.input(),
                        statistics.output(),
                        statistics.time());
                } else {
                    LOG_INFO("Job completed, removal scheduled");
                }
                OnJobCompleted(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Failed: {
                auto error = FromProto<TError>(jobStatus->result().error());
                LOG_WARNING(error, "Job failed, removal scheduled");
                OnJobFailed(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Aborted: {
                auto error = FromProto<TError>(jobStatus->result().error());
                LOG_INFO(error, "Job aborted, removal scheduled");
                OnJobAborted(job, jobStatus->mutable_result());
                ToProto(response->add_jobs_to_remove(), jobId);
                break;
            }

            case EJobState::Running:
            case EJobState::Waiting:
                if (job->GetState() == EJobState::Aborted) {
                    LOG_INFO("Aborting job (Address: %v, JobType: %v, JobId: %v, OperationId: %v)",
                        ~jobAddress,
                        job->GetType(),
                        jobId,
                        operation->GetId());
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

        auto maxJobStarts = Owner->Config_->MaxStartedJobsPerHeartbeat;
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

ISchedulerStrategy* TScheduler::GetStrategy()
{
    return Impl->GetStrategy();
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

TFuture<TOperationStartResult> TScheduler::StartOperation(
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

