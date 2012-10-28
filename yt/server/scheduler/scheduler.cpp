#include "stdafx.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "null_strategy.h"
#include "fifo_strategy.h"
#include "fair_share_strategy.h"
#include "operation_controller.h"
#include "map_controller.h"
#include "merge_controller.h"
#include "sort_controller.h"
#include "helpers.h"
#include "master_connector.h"
#include "job_resources.h"
#include "private.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/address.h>

#include <ytlib/actions/action_queue.h>
#include <ytlib/actions/async_pipeline.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/fluent.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NCellScheduler;
using namespace NObjectClient;
using namespace NScheduler::NProto;

using NChunkClient::TChunkId;
using NChunkClient::NullChunkId;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;
static NProfiling::TProfiler& Profiler = SchedulerProfiler;

static TDuration ProfilingPreiod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////

class TScheduler::TSchedulingContext
    : public ISchedulingContext
{
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, StartedJobs);

public:
    TSchedulingContext(
        TExecNodePtr node,
        NProto::TRspHeartbeat* response)
        : Node(node)
        , Response(response)
    { }

    virtual TExecNodePtr GetNode() override
    {
        return Node;
    }

    virtual bool HasSpareResources() const override
    {
        return NScheduler::HasSpareResources(Node->ResourceUtilization(), Node->ResourceLimits());
    }

    virtual TJobPtr BeginScheduleJob(TOperationPtr operation) override
    {
        auto id = TJobId::Create();
        auto job = New<TJob>(
            id,
            operation,
            Node,
            TInstant::Now());

        auto* jobInfo = Response->add_jobs_to_start();
        *jobInfo->mutable_job_id() = id.ToProto();

        job->SetSpec(jobInfo->mutable_spec());

        StartedJobs_.push_back(job);

        return job;
    }

    virtual void EndScheduleJob(TJobPtr job) override
    {
        job->ResourceUtilization() = job->GetSpec()->resource_utilization();
        job->GetNode()->ResourceUtilization() += job->ResourceUtilization();
    }

private:
    TExecNodePtr Node;
    NProto::TRspHeartbeat* Response;

};

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public NRpc::TServiceBase
    , public IOperationHost
    , public ISchedulerStrategyHost
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            bootstrap->GetControlInvoker(),
            TSchedulerServiceProxy::GetServiceName(),
            SchedulerLogger.GetCategory())
        , Config(config)
        , Bootstrap(bootstrap)
        , BackgroundQueue(New<TActionQueue>("Background"))
        , MasterConnector(new TMasterConnector(Config, Bootstrap))
        , TotalResourceLimitsProfiler(Profiler.GetPathPrefix() + "/total_resource_limits")
        , TotalResourceUtilizationProfiler(Profiler.GetPathPrefix() + "/total_resource_utilization")
        , JobTypeCounters(EJobType::GetDomainSize())
        , TotalResourceLimits(ZeroNodeResources())
        , TotalResourceUtilization(ZeroNodeResources())
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_AFFINITY(GetControlInvoker(), ControlThread);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WaitForOperation));
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetRequestHeavy(true)
                .SetResponseHeavy(true)
                .SetResponseCodec(ECodecId::Lz4)
                .SetInvoker(Bootstrap->GetControlInvoker(EControlQueue::Heartbeat)));

        ProfilingInvoker = New<TPeriodicInvoker>(
            Bootstrap->GetControlInvoker(),
            BIND(&TThis::OnProfiling, MakeWeak(this)),
            ProfilingPreiod);
    }

    void Start()
    {
        InitStrategy();

        MasterConnector->SubscribeWatcherRequest(BIND(
            &TThis::OnNodesRequest,
            Unretained(this)));
        MasterConnector->SubscribeWatcherResponse(BIND(
            &TThis::OnNodesResponse,
            Unretained(this)));

        MasterConnector->SubscribeMasterConnected(BIND(
            &TThis::OnMasterConnected,
            Unretained(this)));
        MasterConnector->SubscribeMasterDisconnected(BIND(
            &TThis::OnMasterDisconnected,
            Unretained(this)));

        MasterConnector->SubscribePrimaryTransactionAborted(BIND(
            &TThis::OnPrimaryTransactionAborted,
            Unretained(this)));

        MasterConnector->Start();

        ProfilingInvoker->Start();
    }

    TYPathServiceProducer CreateOrchidProducer()
    {
        // TODO(babenko): virtualize
        auto producer = BIND(&TThis::BuildOrchidYson, MakeStrong(this));
        return BIND([=] () {
            return IYPathService::FromProducer(producer);
        });
    }

    std::vector<TOperationPtr> GetOperations()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TOperationPtr> operations;
        FOREACH (const auto& pair, Operations) {
            operations.push_back(pair.second);
        }
        return operations;
    }


    // ISchedulerStrategyHost implementation
    DEFINE_SIGNAL(void(TOperationPtr), OperationStarted);
    DEFINE_SIGNAL(void(TOperationPtr), OperationFinished);

    DEFINE_SIGNAL(void(TJobPtr), JobStarted);
    DEFINE_SIGNAL(void(TJobPtr), JobFinished);

    virtual void PreeemptJob(TJobPtr job) override
    {
        AbortJob(job, TError("Job preempted"));

        LOG_DEBUG("Job preempted (JobId: %s, OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    virtual TMasterConnector* GetMasterConnector() override
    {
        return ~MasterConnector;
    }

    virtual NProto::TNodeResources GetTotalResourceLimits() override
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
        FOREACH (const auto& pair, Nodes) {
            result.push_back(pair.second);
        }
        return result;
    }

    virtual void OnOperationCompleted(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        GetControlInvoker()->Invoke(BIND(
            &TThis::DoOperationCompleted,
            MakeStrong(this),
            operation));
    }

    virtual void OnOperationFailed(
        TOperationPtr operation,
        const TError& error) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        GetControlInvoker()->Invoke(BIND(
            &TThis::DoOperationFailed,
            MakeStrong(this),
            operation,
            EOperationState::Failing,
            EOperationState::Failed,
            error));
    }


private:
    typedef TImpl TThis;
    friend class TSchedulingContext;

    TSchedulerConfigPtr Config;
    TBootstrap* Bootstrap;
    TActionQueuePtr BackgroundQueue;

    THolder<TMasterConnector> MasterConnector;
    TCancelableContextPtr CancelableConnectionContext;
    IInvokerPtr CancelableConnectionControlInvoker;

    TAutoPtr<ISchedulerStrategy> Strategy;

    typedef yhash_map<Stroka, TExecNodePtr> TExecNodeMap;
    TExecNodeMap Nodes;

    typedef yhash_map<TOperationId, TOperationPtr> TOperationMap;
    TOperationMap Operations;

    typedef yhash_map<TJobId, TJobPtr> TJobMap;
    TJobMap Jobs;

    NProfiling::TProfiler TotalResourceLimitsProfiler;
    NProfiling::TProfiler TotalResourceUtilizationProfiler;
    std::vector<int> JobTypeCounters;
    TPeriodicInvokerPtr ProfilingInvoker;

    NProto::TNodeResources TotalResourceLimits;
    NProto::TNodeResources TotalResourceUtilization;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FOREACH (auto jobType, EJobType::GetDomainValues()) {
            Profiler.Enqueue("/job_count/" + FormatEnum(EJobType(jobType)), JobTypeCounters[jobType]);
        }
        
        Profiler.Enqueue("/job_count/total", Jobs.size());
        Profiler.Enqueue("/operation_count", Operations.size());
        Profiler.Enqueue("/node_count", Nodes.size());

        ProfileResources(TotalResourceLimitsProfiler, TotalResourceLimits);
        ProfileResources(TotalResourceUtilizationProfiler, TotalResourceUtilization);
    }


    void OnMasterConnected(const TMasterHandshakeResult& result)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CancelableConnectionContext = New<TCancelableContext>();
        CancelableConnectionControlInvoker = CancelableConnectionContext->CreateInvoker(
            Bootstrap->GetControlInvoker());

        RegisterAndReviveOperations(result.Operations);
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(CancelableConnectionContext);
        CancelableConnectionContext->Cancel();
        CancelableConnectionContext.Reset();
        CancelableConnectionControlInvoker.Reset();

        auto operations = Operations;
        FOREACH (const auto& pair, operations) {
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

        YCHECK(Operations.empty());
        Jobs.clear();
    }


    void OnPrimaryTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Operation belongs to an expired transaction %s, aborting (OperationId: %s)",
            ~operation->GetTransactionId().ToString(),
            ~operation->GetOperationId().ToString());

        AbortOperation(operation, TError("Operation transaction has been expired or was aborted"));
    }


    void OnNodesRequest(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
    {
        LOG_INFO("Updating exec nodes");

        auto req = TYPathProxy::Get("//sys/nodes/@online");
        batchReq->AddRequest(req, "get_online_nodes");
    }

    void OnNodesResponse(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_online_nodes");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting online nodes");

        auto newAddresses = ConvertTo< std::vector<Stroka> >(TYsonString(rsp->value()));
        LOG_INFO("Exec nodes updated, %d nodes found",
            static_cast<int>(newAddresses.size()));

        // Examine the list of nodes returned by master and figure out the difference.

        yhash_set<Stroka> existingAddresses;
        auto nodes = Bootstrap->GetScheduler()->GetExecNodes();
        FOREACH (auto node, nodes) {
            YCHECK(existingAddresses.insert(node->GetAddress()).second);
        }

        FOREACH (const auto& address, newAddresses) {
            auto it = existingAddresses.find(address);
            if (it == existingAddresses.end()) {
                OnNodeOnline(address);
            } else {
                existingAddresses.erase(it);
            }
        }

        FOREACH (const auto& address, existingAddresses) {
            OnNodeOffline(address);
        }

        LOG_INFO("Exec nodes updated");
    }

    void OnNodeOnline(const Stroka& address)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node online: %s", ~address);

        // XXX(babenko): Force the scheduler to precache node's DNS address.
        // Consider removing this.
        TAddressResolver::Get()->Resolve(Stroka(GetServiceHostName(address)));

        auto node = New<TExecNode>(address);
        RegisterNode(node);

        TotalResourceLimits += node->ResourceLimits();
        TotalResourceUtilization += node->ResourceUtilization();
    }

    void OnNodeOffline(const Stroka& address)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Node offline: %s", ~address);
    
        auto node = GetNode(address);
        UnregisterNode(node);

        TotalResourceLimits -= node->ResourceLimits();
        TotalResourceUtilization -= node->ResourceUtilization();
    }


    typedef TValueOrError<TOperationPtr> TStartResult;

    TFuture<TStartResult> StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const IMapNodePtr spec)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Create operation object.
        auto operationId = TOperationId::Create();
        auto operation = New<TOperation>(
            operationId,
            type,
            transactionId,
            spec,
            TInstant::Now());

        LOG_INFO("Starting operation (OperationType: %s, OperationId: %s, TransactionId: %s)",
            ~type.ToString(),
            ~operationId.ToString(),
            ~transactionId.ToString());

        auto controller = CreateController(~operation);
        operation->SetController(controller);
        operation->SetState(EOperationState::Initializing);

        try {
            // The operation owns the controller but not vice versa.
            // Hence we use raw pointers inside controllers.
            InitializeOperation(operation);
        } catch (const std::exception& ex) {
            return MakeFuture(TStartResult(TError("Operation has failed to start") << ex));
        }

        YCHECK(operation->GetState() == EOperationState::Initializing);
        operation->SetState(EOperationState::Preparing);

        return StartAsyncPipeline(controller->GetCancelableControlInvoker())
            ->Add(BIND(&TMasterConnector::CreateOperationNode, ~MasterConnector, operation))
            ->Add(BIND(&TThis::OnOperationNodeCreated, MakeStrong(this), operation))
            ->Run();
    }

    TOperationPtr OnOperationNodeCreated(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        RegisterOperation(operation);

        // Run async preparation.
        LOG_INFO("Preparing operation (OperationId: %s)",
            ~operation->GetOperationId().ToString());

        auto controller = operation->GetController();
        controller->Prepare().Subscribe(
            BIND(&TThis::OnOperationPrepared, MakeStrong(this), operation)
                .Via(controller->GetCancelableControlInvoker()));

        return operation;
    }

    void OnOperationPrepared(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Preparing)
            return;

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation has been prepared and is now running (OperationId: %s)", 
            ~operation->GetOperationId().ToString());

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }


    void InitializeOperation(TOperationPtr operation)
    {
        if (Nodes.empty()) {
            THROW_ERROR_EXCEPTION("No online exec nodes to start operation");
        }

        operation->GetController()->Initialize();
    }

    void RegisterAndReviveOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        
        YCHECK(Operations.empty());
        FOREACH (auto operation, operations) {
            LOG_INFO("Reviving operation (OperationId: %s)", ~operation->GetOperationId().ToString());
            
            auto controller = CreateController(~operation);
            operation->SetController(controller);

            RegisterOperation(operation);

            StartAsyncPipeline(controller->GetCancelableControlInvoker())
                ->Add(BIND(&IOperationController::Revive, controller))
                ->Add(BIND(&TThis::OnOperationRevived, MakeStrong(this), operation))
                ->Run();
        }
    }

    void OnOperationRevived(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Reviving)
            return;

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation has been revived and is now running (OperationId: %s)", 
            ~operation->GetOperationId().ToString());
    }


    TFuture<void> AbortOperation(TOperationPtr operation, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishingState()) {
            LOG_INFO(error, "Operation is already finishing (OperationId: %s, State: %s)",
                ~operation->GetOperationId().ToString(),
                ~operation->GetState().ToString());
            return operation->GetFinished();
        }

        if (operation->IsFinishedState()) {
            LOG_INFO(error, "Operation is already finished (OperationId: %s, State: %s)",
                ~operation->GetOperationId().ToString(),
                ~operation->GetState().ToString());
            return operation->GetFinished();
        }

        LOG_INFO(error, "Aborting operation (OperationId: %s, State: %s)",
            ~operation->GetOperationId().ToString(),
            ~operation->GetState().ToString());
                
        DoOperationFailed(operation, EOperationState::Aborting, EOperationState::Aborted, error);

        return operation->GetFinished();
    }


    TOperationPtr FindOperation(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = Operations.find(id);
        return it == Operations.end() ? NULL : it->second;
    }

    TOperationPtr GetOperation(const TOperationId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operation = FindOperation(id);
        if (!operation) {
            THROW_ERROR_EXCEPTION("No such operation: %s", ~id.ToString());
        }
        return operation;
    }

    TExecNodePtr FindNode(const Stroka& address)
    {
        auto it = Nodes.find(address);
        return it == Nodes.end() ? NULL : it->second;
    }

    TExecNodePtr GetNode(const Stroka& address)
    {
        auto node = FindNode(address);
        YCHECK(node);
        return node;
    }

    TJobPtr FindJob(const TJobId& jobId)
    {
        auto it = Jobs.find(jobId);
        return it == Jobs.end() ? NULL : it->second;
    }


    void RegisterNode(TExecNodePtr node)
    {
        YCHECK(Nodes.insert(MakePair(node->GetAddress(), node)).second);    
    }

    void UnregisterNode(TExecNodePtr node)
    {
        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        FOREACH (auto job, jobs) {
            LOG_INFO("Aborting job on an offline node %s (JobId: %s, OperationId: %s)",
                ~node->GetAddress(),
                ~job->GetId().ToString(),
                ~job->GetOperation()->GetOperationId().ToString());
            AbortJob(job, TError("Node offline"));
            UnregisterJob(job);
        }
        YCHECK(Nodes.erase(node->GetAddress()) == 1);
    }

    
    void RegisterOperation(TOperationPtr operation)
    {
        YCHECK(Operations.insert(MakePair(operation->GetOperationId(), operation)).second);
        OperationStarted_.Fire(operation);

        LOG_DEBUG("Operation registered (OperationId: %s)", ~operation->GetOperationId().ToString());
    }

    void AbortOperationJobs(TOperationPtr operation)
    {
        auto jobs = operation->Jobs();
        FOREACH (auto job, jobs) {
            AbortJob(job, TError("Operation aborted"));
            UnregisterJob(job);
        }

        YCHECK(operation->Jobs().empty());
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(Operations.erase(operation->GetOperationId()) == 1);
        OperationFinished_.Fire(operation);

        LOG_DEBUG("Operation unregistered (OperationId: %s)", ~operation->GetOperationId().ToString());
    }

    void SetOperationFinalState(TOperationPtr operation, EOperationState state, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->SetState(state);
        operation->SetEndTime(TInstant::Now());
        ToProto(operation->Result().mutable_error(), error);
    }

    void FinishOperation(TOperationPtr operation)
    {
        operation->SetFinished();
        operation->SetController(NULL);
        UnregisterOperation(operation);
    }


    void RegisterJob(TJobPtr job)
    {
        ++JobTypeCounters[job->GetType()];

        YCHECK(Jobs.insert(MakePair(job->GetId(), job)).second);
        YCHECK(job->GetOperation()->Jobs().insert(job).second);
        YCHECK(job->GetNode()->Jobs().insert(job).second);

        JobStarted_.Fire(job);
        
        LOG_DEBUG("Job registered (JobId: %s, OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    void UnregisterJob(TJobPtr job)
    {
        --JobTypeCounters[job->GetType()];

        YCHECK(Jobs.erase(job->GetId()) == 1);
        YCHECK(job->GetOperation()->Jobs().erase(job) == 1);
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);

        JobFinished_.Fire(job);

        LOG_DEBUG("Job unregistered (JobId: %s, OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    void AbortJob(TJobPtr job, const TError& error)
    {
        // This method must be safe to call for any job.
        if (job->GetState() != EJobState::Running)
            return;

        job->SetState(EJobState::Aborted);
        ToProto(job->Result().mutable_error(), error);

        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobAborted(job);
        }

        UpdateFinishedJobNode(job);
    }


    void OnJobRunning(TJobPtr job, const NProto::TJobStatus& status)
    {
        auto operation = job->GetOperation();
        if (job->GetState() == EJobState::Running &&
            operation->GetState() == EOperationState::Running)
        {
            operation->GetController()->OnJobRunning(job, status);
        }
    }

    void OnJobCompleted(TJobPtr job, NProto::TJobResult* result)
    {
        if (job->GetState() == EJobState::Running) {
            job->SetState(EJobState::Completed);
            job->Result().Swap(result);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobCompleted(job);
            }

            UpdateFinishedJobNode(job);
        }

        UnregisterJob(job);
    }

    void OnJobFailed(TJobPtr job, NProto::TJobResult* result)
    {
        if (job->GetState() == EJobState::Running) {
            job->SetState(EJobState::Failed);
            job->Result().Swap(result);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobFailed(job);
            }

            UpdateFinishedJobNode(job);
        }

        UnregisterJob(job);
    }

    void OnJobAborted(TJobPtr job, NProto::TJobResult* result)
    {
        // Only update the result for the first time.
        // Typically the scheduler decides to abort the job on its own.
        // In this case we should ignore the result returned from the node
        // and void notifying the controller twice.
        if (job->GetState() == EJobState::Running) {
            job->SetState(EJobState::Aborted);
            job->Result().Swap(result);

            auto operation = job->GetOperation();
            if (operation->GetState() == EOperationState::Running) {
                operation->GetController()->OnJobAborted(job);
            }

            UpdateFinishedJobNode(job);
        }

        UnregisterJob(job);
    }


    void UpdateFinishedJobNode(TJobPtr job)
    {
        const auto& result = job->Result();

        if (result.HasExtension(TMapJobResultExt::map_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TMapJobResultExt::map_job_result_ext);
            ProcessFinishedJob(job, resultExt.mapper_result());
        } else if (result.HasExtension(TReduceJobResultExt::reduce_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TReduceJobResultExt::reduce_job_result_ext);
            ProcessFinishedJob(job, resultExt.reducer_result());
        } else if (result.HasExtension(TPartitionJobResultExt::partition_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TPartitionJobResultExt::partition_job_result_ext);
            if (resultExt.has_mapper_result()) {
                ProcessFinishedJob(job, resultExt.mapper_result());
            }
        } else {
            // Dummy empty job result without stderr.
            static TUserJobResult jobResult;
            ProcessFinishedJob(job, jobResult);
        }
    }

    void ProcessFinishedJob(TJobPtr job, const TUserJobResult& result)
    {
        if (result.has_stderr_chunk_id()) {
            auto chunkId = TChunkId::FromProto(result.stderr_chunk_id());
            MasterConnector->CreateJobNode(job, chunkId);
        } else if (job->GetState() == EJobState::Failed) {
            MasterConnector->CreateJobNode(job, NullChunkId);
        }
    }

    void InitStrategy()
    {
        switch (Config->Strategy) {
            case ESchedulerStrategy::Null:
                Strategy = CreateNullStrategy(this);
                break;
            case ESchedulerStrategy::Fifo:
                Strategy =  CreateFifoStrategy(this);
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
    

    void DoOperationCompleted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Operation is probably being aborted.
            return;
        }

        // The operation may still have running jobs (e.g. those started speculatively).
        AbortOperationJobs(operation);

        operation->SetState(EOperationState::Completing);
        
        auto controller = operation->GetController();
        StartAsyncPipeline(controller->GetCancelableControlInvoker())
            ->Add(BIND(&TMasterConnector::FlushOperationNode, ~MasterConnector, operation))
            ->Add(BIND(&IOperationController::Commit, controller))
            ->Add(BIND(&TThis::SetOperationFinalState, MakeStrong(this), operation, EOperationState::Completed, TError()))
            ->Add(BIND(&TMasterConnector::FinalizeOperationNode, ~MasterConnector, operation))
            ->Add(BIND(&TThis::FinishOperation, MakeStrong(this), operation))
            ->Run();
    }

    void DoOperationFailed(TOperationPtr operation, EOperationState pendingState, EOperationState finalState, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinishedState() || operation->IsFinishingState()) {
            // Safe to call OnOperationFailed multiple times, just ignore it.
            return;
        }

        AbortOperationJobs(operation);

        operation->SetState(pendingState);

        auto controller = operation->GetController();
        StartAsyncPipeline(CancelableConnectionControlInvoker)
            ->Add(BIND(&TMasterConnector::FlushOperationNode, ~MasterConnector, operation))
            ->Add(BIND(&IOperationController::Abort, controller))
            ->Add(BIND(&TThis::SetOperationFinalState, MakeStrong(this), operation, finalState, error))
            ->Add(BIND(&TMasterConnector::FinalizeOperationNode, ~MasterConnector, operation))
            ->Add(BIND(&TThis::FinishOperation, MakeStrong(this), operation))
            ->Run();
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("cell").BeginMap()
                    .Item("resource_limits").Scalar(TotalResourceLimits)
                    .Item("resource_utilization").Scalar(TotalResourceUtilization)
                .EndMap()
                .Item("operations").DoMapFor(Operations, [=] (TFluentMap fluent, TOperationMap::value_type pair) {
                    this->BuildOperationAttributes(fluent, pair.second);
                })
                .Item("jobs").DoMapFor(Jobs, [=] (TFluentMap fluent, TJobMap::value_type pair) {
                    fluent
                        .Item(pair.first.ToString()).BeginMap()
                            .Do(BIND(&BuildJobAttributes, pair.second))
                        .EndMap();
                })
                .Item("nodes").DoMapFor(Nodes, [=] (TFluentMap fluent, TExecNodeMap::value_type pair) {
                    fluent
                        .Item(pair.first).BeginMap()
                            .Do(BIND(&BuildExecNodeAttributes, pair.second))
                        .EndMap();
                })
                .Do(BIND(&ISchedulerStrategy::BuildOrchidYson, ~Strategy))
            .EndMap();
    }

    void BuildOperationAttributes(TFluentMap fluent, TOperationPtr operation)
    {
        fluent
            .Item(operation->GetOperationId().ToString()).BeginMap()
                .Do(BIND(&NScheduler::BuildOperationAttributes, operation))
                .Item("progress").BeginMap()
                    .Do(BIND(&IOperationController::BuildProgressYson, operation->GetController()))
                    .Do(BIND(&ISchedulerStrategy::BuildOperationProgressYson, ~Strategy, operation))
                .EndMap()
            .EndMap();
    }


    // RPC handlers
    void ValidateConnected()
    {
        if (!MasterConnector->IsConnected()) {
            THROW_ERROR_EXCEPTION("Master is not connected");
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, StartOperation)
    {
        auto type = EOperationType(request->type());
        auto transactionId =
            request->has_transaction_id()
            ? TTransactionId::FromProto(request->transaction_id())
            : NullTransactionId;

        IMapNodePtr spec;
        try {
            spec = ConvertToNode(TYsonString(request->spec()))->AsMap();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operation spec")
                << ex;
        }
        
        context->SetRequestInfo("Type: %s, TransactionId: %s",
            ~type.ToString(),
            ~transactionId.ToString());

        ValidateConnected();

        StartOperation(
            type,
            transactionId,
            spec)
        .Subscribe(BIND([=] (TValueOrError<TOperationPtr> result) {
            if (!result.IsOK()) {
                context->Reply(result);
                return;
            }
            auto operation = result.Value();
            auto id = operation->GetOperationId();
            *response->mutable_operation_id() = id.ToProto();
            context->SetResponseInfo("OperationId: %s", ~id.ToString());
            context->Reply();
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation)
    {
        auto operationId = TTransactionId::FromProto(request->operation_id());

        context->SetRequestInfo("OperationId: %s", ~operationId.ToString());

        ValidateConnected();

        auto operation = GetOperation(operationId);

        AbortOperation(
            operation,
            TError("Operation aborted by user request"))
        .Subscribe(BIND([=] () {
            context->Reply();
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WaitForOperation)
    {
        auto operationId = TTransactionId::FromProto(request->operation_id());
        auto timeout = TDuration(request->timeout());
        context->SetRequestInfo("OperationId: %s, Timeout: %s",
            ~operationId.ToString(),
            ~ToString(timeout));

        ValidateConnected();

        auto operation = GetOperation(operationId);
        operation->GetFinished().Subscribe(
            timeout,
            BIND(&TThis::OnOperationWaitResult, MakeStrong(this), context, operation, true),
            BIND(&TThis::OnOperationWaitResult, MakeStrong(this), context, operation, false));
    }

    void OnOperationWaitResult(
        TCtxWaitForOperationPtr context,
        TOperationPtr operation,
        bool maybeFinished)
    {
        context->SetResponseInfo("MaybeFinished: %s", ~FormatBool(maybeFinished));
        context->Response().set_maybe_finished(maybeFinished);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        const auto& address = request->address();
        const auto& resourceLimits = request->resource_limits();
        const auto& resourceUtilization = request->resource_utilization();

        context->SetRequestInfo("Address: %s, JobCount: %d, Utilization: {%s}",
            ~address,
            request->jobs_size(),
            ~FormatResourceUtilization(resourceUtilization, resourceLimits));

        ValidateConnected();

        auto node = FindNode(address);
        if (!node) {
            context->Reply(TError("Node is not registered, heartbeat ignored"));
            return;
        }

        TotalResourceLimits -= node->ResourceLimits();
        TotalResourceUtilization -= node->ResourceUtilization();

        node->ResourceLimits() = resourceLimits;
        node->ResourceUtilization() = resourceUtilization;

        PROFILE_TIMING ("/analysis_time") {
            auto missingJobs = node->Jobs();

            FOREACH (auto& jobStatus, *request->mutable_jobs()) {
                auto job = ProcessJobHeartbeat(
                    request,
                    response,
                    &jobStatus);
                if (job) {
                    YCHECK(missingJobs.erase(job) == 1);
                }
            }

            // Check for missing jobs.
            FOREACH (auto job, missingJobs) {
                LOG_ERROR("Job is missing (Address: %s, JobId: %s, OperationId: %s)",
                    ~address,
                    ~job->GetId().ToString(),
                    ~job->GetOperation()->GetOperationId().ToString());
                AbortJob(job, TError("Job vanished"));
                UnregisterJob(job);
            }
        }

        TSchedulingContext schedulingContext(node, response);
        PROFILE_TIMING ("/schedule_time") {
            Strategy->ScheduleJobs(&schedulingContext);
        }

        FOREACH (auto job, schedulingContext.StartedJobs()) {
            job->SetType(EJobType(job->GetSpec()->type()));

            LOG_INFO("Starting job (Address: %s, JobType: %s, JobId: %s, Utilization: {%s}, OperationId: %s)",
                ~job->GetNode()->GetAddress(),
                ~job->GetType().ToString(),
                ~job->GetId().ToString(),
                ~FormatResources(job->GetSpec()->resource_utilization()),
                ~job->GetOperation()->GetOperationId().ToString());

            RegisterJob(job);
            job->SetSpec(NULL);
        }

        TotalResourceLimits += node->ResourceLimits();
        TotalResourceUtilization += node->ResourceUtilization();

        context->Reply();
    }

    TJobPtr ProcessJobHeartbeat(
        NProto::TReqHeartbeat* request,
        NProto::TRspHeartbeat* response,
        NProto::TJobStatus* jobStatus)
    {
        const auto& address = request->address();
        auto jobId = TJobId::FromProto(jobStatus->job_id());
        auto state = EJobState(jobStatus->state());
            
        NLog::TTaggedLogger Logger(SchedulerLogger);
        Logger.AddTag(Sprintf("Address: %s, JobId: %s",
            ~address,
            ~jobId.ToString()));

        auto job = FindJob(jobId);
        if (!job) {
            switch (state) {
                case EJobState::Completed:
                    LOG_WARNING("Unknown job has completed, removal scheduled");
                    *response->add_jobs_to_remove() = jobId.ToProto();
                    break;

                case EJobState::Failed:
                    LOG_INFO("Unknown job has failed, removal scheduled");
                    *response->add_jobs_to_remove() = jobId.ToProto();
                    break;

                case EJobState::Aborted:
                    LOG_INFO("Job aborted, removal scheduled");
                    *response->add_jobs_to_remove() = jobId.ToProto();
                    break;

                case EJobState::Running:
                    LOG_WARNING("Unknown job is running, abort scheduled");
                    *response->add_jobs_to_abort() = jobId.ToProto();
                    break;

                case EJobState::Aborting:
                    LOG_DEBUG("Job is aborting");
                    break;

                default:
                    YUNREACHABLE();
            }
            return NULL;
        }

        auto operation = job->GetOperation();
        
        Logger.AddTag(Sprintf("JobType: %s, State: %s, OperationId: %s",
            ~job->GetType().ToString(),
            ~state.ToString(),
            ~operation->GetOperationId().ToString()));

        // Check if the job is running on a proper node.
        auto expectedAddress = job->GetNode()->GetAddress();
        if (address != expectedAddress) {
            // Job has moved from one node to another. No idea how this could happen.
            if (state == EJobState::Aborting) {
                // Do nothing, job is already terminating.
            } else if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
                *response->add_jobs_to_remove() = jobId.ToProto();
                LOG_WARNING("Job status report was expected from %s, removal scheduled",
                    ~expectedAddress);
            } else {
                *response->add_jobs_to_abort() = jobId.ToProto();
                LOG_WARNING("Job status report was expected from %s, abort scheduled",
                    ~expectedAddress);
            }
            return NULL;
        }

        switch (state) {
            case EJobState::Completed:
                LOG_INFO("Job completed, removal scheduled");
                OnJobCompleted(job, jobStatus->mutable_result());
                *response->add_jobs_to_remove() = jobId.ToProto();
                break;

            case EJobState::Failed: {
                auto error = FromProto(jobStatus->result().error());
                LOG_WARNING(error, "Job failed, removal scheduled");
                OnJobFailed(job, jobStatus->mutable_result());
                *response->add_jobs_to_remove() = jobId.ToProto();
                break;
            }

            case EJobState::Aborted: {
                auto error = FromProto(jobStatus->result().error());
                LOG_INFO(error, "Job aborted, removal scheduled");
                OnJobAborted(job, jobStatus->mutable_result());
                *response->add_jobs_to_remove() = jobId.ToProto();
                break;
            }

            case EJobState::Running:
                if (job->GetState() == EJobState::Aborted) {
                    LOG_INFO("Aborting job (Address: %s, JobType: %s, JobId: %s, OperationId: %s)",
                        ~job->GetNode()->GetAddress(),
                        ~job->GetType().ToString(),
                        ~jobId.ToString(),
                        ~operation->GetOperationId().ToString());
                    *response->add_jobs_to_abort() = jobId.ToProto();
                } else {
                    LOG_DEBUG("Job is running");
                    OnJobRunning(job, *jobStatus);
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
    : Impl(New<TImpl>(config, bootstrap))
{ }

TScheduler::~TScheduler()
{ }

void TScheduler::Start()
{
    Impl->Start();
}

NRpc::IServicePtr TScheduler::GetService()
{
    return Impl;
}

TYPathServiceProducer TScheduler::CreateOrchidProducer()
{
    return Impl->CreateOrchidProducer();
}

std::vector<TOperationPtr> TScheduler::GetOperations()
{
    return Impl->GetOperations();
}

std::vector<TExecNodePtr> TScheduler::GetExecNodes()
{
    return Impl->GetExecNodes();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

