#include "stdafx.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "null_strategy.h"
#include "fifo_strategy.h"
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

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NCellScheduler;
using namespace NScheduler::NProto;

using NChunkClient::TChunkId;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;
static NProfiling::TProfiler& Profiler = SchedulerProfiler;

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

    virtual TJobPtr StartJob(TOperation* operation) override
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

private:
    TExecNodePtr Node;
    NProto::TRspHeartbeat* Response;

};

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public NRpc::TServiceBase
    , public IOperationHost
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
    {
        YCHECK(config);
        YCHECK(bootstrap);
        VERIFY_INVOKER_AFFINITY(GetControlInvoker(), ControlThread);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WaitForOperation));
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(Heartbeat)
                .SetHeavyRequest(true)
                .SetHeavyResponse(true)
                .SetResponseCodec(ECodecId::Lz4)
                .SetInvoker(Bootstrap->GetControlInvoker(EControlQueue::Heartbeat)));

        JobTypeCounters.resize(EJobType::GetDomainSize());

        MasterConnector->SubscribePrimaryTransactionAborted(BIND(
            &TThis::OnPrimaryTransactionAborted,
            Unretained(this)));
        MasterConnector->SubscribeNodeOnline(BIND(
            &TThis::OnNodeOnline,
            Unretained(this)));
        MasterConnector->SubscribeNodeOffline(BIND(
            &TThis::OnNodeOffline,
            Unretained(this)));
    }

    void Start()
    {
        InitStrategy();
        MasterConnector->Start();
        LoadOperations();
    }

    NYTree::TYPathServiceProducer CreateOrchidProducer()
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

    virtual std::vector<TExecNodePtr> GetExecNodes() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TExecNodePtr> result;
        FOREACH (const auto& pair, ExecNodes) {
            result.push_back(pair.second);
        }
        return result;
    }

private:
    typedef TImpl TThis;
    friend class TSchedulingContext;

    TSchedulerConfigPtr Config;
    TBootstrap* Bootstrap;
    TActionQueuePtr BackgroundQueue;
    THolder<TMasterConnector> MasterConnector;

    TAutoPtr<ISchedulerStrategy> Strategy;

    typedef yhash_map<Stroka, TExecNodePtr> TExecNodeMap;
    TExecNodeMap ExecNodes;

    typedef yhash_map<TOperationId, TOperationPtr> TOperationMap;
    TOperationMap Operations;

    typedef yhash_map<TJobId, TJobPtr> TJobMap;
    TJobMap Jobs;
    std::vector<int> JobTypeCounters;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void OnPrimaryTransactionAborted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        switch (operation->GetState()) {
            case EOperationState::Preparing:
            case EOperationState::Running:
            case EOperationState::Reviving:
                LOG_INFO("Operation %s belongs to an expired transaction %s, aborting",
                    ~operation->GetOperationId().ToString(),
                    ~operation->GetTransactionId().ToString());
                AbortOperation(operation, EAbortReason::TransactionExpired);
                break;

            case EOperationState::Completed:
            case EOperationState::Aborted:
            case EOperationState::Failed:
                LOG_INFO("Operation %s belongs to an expired transaction %s, sweeping",
                    ~operation->GetOperationId().ToString(),
                    ~operation->GetTransactionId().ToString());
                break;

            default:
                YUNREACHABLE();
        }

        UnregisterOperation(operation);
        MasterConnector->RemoveOperationNode(operation);
    }

    void OnNodeOnline(const Stroka& address)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // XXX(babenko): Force the scheduler to precache node's DNS address.
        // Consider removing this.
        TAddressResolver::Get()->Resolve(Stroka(GetServiceHostName(address)));

        auto node = New<TExecNode>(address);
        RegisterNode(node);
    }

    void OnNodeOffline(const Stroka& address)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto node = GetNode(address);
        UnregisterNode(node);
    }


    typedef TValueOrError<TOperationPtr> TStartResult;

    TFuture< TStartResult > StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec)
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

        try {
            // The operation owns the controller but not vice versa.
            // Hence we use raw pointers inside controllers.
            operation->SetController(CreateController(operation.Get()));
            operation->SetState(EOperationState::Initializing);
            InitializeOperation(operation);
        } catch (const std::exception& ex) {
            return MakeFuture(TStartResult(TError("Operation %s has failed to start\n%s",
                ~operationId.ToString(),
                ex.what())));
        }

        YCHECK(operation->GetState() == EOperationState::Initializing);
        operation->SetState(EOperationState::Preparing);

        // Create a node in Cypress that will represent the operation.
        return MasterConnector->CreateOperationNode(operation).Apply(
            BIND([=] (TError error) -> TStartResult {
                if (!error.IsOK()) {
                    return error;
                }

                RegisterOperation(operation);
                LOG_INFO("Operation has started (OperationId: %s)", ~operationId.ToString());

                PrepareOperation(operation);

                return operation;
            })
            .AsyncVia(GetControlInvoker()));
    }

    void InitializeOperation(TOperationPtr operation)
    {
        if (ExecNodes.empty()) {
            ythrow yexception() << "No online exec nodes to start operation";
        }

        operation->GetController()->Initialize();
    }

    void PrepareOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Run async preparation.
        LOG_INFO("Preparing operation %s", ~operation->GetOperationId().ToString());
        operation->GetController()->Prepare()
            .Subscribe(
                BIND(&TThis::OnOperationPrepared, MakeStrong(this), operation)
            .Via(GetControlInvoker()));
    }

    void OnOperationPrepared(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Preparing)
            return;

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation %s has been prepared and is now running", 
            ~operation->GetOperationId().ToString());

        // From this moment on the controller is fully responsible for the
        // operation's fate. It will eventually call #OnOperationCompleted or
        // #OnOperationFailed to inform the scheduler about the outcome.
    }


    void ReviveOperations(const std::vector<TOperationPtr>& operations)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FOREACH (auto operation, operations) {
            operation->SetController(CreateController(~operation));
            operation->SetState(EOperationState::Reviving);
            RegisterOperation(operation);
        }

        MasterConnector->ReviveOperationNodes(operations);

        FOREACH (auto operation, operations) {
            LOG_INFO("Reviving operation %s", ~operation->GetOperationId().ToString());
            operation ->GetController()->Revive().Subscribe(
                BIND(&TThis::OnOperationRevived, MakeStrong(this), operation)
                .Via(GetControlInvoker()));
        }
    }

    void OnOperationRevived(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->GetState() != EOperationState::Reviving)
            return;

        operation->SetState(EOperationState::Running);

        LOG_INFO("Operation %s has been revived and is now running", 
            ~operation->GetOperationId().ToString());
    }


    DECLARE_ENUM(EAbortReason,
        (TransactionExpired)
        (UserRequest)
    );

    void AbortOperation(TOperationPtr operation, EAbortReason reason)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinished()) {
            return;
        }

        LOG_INFO("Aborting operation (OperationId: %s, State: %s, Reason: %s)",
            ~operation->GetOperationId().ToString(),
            ~operation->GetState().ToString(),
            ~reason.ToString());
                
        TError error("Operation aborted (Reason: %s)", ~reason.ToString());
        DoOperationFailed(operation, error, EOperationState::Aborted);
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
            // TODO(babenko): error code
            ythrow yexception() << Sprintf("No such operation %s", ~id.ToString());
        }
        return operation;
    }

    TExecNodePtr FindNode(const Stroka& address)
    {
        auto it = ExecNodes.find(address);
        return it == ExecNodes.end() ? NULL : it->second;
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
        YCHECK(ExecNodes.insert(MakePair(node->GetAddress(), node)).second);    
    }

    void UnregisterNode(TExecNodePtr node)
    {
        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        FOREACH (auto job, jobs) {
            LOG_INFO("Aborting job %s on an offline node %s (OperationId: %s)",
                ~job->GetId().ToString(),
                ~node->GetAddress(),
                ~job->GetOperation()->GetOperationId().ToString());
            OnJobFailed(job, TError("Node %s has gone offline", ~node->GetAddress().Quote()));
        }
        YCHECK(ExecNodes.erase(node->GetAddress()) == 1);
    }

    
    void RegisterOperation(TOperationPtr operation)
    {
        YCHECK(Operations.insert(MakePair(operation->GetOperationId(), operation)).second);
        Strategy->OnOperationStarted(operation);
        ProfileOperationCounters();

        LOG_DEBUG("Registered operation %s", ~operation->GetOperationId().ToString());
    }

    void AbortOperationJobs(TOperationPtr operation)
    {
        // Take a copy, the collection will be modified.
        auto jobs = operation->Jobs();
        FOREACH (auto job, jobs) {
            AbortJob(job);
        }
        YCHECK(operation->Jobs().empty());
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        YCHECK(Operations.erase(operation->GetOperationId()) == 1);
        Strategy->OnOperationFinished(operation);
        ProfileOperationCounters();

        LOG_DEBUG("Unregistered operation %s", ~operation->GetOperationId().ToString());
    }

    void ProfileOperationCounters()
    {
        Profiler.Enqueue("/operation_count", Operations.size());
    }


    void RegisterJob(TJobPtr job)
    {
        UpdateJobCounters(job, +1);

        YCHECK(Jobs.insert(MakePair(job->GetId(), job)).second);
        YCHECK(job->GetOperation()->Jobs().insert(job).second);
        YCHECK(job->GetNode()->Jobs().insert(job).second);
        
        LOG_DEBUG("Registered job %s (OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    void UnregisterJob(TJobPtr job)
    {
        UpdateJobCounters(job, -1);

        YCHECK(Jobs.erase(job->GetId()) == 1);
        YCHECK(job->GetOperation()->Jobs().erase(job) == 1);
        YCHECK(job->GetNode()->Jobs().erase(job) == 1);

        LOG_DEBUG("Unregistered job %s (OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    void UpdateJobCounters(TJobPtr job, int delta)
    {
        auto jobType = job->GetType();
        JobTypeCounters[jobType] += delta;

        Profiler.Enqueue("/job_count/" + FormatEnum(jobType), JobTypeCounters[jobType]);
        Profiler.Enqueue("/job_count/total", Jobs.size());
    }

    void AbortJob(TJobPtr job)
    {
        job->SetState(EJobState::Aborted);
        MasterConnector->UpdateJobNode(job);
        UnregisterJob(job);
    }


    void OnJobRunning(TJobPtr job)
    {
        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobRunning(job);
        }
    }

    void OnJobCompleted(TJobPtr job, NProto::TJobResult* result)
    {
        job->SetState(EJobState::Completed);
        job->Result().Swap(result);

        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobCompleted(job);
            UpdateJobNodeOnFinish(job);
        }
        UnregisterJob(job);
    }

    void OnJobFailed(TJobPtr job, NProto::TJobResult* result)
    {
        job->SetState(EJobState::Failed);
        job->Result().Swap(result);

        auto operation = job->GetOperation();
        if (operation->GetState() == EOperationState::Running) {
            operation->GetController()->OnJobFailed(job);
            UpdateJobNodeOnFinish(job);
        }
        UnregisterJob(job);
    }

    void OnJobFailed(TJobPtr job, const TError& error)
    {
        NProto::TJobResult result;
        *result.mutable_error() = error.ToProto();

        OnJobFailed(job, &result);
    }

    void UpdateJobNodeOnFinish(TJobPtr job)
    {
        const auto& result = job->Result();
        
        if (result.HasExtension(TMapJobResultExt::map_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TMapJobResultExt::map_job_result_ext);
            SetJobStdErr(job, resultExt.mapper_result());
        }

        if (result.HasExtension(TReduceJobResultExt::reduce_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TReduceJobResultExt::reduce_job_result_ext);
            SetJobStdErr(job, resultExt.reducer_result());
        }

        if (result.HasExtension(TPartitionJobResultExt::partition_job_result_ext)) {
            const auto& resultExt = result.GetExtension(TPartitionJobResultExt::partition_job_result_ext);
            if (resultExt.has_mapper_result()) {
                SetJobStdErr(job, resultExt.mapper_result());
            }
        }

        MasterConnector->UpdateJobNode(job);
    }

    void SetJobStdErr(TJobPtr job, const TUserJobResult& result)
    {
        if (result.has_stderr_chunk_id()) {
            auto chunkId = TChunkId::FromProto(result.stderr_chunk_id());
            MasterConnector->SetJobStdErr(job, chunkId);
        }
    }


    void InitStrategy()
    {
        Strategy = CreateStrategy(Config->Strategy);
    }

    TAutoPtr<ISchedulerStrategy> CreateStrategy(ESchedulerStrategy strategy)
    {
        switch (strategy) {
            case ESchedulerStrategy::Null:
                return CreateNullStrategy();
            case ESchedulerStrategy::Fifo:
                return CreateFifoStrategy();
            default:
                YUNREACHABLE();
        }
    }

    void LoadOperations()
    {
        auto operations = MasterConnector->LoadOperations();
        std::vector<TOperationPtr> operationsToRevive;
        FOREACH (auto operation, operations) {
            if (!operation->IsFinished()) {
                operationsToRevive.push_back(operation);
            }
        } 

        Bootstrap->GetControlInvoker()->Invoke(BIND(
            &TThis::ReviveOperations,
            MakeStrong(this),
            operationsToRevive));
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
    

    // IOperationHost methods
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


    virtual void OnOperationCompleted(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        GetControlInvoker()->Invoke(BIND(
            &TThis::DoOperationCompleted,
            MakeStrong(this),
            operation));
    }

    void DoOperationCompleted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinished()) {
            // Operation is probably being aborted.
            return;
        }

        // The operation may still have running jobs (e.g. those started speculatively).
        AbortOperationJobs(operation);
        
        operation->SetEndTime(TInstant::Now());

        MasterConnector->FlushOperationNode(operation).Subscribe(
            BIND(&TImpl::OnCompletedOperationNodeFlushed, MakeStrong(this), operation)
            .Via(GetControlInvoker()));
    }

    void OnCompletedOperationNodeFlushed(TOperationPtr operation, TError flushError)
    {
        UNUSED(flushError);
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->GetController()->Commit().Subscribe(
            BIND(&TImpl::OnCompletedOperationCommitted, MakeStrong(this), operation)
            .Via(GetControlInvoker()));
    }

    void OnCompletedOperationCommitted(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        operation->SetState(EOperationState::Completed);

        MasterConnector->FinalizeOperationNode(operation).Subscribe(
            BIND(&TThis::OnCompletedOperationNodeFinalized, MakeStrong(this), operation)
            .Via(GetControlInvoker()));
    }
    
    void OnCompletedOperationNodeFinalized(TOperationPtr operation, TError finalizeError)
    {
        // Can't do anything about the error anyway.
        UNUSED(finalizeError);

        operation->SetFinished();
        operation->SetController(NULL);
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
            error,
            EOperationState::Failed));
    }

    void DoOperationFailed(TOperationPtr operation, const TError& finalError, EOperationState finalState)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (operation->IsFinished()) {
            // Safe to call OnOperationFailed multiple times, just ignore it.
            return;
        }

        AbortOperationJobs(operation);
        
        operation->SetEndTime(TInstant::Now());
        operation->SetState(finalState);
        *operation->Result().mutable_error() = finalError.ToProto();

        MasterConnector->FinalizeOperationNode(operation).Subscribe(
            BIND(&TImpl::OnFailedOperationNodeFinalized, MakeStrong(this), operation)
            .Via(GetControlInvoker()));
    }

    void OnFailedOperationNodeFinalized(TOperationPtr operation, TError finalizeError)
    {
        // Can't do anything about the error anyway.
        UNUSED(finalizeError);

        operation->GetController()->Abort();
        operation->SetFinished();
        operation->SetController(NULL);
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("operations").DoMapFor(Operations, [=] (TFluentMap fluent, TOperationMap::value_type pair) {
                    fluent
                        .Item(pair.first.ToString()).BeginMap()
                            .Do(BIND(&BuildOperationAttributes, pair.second))
                        .EndMap();
                })
                .Item("jobs").DoMapFor(Jobs, [=] (TFluentMap fluent, TJobMap::value_type pair) {
                    fluent
                        .Item(pair.first.ToString()).BeginMap()
                            .Do(BIND(&BuildJobAttributes, pair.second))
                        .EndMap();
                })
                .Item("exec_nodes").DoMapFor(ExecNodes, [=] (TFluentMap fluent, TExecNodeMap::value_type pair) {
                    fluent
                        .Item(pair.first).BeginMap()
                            .Do(BIND(&BuildExecNodeAttributes, pair.second))
                        .EndMap();
                })
            .EndMap();
    }

    // RPC handlers
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
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
        }
        
        context->SetRequestInfo("Type: %s, TransactionId: %s",
            ~type.ToString(),
            ~transactionId.ToString());

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

        auto operation = GetOperation(operationId);
        AbortOperation(operation, EAbortReason::UserRequest);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WaitForOperation)
    {
        auto operationId = TTransactionId::FromProto(request->operation_id());
        auto timeout = TDuration(request->timeout());
        context->SetRequestInfo("OperationId: %s, Timeout: %s",
            ~operationId.ToString(),
            ~ToString(timeout));

        auto operation = GetOperation(operationId);
        operation->GetFinished().Subscribe(
            timeout,
            BIND(&TThis::OnOperationWaitResult, MakeStrong(this), context, operation, true),
            BIND(&TThis::OnOperationWaitResult, MakeStrong(this), context, operation, false));
    }

    void OnOperationWaitResult(
        TCtxWaitForOperationPtr context,
        TOperationPtr operation,
        bool finished)
    {
        context->SetResponseInfo("Finished: %s", ~FormatBool(finished));
        context->Response().set_finished(finished);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        auto address = request->address();
        const auto& resourceLimits = request->resource_limits();
        const auto& resourceUtilization = request->resource_utilization();

        context->SetRequestInfo("Address: %s, JobCount: %d, Utilization: {%s}",
            ~address,
            request->jobs_size(),
            ~FormatResourceUtilization(resourceUtilization, resourceLimits));

        auto node = FindNode(address);
        if (!node) {
            // TODO(babenko): error code
            context->Reply(TError("Node is not registered, heartbeat ignored"));
            return;
        }

        node->ResourceUtilization() = resourceUtilization;
        node->ResourceLimits() = resourceLimits;

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
                OnJobFailed(job, TError("Job has vanished"));
            }
        }

        TSchedulingContext schedulingContext(node, response);
        PROFILE_TIMING ("/schedule_time") {
            Strategy->ScheduleJobs(&schedulingContext);
        }

        FOREACH (auto job, schedulingContext.StartedJobs()) {
            job->SetType(EJobType(job->GetSpec()->type()));

            LOG_INFO("Starting job on %s (JobType: %s, JobId: %s, Utilization: {%s}, OperationId: %s)",
                ~job->GetNode()->GetAddress(),
                ~job->GetType().ToString(),
                ~job->GetId().ToString(),
                ~FormatResources(job->GetSpec()->resource_utilization()),
                ~job->GetOperation()->GetOperationId().ToString());

            RegisterJob(job);
            MasterConnector->CreateJobNode(job);
            job->SetSpec(NULL);
        }

        context->Reply();
    }

    TJobPtr ProcessJobHeartbeat(
        NProto::TReqHeartbeat* request,
        NProto::TRspHeartbeat* response,
        NProto::TJobStatus* jobStatus)
    {
        auto address = request->address();
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
            if (state == EJobState::Completed || state == EJobState::Failed) {
                *response->add_jobs_to_remove() = jobId.ToProto();
                LOG_WARNING("Job status report was expected from %s, removal scheduled",
                    ~expectedAddress);
            } else {
                *response->add_jobs_to_remove() = jobId.ToProto();
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

            case EJobState::Failed:
                LOG_INFO("Job failed, removal scheduled\n%s",
                    ~TError::FromProto(jobStatus->result().error()).ToString());
                OnJobFailed(job, jobStatus->mutable_result());
                *response->add_jobs_to_remove() = jobId.ToProto();
                break;

            case EJobState::Aborted:
                LOG_WARNING("Job has aborted unexpectedly, removal scheduled");
                OnJobFailed(job, TError("Job has aborted unexpectedly"));
                *response->add_jobs_to_remove() = jobId.ToProto();
                break;

            case EJobState::Running:
                LOG_DEBUG("Job is running");
                OnJobRunning(job);
                break;

            case EJobState::Aborting:
                LOG_WARNING("Job has started aborting unexpectedly");
                OnJobFailed(job, TError("Job has aborted unexpectedly"));
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

NYTree::TYPathServiceProducer TScheduler::CreateOrchidProducer()
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

