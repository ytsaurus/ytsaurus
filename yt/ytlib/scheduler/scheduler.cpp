#include "stdafx.h"
#include "scheduler.h"
#include "private.h"
#include "config.h"
#include "exec_node.h"
#include "operation.h"
#include "job.h"
#include "scheduler_strategy.h"
#include "fifo_strategy.h"
#include "operation_controller.h"
#include "map_controller.h"
#include "scheduler_service_proxy.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/string.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/cypress/cypress_service_proxy.h>

#include <ytlib/cell_scheduler/config.h>
#include <ytlib/cell_scheduler/bootstrap.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/cypress/id.h>

#include <ytlib/object_server/object_ypath_proxy.h>

#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NCellScheduler;
using namespace NTransactionClient;
using namespace NCypress;
using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////

NLog::TLogger& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

class TScheduler::TImpl
    : public NRpc::TServiceBase
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : NRpc::TServiceBase(
            ~bootstrap->GetControlInvoker(),
            TSchedulerServiceProxy::GetServiceName(),
            SchedulerLogger.GetCategory())
        , Config(config)
        , Bootstrap(bootstrap)
        , CypressProxy(bootstrap->GetMasterChannel())
    {
        YASSERT(config);
        YASSERT(bootstrap);
        VERIFY_INVOKER_AFFINITY(GetControlInvoker(), ControlThread);
    }

    void Start()
    {
        InitStrategy();
        RegisterAtMaster();
        StartRefresh();
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    NCypress::TCypressServiceProxy CypressProxy;

    TAutoPtr<ISchedulerStrategy> Strategy;

    NTransactionClient::ITransaction::TPtr BootstrapTransaction;

    TPeriodicInvoker::TPtr TransactionRefreshInvoker;
    TPeriodicInvoker::TPtr NodesRefreshInvoker;

    yhash_map<Stroka, TExecNodePtr> Nodes;
    yhash_map<TOperationId, TOperationPtr> Operations;
    yhash_map<TJobId, TJobPtr> Jobs;


    typedef TValueOrError<TOperationPtr> TStartResult;

    TFuture< TStartResult >::TPtr StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Generate operation id.
        auto operationId = TOperationId::Create();

        LOG_INFO("Starting operation (Type: %s, OperationId: %s, TransactionId: %s)",
            ~type.ToString(),
            ~operationId.ToString(),
            ~transactionId.ToString());

        // Create operation object.
        auto operation = New<TOperation>(
            operationId,
            type,
            transactionId,
            spec,
            TInstant::Now());
        // The operation owns the controller but not vice versa.
        // Hence we use raw pointers inside controllers.
        operation->SetController(CreateController(operation.Get()));

        try {
            InitializeOperation(operation);
        } catch (const std::exception& ex) {
            return MakeFuture(TStartResult(TError(Sprintf("Operation cannot be started\n%s", ex.what()))));
        }

        // Create a node in Cypress that will represent the operation.
        auto setReq = TYPathProxy::Set(GetOperationPath(operationId));
        setReq->set_value(BuildYsonFluently()
            .WithAttributes().BeginMap()
            .EndMap()
            .BeginAttributes()
            .Item("type").Scalar(CamelCaseToUnderscoreCase(type.ToString()))
            .Item("transaction_id").Scalar(transactionId.ToString())
            .Item("spec").Node(spec)
            .EndAttributes());

        return CypressProxy.Execute(setReq)->Apply(
            FromMethod(
                &TImpl::OnOperationNodeCreated,
                this,
                operation)
            ->AsyncVia(GetControlInvoker()));
    }

    void InitializeOperation(TOperationPtr operation)
    {
        operation->GetController()->Initialize();
    }

    TValueOrError<TOperationPtr> OnOperationNodeCreated(
        NYTree::TYPathProxy::TRspSet::TPtr rsp,
        TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            auto error = rsp->GetError();
            LOG_ERROR("Error creating operation node\n%s", ~error.ToString());
            return error;
        }

        RegisterOperation(operation);

        LOG_INFO("Operation started (OperationId: %s)", 
            ~operation->GetOperationId().ToString());
        return operation;
    }


    void AbortOperation(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Aborting operation (OperationId: %s)",
            ~operation->GetOperationId().ToString());

        operation->GetController()->Abort();
        UnregisterOperation(operation);

        LOG_INFO("Operation aborted (OperationId: %s)",
            ~operation->GetOperationId().ToString());
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
        auto it = Nodes.find(address);
        return it == Nodes.end() ? NULL : it->second;
    }


    TJobPtr FindJob(const TJobId& jobId)
    {
        auto it = Jobs.find(jobId);
        return it == Jobs.end() ? NULL : it->second;
    }


    void RegisterNode(TExecNodePtr node)
    {
        YVERIFY(Nodes.insert(MakePair(node->GetAddress(), node)).second);    
    }

    void UnregisterNode(TExecNodePtr node)
    {
        YVERIFY(Nodes.erase(node->GetAddress()) == 1);
    }

    
    void RegisterOperation(TOperationPtr operation)
    {
        YVERIFY(Operations.insert(MakePair(operation->GetOperationId(), operation)).second);
        LOG_DEBUG("Operation registered (OperationId: %s)", ~operation->GetOperationId().ToString());
    }

    void UnregisterOperation(TOperationPtr operation)
    {
        FOREACH (auto job, operation->Jobs()) {
            UnregisterJob(job);
        }
        YVERIFY(Operations.erase(operation->GetOperationId()) == 1);
        LOG_DEBUG("Operation unregistered (OperationId: %s)", ~operation->GetOperationId().ToString());
    }

    
    void RegisterJob(TJobPtr job)
    {
        YVERIFY(Jobs.insert(MakePair(job->GetId(), job)).second);
        LOG_DEBUG("Job registered (JobId: %s, OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }

    void UnregisterJob(TJobPtr job)
    {
        YVERIFY(Jobs.erase(job->GetId()) == 1);
        LOG_DEBUG("Job unregistered (JobId: %s, OperationId: %s)",
            ~job->GetId().ToString(),
            ~job->GetOperation()->GetOperationId().ToString());
    }


    void InitStrategy()
    {
        // TODO(babenko): make configurable
        Strategy = CreateFifoStrategy();
    }

    void RegisterAtMaster()
    {
        // TODO(babenko): Currently we use succeed-or-die strategy. Add retries later.

        // Take the lock to prevent multiple instances of scheduler from running simultaneously.
        // To this aim, we create an auxiliary transaction that takes care of this lock.
        // We never commit or commit this transaction, so it gets aborted (and the lock gets released)
        // when the scheduler dies.
        try {
            BootstrapTransaction = Bootstrap->GetTransactionManager()->Start();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Failed to start bootstrap transaction\n%s", ex.what());
        }

        LOG_INFO("Taking lock");
        {
            auto req = TCypressYPathProxy::Lock(WithTransaction(
                "/sys/scheduler/lock",
                BootstrapTransaction->GetId()));
            req->set_mode(ELockMode::Exclusive);
            auto rsp = CypressProxy.Execute(req)->Get();
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to take scheduler lock, check for another running scheduler instances\n%s",
                    ~rsp->GetError().ToString());
            }
        }
        LOG_INFO("Lock taken");

        LOG_INFO("Publishing scheduler address");
        {
            auto req = TYPathProxy::Set("/sys/scheduler/runtime@address");
            req->set_value(SerializeToYson(Bootstrap->GetPeerAddress()));
            auto rsp = CypressProxy.Execute(req)->Get();
            if (!rsp->IsOK()) {
                ythrow yexception() << Sprintf("Failed to publish scheduler address\n%s",
                    ~rsp->GetError().ToString());
            }
        }
        LOG_INFO("Scheduler address published");
    }


    void StartRefresh()
    {
        TransactionRefreshInvoker = New<TPeriodicInvoker>(
            FromMethod(&TImpl::RefreshTransactions, this)
            ->Via(GetControlInvoker()),
            Config->TransactionsRefreshPeriod);
        TransactionRefreshInvoker->Start();

        NodesRefreshInvoker = New<TPeriodicInvoker>(
            FromMethod(&TImpl::RefreshNodes, this)
            ->Via(GetControlInvoker()),
            Config->NodesRefreshPeriod);
        NodesRefreshInvoker->Start();
    }


    void RefreshTransactions()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Check if any operations are running.
        if (Operations.empty())
            return;

        // Collect all transactions that are used by currently running operations.
        yhash_set<TTransactionId> transactionIds;
        FOREACH (const auto& pair, Operations) {
            transactionIds.insert(pair.second->GetTransactionId());
        }

        // Invoke GetId verbs for these transactions to see if they are alive.
        std::vector<TTransactionId> transactionIdsList;
        auto batchReq = CypressProxy.ExecuteBatch();
        FOREACH (const auto& id, transactionIds) {
            auto checkReq = TObjectYPathProxy::GetId(FromObjectId(id));
            transactionIdsList.push_back(id);
            batchReq->AddRequest(checkReq);
        }

        LOG_INFO("Refreshing %d transactions", batchReq->GetSize());
        batchReq->Invoke()->Subscribe(
            FromMethod(&TImpl::OnTransactionsRefreshed, this, transactionIdsList)
            ->Via(GetControlInvoker()));
    }

    void OnTransactionsRefreshed(
        NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr rsp,
        std::vector<TTransactionId> transactionIds)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing transactions\n%s", rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Transactions refreshed successfully");

        // Collect the list of dead transactions.
        yhash_set<TTransactionId> deadTransactionIds;
        for (int index = 0; index < rsp->GetSize(); ++index) {
            if (!rsp->GetResponse(index)->IsOK()) {
                YVERIFY(deadTransactionIds.insert(transactionIds[index]).second);
            }
        }

        // Collect the list of operations corresponding to dead transactions.
        std::vector<TOperationPtr> deadOperations;
        FOREACH (const auto& pair, Operations) {
            auto operation = pair.second;
            if (deadTransactionIds.find(operation->GetTransactionId()) != deadTransactionIds.end()) {
                deadOperations.push_back(operation);
            }
        }

        // Abort dead operations.
        FOREACH (auto operation, deadOperations) {
            LOG_INFO("Operation corresponds to a dead transaction, aborting (OperationId: %s, TransactionId: %s)",
                ~operation->GetOperationId().ToString(),
                ~operation->GetTransactionId().ToString());
            AbortOperation(operation);
        }
    }


    void RefreshNodes()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Get the list of online nodes from the master.
        LOG_INFO("Refreshing nodes");
        auto req = TYPathProxy::Get("/sys/holders@online");
        CypressProxy.Execute(req)->Subscribe(
            FromMethod(&TImpl::OnNodesRefreshed, this)
            ->Via(GetControlInvoker()));
    }

    void OnNodesRefreshed(NYTree::TYPathProxy::TRspGet::TPtr rsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!rsp->IsOK()) {
            LOG_ERROR("Error refreshing nodes\n%s", rsp->GetError().ToString());
            return;
        }

        LOG_INFO("Nodes refreshed successfully");

        // Examine the list of nodes returned by master and figure out the difference.

        yhash_set<TExecNodePtr> deadNodes;
        FOREACH (const auto& pair, Nodes) {
            YVERIFY(deadNodes.insert(pair.second).second);
        }

        auto onlineAddresses = DeserializeFromYson< yvector<Stroka> >(rsp->value());
        
        FOREACH (const auto& address, onlineAddresses) {
            auto node = FindNode(address);
            if (node) {
                YVERIFY(deadNodes.erase(node) == 1);
            } else {
                LOG_INFO("Node %s has become online", ~address.Quote());
                auto node = New<TExecNode>(address);
                RegisterNode(node);
            }
        }

        FOREACH (auto node, deadNodes) {
            LOG_INFO("Node %s has become offline", ~node->GetAddress().Quote());
            UnregisterNode(node);
        }
    }


    IInvoker::TPtr GetControlInvoker()
    {
        return Bootstrap->GetControlInvoker();
    }

    static NYTree::TYPath GetOperationPath(const TOperationId& id)
    {
        return CombineYPaths("/sys/scheduler/operations", id.ToString());
    }

    static TAutoPtr<IOperationController> CreateController(TOperation* operation)
    {
        // TODO(babenko): add more operation types
        switch (operation->GetType()) {
            case EOperationType::Map:
                return CreateMapController(operation);
                break;
            default:
                YUNREACHABLE();
        }
    }


    DECLARE_RPC_SERVICE_METHOD(NProto, StartOperation)
    {
        auto type = EOperationType(request->type());
        auto transactionId = TTransactionId::FromProto(request->transaction_id());
        auto spec = DeserializeFromYson(request->spec())->AsMap();

        context->SetRequestInfo("Type: %s, TransactionId: %s",
            ~type.ToString(),
            ~transactionId.ToString());

        StartOperation(
            type,
            transactionId,
            spec)
        ->Subscribe(FromFunctor([=] (TValueOrError<TOperationPtr> result) {
            if (!result.IsOK()) {
                context->Reply(result);
                return;
            }
            auto operation = result.Value();
            auto id = operation->GetOperationId();
            response->set_operation_id(id.ToProto());
            context->SetResponseInfo("OperationId: %s", ~id.ToString());
            context->Reply();
        }));
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation)
    {
        auto operationId = TTransactionId::FromProto(request->operation_id());

        context->SetRequestInfo("OperationId: %s", ~operationId.ToString());

        auto operation = GetOperation(operationId);
        AbortOperation(operation);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WaitForOperation)
    {
        // TODO(babenko): implement
        YUNIMPLEMENTED();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat)
    {
        auto address = request->address();

        context->SetRequestInfo("Address: %s, JobCount: %d, TotalSlotCount: %d, FreeSlotCount: %d",
            ~address,
            request->jobs_size(),
            request->total_slot_count(),
            request->free_slot_count());

        FOREACH (const auto& jobStatus, request->jobs()) {
            auto jobId = TJobId::FromProto(jobStatus.job_id());
            auto state = EJobState(jobStatus.state());
            auto job = FindJob(jobId);
            Stroka message;
            if (job) {
                switch (state) {
                    case EJobState::Completed:
                        message = "Job completed";

                        response->add_jobs_to_remove(jobId.ToProto());
                        break;

                    case EJobState::Failed:
                    case EJobState::Aborted:
                        LOG_INFO("Finished job reported by node, removal scheduled (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        response->add_jobs_to_remove(jobId.ToProto());
                        break;

                    case EJobState::Running:
                        LOG_WARNING("Unknown job reported by node, stop scheduled (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        response->add_jobs_to_stop(jobId.ToProto());
                        break;

                    case EJobState::Aborting:
                        LOG_DEBUG("Job abort in progress reported by node (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        break;

                    default:
                        YUNREACHABLE();
                }
            } else {
                switch (state) {
                    case EJobState::Completed:
                    case EJobState::Failed:
                    case EJobState::Aborted:
                        LOG_INFO("Finished job reported by node, removal scheduled (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        response->add_jobs_to_remove(jobId.ToProto());
                        break;

                    case EJobState::Running:
                        LOG_WARNING("Unknown job reported by node, stop scheduled (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        response->add_jobs_to_stop(jobId.ToProto());
                        break;

                    case EJobState::Aborting:
                        LOG_DEBUG("Job abort in progress reported by node (Address: %s, JobId: %s, State: %s)",
                            ~address,
                            ~jobId.ToString(),
                            ~state.ToString());
                        break;

                    default:
                        YUNREACHABLE();
                }
            }
            LOG_INFO("%s (Address: %s, JobId: %s, State: %s)",

                ~address,
                ~jobId.ToString(),
                ~state.ToString());
        }
    }

};

TScheduler::TScheduler(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

void TScheduler::Start()
{
    Impl->Start();
}

NRpc::IService::TPtr TScheduler::GetService()
{
    return Impl;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

