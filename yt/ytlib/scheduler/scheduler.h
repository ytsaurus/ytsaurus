#pragma once

#include "public.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/cell_scheduler/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/cypress/cypress_service_proxy.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
{
public:
    TOperation(
        const TOperationId& operationId,
        EOperationType type,
        const TTransactionId& transactionId,
        NYTree::IMapNodePtr spec);

    TOperationId GetOperationId();
    EOperationType GetType() const;
    TTransactionId GetTransactionId() const;
    NYTree::IMapNodePtr GetSpec() const;

private:
    TOperationId OperationId;
    EOperationType Type;
    TTransactionId TransactionId;
    NYTree::IMapNodePtr Spec;

};

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    void Start();

    TFuture< TValueOrError<TOperationPtr> >::TPtr StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec);

    void AbortOperation(TOperationPtr operation);

private:
    TSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    NCypress::TCypressServiceProxy CypressProxy;

    NTransactionClient::ITransaction::TPtr BootstrapTransaction;

    TPeriodicInvoker::TPtr TransactionRefreshInvoker;
    TPeriodicInvoker::TPtr NodesRefreshInvoker;

    yhash_map<TOperationId, TOperationPtr> Operations;

    void RegisterOperation(TOperationPtr operation);
    void UnregisterOperation(TOperationPtr operation);

    void RegisterAtMaster();

    void StartRefresh();
    void RefreshTransactions();
    void OnTransactionsRefreshed(
        NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr rsp,
        std::vector<TTransactionId> transactionIds);
    void RefreshNodes();
    void OnNodesRefreshed(NYTree::TYPathProxy::TRspGet::TPtr rsp);
    
    TValueOrError<TOperationPtr> OnOperationNodeCreated(
        NYTree::TYPathProxy::TRspSet::TPtr rsp,
        TOperationPtr operation);

    NYTree::TYPath GetOperationPath(const TOperationId& id);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

