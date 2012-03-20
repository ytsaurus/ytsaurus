#pragma once

#include "public.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/error.h>
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
        NCellScheduler::TCellSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    void Start();

    TFuture< TValueOrError<TOperationPtr> >::TPtr StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec);

private:
    NCellScheduler::TCellSchedulerConfigPtr Config;
    NCellScheduler::TBootstrap* Bootstrap;

    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NCypress::TCypressServiceProxy CypressProxy;

    NTransactionClient::ITransaction::TPtr BootstrapTransaction;

    void Register();
    
    TValueOrError<TOperationPtr> OnOperationNodeCreated(
        NYTree::TYPathProxy::TRspSet::TPtr rsp,
        TOperationPtr operation);

    NYTree::TYPath GetOperationPath(const TOperationId& id);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

