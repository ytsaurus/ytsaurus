#pragma once

#include "private.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/actions/signal.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TOperationReport
{
    TOperationPtr Operation;
    TControllerTransactionsPtr ControllerTransactions;
    bool UserTransactionAborted = false;
};

//! Information retrieved during scheduler-master handshake.
struct TMasterHandshakeResult
{
    std::vector<TOperationReport> OperationReports;
};

typedef TCallback<void(NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr)> TWatcherRequester;
typedef TCallback<void(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr)> TWatcherHandler;

//! Mediates communication between scheduler and master.
class TMasterConnector
{
public:
    TMasterConnector(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);
    ~TMasterConnector();

    void Start();

    IInvokerPtr GetCancelableControlInvoker() const;

    TControllersMasterConnectorPtr GetControllersMasterConnector() const;

    bool IsConnected() const;

    TFuture<void> CreateOperationNode(TOperationPtr operation, const TOperationControllerInitializeResult& initializeResult);
    TFuture<void> ResetRevivingOperationNode(TOperationPtr operation);
    TFuture<void> FlushOperationNode(TOperationPtr operation);

    void SetSchedulerAlert(EAlertType alertType, const TError& alert);

    void AddGlobalWatcherRequester(TWatcherRequester requester);
    void AddGlobalWatcherHandler(TWatcherHandler handler);

    void AddOperationWatcherRequester(TOperationPtr operation, TWatcherRequester requester);
    void AddOperationWatcherHandler(TOperationPtr operation, TWatcherHandler handler);

    void UpdateConfig(const TSchedulerConfigPtr& config);

    DECLARE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DECLARE_SIGNAL(void(), MasterDisconnected);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
