#pragma once

#include "private.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TCreateJobNodeRequest
{
    TOperationId OperationId;
    TJobId JobId;
    NYson::TYsonString Attributes;
    NChunkClient::TChunkId StderrChunkId;
    NChunkClient::TChunkId FailContextChunkId;
    TFuture<NYson::TYsonString> InputPathsFuture;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationReport
{
    TOperationPtr Operation;
    TControllerTransactionsPtr ControllerTransactions;
    bool UserTransactionAborted = false;
    bool IsCommitted = false;
};

//! Information retrieved during scheduler-master handshake.
struct TMasterHandshakeResult
{
    std::vector<TOperationReport> OperationReports;
};

struct TOperationSnapshot
{
    int Version = -1;
    TSharedRef Data;
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

    bool IsConnected() const;

    void Disconnect();

    TFuture<void> CreateOperationNode(TOperationPtr operation);
    TFuture<void> ResetRevivingOperationNode(TOperationPtr operation);
    TFuture<void> FlushOperationNode(TOperationPtr operation);

    TFuture<TOperationSnapshot> DownloadSnapshot(const TOperationId& operationId);
    TFuture<void> RemoveSnapshot(const TOperationId& operationId);

    void CreateJobNode(const TCreateJobNodeRequest& createJobNodeRequest);

    void RegisterAlert(EAlertType alertType, const TError& alert);
    void UnregisterAlert(EAlertType alertType);

    void AttachJobContext(
        const NYPath::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);

    TFuture<void> AttachToLivePreview(
        const TOperationId& operationId,
        const NObjectClient::TTransactionId& transactionId,
        const NCypressClient::TNodeId& tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds);

    void AddGlobalWatcherRequester(TWatcherRequester requester);
    void AddGlobalWatcherHandler(TWatcherHandler handler);

    void AddOperationWatcherRequester(TOperationPtr operation, TWatcherRequester requester);
    void AddOperationWatcherHandler(TOperationPtr operation, TWatcherHandler handler);

    void UpdateConfig(const TSchedulerConfigPtr& config);

    DECLARE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DECLARE_SIGNAL(void(), MasterDisconnected);

    DECLARE_SIGNAL(void(TOperationPtr operation), UserTransactionAborted);
    DECLARE_SIGNAL(void(TOperationPtr operation), SchedulerTransactionAborted);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
