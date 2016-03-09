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

//! Information retrieved during scheduler-master handshake.
struct TMasterHandshakeResult
{
    std::vector<TOperationPtr> Operations;
    std::vector<TOperationPtr> RevivingOperations;
    std::vector<TOperationPtr> AbortingOperations;
    NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr WatcherResponses;
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

    TFuture<void> CreateOperationNode(TOperationPtr operation);
    TFuture<void> ResetRevivingOperationNode(TOperationPtr operation);
    TFuture<void> FlushOperationNode(TOperationPtr operation);

    TFuture<TSharedRef> DownloadSnapshot(const TOperationId& operationId);
    TFuture<void> RemoveSnapshot(const TOperationId& operationId);

    void CreateJobNode(TJobPtr job,
        const NChunkClient::TChunkId& stderrChunkId,
        const NChunkClient::TChunkId& failContextChunkId);

    void AttachJobContext(
        const NYPath::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        TJobPtr job);

    TFuture<void> AttachToLivePreview(
        TOperationPtr operation,
        const NCypressClient::TNodeId& tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childrenIds);

    void AddGlobalWatcherRequester(TWatcherRequester requester);
    void AddGlobalWatcherHandler(TWatcherHandler handler);

    void AddOperationWatcherRequester(TOperationPtr operation, TWatcherRequester requester);
    void AddOperationWatcherHandler(TOperationPtr operation, TWatcherHandler handler);

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
