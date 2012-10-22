#pragma once

#include "private.h"

#include <ytlib/actions/signal.h>
#include <ytlib/ytree/public.h>

#include <ytlib/object_client/public.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <server/cell_scheduler/public.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Information retrieved during scheduler-master handshake.
struct TMasterHandshakeResult
{
    std::vector<TOperationPtr> Operations;
    NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr WatcherResponses;
};

//! Mediates communication between scheduler and master.
class TMasterConnector
{
public:
    TMasterConnector(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);
    ~TMasterConnector();

    void Start();

    bool IsConnected() const;

    TAsyncError CreateOperationNode(TOperationPtr operation);
    TFuture<void> FlushOperationNode(TOperationPtr operation);
    TFuture<void> FinalizeOperationNode(TOperationPtr operation);

    void CreateJobNode(TJobPtr job, const NChunkClient::TChunkId& chunkId);

    DECLARE_SIGNAL(void(NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr), WatcherRequest);
    DECLARE_SIGNAL(void(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr), WatcherResponse);

    DECLARE_SIGNAL(void(const TMasterHandshakeResult& result), MasterConnected);
    DECLARE_SIGNAL(void(), MasterDisconnected);

    DECLARE_SIGNAL(void(TOperationPtr operation), PrimaryTransactionAborted);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
