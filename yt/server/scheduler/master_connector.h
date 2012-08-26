#pragma once

#include "private.h"

#include <ytlib/actions/signal.h>
#include <ytlib/ytree/public.h>
#include <server/cell_scheduler/public.h>
#include <ytlib/object_client/object_service_proxy.h>
#include <server/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
{
public:
    TMasterConnector(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);
    ~TMasterConnector();

    void Start();

    std::vector<TOperationPtr> LoadOperations();

    TAsyncError CreateOperationNode(TOperationPtr operation);
    void ReviveOperationNodes(const std::vector<TOperationPtr> operations);
    void RemoveOperationNode(TOperationPtr operation);
    TAsyncError FlushOperationNode(TOperationPtr operation);
    TAsyncError FinalizeOperationNode(TOperationPtr operation);

    void CreateJobNode(TJobPtr job);
    void UpdateJobNode(TJobPtr job);
    void SetJobStdErr(TJobPtr job, const NChunkClient::TChunkId& chunkId);

    DECLARE_SIGNAL(void(TOperationPtr operation), PrimaryTransactionAborted);
    DECLARE_SIGNAL(void(const Stroka& address), NodeOnline);
    DECLARE_SIGNAL(void(const Stroka& address), NodeOffline);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
