#pragma once

#include "private.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/ytlib/cypress_client/public.h>

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
};

struct TOperationSnapshot
{
    int Version = -1;
    TSharedRef Data;
};

////////////////////////////////////////////////////////////////////

class TControllersMasterConnector
    : public TRefCounted
{
public:
    TControllersMasterConnector(
        IInvokerPtr invoker,
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    const IInvokerPtr& GetInvoker() const;

    void RegisterOperation(
        const TOperationId& operationId,
        const IOperationControllerPtr& controller);
    
    void UnregisterOperation(const TOperationId& operationId);

    void CreateJobNode(TCreateJobNodeRequest createJobNodeRequest);

    TFuture<void> FlushOperationNode(const TOperationId& operationId);

    TFuture<void> AttachToLivePreview(
        const TOperationId& operationId,
        const NObjectClient::TTransactionId& transactionId,
        const NCypressClient::TNodeId& tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds);

    TFuture<TOperationSnapshot> DownloadSnapshot(const TOperationId& operationId);
    TFuture<void> RemoveSnapshot(const TOperationId& operationId);

    void AttachJobContext(
        const NYPath::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);

    void UpdateConfig(const TSchedulerConfigPtr& config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TControllersMasterConnector)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

