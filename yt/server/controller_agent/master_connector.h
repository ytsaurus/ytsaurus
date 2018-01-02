#pragma once

#include "private.h"

#include <yt/server/cell_scheduler/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NControllerAgent {

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

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public TRefCounted
{
public:
    TMasterConnector(
        IInvokerPtr invoker,
        TControllerAgentConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    const IInvokerPtr& GetInvoker() const;

    void RegisterOperation(
        const TOperationId& operationId,
        NScheduler::EOperationCypressStorageMode storageMode,
        const IOperationControllerPtr& controller);
    
    void UnregisterOperation(const TOperationId& operationId);

    void CreateJobNode(TCreateJobNodeRequest createJobNodeRequest);

    TFuture<void> FlushOperationNode(const TOperationId& operationId);

    TFuture<void> AttachToLivePreview(
        const TOperationId& operationId,
        const NObjectClient::TTransactionId& transactionId,
        const std::vector<NCypressClient::TNodeId>& tableIds,
        const std::vector<NChunkClient::TChunkTreeId>& childIds);

    TFuture<TOperationSnapshot> DownloadSnapshot(const TOperationId& operationId);
    TFuture<void> RemoveSnapshot(const TOperationId& operationId);

    void AddChunkTreesToUnstageList(std::vector<NChunkClient::TChunkTreeId> chunkTreeIds, bool recursive);

    void AttachJobContext(
        const NYPath::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId);

    void UpdateConfig(const TControllerAgentConfigPtr& config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

