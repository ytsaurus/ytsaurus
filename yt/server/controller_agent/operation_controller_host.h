#pragma once

#include "operation_controller.h"

#include <yt/server/scheduler/message_queue.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TAgentToSchedulerOperationEvent
{
    NScheduler::EAgentToSchedulerOperationEventType EventType;
    TOperationId OperationId;
    TError Error;
    TString TentativeTreeId;
    std::vector<TJobId> TentativeTreeJobIds;
};

struct TAgentToSchedulerJobEvent
{
    NScheduler::EAgentToSchedulerJobEventType EventType;
    TJobId JobId;
    TError Error;
    std::optional<EInterruptReason> InterruptReason;
    std::optional<bool> ArchiveJobSpec;
    std::optional<bool> ArchiveStderr;
    std::optional<bool> ArchiveFailContext;
    std::optional<bool> ArchiveProfile;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerHost
    : public IOperationControllerHost
{
public:
    TOperationControllerHost(
        TOperation* operation,
        IInvokerPtr cancelableControlInvoker,
        TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> operationEventsOutbox,
        TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>> jobEventsOutbox,
        TBootstrap* bootstrap);

    virtual void InterruptJob(const TJobId& jobId, EInterruptReason reason) override;
    virtual void AbortJob(const TJobId& jobId, const TError& error) override;
    virtual void FailJob(const TJobId& jobId) override;
    virtual void ReleaseJobs(const std::vector<NScheduler::TJobToRelease>& TJobToRelease) override;

    virtual TFuture<TOperationSnapshot> DownloadSnapshot() override;
    virtual TFuture<void> RemoveSnapshot() override;

    virtual TFuture<void> FlushOperationNode() override;
    virtual TFuture<void> UpdateInitializedOperationNode() override;
    virtual void CreateJobNode(const TCreateJobNodeRequest& request) override;

    virtual TFuture<void> AttachChunkTreesToLivePreview(
        const NTransactionClient::TTransactionId& transactionId,
        const NCypressClient::TNodeId& tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds) override;
    virtual void AddChunkTreesToUnstageList(
        const std::vector<NChunkClient::TChunkId>& chunkTreeIds,
        bool recursive) override;

    virtual const NApi::NNative::IClientPtr& GetClient() override;
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() override;
    virtual const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() override;
    virtual const IInvokerPtr& GetControllerThreadPoolInvoker() override;
    virtual const NEventLog::TEventLogWriterPtr& GetEventLogWriter() override;
    virtual const ICoreDumperPtr& GetCoreDumper() override;
    virtual const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() override;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() override;
    virtual TMemoryTagQueue* GetMemoryTagQueue() override;

    virtual int GetExecNodeCount() override;
    virtual TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter) override;

    virtual TInstant GetConnectionTime() override;
    virtual const TIncarnationId& GetIncarnationId() override;

    virtual void OnOperationCompleted() override;
    virtual void OnOperationAborted(const TError& error) override;
    virtual void OnOperationFailed(const TError& error) override;
    virtual void OnOperationSuspended(const TError& error) override;
    virtual void OnOperationBannedInTentativeTree(const TString& treeId, const std::vector<TJobId>& jobIds) override;

    virtual void ValidateOperationAccess(
        const TString& user,
        NScheduler::EAccessType accessType) override;

private:
    const TOperationId OperationId_;
    const IInvokerPtr CancelableControlInvoker_;
    const TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>> OperationEventsOutbox_;
    const TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>> JobEventsOutbox_;
    TBootstrap* const Bootstrap_;
    const TIncarnationId IncarnationId_;
};

DEFINE_REFCOUNTED_TYPE(TOperationControllerHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

