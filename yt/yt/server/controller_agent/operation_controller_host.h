#pragma once

#include "operation_controller.h"

#include <yt/yt/server/lib/scheduler/message_queue.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TAgentToSchedulerOperationEvent
{
    TAgentToSchedulerOperationEvent(
        NScheduler::EAgentToSchedulerOperationEventType eventType,
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error = TError());

    static TAgentToSchedulerOperationEvent CreateCompletedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch);
    static TAgentToSchedulerOperationEvent CreateSuspendedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error);
    static TAgentToSchedulerOperationEvent CreateFailedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error);
    static TAgentToSchedulerOperationEvent CreateAbortedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error);
    static TAgentToSchedulerOperationEvent CreateBannedInTentativeTreeEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TString treeId,
        std::vector<TAllocationId> allocationIds);
    static TAgentToSchedulerOperationEvent CreateHeavyControllerActionFinishedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error,
        std::optional<TOperationControllerInitializeResult> maybeResult);
    static TAgentToSchedulerOperationEvent CreateHeavyControllerActionFinishedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error,
        std::optional<TOperationControllerPrepareResult> maybeResult);
    static TAgentToSchedulerOperationEvent CreateHeavyControllerActionFinishedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error,
        std::optional<TOperationControllerMaterializeResult> maybeResult);
    static TAgentToSchedulerOperationEvent CreateHeavyControllerActionFinishedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error,
        std::optional<TOperationControllerReviveResult> maybeResult);
    static TAgentToSchedulerOperationEvent CreateHeavyControllerActionFinishedEvent(
        TOperationId operationId,
        NScheduler::TControllerEpoch controllerEpoch,
        TError error,
        std::optional<TOperationControllerCommitResult> maybeResult);

    NScheduler::EAgentToSchedulerOperationEventType EventType;
    TOperationId OperationId;
    NScheduler::TControllerEpoch ControllerEpoch;
    TError Error;
    TString TentativeTreeId;
    std::vector<TAllocationId> TentativeTreeAllocationIds;
    std::optional<TOperationControllerInitializeResult> InitializeResult;
    std::optional<TOperationControllerPrepareResult> PrepareResult;
    std::optional<TOperationControllerMaterializeResult> MaterializeResult;
    std::optional<TOperationControllerReviveResult> ReviveResult;
    std::optional<TOperationControllerCommitResult> CommitResult;
};

using TAgentToSchedulerOperationEventOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>>;
using TAgentToSchedulerRunningAllocationStatisticsOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerRunningAllocationStatistics>>;

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerHost
    : public IOperationControllerHost
{
public:
    TOperationControllerHost(
        TOperation* operation,
        IInvokerPtr cancelableControlInvoker,
        IInvokerPtr uncancelableControlInvoker,
        TAgentToSchedulerOperationEventOutboxPtr operationEventsOutbox,
        TAgentToSchedulerRunningAllocationStatisticsOutboxPtr runningAllocationStatisticsUpdatesOutbox,
        TBootstrap* bootstrap);

    void SetJobTrackerOperationHandler(TJobTrackerOperationHandlerPtr jobTrackerOperationHandler);
    const TJobTrackerOperationHandlerPtr& GetJobTrackerOperationHandler() const override;

    void Disconnect(const TError& error) override;

    void InterruptJob(TJobId jobId, EInterruptReason reason, TDuration timeout) override;
    void RequestJobGracefulAbort(TJobId jobId, EAbortReason reason) override;
    void UpdateRunningAllocationsStatistics(std::vector<TAgentToSchedulerRunningAllocationStatistics> runningAllocationStatisticsUpdates) override;

    void RegisterJob(TStartedJobInfo jobInfo) override;
    void ReviveJobs(std::vector<TStartedJobInfo> jobs) override;
    void ReleaseJobs(std::vector<TJobToRelease> jobs) override;
    void AbortJob(
        TJobId jobId,
        NScheduler::EAbortReason abortReason) override;

    std::optional<TJobMonitoringDescriptor> TryAcquireJobMonitoringDescriptor(TOperationId operationId) override;
    bool ReleaseJobMonitoringDescriptor(TOperationId operationId, TJobMonitoringDescriptor descriptor) override;

    TFuture<TOperationSnapshot> DownloadSnapshot() override;
    TFuture<void> RemoveSnapshot() override;

    TFuture<void> FlushOperationNode() override;
    TFuture<void> UpdateInitializedOperationNode(bool isCleanOperationStart) override;

    TFuture<void> UpdateControllerFeatures(const NYson::TYsonString& featureYson) override;

    TFuture<void> AttachChunkTreesToLivePreview(
        NTransactionClient::TTransactionId transactionId,
        NCypressClient::TNodeId tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds) override;
    void AddChunkTreesToUnstageList(
        const std::vector<NChunkClient::TChunkId>& chunkTreeIds,
        bool recursive) override;

    const NApi::NNative::IClientPtr& GetClient() override;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() override;
    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() override;
    const IInvokerPtr& GetControllerThreadPoolInvoker() override;
    const IInvokerPtr& GetJobSpecBuildPoolInvoker() override;
    const IInvokerPtr& GetStatisticsOffloadInvoker() override;
    const IInvokerPtr& GetExecNodesUpdateInvoker() override;
    const IInvokerPtr& GetConnectionInvoker() override;
    const NEventLog::IEventLogWriterPtr& GetEventLogWriter() override;
    const NCoreDump::ICoreDumperPtr& GetCoreDumper() override;
    const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() override;
    const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() override;
    const TJobReporterPtr& GetJobReporter() override;
    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override;

    TJobProfiler* GetJobProfiler() const override;

    int GetAvailableExecNodeCount() override;
    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter, bool onlineOnly = false) override;
    TJobResources GetMaxAvailableResources(const NScheduler::TSchedulingTagFilter& filter) override;

    TInstant GetConnectionTime() override;
    TIncarnationId GetIncarnationId() override;

    void OnOperationCompleted() override;
    void OnOperationAborted(const TError& error) override;
    void OnOperationFailed(const TError& error) override;
    void OnOperationSuspended(const TError& error) override;
    void OnOperationBannedInTentativeTree(
        const TString& treeId,
        const std::vector<TAllocationId>& allocationIds) override;

    void ValidateOperationAccess(
        const TString& user,
        NYTree::EPermission permission) override;

    TFuture<void> UpdateAccountResourceUsageLease(
        NSecurityClient::TAccountResourceUsageLeaseId leaseId,
        const NScheduler::TDiskQuota& diskQuota) override;

private:
    const TOperationId OperationId_;
    const IInvokerPtr CancelableControlInvoker_;
    const IInvokerPtr UncancelableControlInvoker_;
    TJobTrackerOperationHandlerPtr JobTrackerOperationHandler_;
    const TAgentToSchedulerOperationEventOutboxPtr OperationEventsOutbox_;
    const TAgentToSchedulerRunningAllocationStatisticsOutboxPtr RunningAllocationStatisticsUpdatesOutbox_;
    TBootstrap* const Bootstrap_;
    const TIncarnationId IncarnationId_;
    const NScheduler::TControllerEpoch ControllerEpoch_;
};

DEFINE_REFCOUNTED_TYPE(TOperationControllerHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

