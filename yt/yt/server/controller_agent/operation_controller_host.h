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
        std::vector<TJobId> jobIds);
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
    std::vector<TJobId> TentativeTreeJobIds;
    std::optional<TOperationControllerInitializeResult> InitializeResult;
    std::optional<TOperationControllerPrepareResult> PrepareResult;
    std::optional<TOperationControllerMaterializeResult> MaterializeResult;
    std::optional<TOperationControllerReviveResult> ReviveResult;
    std::optional<TOperationControllerCommitResult> CommitResult;
};

// TODO(eshcherbin): Add static CreateXXXEvent methods as in TAgentToSchedulerOperationEvent.
struct TAgentToSchedulerJobEvent
{
    NScheduler::EAgentToSchedulerJobEventType EventType;
    TJobId JobId;
    NScheduler::TControllerEpoch ControllerEpoch;
    TError Error;
    std::optional<EInterruptReason> InterruptReason;
    std::optional<TReleaseJobFlags> ReleaseFlags;
};

using TAgentToSchedulerJobEventOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerJobEvent>>;
using TAgentToSchedulerOperationEventOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerOperationEvent>>;
using TAgentToSchedulerRunningJobStatisticsOutboxPtr = TIntrusivePtr<NScheduler::TMessageQueueOutbox<TAgentToSchedulerRunningJobStatistics>>;

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
        TAgentToSchedulerJobEventOutboxPtr jobEventsOutbox,
        TAgentToSchedulerRunningJobStatisticsOutboxPtr runningJobStatisticsUpdatesOutbox,
        TBootstrap* bootstrap);

    void SetJobTrackerOperationHandler(TJobTrackerOperationHandlerPtr jobTrackerOperationHandler);

    void Disconnect(const TError& error) override;

    void InterruptJob(TJobId jobId, EInterruptReason reason) override;
    void AbortJob(TJobId jobId, const TError& error) override;
    void FailJob(TJobId jobId) override;
    void UpdateRunningJobsStatistics(std::vector<TAgentToSchedulerRunningJobStatistics> runningJobStatisticsUpdates) override;

    void RegisterJob(TStartedJobInfo jobInfo) override;
    void ReviveJobs(std::vector<TStartedJobInfo> jobs) override;
    void ReleaseJobs(std::vector<TJobToRelease> jobs) override;
    void AbortJobOnNode(
        TJobId jobId,
        NScheduler::EAbortReason abortReason) override;

    std::optional<TString> RegisterJobForMonitoring(TOperationId operationId, TJobId jobId) override;
    bool UnregisterJobForMonitoring(TOperationId operationId, TJobId jobId) override;

    TFuture<TOperationSnapshot> DownloadSnapshot() override;
    TFuture<void> RemoveSnapshot() override;

    TFuture<void> FlushOperationNode() override;
    TFuture<void> UpdateInitializedOperationNode(bool isCleanOperationStart) override;

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
    const NJobAgent::TJobReporterPtr& GetJobReporter() override;
    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() override;
    TMemoryTagQueue* GetMemoryTagQueue() override;

    TJobProfiler* GetJobProfiler() const override;

    TJobTracker* GetJobTracker() const override;

    int GetOnlineExecNodeCount() override;
    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter, bool onlineOnly = false) override;
    TJobResources GetMaxAvailableResources(const NScheduler::TSchedulingTagFilter& filter) override;

    TInstant GetConnectionTime() override;
    TIncarnationId GetIncarnationId() override;

    void OnOperationCompleted() override;
    void OnOperationAborted(const TError& error) override;
    void OnOperationFailed(const TError& error) override;
    void OnOperationSuspended(const TError& error) override;
    void OnOperationBannedInTentativeTree(const TString& treeId, const std::vector<TJobId>& jobIds) override;

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
    const TAgentToSchedulerJobEventOutboxPtr JobEventsOutbox_;
    const TAgentToSchedulerRunningJobStatisticsOutboxPtr RunningJobStatisticsUpdatesOutbox_;
    TBootstrap* const Bootstrap_;
    const TIncarnationId IncarnationId_;
    const NScheduler::TControllerEpoch ControllerEpoch_;
};

DEFINE_REFCOUNTED_TYPE(TOperationControllerHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

