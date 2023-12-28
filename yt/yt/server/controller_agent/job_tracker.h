#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobStage,
    (Running)
    (Finished)
);

////////////////////////////////////////////////////////////////////

class TJobTracker
    : public TRefCounted
{
public:
    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NProto::TReqHeartbeat,
        NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;

    using TCtxSettleJob = NRpc::TTypedServiceContext<
        NProto::TReqSettleJob,
        NProto::TRspSettleJob>;
    using TCtxSettleJobPtr = TIntrusivePtr<TCtxSettleJob>;

    TJobTracker(TBootstrap* bootstrap, TJobReporterPtr jobReporter);

    TFuture<void> Initialize();
    void OnSchedulerConnected(TIncarnationId incarnationId);
    void Cleanup();

    void ProcessHeartbeat(const TCtxHeartbeatPtr& context);
    void SettleJob(const TCtxSettleJobPtr& context);

    TJobTrackerOperationHandlerPtr RegisterOperation(
        TOperationId operationId,
        TWeakPtr<IOperationController> operationController);
    void UnregisterOperation(TOperationId operationId);

    void UpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes);

    void UpdateConfig(const TControllerAgentConfigPtr& config);

    NYTree::IYPathServicePtr GetOrchidService() const;

private:
    TBootstrap* const Bootstrap_;

    TJobReporterPtr JobReporter_;

    TJobTrackerConfigPtr Config_;

    EOperationControllerQueue JobEventsControllerQueue_;

    NProfiling::TCounter HeartbeatStatisticsBytes_;
    NProfiling::TCounter HeartbeatDataStatisticsBytes_;
    NProfiling::TCounter HeartbeatJobResultBytes_;
    NProfiling::TCounter HeartbeatProtoMessageBytes_;
    NProfiling::TGauge HeartbeatEnqueuedControllerEvents_;
    std::atomic<i64> EnqueuedControllerEventCount_ = 0;
    NProfiling::TCounter HeartbeatCount_;
    NProfiling::TCounter ReceivedJobCount_;
    NProfiling::TCounter ReceivedUnknownOperationCount_;
    NProfiling::TCounter ReceivedRunningJobCount_;
    NProfiling::TCounter ReceivedStaleRunningJobCount_;
    NProfiling::TCounter ReceivedFinishedJobCount_;
    NProfiling::TCounter ReceivedDuplicatedFinishedJobCount_;
    NProfiling::TCounter ReceivedUnknownJobCount_;
    NProfiling::TCounter UnconfirmedJobCount_;
    NProfiling::TCounter ConfirmedJobCount_;
    NProfiling::TCounter DisappearedFromNodeJobAbortCount_;
    NProfiling::TCounter JobAbortRequestCount_;
    NProfiling::TCounter JobReleaseRequestCount_;
    NProfiling::TCounter JobInterruptionRequestCount_;
    NProfiling::TCounter JobFailureRequestCount_;
    NProfiling::TCounter NodeRegistrationCount_;
    NProfiling::TCounter NodeUnregistrationCount_;
    NProfiling::TCounter WrongIncarnationRequestCount_;

    NConcurrency::TActionQueuePtr JobTrackerQueue_;

    TAtomicIntrusivePtr<IInvoker> CancelableInvoker_;

    TIncarnationId IncarnationId_;

    // Used only for logging
    TRefCountedExecNodeDescriptorMapPtr ExecNodes_;

    using TNodeId = NNodeTrackerClient::TNodeId;

    struct TOperationInfo
    {
        bool JobsReady = false;
        const TWeakPtr<IOperationController> OperationController;
        THashSet<TJobId> TrackedJobIds;
    };

    struct TNoActionRequested { };

    struct TInterruptionRequestOptions
    {
        EInterruptReason Reason;
        TDuration Timeout;
    };

    struct TGracefulAbortRequestOptions
    {
        EAbortReason Reason;
    };

    using TRequestedActionInfo = std::variant<
        TNoActionRequested,
        TInterruptionRequestOptions,
        TGracefulAbortRequestOptions>;

    struct TFinishedJobStatus { };
    struct TRunningJobStatus
    {
        TRequestedActionInfo RequestedActionInfo;
        TInstant DisappearedFromNodeSince;
    };

    struct TJobInfo
    {
        using TJobStatus = std::variant<
            TRunningJobStatus,
            TFinishedJobStatus>;

        TJobStatus Status;
        const TOperationId OperationId;
    };

    struct TJobToConfirmInfo
    {
        TRequestedActionInfo RequestedActionInfo;
        const TOperationId OperationId;
    };

    struct TNodeJobs
    {
        THashMap<TJobId, TJobInfo> Jobs;
        THashMap<TJobId, TJobToConfirmInfo> JobsToConfirm;
        THashMap<TJobId, TReleaseJobFlags> JobsToRelease;
        THashMap<TJobId, EAbortReason> JobsToAbort;
        THashMap<TAllocationId, TAbortedAllocationSummary> AbortedAllocations;
    };

    struct TNodeInfo
    {
        TNodeJobs Jobs;
        NConcurrency::TLease Lease;

        TGuid RegistrationId;

        TString NodeAddress;
    };

    THashMap<TNodeId, TNodeInfo> RegisteredNodes_;
    THashMap<TString, TNodeId> NodeAddressToNodeId_;

    THashMap<TOperationId, TOperationInfo> RegisteredOperations_;

    NYTree::IYPathServicePtr OrchidService_;

    struct THeartbeatCounters
    {
        int UnknownOperationCount = 0;
        int RunningJobCount = 0;
        int StaleRunningJobCount = 0;
        int FinishedJobCount = 0;
        int DuplicatedFinishedJobCount = 0;
        int UnknownJobCount = 0;
        int UnconfirmedJobCount = 0;
        int ConfirmedJobCount = 0;
        int DisappearedFromNodeJobAbortCount = 0;
        int JobAbortRequestCount = 0;
        int JobReleaseRequestCount = 0;
        int JobInterruptionRequestCount = 0;
        int JobFailureRequestCount = 0;
    };

    IInvokerPtr GetInvoker() const;
    IInvokerPtr TryGetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvokerOrThrow() const;

    NYTree::IYPathServicePtr CreateOrchidService() const;

    void DoUpdateConfig(const TControllerAgentConfigPtr& config);

    void DoUpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes);

    void ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request);
    void AccountEnqueuedControllerEvent(int delta);
    void ProfileHeartbeatProperties(const THeartbeatCounters& heartbeatCounters);

    struct THeartbeatRequest
    {
        THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> GroupedJobSummaries;
        THashSet<TAllocationId> AllocationIdsRunningOnNode;
        std::vector<TJobId> UnconfirmedJobIds;
    };

    struct THeartbeatProcessingContext
    {
        TCtxHeartbeatPtr Context;
        NLogging::TLogger Logger;
        TString NodeAddress;
        TNodeId NodeId;
        TIncarnationId IncarnationId;
        THeartbeatRequest Request;
    };
    THeartbeatCounters DoProcessHeartbeat(
        THeartbeatProcessingContext heartbeatProcessingContext);

    bool IsJobRunning(const TJobInfo& jobInfo) const;

    struct TJobsToProcessInOperationController
    {
        std::vector<std::unique_ptr<TJobSummary>> JobSummaries;
        std::vector<TJobToAbort> JobsToAbort;
    };
    void HandleJobInfo(
        TJobInfo& jobInfo,
        TNodeJobs& nodeJobs,
        TOperationInfo& operationInfo,
        TJobsToProcessInOperationController& jobsToProcessInOperationController,
        TCtxHeartbeat::TTypedResponse* response,
        std::unique_ptr<TJobSummary> jobSummary,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters);

    void HandleRunningJobInfo(
        TJobInfo& jobInfo,
        TNodeJobs& nodeJobs,
        TOperationInfo& operationInfo,
        TJobsToProcessInOperationController& jobsToProcessInOperationController,
        TCtxHeartbeat::TTypedResponse* response,
        const TRunningJobStatus& jobStatus,
        std::unique_ptr<TJobSummary> jobSummary,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters);
    void HandleFinishedJobInfo(
        TJobInfo& jobInfo,
        TNodeJobs& nodeJobs,
        TOperationInfo& operationInfo,
        TJobsToProcessInOperationController& jobsToProcessInOperationController,
        TCtxHeartbeat::TTypedResponse* response,
        const TFinishedJobStatus& jobStatus,
        std::unique_ptr<TJobSummary> jobSummary,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters);

    void ProcessInterruptionRequest(
        TJobTracker::TCtxHeartbeat::TTypedResponse* response,
        const TInterruptionRequestOptions& requestOptions,
        TJobId jobId,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters);

    void ProcessGracefulAbortRequest(
        TJobTracker::TCtxHeartbeat::TTypedResponse* response,
        const TGracefulAbortRequestOptions& requestOptions,
        TJobId jobId,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters);

    void DoRegisterOperation(
        TOperationId operationId,
        TWeakPtr<IOperationController> operationController);
    void DoUnregisterOperation(TOperationId operationId);

    void DoRegisterJob(TStartedJobInfo jobInfo, TOperationId operationId);

    void DoReviveJobs(
        TOperationId operationId,
        std::vector<TStartedJobInfo> jobs);

    void DoReleaseJobs(
        TOperationId operationId,
        const std::vector<TJobToRelease>& jobs);

    void RequestJobAbortion(TJobId jobId, TOperationId operationId, EAbortReason reason);

    template <class TAction>
    void TryRequestJobAction(
        TJobId jobId,
        TOperationId operationId,
        TAction action,
        TStringBuf actionName);

    void RequestJobInterruption(
        TJobId jobId,
        TOperationId operationId,
        EInterruptReason reason,
        TDuration timeout);
    void DoRequestJobInterruption(
        TRequestedActionInfo& requestedActionInfo,
        TJobId jobId,
        TOperationId operationId,
        EInterruptReason reason,
        TDuration timeout);

    void RequestJobGracefulAbort(
        TJobId jobId,
        TOperationId operationId,
        EAbortReason reason);
    void DoRequestJobGracefulAbort(
        TRequestedActionInfo& requestedActionInfo,
        TJobId jobId,
        TOperationId operationId,
        EAbortReason reason);

    void ReportUnknownJobInArchive(
        TJobId jobId,
        TOperationId operationId,
        const TString& nodeAddress);

    TNodeInfo& GetOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);
    TNodeInfo& RegisterNode(TNodeId nodeId, TString nodeAddress);
    TNodeInfo& UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);
    void UnregisterNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        TGuid maybeRegistrationId = {});

    TNodeInfo* FindNodeInfo(TNodeId nodeId);

    void OnNodeHeartbeatLeaseExpired(
        TGuid registrationId,
        TNodeId nodeId,
        const TString& nodeAddress);

    void OnAllocationsAborted(
        TOperationId operationId,
        std::vector<TAbortedAllocationSummary> abortedAllocations);

    const TString& GetNodeAddressForLogging(TNodeId nodeId);

    using TOperationIdToJobsToAbort = THashMap<TOperationId, std::vector<TJobToAbort>>;
    void AbortJobs(TOperationIdToJobsToAbort jobsToAbort) const;

    void AbortUnconfirmedJobs(TOperationId operationId, std::vector<TJobId> jobs);

    void DoInitialize(IInvokerPtr cancelableInvoker);
    void SetIncarnationId(TIncarnationId incarnationId);
    void DoCleanup();

    friend class TJobTrackerOperationHandler;

    class TJobTrackerNodeOrchidService;
    friend class TJobTrackerNodeOrchidService;

    class TJobTrackerJobOrchidService;
    friend class TJobTrackerJobOrchidService;

    class TJobTrackerOperationOrchidService;
    friend class TJobTrackerOperationOrchidService;
};

DEFINE_REFCOUNTED_TYPE(TJobTracker)

////////////////////////////////////////////////////////////////////

class TJobTrackerOperationHandler
    : public TRefCounted
{
public:
    TJobTrackerOperationHandler(
        TJobTracker* jobTracker,
        IInvokerPtr cancelableInvoker,
        TOperationId operationId);

    void RegisterJob(TStartedJobInfo jobInfo);
    void ReviveJobs(std::vector<TStartedJobInfo> jobs);
    void ReleaseJobs(std::vector<TJobToRelease> jobs);

    void RequestJobAbortion(
        TJobId jobId,
        EAbortReason reason);
    void RequestJobInterruption(
        TJobId jobId,
        EInterruptReason reason,
        TDuration timeout);
    void RequestJobGracefulAbort(
        TJobId jobId,
        EAbortReason reason);

    void OnAllocationsAborted(std::vector<TAbortedAllocationSummary> abortedAllocations);

private:
    TJobTracker* const JobTracker_;
    const IInvokerPtr CancelableInvoker_;

    const TOperationId OperationId_;

    const NTracing::TTraceContextPtr TraceContext_;
    const NTracing::TTraceContextFinishGuard TraceContextFinishGuard_;
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerOperationHandler)

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
