#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

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

    explicit TJobTracker(TBootstrap* bootstrap);

    TFuture<void> Initialize();
    void OnSchedulerConnected(TIncarnationId incarnationId);
    void Cleanup();

    void ProcessHeartbeat(const TCtxHeartbeatPtr& context);

    TJobTrackerOperationHandlerPtr RegisterOperation(
        TOperationId operationId,
        bool controlJobLifetimeAtControllerAgent,
        TWeakPtr<IOperationController> operationController);

    void UnregisterOperation(TOperationId operationId);

    void UpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes);

    void UpdateConfig(const TControllerAgentConfigPtr& config);

    NYTree::IYPathServicePtr GetOrchidService() const;

private:
    TBootstrap* const Bootstrap_;

    TJobTrackerConfigPtr Config_;

    EOperationControllerQueue JobEventsControllerQueue_;

    NProfiling::TCounter HeartbeatStatisticsBytes_;
    NProfiling::TCounter HeartbeatDataStatisticsBytes_;
    NProfiling::TCounter HeartbeatJobResultBytes_;
    NProfiling::TCounter HeartbeatProtoMessageBytes_;
    NProfiling::TGauge HeartbeatEnqueuedControllerEvents_;
    std::atomic<i64> EnqueuedControllerEventCount_ = 0;
    NProfiling::TCounter HeartbeatCount_;

    NConcurrency::TActionQueuePtr JobTrackerQueue_;

    TAtomicIntrusivePtr<IInvoker> CancelableInvoker_;

    TIncarnationId IncarnationId_;

    // Used only for logging
    TRefCountedExecNodeDescriptorMapPtr ExecNodes_;

    using TNodeId = NNodeTrackerClient::TNodeId;

    struct TOperationInfo
    {
        const bool ControlJobLifetimeAtControllerAgent;
        bool JobsReady = false;
        const TWeakPtr<IOperationController> OperationController;
        THashSet<TJobId> TrackedJobIds;
    };


    struct TInterruptionRequestOptions
    {
        EInterruptReason Reason;
        TDuration Timeout;
    };

    struct TFailureRequestOptions { };

    using TRequestedActionInfo = std::variant<
        std::monostate,
        TInterruptionRequestOptions,
        TFailureRequestOptions>;

    struct TFinishedJobStatus { };
    struct TRunningJobStatus
    {
        TRequestedActionInfo RequestedActionInfo;
        TInstant VanishedSince;
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
    };

    struct TNodeInfo
    {
        TNodeJobs Jobs;
        NConcurrency::TLease Lease;

        TString NodeAddress;
    };

    THashMap<TNodeId, TNodeInfo> RegisteredNodes_;
    THashMap<TString, TNodeId> NodeAddressToNodeId_;

    THashMap<TOperationId, TOperationInfo> RegisteredOperations_;

    NYTree::IYPathServicePtr OrchidService_;

    bool IsJobRunning(const TJobInfo& jobInfo) const;

    bool HandleJobInfo(
        TJobInfo& jobInfo,
        TCtxHeartbeat::TTypedResponse* response,
        TJobId jobId,
        EJobStage newJobStage,
        const NLogging::TLogger& Logger);

    void HandleRunningJobInfo(
        TJobInfo& jobInfo,
        TCtxHeartbeat::TTypedResponse* response,
        const TRunningJobStatus& jobStatus,
        TJobId jobId,
        EJobStage newJobStage,
        const NLogging::TLogger& Logger);
    void HandleFinishedJobInfo(
        TJobInfo& jobInfo,
        TCtxHeartbeat::TTypedResponse* response,
        const TFinishedJobStatus& jobStatus,
        TJobId jobId,
        EJobStage newJobStage,
        const NLogging::TLogger& Logger);

    void ProcessInterruptionRequest(
        TJobTracker::TCtxHeartbeat::TTypedResponse* response,
        const TInterruptionRequestOptions& requestOptions,
        TJobId jobId,
        const NLogging::TLogger& Logger);

    void ProcessFailureRequest(
        TJobTracker::TCtxHeartbeat::TTypedResponse* response,
        const TFailureRequestOptions& requestOptions,
        TJobId jobId,
        const NLogging::TLogger& Logger);

    IInvokerPtr GetInvoker() const;
    IInvokerPtr TryGetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvokerOrThrow() const;

    NYTree::IYPathServicePtr CreateOrchidService() const;

    void DoUpdateConfig(const TControllerAgentConfigPtr& config);

    void DoUpdateExecNodes(TRefCountedExecNodeDescriptorMapPtr newExecNodes);

    void ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request);
    void AccountEnqueuedControllerEvent(int delta);

    void DoRegisterOperation(
        TOperationId operationId,
        bool controlJobLifetimeAtControllerAgent,
        TWeakPtr<IOperationController> operationController);
    void DoUnregisterOperation(TOperationId operationId);

    void DoRegisterJob(TStartedJobInfo jobInfo, TOperationId operationId);

    void DoReviveJobs(
        TOperationId operationId,
        std::vector<TStartedJobInfo> jobs);

    void DoReleaseJobs(
        TOperationId operationId,
        const std::vector<TJobToRelease>& jobs);

    void DoAbortJobOnNode(TJobId jobId, TOperationId operationId, EAbortReason reason);

    template <class TAction>
    void TryRequestJobAction(
        TJobId jobId,
        TOperationId operationId,
        TAction action,
        TStringBuf actionName);

    void TryInterruptJob(
        TJobId jobId,
        TOperationId operationId,
        EInterruptReason reason,
        TDuration timeout);
    void DoInterruptJob(
        TRequestedActionInfo& requestedActionInfo,
        TJobId jobId,
        TOperationId operationId,
        EInterruptReason reason,
        TDuration timeout);

    void TryFailJob(
        TJobId jobId,
        TOperationId operationId);
    void DoFailJob(
        TRequestedActionInfo& requestedActionInfo,
        TJobId jobId,
        TOperationId operationId);

    TNodeInfo& GetOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);
    TNodeInfo& RegisterNode(TNodeId nodeId, TString nodeAddress);
    TNodeInfo& UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);
    void UnregisterNode(TNodeId nodeId, const TString& nodeAddress);

    TNodeInfo* FindNodeInfo(TNodeId nodeId);

    void OnNodeHeartbeatLeaseExpired(TNodeId nodeId, const TString& nodeAddress);

    const TString& GetNodeAddressForLogging(TNodeId nodeId);

    using TOperationIdToJobIds = THashMap<TOperationId, std::vector<TJobId>>;
    void AbortJobs(TOperationIdToJobIds operationIdToJobIds, EAbortReason abortReason) const;

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
        TOperationId operationId,
        bool controlJobLifetimeAtControllerAgent);

    void RegisterJob(TStartedJobInfo jobInfo);

    void ReviveJobs(std::vector<TStartedJobInfo> jobs);

    void ReleaseJobs(std::vector<TJobToRelease> jobs);

    void AbortJobOnNode(
        TJobId jobId,
        EAbortReason reason);

    void InterruptJob(
        TJobId jobId,
        EInterruptReason reason,
        TDuration timeout);

    void FailJob(TJobId jobId);

private:
    TJobTracker* const JobTracker_;
    const IInvokerPtr CancelableInvoker_;

    const TOperationId OperationId_;
    const bool ControlJobLifetimeAtControllerAgent_;
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerOperationHandler)

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
