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
        THashSet<TJobId> TrackedJobs;
    };

    struct TJobInfo
    {
        EJobStage Stage;
        const TOperationId OperationId;
    };

    struct TNodeJobs
    {
        THashMap<TJobId, TJobInfo> Jobs;
        THashMap<TJobId, TOperationId> JobsToConfirm;
        THashMap<TJobId, NJobTrackerClient::TReleaseJobFlags> JobsToRelease;
        THashMap<TJobId, NScheduler::EAbortReason> JobsToAbort;
    };

    struct TNodeInfo
    {
        TNodeJobs Jobs;
        NConcurrency::TLease Lease;

        TString NodeAddress;
    };

    THashMap<TNodeId, TNodeInfo> RegisteredNodes_;

    THashMap<TOperationId, TOperationInfo> RegisteredOperations_;

    IInvokerPtr GetInvoker() const;
    IInvokerPtr TryGetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvoker() const;
    IInvokerPtr GetCancelableInvokerOrThrow() const;

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
        const std::vector<NJobTrackerClient::TJobToRelease>& jobs);

    void DoAbortJobOnNode(TJobId jobId, TOperationId operationId, NScheduler::EAbortReason abortReason);

    TNodeInfo& GetOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);
    TNodeInfo& RegisterNode(TNodeId nodeId, TString nodeAddress);
    void UpdateOrRegisterNode(TNodeId nodeId, const TString& nodeAddress);

    TNodeInfo* FindNodeInfo(TNodeId nodeId);

    void OnNodeHeartbeatLeaseExpired(TNodeId nodeId);

    const TString& GetNodeAddressForLogging(TNodeId nodeId);

    using TOperationIdToJobIds = THashMap<TOperationId, std::vector<TJobId>>;
    void AbortJobs(TOperationIdToJobIds jobsByOperation, NScheduler::EAbortReason abortReason) const;

    void AbortUnconfirmedJobs(TOperationId operationId, std::vector<TJobId> jobs);

    void DoInitialize(IInvokerPtr cancelableInvoker);
    void SetIncarnationId(TIncarnationId incarnationId);
    void DoCleanup();

    friend class TJobTrackerOperationHandler;
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

    void ReleaseJobs(std::vector<NJobTrackerClient::TJobToRelease> jobs);

    void AbortJobOnNode(
        TJobId jobId,
        NScheduler::EAbortReason abortReason);

private:
    TJobTracker* const JobTracker_;
    const IInvokerPtr CancelableInvoker_;

    const TOperationId OperationId_;
    const bool ControlJobLifetimeAtControllerAgent_;
};

DEFINE_REFCOUNTED_TYPE(TJobTrackerOperationHandler)

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
