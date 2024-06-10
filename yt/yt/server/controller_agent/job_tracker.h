#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/generic/noncopyable.h>

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
    NProfiling::TCounter StaleHeartbeatCount_;
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
    NProfiling::TCounter ThrottledRunningJobEventCount_;
    NProfiling::TCounter ThrottledHeartbeatCount_;
    NProfiling::TCounter ThrottledOperationCount_;
    NProfiling::TCounter WrongIncarnationRequestCount_;

    NConcurrency::TActionQueuePtr JobTrackerQueue_;

    TAtomicIntrusivePtr<IInvoker> CancelableInvoker_;

    TIncarnationId IncarnationId_;

    // Used only for logging
    TRefCountedExecNodeDescriptorMapPtr ExecNodes_;

    using TNodeId = NNodeTrackerClient::TNodeId;

    struct TOperationInfo
    {
        TOperationId OperationId;
        bool JobsReady = false;
        const TWeakPtr<IOperationController> OperationController;
        THashSet<TAllocationId> TrackedAllocationIds;
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

    struct TRunningJobInfo
    {
        const TJobId JobId;

        TRequestedActionInfo RequestedActionInfo;
        TInstant DisappearedFromNodeSince;

        bool Confirmed = true;
    };

    struct TFinishedJobInfo { };

    class TAllocationInfo
        : NNonCopyable::TMoveOnly
    {
    public:
        const TOperationId OperationId;
        const TAllocationId AllocationId;

        //! All methods have JobTracker->GetInvoker() thread affinity.
        TAllocationInfo(TOperationId operationId, TAllocationId allocationId);

        bool IsEmpty() const noexcept;
        bool HasJob(TJobId jobId) const noexcept;
        bool HasRunningJob(TJobId jobId) const noexcept;
        std::optional<EJobStage> GetJobStage(TJobId jobId) const;

        bool ShouldBeRemoved() const noexcept;

        const std::optional<TRunningJobInfo>& GetRunningJob() const;
        const THashMap<TJobId, TFinishedJobInfo>& GetFinishedJobs() const;
        bool IsFinished() const noexcept;
        const std::optional<TSchedulerToAgentAllocationEvent>& GetPostponedEvent() const noexcept;

        // Actually, sets DisappearedFromNodeSince to running job.
        void SetDisappearedFromNodeSince(TInstant from) noexcept;
        TRequestedActionInfo& GetMutableRunningJobRequestedActionInfo() noexcept;

        void StartJob(TJobId jobId, bool confirmed);
        bool ConfirmRunningJob() noexcept;
        void FinishRunningJob() noexcept;
        void EraseRunningJobOrCrash();
        bool EraseFinishedJob(TJobId jobId);

        void FinishAndClearJobs() noexcept;
        void Finish(TSchedulerToAgentAllocationEvent&& event);

        TSchedulerToAgentAllocationEvent ConsumePostponedEventOrCrash();
        template <class TEventType>
        TEventType ConsumePostponedEventOrCrash();
        template <class TEvent>
        static TEvent& GetEventOrCrash(TSchedulerToAgentAllocationEvent& event);

    private:
        std::optional<TRunningJobInfo> RunningJob_;
        THashMap<TJobId, TFinishedJobInfo> FinishedJobs_;

        bool Finished_ = false;

        std::optional<TSchedulerToAgentAllocationEvent> PostponedAllocationEvent_;
    };

    struct TNodeJobs
    {
        THashMap<TJobId, TReleaseJobFlags> JobsToRelease;
        THashMap<TJobId, EAbortReason> JobsToAbort;

        THashMap<TAllocationId, TAllocationInfo> Allocations;

        using TAllocationIterator = THashMap<TAllocationId, TAllocationInfo>::iterator;

        TAllocationInfo* FindAllocation(TAllocationId allocationId);
        TAllocationInfo* FindAllocation(TJobId jobId);

        const TAllocationInfo* FindAllocation(TAllocationId allocationId) const;
        const TAllocationInfo* FindAllocation(TJobId jobId) const ;

        i64 GetJobToConfirmCount() const;

        i64 GetJobCount() const;
    };

    struct TNodeInfo
    {
    public:
        TNodeJobs Jobs;
        NConcurrency::TLease Lease;

        TGuid RegistrationId;

        TString NodeAddress;

        ui64 LastHeartbeatSequenceNumber = 0;

        //! |false| if heartbeat is stale.
        bool CheckHeartbeatSequenceNumber(ui64 sequenceNumber);
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
        int ThrottledRunningJobEventCount = 0;
        int ThrottledOperationCount = 0;
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
        TCtxHeartbeatPtr RpcContext;
        NLogging::TLogger Logger;
        TString NodeAddress;
        TNodeId NodeId;
        TIncarnationId IncarnationId;
        THeartbeatRequest Request;
    };

    struct THeartbeatProcessingResult
    {
        THeartbeatCounters Counters;
        THeartbeatProcessingContext Context;
    };

    struct TOperationUpdatesProcessingContext
    {
        const TOperationId OperationId;
        std::vector<std::unique_ptr<TJobSummary>> JobSummaries;
        std::vector<TJobToAbort> JobsToAbort;
        std::vector<TAbortedAllocationSummary> AbortedAllocations;
        std::vector<TFinishedAllocationSummary> FinishedAllocations;

        IOperationControllerPtr OperationController;
        NLogging::TLogger OperationLogger;

        //! Should be used only in job thread.
        TOperationInfo* OperationInfo;

        void AddAllocationEvent(TSchedulerToAgentAllocationEvent&& allocationEvent);
    };

    THeartbeatProcessingResult DoProcessHeartbeat(
        THeartbeatProcessingContext heartbeatProcessingContext);
    void DoProcessUnconfirmedJobsInHeartbeat(
        THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
        TNodeInfo& nodeInfo,
        THeartbeatProcessingContext& heartbeatProcessingContext,
        THeartbeatProcessingResult& heartbeatProcessingResult);
    //! Returns ids of jobs that should not be processed in current heartbeat.
    THashSet<TJobId> DoProcessAbortedAndReleasedJobsInHeartbeat(
        TNodeInfo& nodeInfo,
        THeartbeatProcessingContext& heartbeatProcessingContext,
        THeartbeatProcessingResult& heartbeatProcessingResult);
    void DoProcessJobInfosInHeartbeat(
        THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
        TNodeInfo& nodeInfo,
        THeartbeatProcessingContext& heartbeatProcessingContext,
        THeartbeatProcessingResult& heartbeatProcessingResult);
    void DoProcessAllocationsInHeartbeat(
        THashMap<TOperationId, TOperationUpdatesProcessingContext>& operationIdToUpdatesProcessingContext,
        TNodeInfo& nodeInfo,
        THeartbeatProcessingContext& heartbeatProcessingContext,
        THeartbeatProcessingResult& heartbeatProcessingResult);
    TOperationUpdatesProcessingContext& AddOperationUpdatesProcessingContext(
        THashMap<TOperationId, TOperationUpdatesProcessingContext>& contexts,
        THeartbeatProcessingContext& heartbeatProcessingContext,
        THeartbeatProcessingResult& heartbeatProcessingResult,
        TOperationId operationId);

    // Returns |true| iff job event was handled (not throttled).
    bool HandleJobInfo(
        TNodeJobs::TAllocationIterator allocationIt,
        EJobStage currentJobStage,
        TOperationUpdatesProcessingContext& operationUpdatesProcessingContext,
        TCtxHeartbeat::TTypedResponse* response,
        std::unique_ptr<TJobSummary>& jobSummary,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters,
        bool shouldSkipRunningJobEvents = false);

    bool HandleRunningJobInfo(
        TNodeJobs::TAllocationIterator allocationIt,
        TOperationUpdatesProcessingContext& operationUpdatesProcessingContext,
        TCtxHeartbeat::TTypedResponse* response,
        std::unique_ptr<TJobSummary>& jobSummary,
        const NLogging::TLogger& Logger,
        THeartbeatCounters& heartbeatCounters,
        bool shouldSkipRunningJobEvents);
    bool HandleFinishedJobInfo(
        TNodeJobs::TAllocationIterator allocationIt,
        TOperationUpdatesProcessingContext& operationUpdatesProcessingContext,
        TCtxHeartbeat::TTypedResponse* response,
        std::unique_ptr<TJobSummary>& jobSummary,
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

    void DoRegisterAllocation(TStartedAllocationInfo allocationInfo, TOperationId operationId);
    void DoRegisterJob(TStartedJobInfo jobInfo, TOperationId operationId);

    void DoRevive(
        TOperationId operationId,
        std::vector<TStartedAllocationInfo> allocations);

    void DoReleaseJobs(
        TOperationId operationId,
        const std::vector<TJobToRelease>& jobs);

    void RequestJobAbortion(TJobId jobId, TOperationId operationId, EAbortReason reason);


    [[nodiscard]] std::optional<TAllocationInfo>
    EraseAllocationIfNeeded(
        TNodeJobs& nodeJobs,
        THashMap<TAllocationId, TAllocationInfo>::iterator allocationIt,
        TOperationInfo* operationInfo = nullptr);

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

    void ProcessAllocationEvents(
        TOperationId operationId,
        std::vector<TFinishedAllocationSummary> finishedAllocations,
        std::vector<TAbortedAllocationSummary> abortedAllocations);

    template <
        class TAllocationEvent,
        CInvocable<void(
            TStringBuf reason,
            TAllocationEvent event,
            TJobTracker::TNodeInfo* nodeInfo,
            std::optional<TJobTracker::TNodeJobs::TAllocationIterator> maybeAllocationIterator)> TProcessEventCallback>
    void ProcessAllocationEvent(
        TAllocationEvent allocationEvent,
        const TProcessEventCallback& skipAllocationEvent,
        const NLogging::TLogger& Logger);

    void ProcessFinishedAllocations(
        std::vector<TFinishedAllocationSummary> finishedAllocations,
        TOperationUpdatesProcessingContext& operationUpdatesProcessingContext);

    void ProcessAbortedAllocations(
        std::vector<TAbortedAllocationSummary> abortedAllocations,
        TOperationUpdatesProcessingContext& operationUpdatesProcessingContext);

    const TString& GetNodeAddressForLogging(TNodeId nodeId);

    void AbortUnconfirmedJobs(TOperationId operationId, std::vector<TJobId> jobs);

    void ProcessOperationContext(TOperationUpdatesProcessingContext operationUpdatesProcessingContext);
    void ProcessOperationContexts(THashMap<TOperationId, TOperationUpdatesProcessingContext> operationUpdatesProcessingContext);

    void DoInitialize(IInvokerPtr cancelableInvoker);
    void SetIncarnationId(TIncarnationId incarnationId);
    void DoCleanup();

    friend class TJobTrackerOperationHandler;

    class TJobTrackerNodeOrchidService;
    class TJobTrackerAllocationOrchidService;
    class TJobTrackerJobOrchidService;
    class TJobTrackerOperationOrchidService;
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

    void RegisterAllocation(TStartedAllocationInfo allocationInfo);

    void RegisterJob(TStartedJobInfo jobInfo);
    void Revive(std::vector<TStartedAllocationInfo> allocations);
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

    void ProcessAllocationEvents(
        std::vector<TFinishedAllocationSummary> finishedAllocations,
        std::vector<TAbortedAllocationSummary> abortedAllocations);

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
