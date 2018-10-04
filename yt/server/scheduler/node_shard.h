#pragma once

#include "private.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "scheduling_tag.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/public.h>

#include <yt/core/misc/sync_expiring_cache.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct INodeShardHost
{
    virtual ~INodeShardHost() = default;

    virtual int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual TFuture<void> RegisterOrUpdateNode(
        NNodeTrackerClient::TNodeId nodeId,
        const TString& nodeAddress,
        const THashSet<TString>& tags) = 0;

    virtual void UnregisterNode(NNodeTrackerClient::TNodeId nodeId, const TString& nodeAddress) = 0;

    virtual const ISchedulerStrategyPtr& GetStrategy() const = 0;

    virtual void ValidateOperationAccess(
        const TString& user,
        const TOperationId& operationId,
        EAccessType accessType) = 0;

    virtual TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId,
        const TString& user) = 0;

    virtual NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TString& address) = 0;

    virtual int GetOperationArchiveVersion() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobTimeStatisticsDelta
{
    void Reset()
    {
        CompletedJobTimeDelta = 0;
        FailedJobTimeDelta = 0;
        AbortedJobTimeDelta = 0;
    }

    TJobTimeStatisticsDelta& operator += (const TJobTimeStatisticsDelta& rhs)
    {
        CompletedJobTimeDelta += rhs.CompletedJobTimeDelta;
        FailedJobTimeDelta += rhs.FailedJobTimeDelta;
        AbortedJobTimeDelta += rhs.AbortedJobTimeDelta;
        return *this;
    }

    ui64 CompletedJobTimeDelta = 0;
    ui64 FailedJobTimeDelta = 0;
    ui64 AbortedJobTimeDelta = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeShard
    : public TRefCounted
{
public:
    TNodeShard(
        int id,
        TSchedulerConfigPtr config,
        INodeShardHost* host,
        TBootstrap* bootstrap);

    int GetId() const;
    const IInvokerPtr& GetInvoker() const;

    void UpdateConfig(const TSchedulerConfigPtr& config);

    IInvokerPtr OnMasterConnected();
    void OnMasterDisconnected();

    void RegisterOperation(
        const TOperationId& operationId,
        const IOperationControllerPtr& controller,
        bool jobsReady);
    void StartOperationRevival(const TOperationId& operationId);
    void FinishOperationRevival(const TOperationId& operationId, const std::vector<TJobPtr>& jobs);
    void ResetOperationRevival(const TOperationId& operationId);
    void UnregisterOperation(const TOperationId& operationId);

    void ProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context);

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors();
    void UpdateExecNodeDescriptors();

    void HandleNodesAttributes(const std::vector<std::pair<TString, NYTree::INodePtr>>& nodeMaps);

    void AbortOperationJobs(const TOperationId& operationId, const TError& abortReason, bool terminated);
    void ResumeOperationJobs(const TOperationId& operationId);

    NNodeTrackerClient::TNodeDescriptor GetJobNode(const TJobId& jobId, const TString& user);

    NYson::TYsonString StraceJob(const TJobId& jobId, const TString& user);
    void DumpJobInputContext(const TJobId& jobId, const NYTree::TYPath& path, const TString& user);
    void SignalJob(const TJobId& jobId, const TString& signalName, const TString& user);
    void AbandonJob(const TJobId& jobId, const TString& user);
    NYson::TYsonString PollJobShell(const TJobId& jobId, const NYson::TYsonString& parameters, const TString& user);
    void AbortJobByUserRequest(const TJobId& jobId, TNullable<TDuration> interruptTimeout, const TString& user);

    void AbortJob(const TJobId& jobId, const TError& error);
    void AbortJobs(const std::vector<TJobId>& jobIds, const TError& error);
    void InterruptJob(const TJobId& jobId, EInterruptReason reason);
    void FailJob(const TJobId& jobId);
    void ReleaseJob(const TJobId& jobId, bool archiveJobSpec, bool archiveStderr, bool archiveFailContext);

    void BuildNodesYson(NYTree::TFluentMap fluent);

    TOperationId FindOperationIdByJobId(const TJobId& job);

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter);
    TJobResources GetResourceUsage(const TSchedulingTagFilter& filter);

    int GetActiveJobCount();

    TJobCounter GetJobCounter();
    TAbortedJobCounter GetAbortedJobCounter();
    TCompletedJobCounter GetCompletedJobCounter();

    TJobTimeStatisticsDelta GetJobTimeStatisticsDelta();

    int GetExecNodeCount();
    int GetTotalNodeCount();

    TFuture<NControllerAgent::TScheduleJobResultPtr> BeginScheduleJob(
        const TIncarnationId& incarnationId,
        const TOperationId& operationId,
        const TJobId& jobId);
    void EndScheduleJob(
        const NProto::TScheduleJobResponse& response);

    int ExtractJobReporterWriteFailuresCount();
    int GetJobReporterQueueIsTooLargeNodeCount();

private:
    const int Id_;
    TSchedulerConfigPtr Config_;
    INodeShardHost* const Host_;
    TBootstrap* const Bootstrap_;

    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr CachedExecNodeDescriptorsRefresher_;

    struct TResourceStatistics
    {
        TJobResources Usage;
        TJobResources Limits;
    };
    const TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TResourceStatistics>> CachedResourceStatisticsByTags_;

    const NLogging::TLogger Logger;

    bool Connected_ = false;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    int ConcurrentHeartbeatCount_ = 0;

    bool HasOngoingNodesAttributesUpdate_ = false;

    std::atomic<int> ActiveJobCount_ = {0};

    NConcurrency::TReaderWriterSpinLock ResourcesLock_;
    TJobResources TotalResourceUsage_;

    NConcurrency::TReaderWriterSpinLock CachedExecNodeDescriptorsLock_;
    TRefCountedExecNodeDescriptorMapPtr CachedExecNodeDescriptors_ = New<TRefCountedExecNodeDescriptorMap>();

    THashMap<NNodeTrackerClient::TNodeId, TExecNodePtr> IdToNode_;
    // Exec node is the node that is online and has user slots.
    std::atomic<int> ExecNodeCount_ = {0};
    std::atomic<int> TotalNodeCount_ = {0};

    NConcurrency::TReaderWriterSpinLock JobTimeStatisticsDeltaLock_;
    TJobTimeStatisticsDelta JobTimeStatisticsDelta_;

    std::atomic<int> JobReporterWriteFailuresCount_ = {0};
    std::atomic<int> JobReporterQueueIsTooLargeNodeCount_ = {0};

    NConcurrency::TReaderWriterSpinLock JobCounterLock_;
    TJobCounter JobCounter_;
    TAbortedJobCounter AbortedJobCounter_;
    TCompletedJobCounter CompletedJobCounter_;

    THashMap<TJobId, TJobUpdate> JobsToSubmitToStrategy_;

    struct TScheduleJobEntry
    {
        TOperationId OperationId;
        TIncarnationId IncarnationId;
        TPromise<NControllerAgent::TScheduleJobResultPtr> Promise;
        THashMultiMap<TOperationId, THashMap<TJobId, TScheduleJobEntry>::iterator>::iterator OperationIdToJobIdsIterator;
        NProfiling::TCpuInstant StartTime;
    };
    // NB: It is important to use THash* instead of std::unordered_* since we rely on
    // iterators not to be invalidated.
    THashMap<TJobId, TScheduleJobEntry> JobIdToScheduleEntry_;
    THashMultiMap<TOperationId, THashMap<TJobId, TScheduleJobEntry>::iterator> OperationIdToJobIterators_;

    NConcurrency::TPeriodicExecutorPtr SubmitJobsToStrategyExecutor_;

    using TEpoch = ui64;

    struct TOperationState
    {
        TOperationState(IOperationControllerPtr controller, bool jobsReady, TEpoch epoch)
            : Controller(std::move(controller))
            , JobsReady(jobsReady)
            , Epoch(epoch)
        { }

        THashMap<TJobId, TJobPtr> Jobs;
        THashSet<TJobId> JobsToSubmitToStrategy;
        THashSet<TJobId> RecentlyFinishedJobIds;
        //! Used only to decrease logging size.
        THashSet<TJobId> SkippedJobIds;
        IOperationControllerPtr Controller;
        bool Terminated = false;
        //! Raised to prevent races between suspension and scheduler strategy scheduling new jobs.
        bool ForbidNewJobs = false;
        //! Flag showing that we already know about all jobs of this operation
        //! and it is OK to abort unknown jobs that claim to be a part of this operation.
        bool JobsReady = false;
        //! Prevents leaking #AbortUnconfirmedJobs between different incarnations of the same operation.
        TEpoch Epoch;
    };

    THashMap<TOperationId, TOperationState> IdToOpertionState_;
    TEpoch CurrentEpoch_ = 0;

    void ValidateConnected();

    void DoCleanup();

    void DoProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context);

    NLogging::TLogger CreateJobLogger(
        const TJobId& jobId,
        const TOperationId& operationId,
        EJobState state,
        const TString& address);

    TResourceStatistics CalculateResourceStatistics(const TSchedulingTagFilter& filter);

    TExecNodePtr GetOrRegisterNode(NNodeTrackerClient::TNodeId nodeId, const NNodeTrackerClient::TNodeDescriptor& descriptor);
    TExecNodePtr RegisterNode(NNodeTrackerClient::TNodeId nodeId, const NNodeTrackerClient::TNodeDescriptor& descriptor);
    void UnregisterNode(const TExecNodePtr& node);
    void DoUnregisterNode(const TExecNodePtr& node);
    void OnNodeLeaseExpired(NNodeTrackerClient::TNodeId nodeId);

    void AbortAllJobsAtNode(const TExecNodePtr& node);
    void AbortUnconfirmedJobs(
        const TOperationId& operationId,
        TEpoch epoch,
        const std::vector<TJobPtr>& jobs);

    void ProcessHeartbeatJobs(
        const TExecNodePtr& node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        std::vector<TJobPtr>* runningJobs,
        bool* hasWaitingJobs);

    TJobPtr ProcessJobHeartbeat(
        const TExecNodePtr& node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        TJobStatus* jobStatus,
        bool forceJobsLogging);

    void SubtractNodeResources(const TExecNodePtr& node);
    void AddNodeResources(const TExecNodePtr& node);
    void UpdateNodeResources(
        const TExecNodePtr& node,
        const TJobResources& limits,
        const TJobResources& usage,
        const NNodeTrackerClient::NProto::TDiskResources& diskInfo);

    void BeginNodeHeartbeatProcessing(const TExecNodePtr& node);
    void EndNodeHeartbeatProcessing(const TExecNodePtr& node);

    void SubmitJobsToStrategy();

    void ProcessScheduledJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TScheduler::TCtxNodeHeartbeatPtr& rpcContext);

    void OnJobAborted(const TJobPtr& job, TJobStatus* status, bool byScheduler, bool operationTerminated = false);
    void OnJobFinished(const TJobPtr& job);
    void OnJobRunning(const TJobPtr& job, TJobStatus* status, bool shouldLogJob);
    void OnJobCompleted(const TJobPtr& job, TJobStatus* status, bool abandoned = false);
    void OnJobFailed(const TJobPtr& job, TJobStatus* status);

    void IncreaseProfilingCounter(const TJobPtr& job, int value);

    void SetJobState(const TJobPtr& job, EJobState state);

    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(const TJobPtr& job, bool enableLogging = true);

    void SetJobWaitingForConfirmation(const TJobPtr& job);
    void ResetJobWaitingForConfirmation(const TJobPtr& job);

    void AddRecentlyFinishedJob(const TJobPtr& job);
    void RemoveRecentlyFinishedJob(const TJobId& jobId);

    void SetOperationJobsReleaseDeadline(TOperationState* operationState);

    void PreemptJob(const TJobPtr& job, TNullable<NProfiling::TCpuDuration> interruptTimeout);

    void DoInterruptJob(
        const TJobPtr& job,
        EInterruptReason reason,
        NProfiling::TCpuDuration interruptTimeout = 0,
        const TNullable<TString>& interruptUser = Null);

    TExecNodePtr FindNodeByJob(const TJobId& jobId);

    TJobPtr FindJob(const TJobId& jobId, const TExecNodePtr& node);
    TJobPtr FindJob(const TJobId& jobId);
    TJobPtr GetJobOrThrow(const TJobId& jobId);

    NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TJobPtr& job);

    TOperationState* FindOperationState(const TOperationId& operationId);
    TOperationState& GetOperationState(const TOperationId& operationId);

    void BuildNodeYson(const TExecNodePtr& node, NYTree::TFluentMap consumer);

    void UpdateNodeState(const TExecNodePtr& execNode, NNodeTrackerServer::ENodeState newState, TError error = TError());
};

DEFINE_REFCOUNTED_TYPE(TNodeShard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
