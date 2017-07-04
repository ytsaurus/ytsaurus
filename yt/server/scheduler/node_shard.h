#include "public.h"
#include "scheduler.h"
#include "scheduling_tag.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

typedef TEnumIndexedVector<TEnumIndexedVector<i64, EJobType>, EJobState> TJobCounter;
typedef TEnumIndexedVector<TJobCounter, EAbortReason> TAbortedJobCounter;
typedef TEnumIndexedVector<TJobCounter, EInterruptReason> TCompletedJobCounter;

////////////////////////////////////////////////////////////////////////////////

struct INodeShardHost
{
    virtual ~INodeShardHost() = default;

    virtual int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual const ISchedulerStrategyPtr& GetStrategy() const = 0;

    virtual const IInvokerPtr& GetStatisticsAnalyzerInvoker() const = 0;

    virtual const IInvokerPtr& GetJobSpecBuilderInvoker() const = 0;

    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const = 0;

    virtual void ValidateOperationPermission(
        const TString& user,
        const TOperationId& operationId,
        NYTree::EPermission permission) = 0;

    virtual TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId) = 0;

    virtual NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TString& address) = 0;
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
    : public virtual TRefCounted
{
public:
    TNodeShard(
        int id,
        const NObjectClient::TCellTag& PrimaryMasterCellTag,
        TSchedulerConfigPtr config,
        INodeShardHost* host,
        NCellScheduler::TBootstrap* bootstrap);

    IInvokerPtr GetInvoker();

    void UpdateConfig(const TSchedulerConfigPtr& config);

    void OnMasterDisconnected();

    void RegisterOperation(const TOperationId& operationId, const NControllerAgent::IOperationControllerPtr& operationController);
    void UnregisterOperation(const TOperationId& operationId);

    void ProcessHeartbeat(const TScheduler::TCtxHeartbeatPtr& context);

    TExecNodeDescriptorListPtr GetExecNodeDescriptors();
    void RemoveOutdatedSchedulingTagFilter(const TSchedulingTagFilter& filter);

    void HandleNodesAttributes(const std::vector<std::pair<TString, NYTree::INodePtr>>& nodeMaps);

    void AbortAllJobs(const TError& error);

    void AbortOperationJobs(const TOperationId& operationId, const TError& abortReason, bool terminated);

    void ResumeOperationJobs(const TOperationId& operationId);

    NYson::TYsonString StraceJob(const TJobId& jobId, const TString& user);

    void DumpJobInputContext(const TJobId& jobId, const NYTree::TYPath& path, const TString& user);

    NNodeTrackerClient::TNodeDescriptor GetJobNode(const TJobId& jobId, const TString& user);

    void SignalJob(const TJobId& jobId, const TString& signalName, const TString& user);

    void AbandonJob(const TJobId& jobId, const TString& user);

    NYson::TYsonString PollJobShell(const TJobId& jobId, const NYson::TYsonString& parameters, const TString& user);

    void AbortJob(const TJobId& jobId, const TNullable<TDuration>& interruptTimeout, const TString& user);

    void AbortJob(const TJobId& jobId, const TError& error);

    void InterruptJob(const TJobId& jobId, EInterruptReason reason);

    void FailJob(const TJobId& jobId);

    void BuildNodesYson(NYson::IYsonConsumer* consumer);

    TOperationId GetOperationIdByJobId(const TJobId& job);

    TJobResources GetTotalResourceLimits();
    TJobResources GetTotalResourceUsage();
    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter);

    int GetActiveJobCount();

    TJobCounter GetJobCounter();
    TAbortedJobCounter GetAbortedJobCounter();
    TCompletedJobCounter GetCompletedJobCounter();

    TJobTimeStatisticsDelta GetJobTimeStatisticsDelta();

    int GetExecNodeCount();
    int GetTotalNodeCount();

    void SetInterruptHint(const TJobId& jobId, bool hint);

private:
    const int Id_;
    const NConcurrency::TActionQueuePtr ActionQueue_;

    int ConcurrentHeartbeatCount_ = 0;

    std::atomic<int> ActiveJobCount_ = {0};

    NConcurrency::TReaderWriterSpinLock ResourcesLock_;
    TJobResources TotalResourceLimits_ = ZeroJobResources();
    TJobResources TotalResourceUsage_ = ZeroJobResources();

    NProfiling::TCpuInstant CachedExecNodeDescriptorsLastUpdateTime_ = 0;
    NConcurrency::TReaderWriterSpinLock CachedExecNodeDescriptorsLock_;
    TExecNodeDescriptorListPtr CachedExecNodeDescriptors_ = New<TExecNodeDescriptorList>();

    yhash<TSchedulingTagFilter, TJobResources> SchedulingTagFilterToResources_;

    // Exec node is the node that is online and has user slots.
    std::atomic<int> ExecNodeCount_ = {0};
    std::atomic<int> TotalNodeCount_ = {0};

    NConcurrency::TReaderWriterSpinLock JobTimeStatisticsDeltaLock_;
    TJobTimeStatisticsDelta JobTimeStatisticsDelta_;

    NConcurrency::TReaderWriterSpinLock JobCounterLock_;
    TJobCounter JobCounter_;
    TAbortedJobCounter AbortedJobCounter_;
    TCompletedJobCounter CompletedJobCounter_;

    std::vector<TUpdatedJob> UpdatedJobs_;
    std::vector<TCompletedJob> CompletedJobs_;

    struct TOperationState
    {
        TOperationState(const NControllerAgent::IOperationControllerPtr& controller)
            : Controller(controller)
        { }

        yhash<TJobId, TJobPtr> Jobs;
        NControllerAgent::IOperationControllerPtr Controller;
        bool Terminated = false;
        bool JobsAborted = false;
    };

    yhash<TOperationId, TOperationState> OperationStates_;

    typedef yhash<NNodeTrackerClient::TNodeId, TExecNodePtr> TExecNodeByIdMap;
    TExecNodeByIdMap IdToNode_;

    const NObjectClient::TCellTag PrimaryMasterCellTag_;
    TSchedulerConfigPtr Config_;
    INodeShardHost* const Host_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    NLogging::TLogger Logger;


    NLogging::TLogger CreateJobLogger(const TJobId& jobId, EJobState state, const TString& address);

    TJobResources CalculateResourceLimits(const TSchedulingTagFilter& filter);

    TExecNodePtr GetOrRegisterNode(NNodeTrackerClient::TNodeId nodeId, const NNodeTrackerClient::TNodeDescriptor& descriptor);
    TExecNodePtr RegisterNode(NNodeTrackerClient::TNodeId nodeId, const NNodeTrackerClient::TNodeDescriptor& descriptor);
    void UnregisterNode(TExecNodePtr node);
    void DoUnregisterNode(TExecNodePtr node);

    void AbortJobsAtNode(TExecNodePtr node);

    void ProcessHeartbeatJobs(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        std::vector<TJobPtr>* runningJobs,
        bool* hasWaitingJobs);

    TJobPtr ProcessJobHeartbeat(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        TJobStatus* jobStatus,
        bool forceJobsLogging);

    void UpdateNodeTags(TExecNodePtr node, const std::vector<TString>& tagsList);

    void SubtractNodeResources(TExecNodePtr node);
    void AddNodeResources(TExecNodePtr node);
    void UpdateNodeResources(TExecNodePtr node, const TJobResources& limits, const TJobResources& usage);

    void BeginNodeHeartbeatProcessing(TExecNodePtr node);
    void EndNodeHeartbeatProcessing(TExecNodePtr node);

    void SubmitUpdatedAndCompletedJobsToStrategy();

    TFuture<void> ProcessScheduledJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TScheduler::TCtxHeartbeatPtr& rpcContext);

    void OnJobAborted(const TJobPtr& job, TJobStatus* status, bool operationTerminated = false);
    void OnJobFinished(const TJobPtr& job);
    void OnJobRunning(const TJobPtr& job, TJobStatus* status);
    void OnJobWaiting(const TJobPtr& /*job*/);
    void OnJobCompleted(const TJobPtr& job, TJobStatus* status, bool abandoned = false);
    void OnJobFailed(const TJobPtr& job, TJobStatus* status);

    void IncreaseProfilingCounter(const TJobPtr& job, i64 value);

    void SetJobState(const TJobPtr& job, EJobState state);

    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(const TJobPtr& job);

    void DoUnregisterJob(const TJobPtr& job);

    void PreemptJob(const TJobPtr& job, TNullable<NProfiling::TCpuDuration> interruptTimeout);

    void DoInterruptJob(
        const TJobPtr& job,
        EInterruptReason reason,
        NProfiling::TCpuDuration interruptTimeout = 0,
        TNullable<TString> interruptUser = Null);

    TExecNodePtr GetNodeByJob(const TJobId& jobId);

    TJobPtr FindJob(const TJobId& jobId, const TExecNodePtr& node);

    TJobPtr FindJob(const TJobId& jobId);

    TJobPtr GetJobOrThrow(const TJobId& jobId);

    NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TJobPtr& job);

    bool OperationExists(const TOperationId& operationId) const;

    TOperationState* FindOperationState(const TOperationId& operationId);

    TOperationState& GetOperationState(const TOperationId& operationId);

    void BuildNodeYson(TExecNodePtr node, NYson::IYsonConsumer* consumer);

};

typedef NYT::TIntrusivePtr<TNodeShard> TNodeShardPtr;
DEFINE_REFCOUNTED_TYPE(TNodeShard)

////////////////////////////////////////////////////////////////////////////////

IJobHostPtr CreateJobHost(const TJobId& jobId, const TNodeShardPtr& nodeShard);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
