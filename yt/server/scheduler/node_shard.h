#include "public.h"
#include "scheduler.h"
#include "job_resources.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/yson/public.h>


namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

typedef TEnumIndexedVector<TEnumIndexedVector<i64, EJobType>, EJobState> TJobCounter;
typedef TEnumIndexedVector<TJobCounter, EAbortReason> TAbortedJobCounter;

////////////////////////////////////////////////////////////////////

struct INodeShardHost
{
    virtual ~INodeShardHost() = default;

    virtual int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual ISchedulerStrategyPtr GetStrategy() = 0;

    virtual IInvokerPtr GetStatisticsAnalyzerInvoker() = 0;

    virtual IInvokerPtr GetJobSpecBuilderInvoker() = 0;

    virtual void ValidateOperationPermission(
        const Stroka& user,
        const TOperationId& operationId,
        NYTree::EPermission permission) = 0;

    virtual TFuture<void> UpdateOperationWithFinishedJob(
        const TOperationId& operationId,
        const TJobId& jobId,
        bool jobFailedOrAborted,
        NYson::TYsonString jobAttributes,
        const NChunkClient::TChunkId& stderrChunkId,
        const NChunkClient::TChunkId& failContextChunkId,
        TFuture<NYson::TYsonString> inputPathsFuture) = 0;

    virtual TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        const NChunkClient::TChunkId& chunkId,
        const TOperationId& operationId,
        const TJobId& jobId) = 0;

    virtual NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const Stroka& address) = 0;

};

////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////

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

    void RegisterOperation(const TOperationId& operationId, const IOperationControllerPtr& operationController);
    void UnregisterOperation(const TOperationId& operationId);

    yhash_set<TOperationId> ProcessHeartbeat(const TScheduler::TCtxHeartbeatPtr& context);

    std::vector<TExecNodeDescriptor> GetExecNodeDescriptors();

    void HandleNodesAttributes(const std::vector<std::pair<Stroka, NYTree::INodePtr>>& nodeMaps);

    void AbortAllJobs(const TError& error);

    void AbortOperationJobs(const TOperationId& operationId, const TError& abortReason, bool terminated);

    void ResumeOperationJobs(const TOperationId& operationId);

    NYson::TYsonString StraceJob(const TJobId& jobId, const Stroka& user);

    TNullable<NYson::TYsonString> GetJobStatistics(const TJobId& jobId);

    void DumpJobInputContext(const TJobId& jobId, const NYTree::TYPath& path, const Stroka& user);

    NNodeTrackerClient::TNodeDescriptor GetJobNode(const TJobId& jobId, const Stroka& user);

    void SignalJob(const TJobId& jobId, const Stroka& signalName, const Stroka& user);

    void AbandonJob(const TJobId& jobId, const Stroka& user);

    NYson::TYsonString PollJobShell(const TJobId& jobId, const NYson::TYsonString& parameters, const Stroka& user);

    void AbortJob(const TJobId& jobId, const TNullable<TDuration>& interruptTimeout, const Stroka& user);
    void OnInterruptTimeout(const TJobId& jobId, const Stroka& user);

    void BuildNodesYson(NYson::IYsonConsumer* consumer);
    void BuildOperationJobsYson(const TOperationId& operationId, NYson::IYsonConsumer* consumer);
    void BuildJobYson(const TJobId& job, NYson::IYsonConsumer* consumer);
    void BuildSuspiciousJobsYson(NYson::IYsonConsumer* consumer);

    TJobResources GetTotalResourceLimits();
    TJobResources GetTotalResourceUsage();
    TJobResources GetResourceLimits(const TNullable<Stroka>& tag);

    int GetActiveJobCount();

    TJobCounter GetJobCounter();
    TAbortedJobCounter GetAbortedJobCounter();

    TJobTimeStatisticsDelta GetJobTimeStatisticsDelta();

    int GetExecNodeCount();
    int GetTotalNodeCount();

    struct TOperationStatePatch
    {
        bool CanCreateJobNodeForAbortedOrFailedJobs;
        bool CanCreateJobNodeForJobsWithStderr;
    };

    struct TNodeShardPatch
    {
        yhash_map<TOperationId, TOperationStatePatch> OperationPatches;
    };

    void UpdateState(const TNodeShardPatch& patch);

private:
    const int Id_;
    const NConcurrency::TActionQueuePtr ActionQueue_;

    int ConcurrentHeartbeatCount_ = 0;

    std::atomic<int> ActiveJobCount_ = {0};

    NConcurrency::TReaderWriterSpinLock ResourcesLock_;
    TJobResources TotalResourceLimits_ = ZeroJobResources();
    TJobResources TotalResourceUsage_ = ZeroJobResources();

    yhash_map<Stroka, TJobResources> NodeTagToResources_;

    // Exec node is the node that is online and has user slots.
    std::atomic<int> ExecNodeCount_ = {0};
    std::atomic<int> TotalNodeCount_ = {0};

    NConcurrency::TReaderWriterSpinLock JobTimeStatisticsDeltaLock_;
    TJobTimeStatisticsDelta JobTimeStatisticsDelta_;

    NConcurrency::TReaderWriterSpinLock JobCounterLock_;
    TJobCounter JobCounter_;
    TAbortedJobCounter AbortedJobCounter_;

    std::vector<TUpdatedJob> UpdatedJobs_;
    std::vector<TCompletedJob> CompletedJobs_;

    struct TOperationState
    {
        TOperationState(const IOperationControllerPtr& controller)
            : Controller(controller)
        { }

        yhash_map<TJobId, TJobPtr> Jobs;
        IOperationControllerPtr Controller;
        bool Terminated = false;
        bool JobsAborted = false;
        bool CanCreateJobNodeForAbortedOrFailedJobs = true;
        bool CanCreateJobNodeForJobsWithStderr = true;
    };

    yhash_map<TOperationId, TOperationState> OperationStates_;

    typedef yhash_map<NNodeTrackerClient::TNodeId, TExecNodePtr> TExecNodeByIdMap;
    TExecNodeByIdMap IdToNode_;

    const NObjectClient::TCellTag PrimaryMasterCellTag_;
    TSchedulerConfigPtr Config_;
    INodeShardHost* const Host_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    NLogging::TLogger Logger;

    NLogging::TLogger CreateJobLogger(const TJobId& jobId, EJobState state, const Stroka& address);

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
        bool* hasWaitingJobs,
        yhash_set<TOperationId>* operationsToLog);

    TJobPtr ProcessJobHeartbeat(
        TExecNodePtr node,
        NJobTrackerClient::NProto::TReqHeartbeat* request,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        TJobStatus* jobStatus,
        bool forceJobsLogging,
        bool updateRunningJobs);

    void UpdateNodeTags(TExecNodePtr node, const std::vector<Stroka>& tagsList);

    void SubtractNodeResources(TExecNodePtr node);
    void AddNodeResources(TExecNodePtr node);
    void UpdateNodeResources(TExecNodePtr node, const TJobResources& limits, const TJobResources& usage);

    void BeginNodeHeartbeatProcessing(TExecNodePtr node);
    void EndNodeHeartbeatProcessing(TExecNodePtr node);

    void SubmitUpdatedAndCompletedJobsToStrategy();

    TFuture<void> ProcessScheduledJobs(
        const ISchedulingContextPtr& schedulingContext,
        NJobTrackerClient::NProto::TRspHeartbeat* response,
        yhash_set<TOperationId>* operationsToLog);

    void OnJobAborted(const TJobPtr& job, TJobStatus* status, bool operationTerminated = false);
    void OnJobFinished(const TJobPtr& job);
    void OnJobRunning(const TJobPtr& job, TJobStatus* status);
    void OnJobWaiting(const TJobPtr& /*job*/);
    void OnJobCompleted(const TJobPtr& job, TJobStatus* status, bool abandoned = false);
    void OnJobFailed(const TJobPtr& job, TJobStatus* status);

    void ProcessFinishedJobResult(const TJobPtr& job);

    void IncreaseProfilingCounter(const TJobPtr& job, i64 value);

    void SetJobState(const TJobPtr& job, EJobState state);

    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(const TJobPtr& job);

    void DoUnregisterJob(const TJobPtr& job);

    void PreemptJob(const TJobPtr& job, const TNullable<TInstant>& interruptDeadline);

    TExecNodePtr GetNodeByJob(const TJobId& jobId);

    TJobPtr FindJob(const TJobId& jobId, const TExecNodePtr& node);

    TJobPtr FindJob(const TJobId& jobId);

    TJobPtr GetJobOrThrow(const TJobId& jobId);

    NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TJobPtr& job);

    bool OperationExists(const TOperationId& operationId) const;

    TOperationState* FindOperationState(const TOperationId& operationId);

    TOperationState& GetOperationState(const TOperationId& operationId);

    void BuildNodeYson(TExecNodePtr node, NYson::IYsonConsumer* consumer);
    void BuildSuspiciousJobYson(const TJobPtr& job, NYson::IYsonConsumer* consumer);

};

typedef NYT::TIntrusivePtr<TNodeShard> TNodeShardPtr;
DEFINE_REFCOUNTED_TYPE(TNodeShard)

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
