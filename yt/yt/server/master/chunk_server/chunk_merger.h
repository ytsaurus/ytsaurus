#pragma once

#include "private.h"
#include "chunk_merger_traversal_info.h"
#include "chunk_replacer.h"
#include "chunk_owner_data_statistics.h"
#include "job.h"
#include "job_controller.h"

#include <yt/yt/server/master/chunk_server/proto/chunk_merger.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_rotator.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/profiling/producer.h>

#include <queue>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Items of this enum are compared by <, greater values always
// override smaller ones.
DEFINE_ENUM(EMergeSessionResult,
    ((None)              (0))
    // Everything went OK, no need to reschedule merge.
    ((OK)                (1))
    // Some jobs failed, reschedule.
    ((TransientFailure)  (2))
    // Some jobs failed, but there was no chance to succeed (typically node is dead), no need to reschedule.
    ((PermanentFailure)  (3))
);

struct TMergeJobInfo
{
    TJobId JobId;
    int JobIndex;
    // TODO(shakurov): ephemeral ptr?
    NCypressClient::TObjectId NodeId;
    TChunkListId ParentChunkListId;

    std::vector<TChunkId> InputChunkIds;
    TChunkId OutputChunkId;

    NChunkClient::EChunkMergerMode MergeMode;

    NSecurityServer::TAccountId AccountId;
};

struct TChunkMergerSession
{
    THashMap<NCypressClient::TObjectId, THashSet<TJobId>> ChunkListIdToRunningJobs;
    THashMap<NCypressClient::TObjectId, std::vector<TMergeJobInfo>> ChunkListIdToCompletedJobs;
    EMergeSessionResult Result = EMergeSessionResult::None;

    NCypressClient::TObjectId AccountId;

    TChunkMergerTraversalStatistics TraversalStatistics;
    bool TraversalFinished = false;

    int JobCount = 0;

    TInstant SessionCreationTime;

    bool IsReadyForFinalization() const;
};

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TNodePtrWithReplicaAndMediumIndexList, TargetReplicas);

    DEFINE_BYREF_RO_PROPERTY(TMergeJobInfo, JobInfo);

public:
    using TChunkVector = TCompactVector<NObjectServer::TEphemeralObjectPtr<TChunk>, 16>;
    TMergeJob(
        TJobId jobId,
        TJobEpoch jobEpoch,
        TMergeJobInfo jobInfo,
        NNodeTrackerServer::TNode* node,
        NChunkClient::TChunkIdWithIndexes chunkIdWithIndexes,
        TChunkVector inputChunks,
        NChunkClient::NProto::TChunkMergerWriterOptions chunkMergerWriterOptions,
        TNodePtrWithReplicaAndMediumIndexList targetReplicas,
        bool validateShallowMerge);

    bool FillJobSpec(NCellMaster::TBootstrap* bootstrap, NProto::TJobSpec* jobSpec) const override;

private:
    const TChunkVector InputChunks_;
    const NChunkClient::NProto::TChunkMergerWriterOptions ChunkMergerWriterOptions_;
    const bool ValidateShallowMerge_;

    static NNodeTrackerClient::NProto::TNodeResources GetResourceUsage(const TChunkVector& inputChunks);
};

DECLARE_REFCOUNTED_TYPE(TMergeJob)
DEFINE_REFCOUNTED_TYPE(TMergeJob)

////////////////////////////////////////////////////////////////////////////////

struct IMergeChunkVisitorHost
    : public virtual TRefCounted
{
    virtual void RegisterJobAwaitingChunkCreation(
        TJobId jobId,
        NChunkClient::EChunkMergerMode mode,
        int jobIndex,
        NCypressClient::TObjectId nodeId,
        TChunkListId parentChunkListId,
        std::vector<TChunkId> inputChunkIds,
        NSecurityServer::TAccountId accountId) = 0;
    virtual void OnTraversalFinished(
        NCypressClient::TObjectId nodeId,
        EMergeSessionResult result,
        TChunkMergerTraversalStatistics traversalStatistics) = 0;
};

class TChunkMerger
    : public NCellMaster::TMasterAutomatonPart
    , public virtual ITypedJobController<TMergeJob>
    , public virtual IMergeChunkVisitorHost
{
public:
    explicit TChunkMerger(NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    void ScheduleMerge(NCypressClient::TObjectId chunkOwnerId);
    bool CanRegisterMergeSession(TChunkOwnerBase* trunkChunkOwner);
    void ScheduleMerge(TChunkOwnerBase* trunkChunkOwner);

    EChunkMergerStatus GetNodeChunkMergerStatus(NCypressServer::TNodeId nodeId) const;

    void OnProfiling(NProfiling::TSensorBuffer* buffer);

    // IJobController implementation.
    void ScheduleJobs(EJobType jobType, IJobSchedulingContext* context) override;

    void OnJobWaiting(const TMergeJobPtr& job, IJobControllerCallbacks* callbacks) override;
    void OnJobRunning(const TMergeJobPtr& job, IJobControllerCallbacks* callbacks) override;

    void OnJobCompleted(const TMergeJobPtr& job) override;
    void OnJobAborted(const TMergeJobPtr& job) override;
    void OnJobFailed(const TMergeJobPtr& job) override;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    IChunkReplacerCallbacksPtr ChunkReplacerCallbacks_;

    NConcurrency::TPeriodicExecutorPtr ScheduleExecutor_;
    NConcurrency::TPeriodicExecutorPtr ChunkCreatorExecutor_;
    NConcurrency::TPeriodicExecutorPtr StartTransactionExecutor_;
    NConcurrency::TPeriodicExecutorPtr FinalizeSessionExecutor_;

    TJobEpoch JobEpoch_ = InvalidJobEpoch;

    bool Enabled_ = false;

    THashMap<NCypressClient::TObjectId, EChunkMergerStatus> NodeToChunkMergerStatus_;

    // Persistent fields.
    NTransactionServer::TTransactionRotator TransactionRotator_;
    THashMap<NCypressClient::TObjectId, NSecurityServer::TAccountId> NodesBeingMerged_;
    THashMap<NSecurityServer::TAccountId, int> NodesBeingMergedPerAccount_;

    i64 ConfigVersion_ = 0;

    struct TChunkMergerStatistics
    {
        i64 ChunkReplacementsSucceeded = 0;
        i64 ChunkReplacementsFailed = 0;
        i64 ChunkCountSaving = 0;

        TChunkMergerViolatedCriteriaStatistics ViolatedCriteria;
    };
    THashMap<NCypressClient::TObjectId, TChunkMergerStatistics> AccountToChunkMergerStatistics_;

    TEnumIndexedArray<NChunkClient::EChunkMergerMode, i64> CompletedJobCountPerMode_;
    i64 AutoMergeFallbackJobCount_ = 0;

    THashMap<NCypressClient::TObjectId, TChunkMergerSession> RunningSessions_;

    // COMPAT(vovamelnikov): ChunkMergerQueuesUsagePerAccount
    // It is only used to store old version of NodesBeingMerged
    // until accountId will be restored from loaded chunks in OnAfterSnapshotLoaded.
    std::unique_ptr<THashSet<NCypressClient::TObjectId>> OldNodesBeingMerged_;

    // TODO(shakurov): ephemeral ptrs?
    using TNodeQueue = std::queue<NCypressClient::TObjectId>;
    // Per-account queue. All touched tables start here.
    THashMap<NObjectServer::TEphemeralObjectPtr<NSecurityServer::TAccount>, TNodeQueue> AccountToNodeQueue_;

    // After traversal, before creating chunks. We want to batch chunk creation,
    // so we do not create them right away.
    std::queue<TMergeJobInfo> JobsAwaitingChunkCreation_;

    // Chunk creation in progress. Stores i64 -> TMergeJobInfo to find the right TMergeJobInfo
    // after creating chunk.
    THashMap<TJobId, TMergeJobInfo> JobsUndergoingChunkCreation_;

    // After creating chunks, before scheduling (waiting for node heartbeat to schedule jobs).
    std::queue<TMergeJobInfo> JobsAwaitingNodeHeartbeat_;

    // Already merged nodes waiting to be erased from NodesBeingMerged_.
    struct TMergeSessionResult
    {
        NCypressClient::TObjectId NodeId;
        EMergeSessionResult Result;
        TChunkMergerTraversalStatistics TraversalStatistics;
        int JobCount = 0;
        NSecurityServer::TAccountId AccountId;
        TInstant SessionCreationTime;
    };
    std::queue<TMergeSessionResult> SessionsAwaitingFinalization_;

    struct TAccountQueuesUsage
    {
        int JobsAwaitingChunkCreation = 0;
        int JobsUndergoingChunkCreation = 0;
        int JobsAwaitingNodeHeartbeat = 0;

        bool operator==(const TAccountQueuesUsage& another) const = default;
    };

    THashMap<NSecurityServer::TAccountId, TAccountQueuesUsage> QueuesUsage_;

    // COMPAT(aleksandra-zh)
    bool NeedRestorePersistentStatistics_ = false;

    THashMap<NSecurityServer::TAccountId, std::vector<TDuration>> AccountIdToNodeMergeDurations_;
    THashMap<NObjectClient::TObjectId, int> NodeToRescheduleCountAfterMaxBackoffDelay_;
    THashMap<NSecurityServer::TAccountId, THashSet<NObjectClient::TObjectId>> AccountIdToStuckNodes_;
    THashMap<NObjectClient::TObjectId, TDuration> NodeToBackoffPeriod_;

    void IncrementTracker(int TAccountQueuesUsage::* queue, NSecurityServer::TAccountId accountId);
    void DecrementTracker(int TAccountQueuesUsage::* queue, NSecurityServer::TAccountId accountId);

    void IncrementPersistentTracker(NSecurityServer::TAccountId accountId);
    void DecrementPersistentTracker(NSecurityServer::TAccountId accountId);

    void OnLeaderActive() override;
    void OnStopLeading() override;

    void RescheduleMerge(NCypressClient::TObjectId nodeId, NSecurityClient::TAccountId accountId);

    void RegisterSession(TChunkOwnerBase* chunkOwner);
    void DoRegisterSession(TChunkOwnerBase* chunkOwner);
    void RegisterSessionTransient(TChunkOwnerBase* chunkOwner);
    void FinalizeJob(
        TMergeJobInfo jobInfo,
        EMergeSessionResult result);

    void RegisterJobAwaitingChunkCreation(
        TJobId jobId,
        NChunkClient::EChunkMergerMode mode,
        int jobIndex,
        NCypressClient::TObjectId nodeId,
        TChunkListId parentChunkListId,
        std::vector<TChunkId> inputChunkIds,
        NSecurityServer::TAccountId accountId) override;

    void OnTraversalFinished(
        NCypressClient::TObjectId nodeId,
        EMergeSessionResult result,
        TChunkMergerTraversalStatistics traversalStatistics) override;

    void ScheduleSessionFinalization(NCypressClient::TObjectId nodeId, EMergeSessionResult result);
    void FinalizeSessions();

    void FinalizeReplacement(
        NCypressClient::TObjectId nodeId,
        TChunkListId chunkListId,
        EMergeSessionResult result);

    void Clear() override;

    void ResetTransientState();

    bool IsMergeTransactionAlive() const;

    bool CanScheduleMerge(TChunkOwnerBase* chunkOwner) const;

    void StartMergeTransaction();

    void OnTransactionFinished(NTransactionServer::TTransaction* transaction);

    bool CanAdvanceNodeInMergePipeline();

    void ProcessTouchedNodes();

    void CreateChunks();

    bool TryScheduleMergeJob(
        IJobSchedulingContext* context,
        const TMergeJobInfo& jobInfo);

    void ScheduleReplaceChunks(
        NCypressClient::TObjectId nodeId,
        TChunkListId parentChunkListId,
        NCypressClient::TObjectId accountId,
        std::vector<TMergeJobInfo>* jobInfos);

    void OnJobFinished(const TMergeJobPtr& job);

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const;
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);

    TChunkOwnerBase* FindChunkOwner(NCypressClient::TObjectId nodeId);

    void DisableChunkMerger();
    void GuardedDisableChunkMerger();

    void ValidateStatistics(
        NCypressClient::TObjectId nodeId,
        const TChunkOwnerDataStatistics& oldStatistics,
        const TChunkOwnerDataStatistics& newStatistics);

    void RemoveNodeFromRescheduleMaps(NSecurityServer::TAccountId accountId, NCypressClient::TNodeId nodeId);

    void HydraCreateChunks(NProto::TReqCreateChunks* request);
    void HydraReplaceChunks(NProto::TReqReplaceChunks* request);
    void HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* request);
    void HydraFinalizeChunkMergeSessions(NProto::TReqFinalizeChunkMergeSessions* request);
    void HydraRescheduleMerge(NProto::TReqRescheduleMerge* request);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // COMPAT(vovamelnikov): ChunkMergerQueuesUsagePerAccount
    // It is only used to restore accountId from old snapshot.
    void OnAfterSnapshotLoaded() override;
};

DEFINE_REFCOUNTED_TYPE(TChunkMerger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
