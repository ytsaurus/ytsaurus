#pragma once

#include "private.h"
#include "chunk_replacer.h"
#include "job_controller.h"

#include <yt/yt/server/master/chunk_server/proto/chunk_merger.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/master/cypress_server/public.h>

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

struct TChunkMergerSession
{
    THashSet<TJobId> Jobs;
    EMergeSessionResult Result = EMergeSessionResult::None;
};

struct TMergeJobInfo
{
    TJobId JobId;

    NCypressClient::TObjectId NodeId;
    TChunkListId RootChunkListId;

    std::vector<TChunkId> InputChunkIds;
    TChunkId OutputChunkId;

    NChunkClient::EChunkMergerMode MergeMode;
};

////////////////////////////////////////////////////////////////////////////////

struct IMergeChunkVisitorHost
    : public virtual TRefCounted
{
    virtual void RegisterJobAwaitingChunkCreation(
        TJobId jobId,
        NChunkClient::EChunkMergerMode mode,
        NCypressClient::TObjectId nodeId,
        TChunkListId rootChunkListId,
        std::vector<TChunkId> inputChunkIds) = 0;
    virtual void OnTraversalFinished(
        NCypressClient::TObjectId nodeId,
        EMergeSessionResult result) = 0;
};

class TChunkMerger
    : public NCellMaster::TMasterAutomatonPart
    , public virtual IJobController
    , public virtual IMergeChunkVisitorHost
{
public:
    explicit TChunkMerger(NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    void ScheduleMerge(NCypressClient::TObjectId nodeId);
    void ScheduleMerge(TChunkOwnerBase* trunkNode);

    bool IsNodeBeingMerged(NCypressClient::TObjectId nodeId) const;

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;

    // IJobController implementation.
    virtual void ScheduleJobs(IJobSchedulingContext* context) override;

    virtual void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;
    virtual void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) override;

    virtual void OnJobCompleted(const TJobPtr& job) override;
    virtual void OnJobAborted(const TJobPtr& job) override;
    virtual void OnJobFailed(const TJobPtr& job) override;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NCellMaster::TBootstrap* const Bootstrap_;

    TChunkReplacer ChunkReplacer_;

    NConcurrency::TPeriodicExecutorPtr ScheduleExecutor_;
    NConcurrency::TPeriodicExecutorPtr ChunkCreatorExecutor_;
    NConcurrency::TPeriodicExecutorPtr StartTransactionExecutor_;
    NConcurrency::TPeriodicExecutorPtr FinalizeSessionExecutor_;

    bool Enabled_ = false;

    // Persistent fields.
    TTransactionId TransactionId_;
    TTransactionId PreviousTransactionId_;
    THashSet<NCypressClient::TObjectId> NodesBeingMerged_;

    i64 ChunkReplacementSucceded_ = 0;
    i64 ChunkReplacementFailed_ = 0;
    i64 ChunkCountSaving_ = 0;

    THashMap<NCypressClient::TObjectId, TChunkMergerSession> RunningSessions_;

    // Per-account queue. All touched tables start here.
    // Keys (accounts) are locked ephemerally.
    using TNodeQueue = std::queue<NCypressClient::TObjectId>;
    THashMap<NSecurityServer::TAccount*, TNodeQueue> AccountToNodeQueue_;

    // After traversal, before creating chunks. We want to batch chunk creation,
    // so we do not create them right away.
    std::queue<TMergeJobInfo> JobsAwaitingChunkCreation_;

    // Chunk creation in progress. Stores i64 -> TMergeJobInfo to find the right TMergeJobInfo
    // after creating chunk.
    THashMap<TJobId, TMergeJobInfo> JobsUndergoingChunkCreation_;

    // After creating chunks, before scheduling (waiting for node heartbeat to schedule jobs).
    std::queue<TMergeJobInfo> JobsAwaitingNodeHeartbeat_;

    // Scheduled jobs (waiting for node heartbeat with job result).
    THashMap<TJobId, TMergeJobInfo> RunningJobs_;

    // Already merged nodes waiting to be erased from NodesBeingMerged_.
    struct TMergeSessionResult
    {
        NCypressClient::TObjectId NodeId;
        EMergeSessionResult Result;
    };
    std::queue<TMergeSessionResult> SessionsAwaitingFinalizaton_;

    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    void RegisterSession(TChunkOwnerBase* chunkOwner);
    void RegisterSessionTransient(TChunkOwnerBase* chunkOwner);
    void FinalizeJob(NCypressClient::TObjectId nodeId, TJobId jobId, EMergeSessionResult result);

    virtual void RegisterJobAwaitingChunkCreation(
        TJobId jobId,
        NChunkClient::EChunkMergerMode mode,
        NCypressClient::TObjectId nodeId,
        TChunkListId rootChunkListId,
        std::vector<TChunkId> inputChunkIds) override;
    virtual void OnTraversalFinished(NCypressClient::TObjectId nodeId, EMergeSessionResult result) override;

    void ScheduleSessionFinalization(NCypressClient::TObjectId nodeId, EMergeSessionResult result);
    void FinalizeSessions();

    virtual void Clear() override;

    void ResetTransientState();

    bool IsMergeTransactionAlive() const;

    bool CanScheduleMerge(TChunkOwnerBase* chunkOwner) const;

    void StartMergeTransaction();

    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);

    void ProcessTouchedNodes();

    void CreateChunks();

    bool TryScheduleMergeJob(
        IJobSchedulingContext* context,
        const TMergeJobInfo& jobInfo);

    void ScheduleReplaceChunks(const TMergeJobInfo& jobInfo);

    void OnJobFinished(const TJobPtr& job);

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const;
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/ = nullptr);

    TChunkOwnerBase* FindChunkOwner(NCypressClient::TObjectId nodeId);

    void HydraCreateChunks(NProto::TReqCreateChunks* request);
    void HydraReplaceChunks(NProto::TReqReplaceChunks* request);
    void HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* request);
    void HydraFinalizeChunkMergeSessions(NProto::TReqFinalizeChunkMergeSessions* request);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_REFCOUNTED_TYPE(TChunkMerger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
