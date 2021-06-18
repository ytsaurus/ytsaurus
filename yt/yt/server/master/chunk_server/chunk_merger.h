#pragma once

#include "private.h"
#include "chunk_replacer.h"

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

struct TMergeJobInfo
{
    TJobId JobId;

    NCypressServer::TNodeId NodeId;
    TChunkListId RootChunkListId;

    std::vector<TChunkId> InputChunkIds;
    TChunkId OutputChunkId;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkMerger
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TChunkMerger(NCellMaster::TBootstrap* bootstrap);

    void ScheduleMerge(NCypressServer::TNodeId nodeId);
    void ScheduleMerge(TChunkOwnerBase* trunkNode);

    void ScheduleJobs(
        TNode* node,
        NNodeTrackerClient::NProto::TNodeResources* resourceUsage,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        std::vector<TJobPtr>* jobsToStart);

    void ProcessJobs(const std::vector<TJobPtr>& jobs);

    void SetJobTracker(TJobTrackerPtr jobTracker);

    void OnProfiling(NProfiling::TSensorBuffer* buffer) const;

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NCellMaster::TBootstrap* const Bootstrap_;

    TChunkReplacer ChunkReplacer_;

    TJobTrackerPtr JobTracker_;

    NConcurrency::TPeriodicExecutorPtr ScheduleExecutor_;
    NConcurrency::TPeriodicExecutorPtr ChunkCreatorExecutor_;
    NConcurrency::TPeriodicExecutorPtr StartTransactionExecutor_;

    bool Enabled_ = false;

    TTransactionId TransactionId_;
    TTransactionId PreviousTransactionId_;

    i64 ChunkReplacementSucceded_ = 0;
    i64 ChunkReplacementFailed_ = 0;
    i64 ChunkCountSaving_ = 0;

    const TCallback<void(NTransactionServer::TTransaction* transaction)> TransactionAbortedCallback_ =
        BIND(&TChunkMerger::OnTransactionAborted, MakeWeak(this));

    // Per-account queue. All touched tables start here.
    // Keys (accounts) and  values (Cypress nodes) are locked ephemerally.
    using TNodeQueue = std::queue<TChunkOwnerBase*>;
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

    virtual void OnRecoveryComplete() override;
    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    virtual void Clear() override;

    void ResetTransientState();

    bool IsMergeTransactionAlive() const;

    bool CanScheduleMerge(TChunkOwnerBase* chunkOwner) const;

    void StartMergeTransaction();

    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);

    void DoScheduleMerge(TChunkOwnerBase* chunkOwner);

    void ProcessTouchedNodes();

    void CreateChunks();

    bool CreateMergeJob(TNode* node, const TMergeJobInfo& jobInfo, TJobPtr* job);

    void ScheduleReplaceChunks(const TMergeJobInfo& jobInfo);

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const;
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/ = nullptr);

    TChunkOwnerBase* FindChunkOwner(NCypressServer::TNodeId nodeId);

    void HydraCreateChunks(NProto::TReqCreateChunks* request);
    void HydraReplaceChunks(NProto::TReqReplaceChunks* request);
    void HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* request);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_REFCOUNTED_TYPE(TChunkMerger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
