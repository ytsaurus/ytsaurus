#pragma once

#include "private.h"
#include "chunk_replacer.h"

#include <yt/yt/server/master/chunk_server/proto/chunk_merger.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/library/profiling/producer.h>

#include <queue>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TMergeJobInfo
{
    std::vector<TChunk*> InputChunks;
    TChunkOwnerBase* Root;
    TChunkList* RootChunkList;

    i64 OutputChunkCounter;
    TChunkId OutputChunkId;

    TMergeJobInfo(TMergeJobInfo&&) = default;
    TMergeJobInfo(const TMergeJobInfo&) = delete;

    TMergeJobInfo(
        std::vector<TChunk*> inputChunks,
        TChunkOwnerBase* root,
        TChunkList* rootChunkList,
        i64 outputChunkCounter);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkMerger
    : public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TChunkMerger(NCellMaster::TBootstrap* bootstrap);

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

    i64 CreatedChunkCounter_ = 0;

    const TCallback<void(NTransactionServer::TTransaction* transaction)> TransactionAbortedCallback_ =
        BIND(&TChunkMerger::OnTransactionAborted, MakeWeak(this));

    // Per-account queue. All touched tables start here.
    THashMap<NSecurityServer::TAccount*, std::queue<TChunkOwnerBase*>> TouchedNodes_;

    // After traversal, before creating chunks. We want to batch chunk creation,
    // so we do not create them right away.
    std::queue<TMergeJobInfo> JobsAwaitingChunkCreation_;

    // Chunk creation in progress. Stores i64 -> TMergeJobInfo to find the right TMergeJobInfo
    // after creating chunk. This queue is transient, so we cannot rely on FIFO order
    THashMap<i64, TMergeJobInfo> JobsUndergoingChunkCreation_;

    // After creating chunks, before scheduling (waiting for node heartbeat to schedule jobs).
    std::queue<TMergeJobInfo> JobsAwaitingNodeHeartbeat_;

    // Scheduled jobs (waiting for node heartbeat with job result).
    THashMap<TJobId, TMergeJobInfo> RunningJobs_;

    virtual void OnRecoveryComplete() override;
    virtual void OnLeaderActive() override;
    virtual void OnStopLeading() override;

    bool IsMergeTransactionAlive() const;

    bool CanScheduleMerge(TChunkOwnerBase* root, TChunkList* rootChunkList) const;

    void StartMergeTransaction();

    void OnTransactionAborted(NTransactionServer::TTransaction* transaction);

    void DoScheduleMerge(TChunkOwnerBase* root);

    void ProcessTouchedNodes();

    void CreateChunks();

    bool CreateMergeJob(TNode* node, const TMergeJobInfo& jobInfo, TJobPtr* job);

    void ReplaceChunks(const TMergeJobInfo& jobInfo);

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const;
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr /*oldConfig*/ = nullptr);

    void HydraCreateChunks(NProto::TReqCreateChunks* request);
    void HydraReplaceChunks(NProto::TReqReplaceChunks* request);
    void HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* request);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_REFCOUNTED_TYPE(TChunkMerger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
