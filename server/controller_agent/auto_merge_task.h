#pragma once

#include "private.h"

#include "chunk_pool_adapters.h"
#include "task.h"

#include <yt/server/chunk_pools/unordered_chunk_pool.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeChunkPoolAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    //! Used only for persistence.
    TAutoMergeChunkPoolAdapter() = default;

    TAutoMergeChunkPoolAdapter(
        NChunkPools::IChunkPoolInput* underlyingInput,
        TAutoMergeTask* task);

    virtual NChunkPools::IChunkPoolInput::TCookie AddWithKey(
        NChunkPools::TChunkStripePtr stripe,
        NChunkPools::TChunkStripeKey key) override;

    virtual NChunkPools::IChunkPoolInput::TCookie Add(
        NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie);

    void Persist(const TPersistenceContext& context);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter, 0xfb888bac);

    TAutoMergeTask* Task_;
    std::vector<int> CookieChunkCount_;
};

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeTask
    : public TTask
{
public:
    friend class TAutoMergeChunkPoolAdapter;

    //! Used only for persistense.
    TAutoMergeTask() = default;

    TAutoMergeTask(
        ITaskHostPtr taskHost,
        int tableIndex,
        int maxChunksPerJob,
        i64 chunkSizeThreshold,
        i64 dataWeightPerJob,
        i64 maxDataWeightPerJob,
        TEdgeDescriptor edgeDescriptor);

    virtual TString GetTitle() const override;
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;

    virtual TTaskGroupPtr GetGroup() const override;

    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override;

    virtual NChunkPools::IChunkPoolInput* GetChunkPoolInput() const override;

    virtual NChunkPools::IChunkPoolOutput* GetChunkPoolOutput() const override;

    virtual EJobType GetJobType() const override;

    virtual int GetPendingJobCount() const override;

    virtual std::optional<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context) override;

    virtual void OnJobStarted(TJobletPtr joblet) override;
    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    void RegisterNewTeleportChunks();

    virtual void SetupCallbacks() override;

    void Persist(const TPersistenceContext& context);

protected:
    NScheduler::TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask, 0x4ef99f1a);

    std::unique_ptr<NChunkPools::IChunkPool> ChunkPool_;
    std::unique_ptr<TAutoMergeChunkPoolAdapter> ChunkPoolInput_;

    int TableIndex_;
    int CurrentChunkCount_ = 0;
    int RegisteredTeleportChunkCount_ = 0;

    // NB: this field is intentionally transient (otherwise automerge can stuck after loading from snapshot).
    bool CanScheduleJob_ = true;

    void UpdateSelf();
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

#define AUTO_MERGE_TASK_INL_H
#include "auto_merge_task-inl.h"
#undef AUTO_MERGE_TASK_INL_H
