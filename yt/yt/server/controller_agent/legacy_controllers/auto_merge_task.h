#pragma once

#include "private.h"

#include "chunk_pool_adapters.h"
#include "task.h"

#include <yt/server/lib/legacy_chunk_pools/unordered_chunk_pool.h>

namespace NYT::NControllerAgent::NLegacyControllers {

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeChunkPoolAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    //! Used only for persistence.
    TAutoMergeChunkPoolAdapter() = default;

    TAutoMergeChunkPoolAdapter(
        NLegacyChunkPools::IChunkPoolInputPtr underlyingInput,
        TAutoMergeTask* task);

    virtual NLegacyChunkPools::IChunkPoolInput::TCookie AddWithKey(
        NLegacyChunkPools::TChunkStripePtr stripe,
        NLegacyChunkPools::TChunkStripeKey key) override;

    virtual NLegacyChunkPools::IChunkPoolInput::TCookie Add(
        NLegacyChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie);

    void Persist(const TPersistenceContext& context);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter, 0xfb888bad);

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
        int maxChunksPerJob,
        i64 chunkSizeThreshold,
        i64 dataWeightPerJob,
        i64 maxDataWeightPerJob,
        std::vector<TEdgeDescriptor> edgeDescriptors);

    virtual TString GetTitle() const override;
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;

    virtual TTaskGroupPtr GetGroup() const override;

    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override;

    virtual NLegacyChunkPools::IChunkPoolInputPtr GetChunkPoolInput() const override;

    virtual NLegacyChunkPools::IChunkPoolOutputPtr GetChunkPoolOutput() const override;

    virtual EJobType GetJobType() const override;

    virtual int GetPendingJobCount() const override;

    virtual std::optional<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context) override;

    virtual void OnJobStarted(TJobletPtr joblet) override;
    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    void RegisterNewTeleportChunks();

    virtual void SetupCallbacks() override;

    virtual bool IsCompleted() const override;

    void Persist(const TPersistenceContext& context);

protected:
    NScheduler::TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) override;
    bool IsJobInterruptible() const override;

    virtual void OnChunkTeleported(NChunkClient::TInputChunkPtr teleportChunk, std::any tag) override;

    virtual void SetEdgeDescriptors(TJobletPtr joblet) const override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask, 0x4ef99f1b);

    //! Multi chunk pool built over unordered chunk pools.
    NLegacyChunkPools::IChunkPoolPtr ChunkPool_;

    //! Input adapter built over multi chunk pool.
    NLegacyChunkPools::IChunkPoolInputPtr ChunkPoolInput_;

    int CurrentChunkCount_ = 0;

    // NB: this field is intentionally transient (otherwise automerge can stuck after loading from snapshot).
    bool CanScheduleJob_ = true;

    void UpdateSelf();

    int GetTableIndex(int poolIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NLegacyControllers

#define AUTO_MERGE_TASK_INL_H
#include "auto_merge_task-inl.h"
#undef AUTO_MERGE_TASK_INL_H
