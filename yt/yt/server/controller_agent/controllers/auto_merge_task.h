#pragma once

#include "private.h"

#include "chunk_pool_adapters.h"
#include "task.h"

#include <yt/server/lib/chunk_pools/unordered_chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeInputChunkPoolAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    //! Used only for persistence.
    TAutoMergeInputChunkPoolAdapter() = default;

    TAutoMergeInputChunkPoolAdapter(
        NChunkPools::IChunkPoolInputPtr underlyingInput,
        TAutoMergeTask* task);

    virtual NChunkPools::IChunkPoolInput::TCookie AddWithKey(
        NChunkPools::TChunkStripePtr stripe,
        NChunkPools::TChunkStripeKey key) override;

    virtual NChunkPools::IChunkPoolInput::TCookie Add(
        NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie);

    void Persist(const TPersistenceContext& context);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeInputChunkPoolAdapter, 0xfb888bac);

    TAutoMergeTask* Task_;
    std::vector<int> CookieChunkCount_;
};

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeOutputChunkPoolAdapter
    : public TChunkPoolOutputAdapterBase
{
public:
    //! Used only for persistence.
    TAutoMergeOutputChunkPoolAdapter() = default;

    explicit TAutoMergeOutputChunkPoolAdapter(NChunkPools::IChunkPoolOutputPtr underlyingOutput);

    virtual const TProgressCounterPtr& GetJobCounter() const override;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;

    void SetShouldScheduleJob(bool shouldScheduleJob);

    void Persist(const TPersistenceContext& context);

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeOutputChunkPoolAdapter, 0xaf23bcf0);

    bool ShouldScheduleJob_ = false;

    TProgressCounterPtr JobCounter_ = New<TProgressCounter>();

    void SetupCallbacks();

    void UpdatePendingJobCount();
};

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeTask
    : public TTask
{
public:
    friend class TAutoMergeInputChunkPoolAdapter;

    //! Used only for persistense.
    TAutoMergeTask() = default;

    TAutoMergeTask(
        ITaskHostPtr taskHost,
        int maxChunksPerJob,
        i64 chunkSizeThreshold,
        i64 dataWeightPerJob,
        i64 maxDataWeightPerJob,
        std::vector<TStreamDescriptor> streamDescriptors);

    virtual TString GetTitle() const override;
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;

    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override;

    virtual NChunkPools::IChunkPoolInputPtr GetChunkPoolInput() const override;

    virtual NChunkPools::IChunkPoolOutputPtr GetChunkPoolOutput() const override;

    virtual EJobType GetJobType() const override;

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

    virtual void SetStreamDescriptors(TJobletPtr joblet) const override;

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask, 0x4ef99f1a);

    //! Multi chunk pool built over unordered chunk pools.
    NChunkPools::IChunkPoolPtr ChunkPool_;

    //! Input adapter built over multi chunk pool.
    NChunkPools::IChunkPoolInputPtr ChunkPoolInput_;

    TIntrusivePtr<TAutoMergeOutputChunkPoolAdapter> ChunkPoolOutput_;

    int CurrentChunkCount_ = 0;

    void UpdateSelf();

    int GetTableIndex(int poolIndex) const;
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

#define AUTO_MERGE_TASK_INL_H
#include "auto_merge_task-inl.h"
#undef AUTO_MERGE_TASK_INL_H
