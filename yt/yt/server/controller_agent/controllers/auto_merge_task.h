#pragma once

#include "private.h"

#include "extended_job_resources.h"
#include "chunk_pool_adapters.h"
#include "task.h"

#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeChunkPoolAdapter
    : public TChunkPoolAdapterBase
{
public:
    //! Used only for persistence.
    TAutoMergeChunkPoolAdapter() = default;

    TAutoMergeChunkPoolAdapter(
        NChunkPools::IPersistentChunkPoolPtr underlyingPool,
        int poolIndex,
        TAutoMergeTask* task);

    virtual NChunkPools::IChunkPoolInput::TCookie AddWithKey(
        NChunkPools::TChunkStripePtr stripe,
        NChunkPools::TChunkStripeKey key) override;

    virtual NChunkPools::IChunkPoolInput::TCookie Add(
        NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override;

    virtual const TProgressCounterPtr& GetJobCounter() const override;

    virtual IChunkPoolOutput::TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;

    void SetShouldScheduleJob(bool shouldScheduleJob);

    void Persist(const TPersistenceContext& context) override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter, 0x54ab375c);

    TAutoMergeTask* Task_;
    std::vector<int> CookieChunkCount_;

    int PoolIndex_ = -1;

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
    friend class TAutoMergeChunkPoolAdapter;

    //! Used only for persistence.
    TAutoMergeTask() = default;

    TAutoMergeTask(
        ITaskHostPtr taskHost,
        int maxChunksPerJob,
        i64 chunkSizeThreshold,
        i64 dataWeightPerJob,
        i64 maxDataWeightPerJob,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    virtual TString GetTitle() const override;
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptorForJoblet(const TJobletPtr& joblet) const override;
    virtual TVertexDescriptorList GetAllVertexDescriptors() const override;

    virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override;

    virtual NChunkPools::IPersistentChunkPoolInputPtr GetChunkPoolInput() const override;

    virtual NChunkPools::IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override;

    virtual EJobType GetJobType() const override;
    void AddJobTypeToJoblet(const TJobletPtr& joblet) const override;

    virtual void OnJobStarted(TJobletPtr joblet) override;
    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override;
    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    void RegisterNewTeleportChunks();

    virtual void SetupCallbacks() override;

    virtual bool IsCompleted() const override;

    void Persist(const TPersistenceContext& context) override;

protected:
    TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, NControllerAgent::NProto::TJobSpec* jobSpec) override;
    bool IsJobInterruptible() const override;

    virtual void OnChunkTeleported(NChunkClient::TInputChunkPtr teleportChunk, std::any tag) override;

    virtual void SetStreamDescriptors(TJobletPtr joblet) const override;

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override;

    void DoRegisterInGraph() override;

    void UpdateInputEdges(
        const NChunkClient::NProto::TDataStatistics& dataStatistics,
        const TJobletPtr& joblet) override;
    void UpdateOutputEdgesForTeleport(const NChunkClient::NProto::TDataStatistics& dataStatistics) override;

private:
    using TJobSpec = NControllerAgent::NProto::TJobSpec;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask, 0x4ef99f1a);

    std::vector<TIntrusivePtr<TAutoMergeChunkPoolAdapter>> ChunkPools_;

    //! Multi chunk pool built over wrapped unordered chunk pools.
    NChunkPools::IPersistentChunkPoolPtr ChunkPool_;

    //! Output chunk pool index -> number of the intermediate chunks in it.
    std::vector<int> CurrentChunkCounts_;

    std::vector<TEnumIndexedVector<EMergeJobType, TJobSpec>> JobSpecTemplates_;

    TEnumIndexedVector<EMergeJobType, TProgressCounterPtr> FakeProgressCounters_;

    std::atomic<bool> EnableShallowMerge_;

    void UpdateSelf();

    int GetTableIndex(int poolIndex) const;

    const TJobSpec& GetJobSpecTemplate(int tableIndex, EMergeJobType type) const;
    void InitAutoMergeJobSpecTemplates();

    TDataFlowGraph::TVertexDescriptor GetVertexDescriptorForMergeType(EMergeJobType type) const;
    EMergeJobType GetMergeTypeFromJobType(EJobType jobType) const;
    EMergeJobType GetCurrentMergeType() const;
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeTask)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

#define AUTO_MERGE_TASK_INL_H
#include "auto_merge_task-inl.h"
#undef AUTO_MERGE_TASK_INL_H
