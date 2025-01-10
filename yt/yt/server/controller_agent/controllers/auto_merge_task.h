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

    void Suspend(IChunkPoolInput::TCookie cookie) override;

    const TProgressCounterPtr& GetJobCounter() const override;

    IChunkPoolOutput::TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;

    void SetShouldScheduleJob(bool shouldScheduleJob);

private:
    TAutoMergeTask* Task_;
    std::vector<int> CookieChunkCount_;

    int PoolIndex_ = -1;

    bool ShouldScheduleJob_ = false;

    TProgressCounterPtr JobCounter_ = New<TProgressCounter>();

    void SetupCallbacks();

    void UpdatePendingJobCount();

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TAutoMergeChunkPoolAdapter, 0x54ab375c);
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
        i64 maxChunkSize,
        i64 maxChunkDataWeight,
        i64 dataWeightPerJob,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    TString GetTitle() const override;
    TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;
    TDataFlowGraph::TVertexDescriptor GetVertexDescriptorForJoblet(const TJobletPtr& joblet) const override;
    TVertexDescriptorList GetAllVertexDescriptors() const override;

    TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override;

    NChunkPools::IPersistentChunkPoolInputPtr GetChunkPoolInput() const override;

    NChunkPools::IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override;

    EJobType GetJobType() const override;
    void AddJobTypeToJoblet(const TJobletPtr& joblet) const override;

    void OnJobStarted(TJobletPtr joblet) override;
    TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override;
    TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override;
    TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    void RegisterNewTeleportChunks();

    void SetupCallbacks() override;

    bool IsCompleted() const override;

protected:
    TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, NControllerAgent::NProto::TJobSpec* jobSpec) override;
    bool IsJobInterruptible() const override;

    void OnChunkTeleported(NChunkClient::TInputChunkPtr teleportChunk, std::any tag) override;

    void SetStreamDescriptors(TJobletPtr joblet) const override;

    TJobSplitterConfigPtr GetJobSplitterConfig() const override;

    void DoRegisterInGraph() override;

    void UpdateInputEdges(
        const NChunkClient::NProto::TDataStatistics& dataStatistics,
        const TJobletPtr& joblet) override;
    void UpdateOutputEdgesForTeleport(const NChunkClient::NProto::TDataStatistics& dataStatistics) override;

private:
    using TJobSpec = NControllerAgent::NProto::TJobSpec;

    std::vector<TIntrusivePtr<TAutoMergeChunkPoolAdapter>> ChunkPools_;

    //! Multi chunk pool built over wrapped unordered chunk pools.
    NChunkPools::IPersistentChunkPoolPtr ChunkPool_;

    //! Output chunk pool index -> number of the intermediate chunks in it.
    std::vector<int> CurrentChunkCounts_;

    std::vector<TEnumIndexedArray<EMergeJobType, TJobSpec>> JobSpecTemplates_;

    TEnumIndexedArray<EMergeJobType, TProgressCounterPtr> FakeProgressCounters_;

    std::atomic<bool> EnableShallowMerge_;

    void UpdateSelf();

    int GetTableIndex(int poolIndex) const;

    const TJobSpec& GetJobSpecTemplate(int tableIndex, EMergeJobType type) const;
    void InitAutoMergeJobSpecTemplates();

    TDataFlowGraph::TVertexDescriptor GetVertexDescriptorForMergeType(EMergeJobType type) const;
    EMergeJobType GetMergeTypeFromJobType(EJobType jobType) const;
    EMergeJobType GetCurrentMergeType() const;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TAutoMergeTask, 0x4ef99f1a);
};

DEFINE_REFCOUNTED_TYPE(TAutoMergeTask)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

#define AUTO_MERGE_TASK_INL_H
#include "auto_merge_task-inl.h"
#undef AUTO_MERGE_TASK_INL_H
