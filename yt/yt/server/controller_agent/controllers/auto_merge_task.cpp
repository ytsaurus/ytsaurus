#include "auto_merge_task.h"

#include "auto_merge_director.h"
#include "data_flow_graph.h"
#include "job_info.h"
#include "task_host.h"

#include <yt/server/controller_agent/config.h>
#include <yt/server/controller_agent/job_size_constraints.h>

#include <yt/server/lib/chunk_pools/multi_chunk_pool.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TAutoMergeChunkPoolAdapter::TAutoMergeChunkPoolAdapter(
    IChunkPoolInputPtr underlyingInput,
    TAutoMergeTask* task)
    : TChunkPoolInputAdapterBase(std::move(underlyingInput))
    , Task_(task)
{ }

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    // We perform an in-place filtration of all large chunks.
    Task_->GetTaskHost()->GetAutoMergeDirector()->AccountMergeInputChunks(stripe->GetChunkCount() /* intermediateChunkCount */);
    Task_->CurrentChunkCount_ += stripe->GetChunkCount();

    auto cookie = TChunkPoolInputAdapterBase::AddWithKey(stripe, key);
    if (CookieChunkCount_.size() <= cookie) {
        CookieChunkCount_.resize(cookie + 1);
    }
    CookieChunkCount_[cookie] = stripe->GetChunkCount();
    return cookie;
}

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::Add(TChunkStripePtr stripe)
{
    return AddWithKey(stripe, TChunkStripeKey());
}

void TAutoMergeChunkPoolAdapter::Suspend(TCookie cookie)
{
    Task_->GetTaskHost()->GetAutoMergeDirector()->AccountMergeInputChunks(-CookieChunkCount_[cookie]);
    Task_->CurrentChunkCount_ -= CookieChunkCount_[cookie];

    TChunkPoolInputAdapterBase::Suspend(cookie);
}

void TAutoMergeChunkPoolAdapter::Persist(const TPersistenceContext& context)
{
    TChunkPoolInputAdapterBase::Persist(context);

    using NYT::Persist;

    Persist(context, Task_);
    Persist(context, CookieChunkCount_);
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter);

////////////////////////////////////////////////////////////////////////////////

TAutoMergeTask::TAutoMergeTask(
    ITaskHostPtr taskHost,
    int maxChunksPerJob,
    i64 chunkSizeThreshold,
    i64 dataWeightPerJob,
    i64 maxDataWeightPerJob,
    std::vector<TStreamDescriptor> streamDescriptors)
    : TTask(taskHost, std::move(streamDescriptors))
{
    std::vector<IChunkPoolPtr> underlyingPools;
    underlyingPools.reserve(StreamDescriptors_.size());
    for (int poolIndex = 0; poolIndex < StreamDescriptors_.size(); ++poolIndex) {
        auto autoMergeJobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /* canAdjustDataSizePerJob */,
            false /* isExplicitJobCount */,
            1 /* jobCount */,
            dataWeightPerJob /* dataWeightPerJob */,
            std::numeric_limits<i64>::max() / 4 /* primaryDataWeightPerJob */,
            maxChunksPerJob /* maxDataSlicesPerJob */,
            std::numeric_limits<i64>::max() / 4 /* maxDataWeightPerJob */,
            std::numeric_limits<i64>::max() / 4 /* primaryMaxDataWeightPerJob */,
            std::numeric_limits<i64>::max() / 4 /* inputSliceDataSize */,
            std::numeric_limits<i64>::max() / 4 /* inputSliceRowCount */,
            0 /* foreignSliceDataWeight */,
            std::nullopt /* samplingRate */);

        TUnorderedChunkPoolOptions options;
        options.RowBuffer = TaskHost_->GetRowBuffer();
        options.Mode = EUnorderedChunkPoolMode::AutoMerge;
        options.JobSizeConstraints = std::move(autoMergeJobSizeConstraints);
        options.MinTeleportChunkDataWeight = 0.5 * maxDataWeightPerJob;
        options.MinTeleportChunkSize = chunkSizeThreshold;
        options.OperationId = TaskHost_->GetOperationId();
        options.Name = Format("%v(%v)", GetTitle(), poolIndex);

        underlyingPools.emplace_back(CreateUnorderedChunkPool(
            std::move(options),
            TeleportableIntermediateInputStreamDirectory));
    }

    auto chunkPool = CreateMultiChunkPool(std::move(underlyingPools));
    chunkPool->Finalize();
    ChunkPool_ = std::move(chunkPool);

    ChunkPool_->SubscribeChunkTeleported(BIND(&TAutoMergeTask::OnChunkTeleported, MakeWeak(this)));

    TaskHost_->GetDataFlowGraph()
        ->RegisterCounter(GetVertexDescriptor(), ChunkPool_->GetJobCounter(), GetJobType());

    ChunkPoolInput_ = New<TAutoMergeChunkPoolAdapter>(
        ChunkPool_,
        this);

    // Tentative trees are not allowed for auto-merge jobs since they are genuinely IO-bound.
    TentativeTreeEligibility_.Disable();
}

TString TAutoMergeTask::GetTitle() const
{
    return Format("AutoMerge");
}

TDataFlowGraph::TVertexDescriptor TAutoMergeTask::GetVertexDescriptor() const
{
    return "auto_merge";
}

TExtendedJobResources TAutoMergeTask::GetNeededResources(const TJobletPtr& joblet) const
{
    auto result = TaskHost_->GetAutoMergeResources(joblet->InputStripeList->GetStatistics());
    AddFootprintAndUserJobResources(result);
    return result;
}

NChunkPools::IChunkPoolInputPtr TAutoMergeTask::GetChunkPoolInput() const
{
    return ChunkPoolInput_;
}

NChunkPools::IChunkPoolOutputPtr TAutoMergeTask::GetChunkPoolOutput() const
{
    return ChunkPool_;
}

EJobType TAutoMergeTask::GetJobType() const
{
    return EJobType::UnorderedMerge;
}

std::optional<EScheduleJobFailReason> TAutoMergeTask::GetScheduleFailReason(ISchedulingContext* /* context */)
{
    return CanScheduleJob_ ? std::nullopt : std::make_optional(EScheduleJobFailReason::TaskRefusal);
}

int TAutoMergeTask::GetPendingJobCount() const
{
    return CanScheduleJob_ ? TTask::GetPendingJobCount() : 0;
}

TExtendedJobResources TAutoMergeTask::GetMinNeededResourcesHeavy() const
{
    auto result = TaskHost_->GetAutoMergeResources(
        ChunkPool_->GetApproximateStripeStatistics());
    AddFootprintAndUserJobResources(result);
    return result;
}

void TAutoMergeTask::BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec)
{
    auto poolIndex = *joblet->InputStripeList->PartitionTag;
    jobSpec->CopyFrom(TaskHost_->GetAutoMergeJobSpecTemplate(GetTableIndex(poolIndex)));
    AddSequentialInputSpec(jobSpec, joblet);
    AddOutputTableSpecs(jobSpec, joblet);
}

bool TAutoMergeTask::IsJobInterruptible() const
{
    return false;
}

void TAutoMergeTask::UpdateSelf()
{
    CanScheduleJob_ = TaskHost_->GetAutoMergeDirector()->CanScheduleMergeJob(CurrentChunkCount_) ||
        ChunkPool_->GetJobCounter()->GetPending() > 1;

    if (CanScheduleJob_) {
        TaskHost_->UpdateTask(this);
    }

    // TODO(gritukan): Rethink it after YT-13373.
    // YT_VERIFY(!(CanScheduleJob_ && GetPendingJobCount() == 0 && CurrentChunkCount_ > 0));
}

void TAutoMergeTask::OnJobStarted(TJobletPtr joblet)
{
    TTask::OnJobStarted(joblet);

    CurrentChunkCount_ -= joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobStarted();
}

TJobFinishedResult TAutoMergeTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    auto result = TTask::OnJobAborted(joblet, jobSummary);

    CurrentChunkCount_ += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(0 /* unregisteredIntermediateChunkCount */);

    return result;
}

TJobFinishedResult TAutoMergeTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    auto result = TTask::OnJobCompleted(joblet, jobSummary);

    // Deciding what to do with these chunks is up to controller.
    // It may do nothing with these chunks, release them immediately
    // or release after next snapshot built but it should eventually
    // discount them in auto merge director.
    TaskHost_->ReleaseIntermediateStripeList(joblet->InputStripeList);

    RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

    return result;
}

TJobFinishedResult TAutoMergeTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    auto result = TTask::OnJobFailed(joblet, jobSummary);

    CurrentChunkCount_ += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(0 /* unregisteredIntermediateChunkCount */);

    return result;
}

void TAutoMergeTask::SetupCallbacks()
{
    TTask::SetupCallbacks();

    TaskHost_->GetAutoMergeDirector()->SubscribeStateChanged(BIND(&TAutoMergeTask::UpdateSelf, MakeWeak(this)));
}

bool TAutoMergeTask::IsCompleted() const
{
    return TaskHost_->GetAutoMergeDirector()->IsTaskCompleted() && TTask::IsCompleted();
}

void TAutoMergeTask::Persist(const TPersistenceContext& context)
{
    TTask::Persist(context);

    using NYT::Persist;

    Persist(context, ChunkPool_);
    Persist(context, ChunkPoolInput_);
    Persist(context, CurrentChunkCount_);

    ChunkPool_->SubscribeChunkTeleported(BIND(&TAutoMergeTask::OnChunkTeleported, MakeWeak(this)));
}

void TAutoMergeTask::OnChunkTeleported(TInputChunkPtr teleportChunk, std::any tag)
{
    TTask::OnChunkTeleported(teleportChunk, tag);

    auto poolIndex = std::any_cast<int>(tag);
    TaskHost_->RegisterTeleportChunk(std::move(teleportChunk), /*key=*/0, GetTableIndex(poolIndex));
    --CurrentChunkCount_;
}

void TAutoMergeTask::SetStreamDescriptors(TJobletPtr joblet) const
{
    auto poolIndex = *joblet->InputStripeList->PartitionTag;
    joblet->StreamDescriptors = {StreamDescriptors_[poolIndex]};
}

int TAutoMergeTask::GetTableIndex(int poolIndex) const
{
    return *StreamDescriptors_[poolIndex].PartitionTag;
}

TJobSplitterConfigPtr TAutoMergeTask::GetJobSplitterConfig() const
{
    auto config = TaskHost_->GetJobSplitterConfigTemplate();

    // TODO(gritukan): YT-13646.
    config->EnableJobSplitting = false;

    return config;
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
