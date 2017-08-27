#include "auto_merge_task.h"

#include "auto_merge_director.h"

#include "job_info.h"
#include "task_host.h"

#include <yt/ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TAutoMergeChunkPoolAdapter::TAutoMergeChunkPoolAdapter(
    IChunkPoolInput* underlyingInput,
    TAutoMergeTask* task)
    : TChunkPoolInputAdapterBase(underlyingInput)
    , Task_(task)
{ }

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::Add(TChunkStripePtr stripe, TChunkStripeKey key)
{
    // We perform an in-place filtration of all large chunks.
    int firstUnusedIndex = 0;
    for (auto& slice : stripe->DataSlices) {
        const auto& chunk = slice->GetSingleUnversionedChunkOrThrow();
        if (chunk->IsLargeCompleteChunk(Task_->GetDesiredChunkSize())) {
            Task_->RegisterTeleportChunk(chunk);
        } else {
            stripe->DataSlices[firstUnusedIndex++] = std::move(slice);
        }
    }
    stripe->DataSlices.resize(firstUnusedIndex);

    Task_->GetTaskHost()->GetAutoMergeDirector()->OnMergeInputProcessed(firstUnusedIndex /* intermediateChunkCount */);

    if (!stripe->DataSlices.empty()) {
        Task_->CurrentChunkCount_ += firstUnusedIndex;
        return TChunkPoolInputAdapterBase::Add(stripe, key);
    } else {
        return IChunkPoolInput::NullCookie;
    }
}

void TAutoMergeChunkPoolAdapter::Persist(const TPersistenceContext& context)
{
    TChunkPoolInputAdapterBase::Persist(context);

    using NYT::Persist;

    Persist(context, Task_);
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter);

////////////////////////////////////////////////////////////////////////////////

TAutoMergeTask::TAutoMergeTask(
    ITaskHostPtr taskHost,
    int tableIndex,
    int maxChunksPerJob,
    i64 desiredChunkSize,
    TEdgeDescriptor edgeDescriptor)
    : TTask(taskHost, {edgeDescriptor})
    , TableIndex_(tableIndex)
    , MaxChunksPerJob_(maxChunksPerJob)
    , DesiredChunkSize_(desiredChunkSize)
{
    auto autoMergeJobSizeConstraints = CreateExplicitJobSizeConstraints(
        false /* canAdjustDataSizePerJob */,
        false /* isExplicitJobCount */,
        1 /* jobCount */,
        DesiredChunkSize_ /* dataSizePerJob */,
        std::numeric_limits<i64>::max() /* primaryDataSizePerJob */,
        MaxChunksPerJob_ /* maxDataSlicesPerJob */,
        std::numeric_limits<i64>::max(),
        std::numeric_limits<i64>::max() /* inputSliceDataSize */,
        std::numeric_limits<i64>::max() /* inputSliceRowCount */);

    ChunkPool_ = CreateUnorderedChunkPool(
        autoMergeJobSizeConstraints,
        nullptr /* jobSizeAdjusterConfig */,
        EUnorderedChunkPoolMode::AutoMerge /* autoMergeMode */);

    ChunkPoolInput_ = std::make_unique<TAutoMergeChunkPoolAdapter>(ChunkPool_.get(), this);
}

TString TAutoMergeTask::GetId() const
{
    return Format("AutoMergeTask(%v)", TableIndex_);
}

TTaskGroupPtr TAutoMergeTask::GetGroup() const
{
    return TaskHost_->GetAutoMergeTaskGroup();
}

TDuration TAutoMergeTask::GetLocalityTimeout() const
{
    return TDuration();
}

TExtendedJobResources TAutoMergeTask::GetNeededResources(const TJobletPtr& joblet) const
{
    auto result = TaskHost_->GetAutoMergeResources(joblet->InputStripeList->GetStatistics());
    AddFootprintAndUserJobResources(result);
    return result;
}

NChunkPools::IChunkPoolInput* TAutoMergeTask::GetChunkPoolInput() const
{
    return ChunkPoolInput_.get();
}

NChunkPools::IChunkPoolOutput* TAutoMergeTask::GetChunkPoolOutput() const
{
    return ChunkPool_.get();
}

EJobType TAutoMergeTask::GetJobType() const
{
    return EJobType::UnorderedMerge;
}

bool TAutoMergeTask::CanScheduleJob(ISchedulingContext* /* context */, const TJobResources& /* jobLimits */)
{
    return CanScheduleJob_;
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
    jobSpec->CopyFrom(TaskHost_->GetAutoMergeJobSpecTemplate(TableIndex_));
    AddSequentialInputSpec(jobSpec, joblet);
    AddOutputTableSpecs(jobSpec, joblet);
}

void TAutoMergeTask::UpdateSelf()
{
    CanScheduleJob_ = TaskHost_->GetAutoMergeDirector()
        ->CanScheduleMergeJob(CurrentChunkCount_);
    if (CanScheduleJob_) {
        TaskHost_->AddTaskPendingHint(this);
    }
    YCHECK(!(CanScheduleJob_ && GetPendingJobCount() == 0 && CurrentChunkCount_ > 0));
}

void TAutoMergeTask::OnJobStarted(TJobletPtr joblet)
{
    TTask::OnJobStarted(joblet);

    CurrentChunkCount_ -= joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobStarted();
}

void TAutoMergeTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    TTask::OnJobAborted(joblet, jobSummary);

    CurrentChunkCount_ += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(0 /* unregisteredIntermediateChunkCount */);
}

void TAutoMergeTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    TTask::OnJobCompleted(joblet, jobSummary);

    for (const auto& stripe : joblet->InputStripeList->Stripes) {
        std::vector<NChunkClient::TChunkId> chunkIds;
        for (const auto& dataSlice : stripe->DataSlices) {
            chunkIds.emplace_back(dataSlice->GetSingleUnversionedChunkOrThrow()->ChunkId());
        }
        TaskHost_->UnstageChunkTreesNonRecursively(std::move(chunkIds));
    }

    RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(
        joblet->InputStripeList->TotalChunkCount /* unregisteredIntermediateChunkCount*/);
}

void TAutoMergeTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    TTask::OnJobFailed(joblet, jobSummary);

    CurrentChunkCount_ += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(0 /* unregisteredIntermediateChunkCount */);
}

i64 TAutoMergeTask::GetDesiredChunkSize() const
{
    return DesiredChunkSize_;
}

void TAutoMergeTask::RegisterTeleportChunk(NChunkClient::TInputChunkPtr chunk)
{
    TaskHost_->RegisterTeleportChunk(chunk, 0 /* key */, TableIndex_);
}

void TAutoMergeTask::SetupCallbacks()
{
    TTask::SetupCallbacks();

    TaskHost_->GetAutoMergeDirector()->SubscribeStateChanged(BIND(&TAutoMergeTask::UpdateSelf, MakeWeak(this)));
}

void TAutoMergeTask::Persist(const TPersistenceContext& context)
{
    TTask::Persist(context);

    using NYT::Persist;

    Persist(context, ChunkPool_);
    Persist(context, ChunkPoolInput_);
    Persist(context, TableIndex_);
    Persist(context, CurrentChunkCount_);
    Persist(context, MaxChunksPerJob_);
    Persist(context, DesiredChunkSize_);
    Persist(context, CanScheduleJob_);
}

bool TAutoMergeTask::SupportsInputPathYson() const
{
    return false;
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT