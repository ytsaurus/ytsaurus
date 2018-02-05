#include "auto_merge_task.h"

#include "auto_merge_director.h"

#include "job_info.h"
#include "task_host.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TAutoMergeChunkPoolAdapter::TAutoMergeChunkPoolAdapter(
    IChunkPoolInput* underlyingInput,
    TAutoMergeTask* task,
    i64 chunkSizeThreshold,
    i64 maxDataWeightPerJob)
    : TChunkPoolInputAdapterBase(underlyingInput)
    , Task_(task)
    , ChunkSizeThreshold_(chunkSizeThreshold)
    , MaxDataWeightPerJob_(maxDataWeightPerJob)
{ }

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    ProcessStripe(stripe, true /* teleportLargeChunks */);

    if (stripe->DataSlices.empty()) {
        return IChunkPoolInput::NullCookie;
    } else {
        auto cookie = TChunkPoolInputAdapterBase::AddWithKey(stripe, key);
        if (CookieChunkCount_.size() <= cookie) {
            CookieChunkCount_.resize(cookie + 1);
        }
        CookieChunkCount_[cookie] = stripe->GetChunkCount();
        return cookie;
    }
}

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::Add(TChunkStripePtr stripe)
{
    return AddWithKey(stripe, TChunkStripeKey());
}

void TAutoMergeChunkPoolAdapter::Resume(TCookie cookie, NChunkPools::TChunkStripePtr stripe)
{
    ProcessStripe(stripe, false /* teleportLargeChunks */);

    CookieChunkCount_[cookie] = stripe->GetChunkCount();
    return TChunkPoolInputAdapterBase::Resume(cookie, stripe);
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
    Persist(context, ChunkSizeThreshold_);

    // COMPAT(max42)
    if (context.GetVersion() >= 201999) {
        Persist(context, MaxDataWeightPerJob_);
    }

    Persist(context, CookieChunkCount_);
}

void TAutoMergeChunkPoolAdapter::ProcessStripe(const TChunkStripePtr& stripe, bool teleportLargeChunks) const
{
    // We perform an in-place filtration of all large chunks.
    int firstUnusedIndex = 0;
    for (const auto& slice : stripe->DataSlices) {
        const auto& chunk = slice->GetSingleUnversionedChunkOrThrow();
        if (chunk->IsLargeCompleteChunk(ChunkSizeThreshold_) ||
            chunk->GetDataWeight() >= 0.5 * MaxDataWeightPerJob_)
        {
            if (teleportLargeChunks) {
                Task_->RegisterTeleportChunk(chunk);
            } else {
                // NB: If we process resumed stripe, we should not teleport its large chunks.
                // Otherwise they would appear twice in the output.
                // Do nothing.
            }
        } else {
            stripe->DataSlices[firstUnusedIndex++] = std::move(slice);
        }
    }
    stripe->DataSlices.resize(firstUnusedIndex);

    Task_->GetTaskHost()->GetAutoMergeDirector()->AccountMergeInputChunks(firstUnusedIndex /* intermediateChunkCount */);
    Task_->CurrentChunkCount_ += firstUnusedIndex;
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter);

////////////////////////////////////////////////////////////////////////////////

TAutoMergeTask::TAutoMergeTask(
    ITaskHostPtr taskHost,
    int tableIndex,
    int maxChunksPerJob,
    i64 chunkSizeThreshold,
    i64 desiredChunkSize,
    i64 dataWeightPerJob,
    i64 maxDataWeightPerJob,
    TEdgeDescriptor edgeDescriptor)
    : TTask(taskHost, {edgeDescriptor})
    , TableIndex_(tableIndex)
{
    auto autoMergeJobSizeConstraints = CreateExplicitJobSizeConstraints(
        false /* canAdjustDataSizePerJob */,
        false /* isExplicitJobCount */,
        1 /* jobCount */,
        dataWeightPerJob /* dataWeightPerJob */,
        std::numeric_limits<i64>::max() /* primaryDataSizePerJob */,
        maxChunksPerJob /* maxDataSlicesPerJob */,
        std::numeric_limits<i64>::max(),
        std::numeric_limits<i64>::max() /* inputSliceDataSize */,
        std::numeric_limits<i64>::max() /* inputSliceRowCount */);

    ChunkPool_ = CreateUnorderedChunkPool(
        autoMergeJobSizeConstraints,
        nullptr /* jobSizeAdjusterConfig */,
        EUnorderedChunkPoolMode::AutoMerge /* autoMergeMode */);
    TaskHost_->GetDataFlowGraph()->RegisterTask(GetVertexDescriptor(), ChunkPool_->GetJobCounter(), GetJobType());

    ChunkPoolInput_ = std::make_unique<TAutoMergeChunkPoolAdapter>(
        ChunkPool_.get(),
        this,
        chunkSizeThreshold,
        maxDataWeightPerJob);
}

TString TAutoMergeTask::GetTitle() const
{
    return Format("AutoMerge(%v)", TableIndex_);
}

TDataFlowGraph::TVertexDescriptor TAutoMergeTask::GetVertexDescriptor() const
{
    return "auto_merge";
}

TTaskGroupPtr TAutoMergeTask::GetGroup() const
{
    return TaskHost_->GetAutoMergeTaskGroup();
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

TNullable<EScheduleJobFailReason> TAutoMergeTask::GetScheduleFailReason(ISchedulingContext* /* context */, const TJobResources& /* jobLimits */)
{
    return MakeNullable(!CanScheduleJob_, EScheduleJobFailReason::TaskRefusal);
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

    // Deciding what to do with these chunks is up to controller.
    // It may do nothing with these chunks, release them immediately
    // or release after next snapshot built but it should eventually
    // discount them in auto merge director.
    TaskHost_->ReleaseIntermediateStripeList(joblet->InputStripeList);

    RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);
}

void TAutoMergeTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    TTask::OnJobFailed(joblet, jobSummary);

    CurrentChunkCount_ += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(0 /* unregisteredIntermediateChunkCount */);
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
}

bool TAutoMergeTask::SupportsInputPathYson() const
{
    return false;
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
