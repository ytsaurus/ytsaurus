#include "auto_merge_task.h"

#include "auto_merge_director.h"

#include "job_info.h"
#include "task_host.h"
#include "job_size_constraints.h"

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
    TAutoMergeTask* task)
    : TChunkPoolInputAdapterBase(underlyingInput)
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
    int tableIndex,
    int maxChunksPerJob,
    i64 chunkSizeThreshold,
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
        std::numeric_limits<i64>::max() /* inputSliceRowCount */,
        Null /* samplingRate */);

    TUnorderedChunkPoolOptions options;
    options.Mode = EUnorderedChunkPoolMode::AutoMerge;
    options.JobSizeConstraints = std::move(autoMergeJobSizeConstraints);
    options.MinTeleportChunkDataWeight = 0.5 * maxDataWeightPerJob;
    options.MinTeleportChunkSize = chunkSizeThreshold;
    options.OperationId = TaskHost_->GetOperationId();
    options.Task = GetTitle();

    ChunkPool_ = CreateUnorderedChunkPool(
        std::move(options),
        TeleportableIntermediateInputStreamDirectory);
    TaskHost_->GetDataFlowGraph()
        ->RegisterCounter(GetVertexDescriptor(), ChunkPool_->GetJobCounter(), GetJobType());

    ChunkPoolInput_ = std::make_unique<TAutoMergeChunkPoolAdapter>(
        ChunkPool_.get(),
        this);

    // Tentative trees are not allowed for auto-merge jobs since they are genuinely IO-bound.
    TentativeTreeEligibility_.Disable();
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

TNullable<EScheduleJobFailReason> TAutoMergeTask::GetScheduleFailReason(ISchedulingContext* /* context */)
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
    RegisterNewTeleportChunks();
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

void TAutoMergeTask::RegisterNewTeleportChunks()
{
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();
    while (RegisteredTeleportChunkCount_ < teleportChunks.size()) {
        TaskHost_->RegisterTeleportChunk(teleportChunks[RegisteredTeleportChunkCount_++], 0 /* key */, TableIndex_);
        --CurrentChunkCount_;
    }
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
    Persist(context, RegisteredTeleportChunkCount_);
}

bool TAutoMergeTask::SupportsInputPathYson() const
{
    return false;
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
