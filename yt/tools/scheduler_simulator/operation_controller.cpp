#include "operation_controller.h"
#include "operation.h"

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/server/controller_agent/scheduling_context.h>

namespace NYT {
namespace NSchedulerSimulator {

using namespace NControllerAgent;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

bool PrecedesImpl(const EJobType& lhs, const EJobType& rhs)
{
    using JT = EJobType;

    const static std::map<EJobType, std::set<EJobType>> precedenceTable = {
        { JT::Partition, { JT::PartitionReduce, JT::FinalSort, JT::SortedReduce, JT::SortedMerge, JT::UnorderedMerge, JT::IntermediateSort } },
        { JT::PartitionMap, { JT::PartitionReduce, JT::FinalSort, JT::IntermediateSort, JT::SortedReduce, JT::ReduceCombiner } },
        { JT::IntermediateSort, { JT::SortedMerge, JT::UnorderedMerge, JT::SortedReduce, JT::FinalSort } },
        { JT::FinalSort, { JT::SortedMerge, JT::UnorderedMerge, JT::SortedReduce } },
        { JT::Map, {JT::UnorderedMerge} },
        { JT::ReduceCombiner, {JT::PartitionReduce, JT::Partition} },
        { JT::SortedReduce, {JT::ReduceCombiner} },
        { JT::PartitionReduce, { JT::FinalSort, JT::IntermediateSort, JT::SortedReduce } },
        { JT::SortedMerge, { JT::UnorderedMerge, JT::SimpleSort } }};

    if (precedenceTable.find(lhs) == precedenceTable.end()) {
        return false;
    }
    return precedenceTable.at(lhs).find(rhs) != precedenceTable.at(lhs).end();
}

bool Precedes(const EJobType& lhs, const EJobType& rhs)
{
    if (lhs == rhs) {
        return false;
    }

    bool lhsPrecedsRhs = PrecedesImpl(lhs, rhs);
    bool rhsPrecedsLhs = PrecedesImpl(rhs, lhs);

    if (lhsPrecedsRhs ^ rhsPrecedsLhs) {
        return lhsPrecedsRhs;
    }

    if (!lhsPrecedsRhs && !rhsPrecedsLhs) {
        THROW_ERROR_EXCEPTION("Incomparable %v and %v", lhs, rhs);
    }

    if (lhsPrecedsRhs && rhsPrecedsLhs) {
        THROW_ERROR_EXCEPTION("Comparsion is not asymmetric for %v and %v", lhs, rhs);
    }

    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TOperationController::TOperationController(const TOperation* operation, const TOperationDescription* operationDescription)
    : Operation_(operation)
    , OperationDescription_(operationDescription)
    , NeededResources_(NScheduler::ZeroJobResources())
    , Logger("OperationController")
{
    auto jobs = operationDescription->JobDescriptions;
    std::sort(
        jobs.begin(),
        jobs.end(),
        [] (const TJobDescription& lhs, const TJobDescription& rhs) {
            return Precedes(lhs.Type, rhs.Type);
        });

    PendingJobs_.resize(1);
    for (const auto& job : jobs) {
        if (!PendingJobs_.back().empty() && PendingJobs_.back().back().Type != job.Type) {
            StageJobCounts_.push_back(PendingJobs_.back().size());
            PendingJobs_.resize(PendingJobs_.size() + 1);
        }
        PendingJobs_.back().push_back(job);
    }
    StageJobCounts_.push_back(PendingJobs_.back().size());

    SetStage(0);
}

int TOperationController::GetPendingJobCount() const
{
    return StagePendingJobCount_;
}

NScheduler::TJobResources TOperationController::GetNeededResources() const
{
    return NeededResources_;
}

void TOperationController::OnJobCompleted(std::unique_ptr<NControllerAgent::TCompletedJobSummary> jobSummary)
{
    StageCompletedJobCount_ += 1;
    CompletedJobCount_ += 1;
    RunningJobCount_ -= 1;

    if (StageCompletedJobCount_ == StageJobCounts_[CurrentStage_]) {
        SetStage(CurrentStage_ + 1);
    }
}

void TOperationController::OnNonscheduledJobAborted(const NScheduler::TJobId& jobId, NScheduler::EAbortReason)
{
    NeededResources_ += IdToDescription_[jobId].ResourceLimits;
    StagePendingJobCount_ += 1;
    RunningJobCount_ -= 1;
    AbortedJobCount_ += 1;
    PendingJobs_[CurrentStage_].push_back(IdToDescription_[jobId]);
}

bool TOperationController::IsOperationCompleted() const
{
    return OperationCompleted_;
}

TFuture<TScheduleJobResultPtr> TOperationController::ScheduleJob(
    const NScheduler::ISchedulingContextPtr& context,
    const NScheduler::TJobResourcesWithQuota& nodeLimits,
    const TString& /* treeId */)
{
    auto scheduleJobResult = New<TScheduleJobResult>();

    if (CurrentStage_ == PendingJobs_.size() || PendingJobs_[CurrentStage_].empty()) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NoPendingJobs);
        return MakeFuture(scheduleJobResult);
    }

    auto jobDescription = PendingJobs_[CurrentStage_].front();
    auto jobNeededResources = jobDescription.ResourceLimits;
    if (!NScheduler::Dominates(nodeLimits.ToJobResources(), jobNeededResources)) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
        return MakeFuture(scheduleJobResult);
    }

    auto jobId = TJobId::Create();
    scheduleJobResult->StartDescriptor.Emplace(
        jobId,
        jobDescription.Type,
        jobNeededResources,
        /* interruptible */ false);

    dynamic_cast<TSchedulingContext*>(context.Get())->SetDurationForStartedJob(jobDescription.Duration);
    IdToDescription_[jobId] = jobDescription;
    PendingJobs_[CurrentStage_].pop_front();
    NeededResources_ -= jobNeededResources;
    StagePendingJobCount_ -= 1;
    RunningJobCount_ += 1;

    return MakeFuture(scheduleJobResult);
}

void TOperationController::UpdateMinNeededJobResources()
{ }

NScheduler::TJobResourcesWithQuotaList TOperationController::GetMinNeededJobResources() const
{
    return {NScheduler::ZeroJobResourcesWithQuota()};
}

TString TOperationController::GetLoggingProgress() const
{
    return Format(
        "Stage: {T: %v, C: %v}, Jobs = {T: %v, R: %v, C: %v, P: %v, A: %v}",
        PendingJobs_.size(),
        CurrentStage_,
        OperationDescription_->JobDescriptions.size(),
        RunningJobCount_,
        CompletedJobCount_,
        GetPendingJobCount(),
        AbortedJobCount_);
}

void TOperationController::SetStage(int stage)
{
    CurrentStage_ = stage;
    if (stage == PendingJobs_.size()) {
        YCHECK(CompletedJobCount_ == OperationDescription_->JobDescriptions.size());
        OperationCompleted_ = true;
        return;
    }

    // Check that all jobs from previous stage are finished.
    YCHECK(NeededResources_ == NScheduler::ZeroJobResources());

    for (const auto& job : PendingJobs_[stage]) {
        NeededResources_ += job.ResourceLimits;
    }
    StagePendingJobCount_ = PendingJobs_[stage].size();
    StageCompletedJobCount_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
