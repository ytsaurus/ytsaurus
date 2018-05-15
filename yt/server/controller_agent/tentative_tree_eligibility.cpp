#include "tentative_tree_eligibility.h"

#include "operation_controller.h"

#include <yt/server/scheduler/config.h>

namespace NYT {
namespace NControllerAgent {

using namespace NJobTrackerClient;
//using namespace NScheduler;

///////////////////////////////////////////////////////////////////////////////

TTentativeTreeEligibility::TTentativeTreeEligibility(
    const TTentativeTreeEligibilityConfigPtr& config)
    : Logger(ControllerAgentLogger)
    , SampleJobCount_(config->SampleJobCount)
    , MaxTentativeTreeJobDurationRatio_(config->MaxTentativeJobDurationRatio)
    , MinJobDuration_(config->MinJobDuration)
{ }

TTentativeTreeEligibility::TTentativeTreeEligibility()
    : Logger(ControllerAgentLogger)
    , SampleJobCount_(-1)
    , MaxTentativeTreeJobDurationRatio_(-1.0)
{ }

void TTentativeTreeEligibility::Initialize(
    const TOperationId& operationId,
    const TString& taskTitle)
{
    Logger.AddTag("OperationId: %v", operationId);
    Logger.AddTag("Task: %v", taskTitle);
}

void TTentativeTreeEligibility::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, NonTentativeTreeDuration_);

    Persist(context, Durations_);

    Persist(context, SampleJobCount_);
    Persist(context, MaxTentativeTreeJobDurationRatio_);
    Persist(context, MinJobDuration_);

    Persist(context, StartedJobsPerPoolTree_);
    Persist(context, FinishedJobsPerPoolTree_);

    Persist(context, BannedTrees_);
}

bool TTentativeTreeEligibility::CanScheduleJob(
    const TString& treeId,
    bool tentative)
{
    if (!tentative) {
        return true;
    }

    if (IsTreeBanned(treeId)) {
        return false;
    }

    auto jobsStarted = StartedJobsPerPoolTree_.Value(treeId, 0);
    auto jobsFinished = FinishedJobsPerPoolTree_.Value(treeId, 0);
    if (jobsStarted == SampleJobCount_ && jobsFinished != jobsStarted) {
        // Wait for a sample of jobs to finish before allowing the rest of the
        // jobs to start.
        return false;
    }

    return true;
}

void TTentativeTreeEligibility::OnJobStarted(const TString& treeId, bool tentative)
{
    if (tentative) {
        ++StartedJobsPerPoolTree_[treeId];
    }
}

TJobCompletedResult TTentativeTreeEligibility::OnJobFinished(const TJobSummary& jobSummary, const TString& treeId, bool tentative)
{
    if (tentative) {
        ++FinishedJobsPerPoolTree_[treeId];
    }

    UpdateDurations(jobSummary, treeId, tentative);
    CheckDurations(treeId, tentative);

    TJobCompletedResult result;
    result.BanTree = IsTreeBanned(treeId);

    return result;
}

void TTentativeTreeEligibility::UpdateDurations(
    const TJobSummary& jobSummary,
    const TString& treeId,
    bool tentative)
{
    auto totalDuration = jobSummary.PrepareDuration.Get({}) + jobSummary.ExecDuration.Get({});
    auto& durationSummary = tentative ? Durations_[treeId] : NonTentativeTreeDuration_;
    durationSummary.AddSample(totalDuration);
}

void TTentativeTreeEligibility::CheckDurations(const TString& treeId, bool tentative)
{
    if (tentative) {
        if (!IsTreeBanned(treeId) && IsSlow(treeId)) {
            BanTree(treeId);
        }
    } else {
        for (const auto& pair : Durations_) {
            CheckDurations(pair.first, true);
        }
    }
}

bool TTentativeTreeEligibility::IsSlow(const TString& treeId) const
{
    auto it = Durations_.find(treeId);
    if (it == Durations_.end()) {
        return false;
    }

    const auto& tentativeTreeDuration = it->second;
    if (tentativeTreeDuration.GetCount() < SampleJobCount_ ||
        NonTentativeTreeDuration_.GetCount() < SampleJobCount_)
    {
        return false;
    }

    if (*tentativeTreeDuration.GetAvg() < MinJobDuration_) {
        return false;
    }

    if (*NonTentativeTreeDuration_.GetAvg() == TDuration::Zero()) {
        return false;
    }

    return (*tentativeTreeDuration.GetAvg() / *NonTentativeTreeDuration_.GetAvg()) >= MaxTentativeTreeJobDurationRatio_;
}

void TTentativeTreeEligibility::BanTree(const TString& treeId)
{
    BannedTrees_.insert(treeId);

    YCHECK(Durations_.has(treeId));

    auto avgTentativeDuration = Durations_[treeId].GetAvg();
    auto avgNonTentativeDuration = NonTentativeTreeDuration_.GetAvg();
    YCHECK(avgTentativeDuration.HasValue());
    YCHECK(avgNonTentativeDuration.HasValue());

    LOG_DEBUG("Tentative tree banned for the task as average tentative job duration is much longer than average job duration "
        "(TreeId: %v, TentativeJobDuration: %v, NonTentativeJobDuration: %v, MaxTentativeJobDurationRatio: %v)",
        treeId,
        *avgTentativeDuration,
        *avgNonTentativeDuration,
        MaxTentativeTreeJobDurationRatio_);
}

bool TTentativeTreeEligibility::IsTreeBanned(const TString& treeId) const
{
    return BannedTrees_.has(treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
