#include "tentative_tree_eligibility.h"

#include "operation_controller.h"

#include <yt/server/scheduler/config.h>

namespace NYT {
namespace NControllerAgent {

using namespace NJobTrackerClient;

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

    if (context.IsLoad() && context.GetVersion() <= 300015) {
        THashMap<TString, int> completedJobsPerPoolTree;
        Persist(context, completedJobsPerPoolTree);
        for (const auto& pair : completedJobsPerPoolTree) {
            const auto& treeName = pair.first;
            auto jobCount = pair.second;
            FinishedJobsPerStatePerPoolTree_[treeName][EJobState::Completed] = jobCount;
        }
    } else {
        Persist(context, FinishedJobsPerStatePerPoolTree_);
    }

    Persist(context, BannedTrees_);
}

bool TTentativeTreeEligibility::CanScheduleJob(
    const TString& treeId,
    bool tentative)
{
    if (!tentative) {
        return true;
    }

    if (Disabled_ || IsTreeBanned(treeId)) {
        return false;
    }

    auto startedJobCount = StartedJobsPerPoolTree_.Value(treeId, 0);
    int finishedJobCount = 0;
    for (const auto& pair : FinishedJobsPerStatePerPoolTree_.Value(treeId, THashMap<EJobState, int>())) {
        auto jobCount = pair.second;
        finishedJobCount += jobCount;
    }
    int runningJobCount = startedJobCount - finishedJobCount;
    int completedJobCount = FinishedJobsPerStatePerPoolTree_.Value(treeId, THashMap<EJobState, int>()).Value(EJobState::Completed, 0);
    int remainingSampleJobCount = SampleJobCount_ - completedJobCount;

    if (remainingSampleJobCount > 0 && runningJobCount >= remainingSampleJobCount)
    {
        // Wait for a sample of jobs to finish before allowing the rest of the
        // jobs to start.
        return false;
    }

    return true;
}

void TTentativeTreeEligibility::Disable()
{
    Disabled_ = true;
}

void TTentativeTreeEligibility::OnJobStarted(const TString& treeId, bool tentative)
{
    if (tentative) {
        ++StartedJobsPerPoolTree_[treeId];
    }
}

TJobFinishedResult TTentativeTreeEligibility::OnJobFinished(const TJobSummary& jobSummary, const TString& treeId, bool tentative)
{
    if (tentative) {
        ++FinishedJobsPerStatePerPoolTree_[treeId][jobSummary.State];
    }

    if (jobSummary.State == EJobState::Completed) {
        UpdateDurations(jobSummary, treeId, tentative);
        CheckDurations(treeId, tentative);
    }

    TJobFinishedResult result;
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
