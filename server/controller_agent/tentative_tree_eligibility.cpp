#include "tentative_tree_eligibility.h"

#include "operation_controller.h"

#include <yt/server/scheduler/config.h>

namespace NYT::NControllerAgent {

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
    TOperationId operationId,
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
    if (context.GetVersion() >= 300024) {
        Persist(context, LastStartJobTimePerPoolTree_);
    }
    Persist(context, FinishedJobsPerStatePerPoolTree_);
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
        LastStartJobTimePerPoolTree_[treeId] = TInstant::Now();
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

std::vector<TString> TTentativeTreeEligibility::FindAndBanSlowTentativeTrees()
{
    std::vector<TString> slowTreeIds;
    for (const auto& pair : StartedJobsPerPoolTree_) {
        const auto& treeId = pair.first;
        if (!IsTreeBanned(treeId) && IsSlow(treeId)) {
            BanTree(treeId);
            slowTreeIds.push_back(treeId);
        }
    }
    return slowTreeIds;
}

void TTentativeTreeEligibility::UpdateDurations(
    const TJobSummary& jobSummary,
    const TString& treeId,
    bool tentative)
{
    auto totalDuration = jobSummary.PrepareDuration.value_or(TDuration()) + jobSummary.ExecDuration.value_or(TDuration());
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

TDuration TTentativeTreeEligibility::GetTentativeTreeAverageJobDuration(const TString& treeId) const
{
    TDuration tentativeDurationSum;
    int tentativeCount = 0;

    {
        auto it = Durations_.find(treeId);
        if (it != Durations_.end()) {
            tentativeDurationSum += it->second.GetSum();
            tentativeCount += it->second.GetCount();
        }
    }

    if (tentativeCount < SampleJobCount_) {
        auto it = LastStartJobTimePerPoolTree_.find(treeId);
        if (it == LastStartJobTimePerPoolTree_.end()) {
            // COMPAT(ignat): for revived operations.
            tentativeCount += 1;
        } else {
            YCHECK(it != LastStartJobTimePerPoolTree_.end());

            tentativeCount += 1;
            tentativeDurationSum += TInstant::Now() - it->second;
        }
    }

    return tentativeDurationSum / tentativeCount;
}

bool TTentativeTreeEligibility::IsSlow(const TString& treeId) const
{
    if (NonTentativeTreeDuration_.GetCount() < SampleJobCount_) {
        return false;
    }
    if (*NonTentativeTreeDuration_.GetAvg() == TDuration::Zero()) {
        return false;
    }

    auto tentativeDurationAvg = GetTentativeTreeAverageJobDuration(treeId);
    if (tentativeDurationAvg < MinJobDuration_) {
        return false;
    }

    return (tentativeDurationAvg / *NonTentativeTreeDuration_.GetAvg()) >= MaxTentativeTreeJobDurationRatio_;
}

void TTentativeTreeEligibility::BanTree(const TString& treeId)
{
    BannedTrees_.insert(treeId);

    auto tentativeDurationAvg = GetTentativeTreeAverageJobDuration(treeId);
    auto nonTentativeDurationAvg = NonTentativeTreeDuration_.GetAvg();
    YCHECK(nonTentativeDurationAvg);

    YT_LOG_DEBUG("Tentative tree banned for the task as average tentative job duration is much longer than average job duration "
        "(TreeId: %v, TentativeJobDuration: %v, NonTentativeJobDuration: %v, MaxTentativeJobDurationRatio: %v)",
        treeId,
        tentativeDurationAvg,
        *nonTentativeDurationAvg,
        MaxTentativeTreeJobDurationRatio_);
}

bool TTentativeTreeEligibility::IsTreeBanned(const TString& treeId) const
{
    return BannedTrees_.contains(treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
