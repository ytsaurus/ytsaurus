#include "tentative_tree_eligibility.h"

#include "operation_controller.h"

namespace NYT::NControllerAgent {

using namespace NJobTrackerClient;
using namespace NLogging;

///////////////////////////////////////////////////////////////////////////////

TTentativeTreeEligibility::TTentativeTreeEligibility(
    std::optional<THashSet<TString>> tentativeTrees,
    const NScheduler::TTentativeTreeEligibilityConfigPtr& config,
    const TLogger& logger)
    : TentativeTrees_(std::move(tentativeTrees.value_or(THashSet<TString>())))
    , SampleJobCount_(config->SampleJobCount)
    , MaxTentativeTreeJobDurationRatio_(config->MaxTentativeJobDurationRatio)
    , MinJobDuration_(config->MinJobDuration)
    , Logger(logger)
{ }

TTentativeTreeEligibility::TTentativeTreeEligibility()
    : SampleJobCount_(-1)
    , MaxTentativeTreeJobDurationRatio_(-1.0)
    , Logger(ControllerAgentLogger)
{ }

void TTentativeTreeEligibility::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, NonTentativeTreeDuration_);

    Persist(context, Durations_);

    Persist(context, SampleJobCount_);
    Persist(context, MaxTentativeTreeJobDurationRatio_);
    Persist(context, MinJobDuration_);

    Persist(context, StartedJobsPerPoolTree_);
    Persist(context, LastStartJobTimePerPoolTree_);
    Persist(context, FinishedJobsPerStatePerPoolTree_);
    Persist(context, BannedTrees_);

    Persist(context, Logger);
}

bool TTentativeTreeEligibility::CanScheduleJob(
    const TString& treeId,
    bool tentative)
{
    if (!tentative) {
        return true;
    }

    if (Disabled_) {
        YT_LOG_DEBUG("Cannot schedule job in tentative tree since tentative trees are disabled in task");
        return false;
    }

    if (IsTreeBanned(treeId)) {
        YT_LOG_DEBUG("Cannot schedule job in tentative tree since tree is banned for operation (TreeId: %v)", treeId);
        return false;
    }

    auto startedJobCount = StartedJobsPerPoolTree_.Value(treeId, 0);
    int finishedJobCount = 0;
    for (const auto& [jobState, jobCount] : FinishedJobsPerStatePerPoolTree_.Value(treeId, THashMap<EJobState, int>())) {
        finishedJobCount += jobCount;
    }
    int runningJobCount = startedJobCount - finishedJobCount;
    int completedJobCount = FinishedJobsPerStatePerPoolTree_.Value(treeId, THashMap<EJobState, int>()).Value(EJobState::Completed, 0);
    int remainingSampleJobCount = SampleJobCount_ - completedJobCount;

    if (remainingSampleJobCount > 0 && runningJobCount >= remainingSampleJobCount)
    {
        YT_LOG_DEBUG("Cannot schedule job in tentative tree since we wait for a sample of jobs to finish before allowing the rest of the jobs to start (TreeId: %v)",
            treeId);
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

void TTentativeTreeEligibility::OnJobFinished(
    const TJobSummary& jobSummary,
    const TString& treeId,
    bool tentative,
    std::vector<TString>* newlyBannedTrees)
{
    if (tentative) {
        ++FinishedJobsPerStatePerPoolTree_[treeId][jobSummary.State];
    }

    if (jobSummary.State == EJobState::Completed) {
        UpdateDurations(jobSummary, treeId, tentative);
        *newlyBannedTrees = FindAndBanSlowTentativeTrees();
    }
}

std::vector<TString> TTentativeTreeEligibility::FindAndBanSlowTentativeTrees()
{
    std::vector<TString> slowTreeIds;
    for (const auto& [treeId, jobs] : StartedJobsPerPoolTree_) {
        if (!IsTreeBanned(treeId) && IsSlow(treeId)) {
            BanTree(treeId);
            slowTreeIds.push_back(treeId);
        }
    }
    return slowTreeIds;
}

void TTentativeTreeEligibility::LogTentativeTreeStatistics() const
{
    if (StartedJobsPerPoolTree_.empty() || Disabled_) {
        return;
    }

    THashMap<TString, TDuration> treeAverageJobDurations;
    for (const auto& [treeId, jobs] : StartedJobsPerPoolTree_) {
        treeAverageJobDurations.emplace(treeId, GetTentativeTreeAverageJobDuration(treeId));
    }

    YT_LOG_DEBUG("Tentative tree statistics (NonTentativeJobCount: %v, NonTentativeAverageDuration: %v, TentativeTreeJobDurations: %v)",
        NonTentativeTreeDuration_.GetCount(),
        NonTentativeTreeDuration_.GetAvg(),
        treeAverageJobDurations);
}

THashMap<TString, int> TTentativeTreeEligibility::GetPendingJobCount() const
{
    return THashMap<TString, int>();
}

void TTentativeTreeEligibility::UpdateDurations(
    const TJobSummary& jobSummary,
    const TString& treeId,
    bool tentative)
{
    auto totalDuration = jobSummary.TimeStatistics.PrepareDuration.value_or(TDuration()) + jobSummary.TimeStatistics.ExecDuration.value_or(TDuration());
    auto& durationSummary = tentative ? Durations_[treeId] : NonTentativeTreeDuration_;
    durationSummary.AddSample(totalDuration);
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
        tentativeCount += 1;
        tentativeDurationSum += TInstant::Now() - it->second;
        return tentativeDurationSum / tentativeCount;
    } else {
        int completedJobCount = FinishedJobsPerStatePerPoolTree_.Value(treeId, THashMap<EJobState, int>()).Value(EJobState::Completed, 0);
        if (completedJobCount == tentativeCount) {
            return tentativeDurationSum / tentativeCount;
        } else { // Consider last job separately.
            const auto& lastStartJobTime = GetOrCrash(LastStartJobTimePerPoolTree_, treeId);
            auto lastJobDuration = TInstant::Now() - lastStartJobTime;

            // Weight average duration with last job duration as if we run only sample jobs.
            tentativeDurationSum = (tentativeDurationSum / tentativeCount) * SampleJobCount_ + lastJobDuration;
            return tentativeDurationSum / (SampleJobCount_ + 1);
        }
    }
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
    YT_VERIFY(nonTentativeDurationAvg);

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
