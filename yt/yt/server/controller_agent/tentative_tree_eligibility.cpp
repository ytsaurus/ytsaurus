#include "tentative_tree_eligibility.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NControllerAgent {

using namespace NJobTrackerClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TTentativeTreeEligibility::TTentativeTreeEligibility(
    std::optional<THashSet<std::string>> tentativeTreeIds,
    const NScheduler::TTentativeTreeEligibilityConfigPtr& config,
    const TLogger& logger)
    : TentativeTreeIds_(std::move(tentativeTreeIds.value_or(THashSet<std::string>())))
    , SampleJobCount_(config->SampleJobCount)
    , MaxTentativeTreeJobDurationRatio_(config->MaxTentativeJobDurationRatio)
    , MinJobDuration_(config->MinJobDuration)
    , Logger(logger)
{ }

TTentativeTreeEligibility::TTentativeTreeEligibility()
    : SampleJobCount_(-1)
    , MaxTentativeTreeJobDurationRatio_(-1.0)
    , Logger(ControllerAgentLogger())
{ }

void TTentativeTreeEligibility::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, NonTentativeTreeDuration_);

    PHOENIX_REGISTER_FIELD(2, TreeIdToDuration_);

    PHOENIX_REGISTER_FIELD(3, SampleJobCount_);
    PHOENIX_REGISTER_FIELD(4, MaxTentativeTreeJobDurationRatio_);
    PHOENIX_REGISTER_FIELD(5, MinJobDuration_);

    PHOENIX_REGISTER_FIELD(6, TreeIdToStartedJobs_);
    PHOENIX_REGISTER_FIELD(7, TreeIdToLastStartJobTime_);
    PHOENIX_REGISTER_FIELD(8, TreeIdToFinishedJobsPerState_);
    PHOENIX_REGISTER_FIELD(9, BannedTreeIds_);

    PHOENIX_REGISTER_FIELD(10, Logger);
}

bool TTentativeTreeEligibility::CanScheduleJob(
    const std::string& treeId,
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

    auto startedJobCount = TreeIdToStartedJobs_.Value(treeId, 0);
    int finishedJobCount = 0;
    for (const auto& [jobState, jobCount] : TreeIdToFinishedJobsPerState_.Value(treeId, THashMap<EJobState, int>())) {
        finishedJobCount += jobCount;
    }
    int runningJobCount = startedJobCount - finishedJobCount;
    int completedJobCount = TreeIdToFinishedJobsPerState_.Value(treeId, THashMap<EJobState, int>()).Value(EJobState::Completed, 0);
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

void TTentativeTreeEligibility::OnJobStarted(const std::string& treeId, bool tentative)
{
    if (tentative) {
        ++TreeIdToStartedJobs_[treeId];
        TreeIdToLastStartJobTime_[treeId] = TInstant::Now();
    }
}

void TTentativeTreeEligibility::OnJobFinished(
    const TJobSummary& jobSummary,
    const std::string& treeId,
    bool tentative,
    std::vector<std::string>* newlyBannedTreeIds)
{
    if (tentative) {
        ++TreeIdToFinishedJobsPerState_[treeId][jobSummary.State];
    }

    if (jobSummary.State == EJobState::Completed) {
        UpdateDurations(jobSummary, treeId, tentative);
        *newlyBannedTreeIds = FindAndBanSlowTentativeTrees();
    }
}

std::vector<std::string> TTentativeTreeEligibility::FindAndBanSlowTentativeTrees()
{
    std::vector<std::string> slowTreeIds;
    for (const auto& [treeId, jobs] : TreeIdToStartedJobs_) {
        if (!IsTreeBanned(treeId) && IsTreeSlow(treeId)) {
            BanTree(treeId);
            slowTreeIds.push_back(treeId);
        }
    }
    return slowTreeIds;
}

void TTentativeTreeEligibility::LogTentativeTreeStatistics() const
{
    if (TreeIdToStartedJobs_.empty() || Disabled_) {
        return;
    }

    THashMap<TString, TDuration> treeAverageJobDurations;
    for (const auto& [treeId, jobs] : TreeIdToStartedJobs_) {
        treeAverageJobDurations.emplace(treeId, GetTentativeTreeAverageJobDuration(treeId));
    }

    YT_LOG_DEBUG("Tentative tree statistics (NonTentativeJobCount: %v, NonTentativeAverageDuration: %v, TentativeTreeJobDurations: %v)",
        NonTentativeTreeDuration_.GetCount(),
        NonTentativeTreeDuration_.GetAvg(),
        treeAverageJobDurations);
}

THashMap<std::string, int> TTentativeTreeEligibility::GetPendingJobCount() const
{
    return THashMap<std::string, int>();
}

void TTentativeTreeEligibility::UpdateDurations(
    const TJobSummary& jobSummary,
    const std::string& treeId,
    bool tentative)
{
    auto totalDuration = jobSummary.TimeStatistics.PrepareDuration.value_or(TDuration()) + jobSummary.TimeStatistics.ExecDuration.value_or(TDuration());
    auto& durationSummary = tentative ? TreeIdToDuration_[treeId] : NonTentativeTreeDuration_;
    durationSummary.AddSample(totalDuration);
}

TDuration TTentativeTreeEligibility::GetTentativeTreeAverageJobDuration(const std::string& treeId) const
{
    TDuration tentativeDurationSum;
    int tentativeCount = 0;

    {
        auto it = TreeIdToDuration_.find(treeId);
        if (it != TreeIdToDuration_.end()) {
            tentativeDurationSum += it->second.GetSum();
            tentativeCount += it->second.GetCount();
        }
    }

    if (tentativeCount < SampleJobCount_) {
        auto it = TreeIdToLastStartJobTime_.find(treeId);
        tentativeCount += 1;
        tentativeDurationSum += TInstant::Now() - it->second;
        return tentativeDurationSum / tentativeCount;
    } else {
        int completedJobCount = TreeIdToFinishedJobsPerState_.Value(treeId, THashMap<EJobState, int>()).Value(EJobState::Completed, 0);
        if (completedJobCount == tentativeCount) {
            return tentativeDurationSum / tentativeCount;
        } else { // Consider last job separately.
            const auto& lastStartJobTime = GetOrCrash(TreeIdToLastStartJobTime_, treeId);
            auto lastJobDuration = TInstant::Now() - lastStartJobTime;

            // Weight average duration with last job duration as if we run only sample jobs.
            tentativeDurationSum = (tentativeDurationSum / tentativeCount) * SampleJobCount_ + lastJobDuration;
            return tentativeDurationSum / (SampleJobCount_ + 1);
        }
    }
}

bool TTentativeTreeEligibility::IsTreeSlow(const std::string& treeId) const
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

void TTentativeTreeEligibility::BanTree(const std::string& treeId)
{
    BannedTreeIds_.insert(treeId);

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

bool TTentativeTreeEligibility::IsTreeBanned(const std::string& treeId) const
{
    return BannedTreeIds_.contains(treeId);
}

PHOENIX_DEFINE_TYPE(TTentativeTreeEligibility);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
