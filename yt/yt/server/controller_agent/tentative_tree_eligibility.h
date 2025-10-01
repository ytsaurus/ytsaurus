#pragma once

#include "helpers.h"

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! This class encapsulates everything a task needs to know in order to decide
//! whether it's allowed to launch jobs in a tentative tree.
/*!
 *  There're several factors that may influence this decision:
 *    - pool tree configuration (viz. the "tentative" flag);
 *    - tentative job durations (in comparison to job durations in other pool trees).
  */
class TTentativeTreeEligibility
{
public:
    TTentativeTreeEligibility(
        std::optional<THashSet<std::string>> tentativeTreeIds,
        const NScheduler::TTentativeTreeEligibilityConfigPtr& config,
        const NLogging::TLogger& logger);

    // For persistence only.
    TTentativeTreeEligibility();

    bool CanScheduleJob(const std::string& treeId, bool tentative);

    void OnJobStarted(const std::string& treeId, bool tentative);

    //! No jobs in tentative trees can start after call to this method.
    void Disable();

    void OnJobFinished(
        const TJobSummary& jobSummary,
        const std::string& treeId,
        bool tentative,
        std::vector<std::string>* newlyBannedTreeIds);

    std::vector<std::string> FindAndBanSlowTentativeTrees();

    void LogTentativeTreeStatistics() const;

    THashMap<std::string, int> GetPendingJobCount() const;

private:
    using TDurationSummary = TAvgSummary<TDuration>;

    TDurationSummary NonTentativeTreeDuration_;

    // Tentative job durations - by pool trees.
    THashMap<std::string, TDurationSummary> TreeIdToDuration_;

    THashSet<std::string> TentativeTreeIds_;

    int SampleJobCount_ = -1;
    double MaxTentativeTreeJobDurationRatio_ = -1.0;
    TDuration MinJobDuration_;

    // Number of started/finished jobs per pool tree.
    THashMap<std::string, int> TreeIdToStartedJobs_;
    THashMap<std::string, TInstant> TreeIdToLastStartJobTime_;
    THashMap<std::string, THashMap<EJobState, int>> TreeIdToFinishedJobsPerState_;

    THashSet<std::string> BannedTreeIds_;

    bool Disabled_ = false;

    NLogging::TSerializableLogger Logger;

    // For documentation on the meaning of parameters, see
    // TTentativeTreeEligibilityConfig::{SampleJobCount,MaxTentativeJobDurationRatio,MinJobDuration} respectively.
    TTentativeTreeEligibility(int sampleJobCount, double maxTentativeJobDurationRatio, TDuration minJobDuration);

    TDuration GetTentativeTreeAverageJobDuration(const std::string& treeId) const;

    void UpdateDurations(const TJobSummary& jobSummary, const std::string& treeId, bool tentative);

    bool IsTreeSlow(const std::string& treeId) const;

    void BanTree(const std::string& treeId);
    bool IsTreeBanned(const std::string& treeId) const;

    PHOENIX_DECLARE_TYPE(TTentativeTreeEligibility, 0xf8113e3d);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
