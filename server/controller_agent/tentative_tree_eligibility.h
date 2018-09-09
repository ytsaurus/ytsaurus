#pragma once

#include "serialize.h"
#include "helpers.h"

#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/scheduling_tag.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NControllerAgent {

///////////////////////////////////////////////////////////////////////////////

struct TJobFinishedResult
{
    bool BanTree = false;
};

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
        const NScheduler::TTentativeTreeEligibilityConfigPtr& config);

    // For persistence only.
    TTentativeTreeEligibility();

    void Initialize(const TOperationId& operationId, const TString& taskTitle);

    void Persist(const TPersistenceContext& context);

    bool CanScheduleJob(const TString& treeId, bool tentative);

    void OnJobStarted(const TString& treeId, bool tentative);

    TJobFinishedResult OnJobFinished(
        const TJobSummary& jobSummary,
        const TString& treeId,
        bool tentative);

private:
    using TDurationSummary = TAvgSummary<TDuration>;

    NLogging::TLogger Logger;

    TDurationSummary NonTentativeTreeDuration_;

    // Tentative job durations - by pool trees.
    THashMap<TString, TDurationSummary> Durations_;

    int SampleJobCount_ = -1;
    double MaxTentativeTreeJobDurationRatio_ = -1.0;
    TDuration MinJobDuration_;

    // Number of started/finished jobs per pool tree.
    THashMap<TString, int> StartedJobsPerPoolTree_;
    THashMap<TString, THashMap<EJobState, int>> FinishedJobsPerStatePerPoolTree_;

    THashSet<TString> BannedTrees_;

    // For documentation on the meaning of parameters, see
    // TTentativeTreeEligibilityConfig::{SampleJobCount,MaxTentativeJobDurationRatio,MinJobDuration} respectively.
    TTentativeTreeEligibility(int sampleJobCount, double maxTentativeJobDurationRatio, TDuration minJobDuration);

    void UpdateDurations(const TJobSummary& jobSummary, const TString& treeId, bool tentative);

    void CheckDurations(const TString& treeId, bool tentative);

    bool IsSlow(const TString& treeId) const;

    void BanTree(const TString& treeId);
    bool IsTreeBanned(const TString& treeId) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
