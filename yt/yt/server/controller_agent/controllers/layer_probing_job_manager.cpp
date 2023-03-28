#include "layer_probing_job_manager.h"
#include "job_info.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TLayerProbingJobManager::TLayerProbingJobManager()
{ }

TLayerProbingJobManager::TLayerProbingJobManager(
    ICompetitiveJobManagerHost* host,
    NLogging::TLogger logger)
    : TCompetitiveJobManagerBase(
        host,
        logger,
        /*maxSecondaryJobCount*/ 1,
        EJobCompetitionType::LayerProbing,
        EAbortReason::LayerProbingResultLost)
{ }

void TLayerProbingJobManager::SetUserJobSpec(TUserJobSpecPtr userJobSpec)
{
    UserJobSpec_ = userJobSpec;
}

void TLayerProbingJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        BanCookie(joblet->OutputCookie);
        return;
    }

    TCompetitiveJobManagerBase::OnJobScheduled(joblet);

    if (IsLayerProbeRequired() && !joblet->CompetitionType) {
        TryAddCompetitiveJob(joblet);
    }
}

void TLayerProbingJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet) || !FindCompetition(joblet)) {
        return;
    }

    OnJobFinished(joblet);
    MarkCompetitionAsCompleted(joblet);
}

bool TLayerProbingJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    const std::function<void(TProgressCounterGuard*)>& updateJobCounter)
{
    auto competition = FindCompetition(joblet);
    if (!IsRelevant(joblet) || !competition) {
        FailedNonLayerProbingJob_ = joblet->JobId;
        return true;
    }

    bool returnCookieToChunkPool = true;

    if (competition->IsNonTrivial) {
        if (competition->Status == ECompetitionStatus::TwoCompetitiveJobs) {
            returnCookieToChunkPool = false;
            if (!joblet->CompetitionType) {
                Host_->AbortJobViaScheduler(competition->GetCompetitorFor(joblet->JobId), EAbortReason::LayerProbingToUnsuccessfulJob);
            }
        } else if (competition->Status == ECompetitionStatus::HasCompletedJob) {
            returnCookieToChunkPool = false;
        } else if (competition->Status == ECompetitionStatus::SingleJobOnly) {
            returnCookieToChunkPool = true;
        }
    }

    if (joblet->CompetitionType == EJobCompetitionType::LayerProbing) {
        ++UnsuccessfulJobCount_;
        FailedLayerProbingJob_ = joblet->JobId;
    } else {
        FailedNonLayerProbingJob_ = joblet->JobId;
    }

    OnJobFinished(joblet);

    if (!returnCookieToChunkPool) {
        updateJobCounter(&competition->ProgressCounterGuard);
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
    }

    return returnCookieToChunkPool;
}

std::optional<EAbortReason> TLayerProbingJobManager::ShouldAbortCompletingJob(const TJobletPtr& joblet)
{
    auto competition = FindCompetition(joblet);
    if (!IsRelevant(joblet) || !competition) {
        return std::nullopt;
    }

    if (joblet->CompetitionType == EJobCompetitionType::LayerProbing) {
        LayerProbingStatus_ = ELayerProbingJobStatus::LayerProbingJobCompleted;
        if (competition->Status == ECompetitionStatus::HasCompletedJob) {
            ++LayerProbingRunLost_;
        }
    }

    if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        return joblet->CompetitionType == EJobCompetitionType::LayerProbing
            ? EAbortReason::LayerProbingRunLost
            : EAbortReason::LayerProbingRunWon;
    }

    return std::nullopt;
}

bool TLayerProbingJobManager::IsLayerProbingEnabled() const
{
    return UserJobSpec_ && UserJobSpec_->DefaultBaseLayerPath && UserJobSpec_->ProbingBaseLayerPath;
}

bool TLayerProbingJobManager::IsLayerProbeReady() const
{
    return GetPendingJobCount() > 0;
}

bool TLayerProbingJobManager::IsLayerProbeRequired() const
{
    return IsLayerProbingEnabled() &&
        LayerProbingStatus_ == ELayerProbingJobStatus::NoLayerProbingJobResult &&
        FailedJobCount() < UserJobSpec_->MaxFailedBaseLayerProbes &&
        GetTotalJobCount() == 0;
}

bool TLayerProbingJobManager::ShouldUseProbingLayer() const
{
    return LayerProbingStatus_ == ELayerProbingJobStatus::LayerProbingJobCompleted &&
        UserJobSpec_->SwitchBaseLayerOnProbeSuccess;
}

int TLayerProbingJobManager::FailedJobCount() const
{
    return UnsuccessfulJobCount_ - LayerProbingRunLost_;
}

NJobTrackerClient::TJobId TLayerProbingJobManager::GetFailedLayerProbingJob() const
{
    return FailedLayerProbingJob_;
}

NJobTrackerClient::TJobId TLayerProbingJobManager::GetFailedNonLayerProbingJob() const
{
    return FailedNonLayerProbingJob_;
}

void TLayerProbingJobManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    TCompetitiveJobManagerBase::Persist(context);

    Persist(context, UserJobSpec_);
    Persist(context, UnsuccessfulJobCount_);
    Persist(context, LayerProbingRunLost_);
    Persist(context, LayerProbingStatus_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
