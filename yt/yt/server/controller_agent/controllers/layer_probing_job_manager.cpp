#include "layer_probing_job_manager.h"
#include "job_info.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NJobTrackerClient;
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

void TLayerProbingJobManager::SetUserJobSpec(TOperationSpecBasePtr operationSpec, TUserJobSpecPtr userJobSpec)
{
    auto competitiveJobsAllowed = !operationSpec->TryAvoidDuplicatingJobs &&
        !operationSpec->FailOnJobRestart &&
        operationSpec->MaxProbingJobCountPerTask != 0 &&
        operationSpec->MaxSpeculativeJobCountPerTask != 0;
    UserJobSpec_ = competitiveJobsAllowed
        ? userJobSpec
        : nullptr;
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
    const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
    EJobState state)
{
    auto competition = FindCompetition(joblet);
    if (!IsRelevant(joblet) || !competition) {
        if (state == EJobState::Failed) {
            if (!FailedNonLayerProbingJob_) {
                FailedNonLayerProbingJob_ = joblet->JobId;
            }
            ++FailedNonLayerProbingJobCount_;
        }
        return true;
    }

    bool returnCookieToChunkPool = true;

    if (competition->IsNonTrivial) {
        if (competition->Status == ECompetitionStatus::TwoCompetitiveJobs) {
            returnCookieToChunkPool = false;
            if (!joblet->CompetitionType) {
                auto competitor = competition->GetCompetitorFor(joblet->JobId);
                Host_->AbortJobViaScheduler(competitor, EAbortReason::LayerProbingToUnsuccessfulJob);
                LostJobs_.insert(competitor);
            }
        } else if (competition->Status == ECompetitionStatus::HasCompletedJob) {
            returnCookieToChunkPool = false;
        } else if (competition->Status == ECompetitionStatus::SingleJobOnly) {
            returnCookieToChunkPool = true;
        }
    }

    if (joblet->CompetitionType == EJobCompetitionType::LayerProbing) {
        if (!LostJobs_.contains(joblet->JobId)) {
            ++FailedLayerProbingJobCount_;
            FailedLayerProbingJob_ = joblet->JobId;
        }
    } else if (state == EJobState::Failed) {
        ++FailedNonLayerProbingJobCount_;
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
        ++SucceededLayerProbingJobCount_;
        LayerProbingStatus_ = ELayerProbingJobStatus::LayerProbingJobCompleted;
    }

    if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        LostJobs_.insert(joblet->JobId);
        return joblet->CompetitionType == EJobCompetitionType::LayerProbing
            ? EAbortReason::LayerProbingRunLost
            : EAbortReason::LayerProbingRunWon;
    }

    return std::nullopt;
}

bool TLayerProbingJobManager::IsLayerProbingEnabled() const
{
    return UserJobSpec_ &&
        UserJobSpec_->LayerPaths.empty() &&
        UserJobSpec_->DefaultBaseLayerPath &&
        UserJobSpec_->ProbingBaseLayerPath;
}

bool TLayerProbingJobManager::IsLayerProbeReady() const
{
    return GetPendingJobCount() > 0;
}

bool TLayerProbingJobManager::IsLayerProbeRequired() const
{
    return IsLayerProbingEnabled() &&
        LayerProbingStatus_ == ELayerProbingJobStatus::NoLayerProbingJobResult &&
        GetFailedLayerProbingJobCount() < UserJobSpec_->MaxFailedBaseLayerProbes &&
        GetTotalJobCount() == 0;
}

bool TLayerProbingJobManager::ShouldUseProbingLayer() const
{
    return LayerProbingStatus_ == ELayerProbingJobStatus::LayerProbingJobCompleted &&
        UserJobSpec_->SwitchBaseLayerOnProbeSuccess;
}

int TLayerProbingJobManager::GetFailedNonLayerProbingJobCount() const
{
    return FailedNonLayerProbingJobCount_;
}

int TLayerProbingJobManager::GetFailedLayerProbingJobCount() const
{
    return FailedLayerProbingJobCount_;
}

int TLayerProbingJobManager::GetSucceededLayerProbingJobCount() const
{
    return SucceededLayerProbingJobCount_;
}

TJobId TLayerProbingJobManager::GetFailedLayerProbingJob() const
{
    return FailedLayerProbingJob_;
}

TJobId TLayerProbingJobManager::GetFailedNonLayerProbingJob() const
{
    return FailedNonLayerProbingJob_;
}

void TLayerProbingJobManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    TCompetitiveJobManagerBase::Persist(context);

    Persist(context, UserJobSpec_);

    // COMPAT(galtsev)
    if (context.GetVersion() >= ESnapshotVersion::ProbingBaseLayerPersistAlertCounts) {
        Persist(context, FailedNonLayerProbingJobCount_);
        Persist(context, SucceededLayerProbingJobCount_);
    } else {
        FailedNonLayerProbingJobCount_ = 0;
        SucceededLayerProbingJobCount_ = 0;
    }

    // COMPAT(galtsev)
    if (context.GetVersion() >= ESnapshotVersion::ProbingBaseLayerPersistLostJobs) {
        Persist(context, FailedLayerProbingJobCount_);
        Persist(context, FailedLayerProbingJob_);
        Persist(context, FailedNonLayerProbingJob_);
        Persist(context, LostJobs_);
    } else {
        int unsuccessfulJobCount = 0;
        int layerProbingRunLost = 0;
        Persist(context, unsuccessfulJobCount);
        Persist(context, layerProbingRunLost);
        FailedLayerProbingJobCount_ = unsuccessfulJobCount - layerProbingRunLost;
    }

    Persist(context, LayerProbingStatus_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
