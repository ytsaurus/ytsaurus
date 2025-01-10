#include "experiment_job_manager.h"

#include "helpers.h"
#include "job_info.h"
#include "task_host.h"

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NJobTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

bool TJobExperimentBase::IsEnabled(
    const TOperationSpecBasePtr& operationSpec,
    const std::vector<TUserJobSpecPtr>& userJobSpecs)
{
    return operationSpec &&
        operationSpec->JobExperiment &&
        !operationSpec->TryAvoidDuplicatingJobs &&
        !HasJobUniquenessRequirements(operationSpec, userJobSpecs) &&
        operationSpec->MaxProbingJobCountPerTask != 0 &&
        operationSpec->MaxSpeculativeJobCountPerTask != 0 &&
        std::all_of(userJobSpecs.begin(), userJobSpecs.end(), [] (const auto& userJobSpec) {
            auto vanillaTaskSpec = dynamic_cast<const TVanillaTaskSpec*>(userJobSpec.Get());
            return !vanillaTaskSpec || !vanillaTaskSpec->RestartCompletedJobs;
        });
}

////////////////////////////////////////////////////////////////////////////////

TLayerJobExperiment::TLayerJobExperiment()
{ }

TLayerJobExperiment::TLayerJobExperiment(
    TString defaultBaseLayerPath,
    TUserFile baseLayer,
    bool enableBypassArtifactCache,
    const NLogging::TLogger& logger)
    : DefaultBaseLayerPath_(defaultBaseLayerPath)
    , BaseLayer_(baseLayer)
    , Logger(logger)
    , EnableBypassArtifactCache_(enableBypassArtifactCache)
{ }

bool TLayerJobExperiment::IsEnabled(
    const TOperationSpecBasePtr& operationSpec,
    const std::vector<TUserJobSpecPtr>& userJobSpecs)
{
    return TJobExperimentBase::IsEnabled(operationSpec, userJobSpecs) &&
        operationSpec->DefaultBaseLayerPath &&
        operationSpec->JobExperiment->BaseLayerPath &&
        std::all_of(
            userJobSpecs.begin(),
            userJobSpecs.end(),
            [] (const auto& userJobSpec) { return userJobSpec->LayerPaths.empty(); }) &&
        std::any_of(
            userJobSpecs.begin(),
            userJobSpecs.end(),
            [] (const auto& userJobSpec) { return !userJobSpec->FilePaths.empty(); });
}

void TLayerJobExperiment::PatchUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const TJobletPtr& joblet) const
{
    YT_LOG_DEBUG("Switching the job to the probing layer (JobId: %v, Layer: %v)",
        joblet->JobId,
        BaseLayer_.Path);

    for (auto& layerSpec : *jobSpec->mutable_layers()) {
        if (layerSpec.data_source().path() == DefaultBaseLayerPath_) {
            BuildFileSpec(&layerSpec, BaseLayer_, layerSpec.copy_file(), EnableBypassArtifactCache_);
        }
    }
}

EOperationAlertType TLayerJobExperiment::GetAlertType() const
{
    return EOperationAlertType::BaseLayerProbeFailed;
}

TError TLayerJobExperiment::GetAlert(const TOperationSpecBasePtr& operationSpec) const
{
    return TError(
        "A job with experimental base layer has failed; "
        "this probably means that your job requires a specific environment "
        "that must be put into user delta layer, or into explicitly-specified base layer")
        << TErrorAttribute("base_layer_path", operationSpec->JobExperiment->BaseLayerPath);
}

void TLayerJobExperiment::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, DefaultBaseLayerPath_)();
    PHOENIX_REGISTER_FIELD(2, BaseLayer_)();
    PHOENIX_REGISTER_FIELD(3, EnableBypassArtifactCache_)();

    PHOENIX_REGISTER_FIELD(4, Logger)();
}

PHOENIX_DEFINE_TYPE(TLayerJobExperiment);

////////////////////////////////////////////////////////////////////////////////

TMtnJobExperiment::TMtnJobExperiment()
{ }

TMtnJobExperiment::TMtnJobExperiment(
    const NApi::NNative::IClientPtr& client,
    const std::string& authenticatedUser,
    TString networkProject,
    NLogging::TLogger logger)
    : NetworkProject_(networkProject)
    , Logger(logger)
{
    auto networkProjectAttributes = GetNetworkProject(client, authenticatedUser, networkProject);

    ProjectId_ = networkProjectAttributes->Get<ui32>("project_id");
    EnableNat64_ = networkProjectAttributes->Get<bool>("enable_nat64", false);
    DisableNetwork_ = networkProjectAttributes->Get<bool>("disable_network", false);
}

bool TMtnJobExperiment::IsEnabled(
    const TOperationSpecBasePtr& operationSpec,
    const std::vector<TUserJobSpecPtr>& userJobSpecs)
{
    return TJobExperimentBase::IsEnabled(operationSpec, userJobSpecs) &&
        operationSpec->JobExperiment->NetworkProject &&
        std::all_of(
            userJobSpecs.begin(),
            userJobSpecs.end(),
            [] (const auto& userJobSpec) { return !userJobSpec->NetworkProject; });
}

void TMtnJobExperiment::PatchUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const TJobletPtr& joblet) const
{
    YT_LOG_DEBUG("Switching the job to the probing network project "
        "(JobId: %v, NetworkProject: %v, NetworkProjectId: %v, EnableNat64: %v, DisableNetwork: %v)",
        joblet->JobId,
        NetworkProject_,
        ProjectId_,
        EnableNat64_,
        DisableNetwork_);

    jobSpec->set_network_project_id(ProjectId_);
    jobSpec->set_enable_nat64(EnableNat64_);
    jobSpec->set_disable_network(DisableNetwork_);
}

EOperationAlertType TMtnJobExperiment::GetAlertType() const
{
    return EOperationAlertType::MtnExperimentFailed;
}

TError TMtnJobExperiment::GetAlert(const TOperationSpecBasePtr& operationSpec) const
{
    return TError(
        "A job with experimental network settings has failed; "
        "this probably means that your job requires a custom network project "
        "that must be specified explicitly")
        << TErrorAttribute("network_project", operationSpec->JobExperiment->NetworkProject);
}

void TMtnJobExperiment::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, NetworkProject_)();
    PHOENIX_REGISTER_FIELD(2, ProjectId_)();
    PHOENIX_REGISTER_FIELD(3, EnableNat64_)();
    PHOENIX_REGISTER_FIELD(4, DisableNetwork_)();

    PHOENIX_REGISTER_FIELD(5, Logger)();
}

PHOENIX_DEFINE_TYPE(TMtnJobExperiment);

////////////////////////////////////////////////////////////////////////////////

TExperimentJobManager::TExperimentJobManager() = default;

TExperimentJobManager::TExperimentJobManager(
    ICompetitiveJobManagerHost* host,
    const TOperationSpecBasePtr& operationSpec,
    NLogging::TLogger logger)
    : TCompetitiveJobManagerBase(
        host,
        logger,
        /*maxSecondaryJobCount*/ 1,
        EJobCompetitionType::Experiment,
        EAbortReason::JobTreatmentResultLost)
    , OperationSpec_(operationSpec)
{ }

void TExperimentJobManager::SetJobExperiment(const TJobExperimentBasePtr& jobExperiment)
{
    if (TJobExperimentBase::IsEnabled(OperationSpec_, /*userJobSpecs*/ {})) {
        JobExperiment_ = jobExperiment;
    }
}

void TExperimentJobManager::OnJobScheduled(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet)) {
        BanCookie(joblet->OutputCookie);
        return;
    }

    TCompetitiveJobManagerBase::OnJobScheduled(joblet);

    if (IsTreatmentRequired() && !joblet->CompetitionType) {
        TryAddCompetitiveJob(joblet);
    }
}

void TExperimentJobManager::OnJobCompleted(const TJobletPtr& joblet)
{
    if (!IsRelevant(joblet) || !FindCompetition(joblet)) {
        return;
    }

    OnJobFinished(joblet);
    MarkCompetitionAsCompleted(joblet);
}

bool TExperimentJobManager::OnUnsuccessfulJobFinish(
    const TJobletPtr& joblet,
    const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
    EJobState state)
{
    auto competition = FindCompetition(joblet);
    if (!IsRelevant(joblet) || !competition) {
        if (state == EJobState::Failed) {
            if (!FailedControlJob_) {
                FailedControlJob_ = joblet->JobId;
            }
            ++FailedControlJobCount_;
        }
        return true;
    }

    bool returnCookieToChunkPool = true;

    if (competition->IsNonTrivial) {
        if (competition->Status == ECompetitionStatus::TwoCompetitiveJobs) {
            returnCookieToChunkPool = false;
            if (!joblet->CompetitionType) {
                auto competitor = competition->GetCompetitorFor(joblet->JobId);
                Host_->AsyncAbortJob(competitor, EAbortReason::JobTreatmentToUnsuccessfulJob);
                LostJobs_.insert(competitor);
            }
        } else if (competition->Status == ECompetitionStatus::HasCompletedJob) {
            returnCookieToChunkPool = false;
        } else if (competition->Status == ECompetitionStatus::SingleJobOnly) {
            returnCookieToChunkPool = true;
        }
    }

    if (joblet->CompetitionType == EJobCompetitionType::Experiment) {
        if (!LostJobs_.contains(joblet->JobId)) {
            ++FailedTreatmentJobCount_;
            FailedTreatmentJob_ = joblet->JobId;
        }
    } else if (state == EJobState::Failed) {
        ++FailedControlJobCount_;
        FailedControlJob_ = joblet->JobId;
    }

    OnJobFinished(joblet);

    if (!returnCookieToChunkPool) {
        updateJobCounter(&competition->ProgressCounterGuard);
        competition->ProgressCounterGuard.SetCategory(EProgressCategory::None);
    }

    return returnCookieToChunkPool;
}

std::optional<EAbortReason> TExperimentJobManager::ShouldAbortCompletingJob(const TJobletPtr& joblet)
{
    auto competition = FindCompetition(joblet);
    if (!IsRelevant(joblet) || !competition) {
        return std::nullopt;
    }

    if (joblet->CompetitionType == EJobCompetitionType::Experiment) {
        ++SucceededTreatmentJobCount_;
        ExperimentStatus_ = EJobExperimentStatus::TreatmentSucceeded;
    }

    if (competition->Status == ECompetitionStatus::HasCompletedJob) {
        LostJobs_.insert(joblet->JobId);
        return joblet->CompetitionType == EJobCompetitionType::Experiment
            ? EAbortReason::JobTreatmentRunLost
            : EAbortReason::JobTreatmentRunWon;
    }

    return std::nullopt;
}

bool TExperimentJobManager::IsEnabled() const
{
    return JobExperiment_ != nullptr;
}

bool TExperimentJobManager::IsTreatmentReady() const
{
    return GetPendingJobCount() > 0;
}

bool TExperimentJobManager::IsTreatmentRequired() const
{
    return IsEnabled() &&
        ExperimentStatus_ == EJobExperimentStatus::NoTreatmentSuccessesYet &&
        OperationSpec_ &&
        OperationSpec_->JobExperiment &&
        GetFailedTreatmentJobCount() < OperationSpec_->JobExperiment->MaxFailedTreatmentJobs &&
        GetTotalJobCount() == 0;
}

bool TExperimentJobManager::ShouldSwitchSettings() const
{
    return ExperimentStatus_ == EJobExperimentStatus::TreatmentSucceeded &&
        OperationSpec_ &&
        OperationSpec_->JobExperiment &&
        OperationSpec_->JobExperiment->SwitchOnExperimentSuccess;
}

int TExperimentJobManager::GetFailedControlJobCount() const
{
    return FailedControlJobCount_;
}

int TExperimentJobManager::GetFailedTreatmentJobCount() const
{
    return FailedTreatmentJobCount_;
}

int TExperimentJobManager::GetSucceededTreatmentJobCount() const
{
    return SucceededTreatmentJobCount_;
}

TJobId TExperimentJobManager::GetFailedTreatmentJob() const
{
    return FailedTreatmentJob_;
}

TJobId TExperimentJobManager::GetFailedControlJob() const
{
    return FailedControlJob_;
}

void TExperimentJobManager::PatchUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const TJobletPtr& joblet) const
{
    if (IsEnabled() &&
        (joblet->CompetitionType == EJobCompetitionType::Experiment || ShouldSwitchSettings()))
    {
        YT_LOG_DEBUG("Switching the job to the experimental setup (JobId: %v, JobIsExperimental: %v)",
            joblet->JobId,
            ExperimentStatus_ != EJobExperimentStatus::TreatmentSucceeded);
        JobExperiment_->PatchUserJobSpec(jobSpec, joblet);
    }
}

void TExperimentJobManager::GenerateAlertIfNeeded(
    ITaskHost* taskHost,
    const TDataFlowGraph::TVertexDescriptor& taskName) const
{
    if (IsEnabled() &&
        OperationSpec_ &&
        OperationSpec_->JobExperiment &&
        (OperationSpec_->JobExperiment->AlertOnAnyTreatmentFailure || GetFailedControlJobCount() == 0) &&
        GetFailedTreatmentJobCount() > 0)
    {
        auto error = JobExperiment_->GetAlert(OperationSpec_)
            << TErrorAttribute("task_name", taskName)
            << TErrorAttribute("failed_treatment_job_count", GetFailedTreatmentJobCount())
            << TErrorAttribute("succeeded_treatment_job_count", GetSucceededTreatmentJobCount())
            << TErrorAttribute("failed_treatment_job", GetFailedTreatmentJob())
            << TErrorAttribute("failed_control_job_count", GetFailedControlJobCount());
        if (GetFailedControlJob()) {
            error = error << TErrorAttribute("failed_control_job", GetFailedControlJob());
        }

        taskHost->SetOperationAlert(JobExperiment_->GetAlertType(), error);
    }
}

void TExperimentJobManager::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TCompetitiveJobManagerBase>();

    PHOENIX_REGISTER_FIELD(1, OperationSpec_)();
    PHOENIX_REGISTER_FIELD(2, FailedControlJobCount_)();
    PHOENIX_REGISTER_FIELD(3, SucceededTreatmentJobCount_)();
    PHOENIX_REGISTER_FIELD(4, FailedTreatmentJobCount_)();
    PHOENIX_REGISTER_FIELD(5, FailedTreatmentJob_)();
    PHOENIX_REGISTER_FIELD(6, FailedControlJob_)();
    PHOENIX_REGISTER_FIELD(7, LostJobs_)();
    PHOENIX_REGISTER_FIELD(8, ExperimentStatus_)();
}

PHOENIX_DEFINE_TYPE(TExperimentJobManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
