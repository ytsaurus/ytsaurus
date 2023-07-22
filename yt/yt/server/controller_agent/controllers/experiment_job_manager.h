#pragma once

#include "competitive_job_manager.h"
#include "data_flow_graph.h"

#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/server/lib/controller_agent/persistence.h>
#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/scheduler/public.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobExperimentStatus,
    (TreatmentSucceeded)
    (NoTreatmentSuccessesYet)
);

////////////////////////////////////////////////////////////////////////////////

class TJobExperimentBase
    : public TRefCounted
    , public IPersistent
{
public:
    static bool IsEnabled(TOperationSpecBasePtr operationSpec);

    virtual void PatchUserJobSpec(NScheduler::NProto::TUserJobSpec* jobSpec, TJobletPtr joblet) const = 0;
    virtual NScheduler::EOperationAlertType GetAlertType() const = 0;
    virtual TError GetAlertMessage(TOperationSpecBasePtr operationSpec) const = 0;
};

DEFINE_REFCOUNTED_TYPE(TJobExperimentBase);

////////////////////////////////////////////////////////////////////////////////

//! An experiment that tries to run operation's jobs with the given base layer.
class TLayerJobExperiment
    : public TJobExperimentBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TLayerJobExperiment();

    TLayerJobExperiment(
        TString defaultBaseLayerPath,
        TUserFile baseLayer,
        bool enableBypassArtifactCache,
        const NLogging::TLogger& logger);

    static bool IsEnabled(
        TOperationSpecBasePtr operationSpec,
        std::vector<NScheduler::TUserJobSpecPtr> userJobSpecs);

    void PatchUserJobSpec(NScheduler::NProto::TUserJobSpec* jobSpec, TJobletPtr joblet) const override;

    NScheduler::EOperationAlertType GetAlertType() const override;
    TError GetAlertMessage(TOperationSpecBasePtr operationSpec) const override;

    void Persist(const TPersistenceContext& context) override;

private:
    TString DefaultBaseLayerPath_;
    TUserFile BaseLayer_;
    NLogging::TSerializableLogger Logger;
    bool EnableBypassArtifactCache_;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TLayerJobExperiment, 0x54698f9d);
};

////////////////////////////////////////////////////////////////////////////////

//! An experiment that tries to run operation's jobs in the network of the given MTN project.
class TMtnJobExperiment
    : public TJobExperimentBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TMtnJobExperiment();

    TMtnJobExperiment(
        const NApi::NNative::IClientPtr& client,
        TString authenticatedUser,
        TString networkProject,
        NLogging::TLogger logger);

    static bool IsEnabled(
        TOperationSpecBasePtr operationSpec,
        std::vector<NScheduler::TUserJobSpecPtr> userJobSpecs);

    void PatchUserJobSpec(NScheduler::NProto::TUserJobSpec* jobSpec, TJobletPtr joblet) const override;

    NScheduler::EOperationAlertType GetAlertType() const override;
    TError GetAlertMessage(TOperationSpecBasePtr operationSpec) const override;

    void Persist(const TPersistenceContext& context) override;

private:
    TString NetworkProject_;
    NLogging::TSerializableLogger Logger;
    ui32 ProjectId_;
    bool EnableNat64_;
    bool DisableNetwork_;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TMtnJobExperiment, 0x05b35208);
};

////////////////////////////////////////////////////////////////////////////////

class TExperimentJobManager
    : public TCompetitiveJobManagerBase
{
public:
    TExperimentJobManager();

    TExperimentJobManager(
        ICompetitiveJobManagerHost* host,
        TOperationSpecBasePtr operationSpec,
        NLogging::TLogger logger);

    void SetJobExperiment(TJobExperimentBasePtr jobExperiment);

    void OnJobScheduled(const TJobletPtr& joblet) override;
    void OnJobCompleted(const TJobletPtr& joblet) override;

    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override;

    bool IsEnabled() const;
    bool IsTreatmentReady() const;

    //! \returns |true| iff:
    //! - The experiment is successful.
    //! - And it is configured to switch settings into the experimental setup for all jobs after any treatment job succeeds.
    bool ShouldSwitchSettings() const;

    int GetFailedControlJobCount() const;
    int GetFailedTreatmentJobCount() const;
    int GetSucceededTreatmentJobCount() const;

    NJobTrackerClient::TJobId GetFailedTreatmentJob() const;
    NJobTrackerClient::TJobId GetFailedControlJob() const;

    void PatchUserJobSpec(NScheduler::NProto::TUserJobSpec* jobSpec, TJobletPtr joblet) const;

    void GenerateAlertIfNeeded(
        ITaskHost* taskHost,
        const TDataFlowGraph::TVertexDescriptor& taskName) const;

    void Persist(const TPersistenceContext& context);

private:
    NJobTrackerClient::TJobId FailedTreatmentJob_;
    NJobTrackerClient::TJobId FailedControlJob_;
    THashSet<NJobTrackerClient::TJobId> LostJobs_;
    TOperationSpecBasePtr OperationSpec_;
    TJobExperimentBasePtr JobExperiment_;
    int FailedControlJobCount_ = 0;
    int FailedTreatmentJobCount_ = 0;
    int SucceededTreatmentJobCount_ = 0;
    EJobExperimentStatus ExperimentStatus_ = EJobExperimentStatus::NoTreatmentSuccessesYet;

    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
        NJobTrackerClient::EJobState state) override;

    bool IsTreatmentRequired() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
