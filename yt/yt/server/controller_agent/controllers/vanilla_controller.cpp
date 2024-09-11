#include "vanilla_controller.h"

#include "job_info.h"
#include "operation_controller_detail.h"
#include "table.h"
#include "task.h"

#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/vanilla_chunk_pool.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NControllerAgent::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NYTree;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TVanillaController;

class TVanillaTask
    : public TTask
{
public:
    TVanillaTask(
        ITaskHostPtr taskHost,
        TVanillaTaskSpecPtr spec,
        TString name,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    //! Used only for persistence.
    TVanillaTask() = default;

    void Persist(const TPersistenceContext& context) override;

    TString GetTitle() const override;

    TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;

    IPersistentChunkPoolInputPtr GetChunkPoolInput() const override;

    IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override;

    TUserJobSpecPtr GetUserJobSpec() const override;

    TExtendedJobResources GetNeededResources(const TJobletPtr& /*joblet*/) const override;

    TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override;

    EJobType GetJobType() const override;

    void FinishInput() override;

    TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    bool IsJobInterruptible() const override;

protected:
    bool IsInputDataWeightHistogramSupported() const override;

    TJobSplitterConfigPtr GetJobSplitterConfig() const override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaTask, 0x55e9aacd);

    TVanillaTaskSpecPtr Spec_;
    TString Name_;

    TJobSpec JobSpecTemplate_;

    //! This chunk pool does not really operate with chunks, it is used as an interface for a job counter in it.
    IPersistentChunkPoolOutputPtr VanillaChunkPool_;

    void InitJobSpecTemplate();
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaTask);
DEFINE_REFCOUNTED_TYPE(TVanillaTask)
DECLARE_REFCOUNTED_CLASS(TVanillaTask)

////////////////////////////////////////////////////////////////////////////////

class TVanillaController
    : public TOperationControllerBase
{
public:
    TVanillaController(
        TVanillaOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TVanillaOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation);

    //! Used only for persistence.
    TVanillaController() = default;

    void Persist(const TPersistenceContext& context) override;

    void CustomMaterialize() override;

    TString GetLoggingProgress() const override;

    std::vector<TRichYPath> GetInputTablePaths() const override;

    void InitOutputTables() override;

    std::vector<TRichYPath> GetOutputTablePaths() const override;

    std::optional<TRichYPath> GetStderrTablePath() const override;

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override;

    std::optional<TRichYPath> GetCoreTablePath() const override;

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override;

    bool GetEnableCudaGpuCoreDump() const override;

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override;

    TYsonStructPtr GetTypedSpec() const override;

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override;

    bool IsCompleted() const override;

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override;

    void ValidateRevivalAllowed() const override;

    void ValidateSnapshot() const override;

    void InitUserJobSpec(
        NControllerAgent::NProto::TUserJobSpec* proto,
        TJobletPtr joblet) const override;

    TJobletPtr CreateJoblet(
        TTask* task,
        TJobId jobId,
        TString treeId,
        int taskJobIndex,
        std::optional<TString> poolPath,
        bool treeIsTentative) final;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaController, 0x99fa99ae);

    TVanillaOperationSpecPtr Spec_;
    TVanillaOperationOptionsPtr Options_;

    std::vector<TVanillaTaskPtr> Tasks_;
    std::vector<std::vector<TOutputTablePtr>> TaskOutputTables_;

    void ValidateOperationLimits();
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaController);

////////////////////////////////////////////////////////////////////////////////

TVanillaTask::TVanillaTask(
    ITaskHostPtr taskHost,
    TVanillaTaskSpecPtr spec,
    TString name,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
    : TTask(std::move(taskHost), std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
    , Spec_(std::move(spec))
    , Name_(std::move(name))
    , VanillaChunkPool_(CreateVanillaChunkPool({Spec_->JobCount, Spec_->RestartCompletedJobs, Logger}))
{ }

void TVanillaTask::Persist(const TPersistenceContext& context)
{
    TTask::Persist(context);

    using NYT::Persist;
    Persist(context, Spec_);
    Persist(context, Name_);
    Persist(context, VanillaChunkPool_);
    Persist(context, JobSpecTemplate_);
}

TString TVanillaTask::GetTitle() const
{
    return Format("Vanilla(%v)", Name_);
}

TDataFlowGraph::TVertexDescriptor TVanillaTask::GetVertexDescriptor() const
{
    return Spec_->TaskTitle;
}

IPersistentChunkPoolInputPtr TVanillaTask::GetChunkPoolInput() const
{
    static IPersistentChunkPoolInputPtr NullPool = nullptr;
    return NullPool;
}

IPersistentChunkPoolOutputPtr TVanillaTask::GetChunkPoolOutput() const
{
    return VanillaChunkPool_;
}

TUserJobSpecPtr TVanillaTask::GetUserJobSpec() const
{
    return Spec_;
}

TExtendedJobResources TVanillaTask::GetNeededResources(const TJobletPtr& /*joblet*/) const
{
    return GetMinNeededResourcesHeavy();
}

TExtendedJobResources TVanillaTask::GetMinNeededResourcesHeavy() const
{
    TExtendedJobResources result;
    result.SetUserSlots(1);
    result.SetCpu(Spec_->CpuLimit);
    // NB: JobProxyMemory is the only memory that is related to IO. Footprint is accounted below.
    result.SetJobProxyMemory(0);
    AddFootprintAndUserJobResources(result);
    return result;
}

void TVanillaTask::BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
{
    VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    jobSpec->CopyFrom(JobSpecTemplate_);
    AddOutputTableSpecs(jobSpec, joblet);
}

EJobType TVanillaTask::GetJobType() const
{
    return EJobType::Vanilla;
}

void TVanillaTask::FinishInput()
{
    TTask::FinishInput();

    InitJobSpecTemplate();
}

TJobFinishedResult TVanillaTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    auto result = TTask::OnJobCompleted(joblet, jobSummary);

    RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

    // When restart_completed_jobs = %true, job completion may create new pending jobs in same task.
    UpdateTask();

    return result;
}

bool TVanillaTask::IsJobInterruptible() const
{
    if (!TTask::IsJobInterruptible()) {
        return false;
    }

    // We do not allow to interrupt job without interruption_signal
    // because there are no more ways to notify vanilla job about it.
    return Spec_->InterruptionSignal.has_value();
}

bool TVanillaTask::IsInputDataWeightHistogramSupported() const
{
    return false;
}

TJobSplitterConfigPtr TVanillaTask::GetJobSplitterConfig() const
{
    // In vanilla operations we don't want neither job splitting nor job speculation.
    auto config = TaskHost_->GetJobSplitterConfigTemplate();
    config->EnableJobSplitting = false;
    config->EnableJobSpeculation = false;

    return config;
}

void TVanillaTask::InitJobSpecTemplate()
{
    JobSpecTemplate_.set_type(static_cast<int>(EJobType::Vanilla));
    auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);

    jobSpecExt->set_io_config(ConvertToYsonString(Spec_->JobIO).ToString());

    TaskHost_->InitUserJobSpecTemplate(
        jobSpecExt->mutable_user_job_spec(),
        Spec_,
        TaskHost_->GetUserFiles(Spec_),
        TaskHost_->GetSpec()->DebugArtifactsAccount);
}

////////////////////////////////////////////////////////////////////////////////

TVanillaController::TVanillaController(
    TVanillaOperationSpecPtr spec,
    TControllerAgentConfigPtr config,
    TVanillaOperationOptionsPtr options,
    IOperationControllerHostPtr host,
    TOperation* operation)
    : TOperationControllerBase(
        spec,
        config,
        options,
        host,
        operation)
    , Spec_(spec)
    , Options_(options)
{ }

void TVanillaController::Persist(const TPersistenceContext& context)
{
    TOperationControllerBase::Persist(context);

    using NYT::Persist;
    Persist(context, Spec_);
    Persist(context, Options_);
    Persist(context, Tasks_);
    Persist(context, TaskOutputTables_);
}

void TVanillaController::CustomMaterialize()
{
    ValidateOperationLimits();

    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        std::vector<TOutputStreamDescriptorPtr> streamDescriptors;
        int taskIndex = Tasks.size();
        for (int index = 0; index < std::ssize(TaskOutputTables_[taskIndex]); ++index) {
            auto streamDescriptor = TaskOutputTables_[taskIndex][index]->GetStreamDescriptorTemplate(index)->Clone();
            streamDescriptor->DestinationPool = GetSink();
            streamDescriptor->TargetDescriptor = TDataFlowGraph::SinkDescriptor;
            streamDescriptors.push_back(std::move(streamDescriptor));
        }
        auto task = New<TVanillaTask>(
            this,
            taskSpec,
            taskName,
            std::move(streamDescriptors),
            std::vector<TInputStreamDescriptorPtr>{});
        RegisterTask(task);
        FinishTaskInput(task);

        GetDataFlowGraph()->RegisterEdge(
            TDataFlowGraph::SourceDescriptor,
            task->GetVertexDescriptor());

        Tasks_.emplace_back(std::move(task));
        ValidateUserFileCount(taskSpec, taskName);
    }
}

TString TVanillaController::GetLoggingProgress() const
{
    const auto& jobCounter = GetTotalJobCounter();
    return Format(
        "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v}, ",
        jobCounter->GetTotal(),
        jobCounter->GetRunning(),
        jobCounter->GetCompletedTotal(),
        GetPendingJobCount(),
        jobCounter->GetFailed(),
        jobCounter->GetAbortedTotal());
}

std::vector<TRichYPath> TVanillaController::GetInputTablePaths() const
{
    return {};
}

void TVanillaController::InitOutputTables()
{
    TOperationControllerBase::InitOutputTables();

    TaskOutputTables_.reserve(Spec_->Tasks.size());
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        auto& taskOutputTables = TaskOutputTables_.emplace_back();
        taskOutputTables.reserve(taskSpec->OutputTablePaths.size());
        for (const auto& outputTablePath : taskSpec->OutputTablePaths) {
            taskOutputTables.push_back(GetOrCrash(PathToOutputTable_, outputTablePath.GetPath()));
        }
    }
}

std::vector<TRichYPath> TVanillaController::GetOutputTablePaths() const
{
    std::vector<TRichYPath> outputTablePaths;
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        outputTablePaths.insert(outputTablePaths.end(), taskSpec->OutputTablePaths.begin(), taskSpec->OutputTablePaths.end());
    }
    return outputTablePaths;
}

std::optional<TRichYPath> TVanillaController::GetStderrTablePath() const
{
    return Spec_->StderrTablePath;
}

TBlobTableWriterConfigPtr TVanillaController::GetStderrTableWriterConfig() const
{
    return Spec_->StderrTableWriter;
}

std::optional<TRichYPath> TVanillaController::GetCoreTablePath() const
{
    return Spec_->CoreTablePath;
}

TBlobTableWriterConfigPtr TVanillaController::GetCoreTableWriterConfig() const
{
    return Spec_->CoreTableWriter;
}

bool TVanillaController::GetEnableCudaGpuCoreDump() const
{
    return Spec_->EnableCudaGpuCoreDump;
}

TStringBuf TVanillaController::GetDataWeightParameterNameForJob(EJobType /*jobType*/) const
{
    return TStringBuf();
}

TYsonStructPtr TVanillaController::GetTypedSpec() const
{
    return Spec_;
}

std::vector<EJobType> TVanillaController::GetSupportedJobTypesForJobsDurationAnalyzer() const
{
    return {};
}

bool TVanillaController::IsCompleted() const
{
    for (const auto& task : Tasks_) {
        if (!task->IsCompleted()) {
            return false;
        }
    }

    return true;
}

std::vector<TUserJobSpecPtr> TVanillaController::GetUserJobSpecs() const
{
    std::vector<TUserJobSpecPtr> specs;
    specs.reserve(Spec_->Tasks.size());
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        specs.emplace_back(taskSpec);
    }
    return specs;
}

void TVanillaController::ValidateRevivalAllowed() const
{
    // Even if fail_on_job_restart is set, we can not decline revival at this point
    // as it is still possible that all jobs are running or completed, thus the revival is permitted.
}

void TVanillaController::ValidateSnapshot() const
{
    if (!HasJobUniquenessRequirements()) {
        return;
    }

    int expectedJobCount = 0;
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        expectedJobCount += taskSpec->JobCount;
    }
    const auto& jobCounter = GetTotalJobCounter();
    int startedJobCount = jobCounter->GetRunning() + jobCounter->GetCompletedTotal();

    if (expectedJobCount != jobCounter->GetRunning()) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::OperationFailedOnJobRestart,
            "Cannot revive operation when \"fail_on_job_restart\" option is set in operation spec or user job spec "
            "and not all jobs have already been started according to the operation snapshot "
            "(i.e. not all jobs are running or completed)")
            << TErrorAttribute("reason", EFailOnJobRestartReason::JobCountMismatchAfterRevival)
            << TErrorAttribute("operation_type", OperationType)
            << TErrorAttribute("expected_job_count", expectedJobCount)
            << TErrorAttribute("started_job_count", startedJobCount);
    }
}

void TVanillaController::InitUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* proto,
    TJobletPtr joblet) const
{
    TOperationControllerBase::InitUserJobSpec(proto, std::move(joblet));
    proto->add_environment(Format("YT_OPERATION_INCARNATION=%v", 0));
}

TJobletPtr TVanillaController::CreateJoblet(
    TTask* task,
    TJobId jobId,
    TString treeId,
    int taskJobIndex,
    std::optional<TString> poolPath,
    bool treeIsTentative)
{
    auto joblet = TOperationControllerBase::CreateJoblet(
        task,
        jobId,
        std::move(treeId),
        taskJobIndex,
        std::move(poolPath),
        treeIsTentative);

    joblet->OperationIncarnation = "0";

    return joblet;
}

void TVanillaController::ValidateOperationLimits()
{
    if (std::ssize(Spec_->Tasks) > Options_->MaxTaskCount) {
        THROW_ERROR_EXCEPTION("Maximum number of tasks exceeded: %v > %v", Spec_->Tasks.size(), Options_->MaxTaskCount);
    }

    i64 totalJobCount = 0;
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        totalJobCount += taskSpec->JobCount;
    }
    if (totalJobCount > Options_->MaxTotalJobCount) {
        THROW_ERROR_EXCEPTION("Maximum total job count exceeded: %v > %v", totalJobCount, Options_->MaxTotalJobCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateVanillaController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->VanillaOperationOptions;
    auto spec = ParseOperationSpec<TVanillaOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TVanillaController>(std::move(spec), std::move(config), std::move(options), std::move(host), operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
