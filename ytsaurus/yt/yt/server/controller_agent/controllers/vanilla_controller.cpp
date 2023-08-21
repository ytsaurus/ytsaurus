#include "vanilla_controller.h"

#include "job_info.h"
#include "operation_controller_detail.h"
#include "table.h"
#include "task.h"

#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/vanilla_chunk_pool.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

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

class TVanillaTask
    : public TTask
{
public:
    TVanillaTask(
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

    //! Used only for persistence.
    TVanillaTask() = default;

    void Persist(const TPersistenceContext& context) override
    {
        TTask::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Name_);
        Persist(context, VanillaChunkPool_);
        Persist(context, JobSpecTemplate_);
    }

    TString GetTitle() const override
    {
        return Format("Vanilla(%v)", Name_);
    }

    TString GetVertexDescriptor() const override
    {
        return Spec_->TaskTitle;
    }

    IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
    {
        static IPersistentChunkPoolInputPtr NullPool = nullptr;
        return NullPool;
    }

    IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
    {
        return VanillaChunkPool_;
    }

    TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec_;
    }

    TExtendedJobResources GetNeededResources(const TJobletPtr& /*joblet*/) const override
    {
        return GetMinNeededResourcesHeavy();
    }

    TExtendedJobResources GetMinNeededResourcesHeavy() const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(Spec_->CpuLimit);
        // NB: JobProxyMemory is the only memory that is related to IO. Footprint is accounted below.
        result.SetJobProxyMemory(0);
        AddFootprintAndUserJobResources(result);
        return result;
    }

    void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

        jobSpec->CopyFrom(JobSpecTemplate_);
        AddOutputTableSpecs(jobSpec, joblet);
    }

    EJobType GetJobType() const override
    {
        return EJobType::Vanilla;
    }

    void FinishInput() override
    {
        TTask::FinishInput();

        InitJobSpecTemplate();
    }

    TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
    {
        auto result = TTask::OnJobCompleted(joblet, jobSummary);

        RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

        // When restart_completed_jobs = %true, job completion may create new pending jobs in same task.
        UpdateTask();

        return result;
    }

    bool IsJobInterruptible() const override
    {
        if (!TTask::IsJobInterruptible()) {
            return false;
        }

        // We do not allow to interrupt job without interruption_signal
        // because there are no more ways to notify vanilla job about it.
        return Spec_->InterruptionSignal.has_value();
    }

protected:
    bool IsInputDataWeightHistogramSupported() const override
    {
        return false;
    }

    TJobSplitterConfigPtr GetJobSplitterConfig() const override
    {
        // In vanilla operations we don't want neither job splitting nor job speculation.
        auto config = TaskHost_->GetJobSplitterConfigTemplate();
        config->EnableJobSplitting = false;
        config->EnableJobSpeculation = false;

        return config;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaTask, 0x55e9aacd);

    TVanillaTaskSpecPtr Spec_;
    TString Name_;

    TJobSpec JobSpecTemplate_;

    //! This chunk pool does not really operate with chunks, it is used as an interface for a job counter in it.
    IPersistentChunkPoolOutputPtr VanillaChunkPool_;

    void InitJobSpecTemplate()
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::Vanilla));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_io_config(ConvertToYsonString(Spec_->JobIO).ToString());

        TaskHost_->InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec_,
            TaskHost_->GetUserFiles(Spec_),
            TaskHost_->GetSpec()->DebugArtifactsAccount);
    }
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

    //! Used only for persistence.
    TVanillaController() = default;

    void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, Tasks_);
        Persist(context, TaskOutputTables_);
    }

    void CustomMaterialize() override
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

    TString GetLoggingProgress() const override
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

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return {};
    }

    void InitOutputTables() override
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

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> outputTablePaths;
        for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
            outputTablePaths.insert(outputTablePaths.end(), taskSpec->OutputTablePaths.begin(), taskSpec->OutputTablePaths.end());
        }
        return outputTablePaths;
    }

    std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec_->StderrTablePath;
    }

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec_->StderrTableWriter;
    }

    std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec_->CoreTablePath;
    }

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec_->CoreTableWriter;
    }

    bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec_->EnableCudaGpuCoreDump;
    }

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf();
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {};
    }

    bool IsCompleted() const override
    {
        for (const auto& task : Tasks_) {
            if (!task->IsCompleted()) {
                return false;
            }
        }

        return true;
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        std::vector<TUserJobSpecPtr> specs;
        specs.reserve(Spec_->Tasks.size());
        for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
            specs.emplace_back(taskSpec);
        }
        return specs;
    }

    void ValidateRevivalAllowed() const override
    {
        // Even if fail_on_job_restart is set, we can not decline revival at this point
        // as it is still possible that all jobs are running or completed, thus the revival is permitted.
    }

    void ValidateSnapshot() const override
    {
        if (!Spec_->FailOnJobRestart) {
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
                "Cannot revive operation when \"fail_on_job_restart\" spec option is set and not "
                "all jobs have already been started according to the operation snapshot"
                " (i.e. not all jobs are running or completed)")
                << TErrorAttribute("reason", EFailOnJobRestartReason::JobCountMismatchAfterRevival)
                << TErrorAttribute("operation_type", OperationType)
                << TErrorAttribute("expected_job_count", expectedJobCount)
                << TErrorAttribute("started_job_count", startedJobCount);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaController, 0x99fa99ae);

    TVanillaOperationSpecPtr Spec_;
    TVanillaOperationOptionsPtr Options_;

    std::vector<TVanillaTaskPtr> Tasks_;
    std::vector<std::vector<TOutputTablePtr>> TaskOutputTables_;

    void ValidateOperationLimits()
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
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaController);

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
