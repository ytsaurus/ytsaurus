#include "vanilla_controller.h"

#include "operation.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "config.h"
#include "table.h"
#include "job_info.h"

#include <yt/server/chunk_pools/vanilla_chunk_pool.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/ypath/rich.h>

namespace NYT {
namespace NControllerAgent {

using namespace NJobTrackerClient::NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NYTree;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TVanillaTask
    : public TTask
{
public:
    TVanillaTask(
        ITaskHostPtr taskHost,
        TVanillaTaskSpecPtr spec,
        TString name, TTaskGroupPtr
        taskGroup,
        std::vector<TEdgeDescriptor> edgeDescriptors)
        : TTask(std::move(taskHost), std::move(edgeDescriptors))
        , Spec_(std::move(spec))
        , Name_(std::move(name))
        , TaskGroup_(taskGroup.Get())
        , VanillaChunkPool_(CreateVanillaChunkPool(Spec_->JobCount))
    { }

    //! Used only for persistence.
    TVanillaTask() = default;

    void Persist(const TPersistenceContext& context) override
    {
        TTask::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Name_);
        Persist(context, TaskGroup_);
        Persist(context, VanillaChunkPool_);
        Persist(context, JobSpecTemplate_);
    }

    virtual TString GetTitle() const override
    {
        return Format("Vanilla(%v)", Name_);
    }

    virtual TString GetVertexDescriptor() const override
    {
        return Spec_->TaskTitle;
    }

    virtual TTaskGroupPtr GetGroup() const override
    {
        return TaskGroup_;
    }

    virtual IChunkPoolInput* GetChunkPoolInput() const override
    {
        return nullptr;
    }

    virtual IChunkPoolOutput* GetChunkPoolOutput() const override
    {
        return VanillaChunkPool_.get();
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec_;
    }

    virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
    {
        return GetMinNeededResourcesHeavy();
    }

    virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(Spec_->CpuLimit);
        // NB: JobProxyMemory is the only memory that is related to IO. Footprint is accounted below.
        result.SetJobProxyMemory(0);
        AddFootprintAndUserJobResources(result);
        return result;
    }

    virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) override
    {
        jobSpec->CopyFrom(JobSpecTemplate_);
        AddOutputTableSpecs(jobSpec, joblet);
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::Vanilla;
    }

    virtual void FinishInput() override
    {
        TTask::FinishInput();

        InitJobSpecTemplate();
    }

    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
    {
        auto result = TTask::OnJobCompleted(joblet, jobSummary);

        RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

        return result;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaTask, 0x55e9aacd);

    TVanillaTaskSpecPtr Spec_;
    TString Name_;
    TTaskGroup* TaskGroup_;

    TJobSpec JobSpecTemplate_;

    //! This chunk pool does not really operate with chunks, it is used as an interface for a job counter in it.
    std::unique_ptr<IChunkPoolOutput> VanillaChunkPool_;

    void InitJobSpecTemplate()
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::Vanilla));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_io_config(ConvertToYsonString(Spec_->JobIO).GetData());

        TaskHost_->InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec_,
            TaskHost_->GetUserFiles(Spec_),
            TaskHost_->GetSpec()->JobNodeAccount);
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

    virtual void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, Tasks_);
        Persist(context, TaskGroup_);
        Persist(context, TaskOutputTables_);
    }

    virtual void CustomPrepare() override
    {
        TaskGroup_ = New<TTaskGroup>();
        RegisterTaskGroup(TaskGroup_);
        for (const auto& pair : Spec_->Tasks) {
            const auto& taskName = pair.first;
            const auto& taskSpec = pair.second;
            std::vector<TEdgeDescriptor> edgeDescriptors;
            int taskIndex = Tasks.size();
            for (int index = 0; index < TaskOutputTables_[taskIndex].size(); ++index) {
                edgeDescriptors.emplace_back(TaskOutputTables_[taskIndex][index]->GetEdgeDescriptorTemplate(index));
            }
            auto task = New<TVanillaTask>(this, taskSpec, taskName, TaskGroup_, std::move(edgeDescriptors));
            RegisterTask(task);
            FinishTaskInput(task);
            Tasks_.emplace_back(std::move(task));
            ValidateUserFileCount(taskSpec, taskName);
        }
    }

    virtual TString GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v}, ",
            JobCounter->GetTotal(),
            JobCounter->GetRunning(),
            JobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter->GetFailed(),
            JobCounter->GetAbortedTotal());
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const
    {
        return {};
    }

    virtual void InitOutputTables() override
    {
        for (const auto& pair : Spec_->Tasks) {
            TaskOutputTables_.emplace_back();
            const auto& taskSpec = pair.second;
            for (const auto& outputTablePath : taskSpec->OutputTablePaths) {
                TaskOutputTables_.back().emplace_back(RegisterOutputTable(outputTablePath));
            }
        }
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const
    {
        std::vector<TRichYPath> outputTablePaths;
        for (const auto& pair : Spec_->Tasks) {
            const auto& taskSpec = pair.second;
            outputTablePaths.insert(outputTablePaths.end(), taskSpec->OutputTablePaths.begin(), taskSpec->OutputTablePaths.end());
        }
        return outputTablePaths;
    }

    virtual std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec_->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec_->StderrTableWriter;
    }

    virtual std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec_->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec_->CoreTableWriter;
    }

    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const
    {
        return TStringBuf();
    }

    virtual TYsonSerializablePtr GetTypedSpec() const
    {
        return Spec_;
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const
    {
        return {};
    }

    virtual bool IsCompleted() const
    {
        for (const auto& task : Tasks_) {
            if (!task->IsCompleted()) {
                return false;
            }
        }

        return true;
    }

    virtual std::vector<TUserJobSpecPtr> GetUserJobSpecs() const
    {
        std::vector<TUserJobSpecPtr> specs;
        specs.reserve(Spec_->Tasks.size());
        for (const auto& pair : Spec_->Tasks) {
            const auto& spec = pair.second;
            specs.emplace_back(spec);
        }
        return specs;
    }

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }

    virtual void ValidateRevivalAllowed() const override
    {
        // Even if fail_on_job_restart is set, we can not decline revival at this point
        // as it is still possible that all jobs are running or completed, thus the revival is permitted.
    }

    virtual void ValidateSnapshot() const override
    {
        if (!Spec_->FailOnJobRestart) {
            return;
        }

        int expectedJobCount = 0;
        for (const auto& pair : Spec_->Tasks) {
            expectedJobCount += pair.second->JobCount;
        }
        int startedJobCount = JobCounter->GetRunning() + JobCounter->GetCompletedTotal();

        if (expectedJobCount != JobCounter->GetRunning()) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::OperationFailedOnJobRestart,
                "Cannot revive operation when spec option fail_on_job_restart is set and not "
                "all jobs have already been started according to the operation snapshot"
                "(i.e. not all jobs are running or completed)")
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
    TTaskGroupPtr TaskGroup_;
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

} // namespace NControllerAgent
} // namespace NYT
