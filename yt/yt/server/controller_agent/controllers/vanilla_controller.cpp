#include "vanilla_controller.h"

#include "job_info.h"
#include "operation_controller_detail.h"
#include "table.h"
#include "task.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/operation.h>

#include <yt/yt/server/lib/chunk_pools/vanilla_chunk_pool.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NConcurrency;
using namespace NControllerAgent::NProto;
using namespace NScheduler::NProto;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TVanillaController;

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
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    //! Used only for persistence.
    TVanillaTask() = default;

    TString GetTitle() const override;
    TString GetName() const;

    TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override;

    IPersistentChunkPoolInputPtr GetChunkPoolInput() const override;

    IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override;

    TUserJobSpecPtr GetUserJobSpec() const override;

    TExtendedJobResources GetNeededResources(const TJobletPtr& /*joblet*/) const override;

    TExtendedJobResources GetMinNeededResourcesHeavy() const override;

    void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override;

    THashMap<TString, TString> BuildJobEnvironment() const override;

    EJobType GetJobType() const override;

    void FinishInput() override;

    TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override;

    bool IsJobInterruptible() const override;

    bool IsJobRestartingEnabled() const noexcept;

    int GetTargetJobCount() const noexcept;

    IVanillaChunkPoolOutputPtr GetVanillaChunkPool() const noexcept;

    TConfigurator<TVanillaTaskSpec> ConfigureUpdate();

protected:
    TVanillaController* VanillaController_ = nullptr;

    TVanillaTaskSpecPtr Spec_;
    TString Name_;

    //! This chunk pool does not really operate with chunks, it is used as an interface for a job counter in it.
    IVanillaChunkPoolOutputPtr VanillaChunkPool_;

private:
    TJobSpec JobSpecTemplate_;

    bool IsInputDataWeightHistogramSupported() const override;

    TJobSplitterConfigPtr GetJobSplitterConfig() const override;

    void InitJobSpecTemplate();

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TVanillaTask, 0x55e9aacd);
};

PHOENIX_DEFINE_TYPE(TVanillaTask);
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

    int GetTotalTargetJobCount() const;

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override;

    void ValidateRevivalAllowed() const override;

    void ValidateSnapshot() const override;

    void InitUserJobSpec(
        NControllerAgent::NProto::TUserJobSpec* proto,
        const TJobletPtr& joblet) const override;

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override;
    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override;

    using TOperationControllerBase::HasJobUniquenessRequirements;

    virtual bool IsOperationGang() const noexcept;

    void AbortJobsByCookies(
        TTask* task,
        const std::vector<NChunkPools::TOutputCookie>& cookies,
        EAbortReason abortReason);

protected:
    std::vector<TVanillaTaskPtr> Tasks_;
    TVanillaOperationOptionsPtr Options_;

private:
    TVanillaOperationSpecPtr Spec_;

    std::vector<std::vector<TOutputTablePtr>> TaskOutputTables_;

    int TotalTargetJobCount_ = 0;

    void ValidateOperationLimits();

    virtual TVanillaTaskPtr CreateTask(
        ITaskHostPtr taskHost,
        TVanillaTaskSpecPtr spec,
        TString name,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TVanillaController, 0x99fa99ae);
};

PHOENIX_DEFINE_TYPE(TVanillaController);

////////////////////////////////////////////////////////////////////////////////

TVanillaTask::TVanillaTask(
    ITaskHostPtr taskHost,
    TVanillaTaskSpecPtr spec,
    TString name,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
    : TTask(std::move(taskHost), std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
    , VanillaController_(dynamic_cast<TVanillaController*>(TaskHost_))
    , Spec_(std::move(spec))
    , Name_(std::move(name))
    , VanillaChunkPool_(CreateVanillaChunkPool(TVanillaChunkPoolOptions{
        .JobCount = Spec_->JobCount,
        .RestartCompletedJobs = Spec_->RestartCompletedJobs,
        .Logger = Logger.WithTag("Name: %v", Name_),
    }))
{ }

void TVanillaTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Name_);
    PHOENIX_REGISTER_FIELD(3, VanillaChunkPool_);
    PHOENIX_REGISTER_FIELD(4, JobSpecTemplate_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        // COMPAT(pogorelov)
        this_->VanillaChunkPool_->SetJobCount(this_->Spec_->JobCount);

        this_->VanillaController_ = dynamic_cast<TVanillaController*>(this_->TaskHost_);
        YT_VERIFY(this_->VanillaController_);
    });
}

TString TVanillaTask::GetTitle() const
{
    return Format("Vanilla(%v)", Name_);
}

TString TVanillaTask::GetName() const
{
    return Name_;
}

TDataFlowGraph::TVertexDescriptor TVanillaTask::GetVertexDescriptor() const
{
    return GetName();
}

IPersistentChunkPoolInputPtr TVanillaTask::GetChunkPoolInput() const
{
    static IPersistentChunkPoolInputPtr NullPool = nullptr;
    return NullPool;
}

IPersistentChunkPoolOutputPtr TVanillaTask::GetChunkPoolOutput() const
{
    return GetVanillaChunkPool();
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
    result.SetJobProxyMemoryWithFixedWriteBufferSize(0);
    AddFootprintAndUserJobResources(result);
    return result;
}

void TVanillaTask::BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    jobSpec->CopyFrom(JobSpecTemplate_);
    AddOutputTableSpecs(jobSpec, joblet);
}

THashMap<TString, TString> TVanillaTask::BuildJobEnvironment() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return THashMap<TString, TString>{
        {"YT_TASK_NAME", GetName()},
    };
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

bool TVanillaTask::IsJobRestartingEnabled() const noexcept
{
    return static_cast<bool>(Spec_->GangOptions);
}

int TVanillaTask::GetTargetJobCount() const noexcept
{
    return Spec_->JobCount;
}

IVanillaChunkPoolOutputPtr TVanillaTask::GetVanillaChunkPool() const noexcept
{
    return VanillaChunkPool_;
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
    JobSpecTemplate_.set_type(ToProto(EJobType::Vanilla));
    auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);

    jobSpecExt->set_io_config(ConvertToYsonString(Spec_->JobIO).ToString());

    TaskHost_->InitUserJobSpecTemplate(
        jobSpecExt->mutable_user_job_spec(),
        Spec_,
        TaskHost_->GetUserFiles(Spec_),
        TaskHost_->GetSpec()->DebugArtifactsAccount);
}

TConfigurator<TVanillaTaskSpec> TVanillaTask::ConfigureUpdate()
{
    TConfigurator<TVanillaTaskSpec> configurator;
    configurator.Field("job_count", &TVanillaTaskSpec::JobCount)
        .Validator(BIND_NO_PROPAGATE([&] (const int& oldJobCount, const int& /*newJobCount*/) {
            YT_VERIFY(oldJobCount == VanillaChunkPool_->GetJobCount());
            THROW_ERROR_EXCEPTION_IF(
                VanillaController_->IsOperationGang(),
                "Cannot update \"job_count\" for gang operations");

            // TODO(coteeq): It is actually saner to check for a single task's
            // FailOnJobRestart but it is probably not very useful anyway.
            THROW_ERROR_EXCEPTION_IF(
                VanillaController_->HasJobUniquenessRequirements(),
                "Cannot update \"job_count\" when \"fail_on_job_restart\" is set");
        }))
        .Updater(BIND_NO_PROPAGATE([&] (const int& oldJobCount, const int& newJobCount) {
            YT_LOG_DEBUG_IF(
                oldJobCount != VanillaChunkPool_->GetJobCount(),
                "Chunk pool's job count is inconsistent with the spec; "
                "assuming revival (SpecJobCount: %v, ChunkPoolJobCount: %v)",
                oldJobCount,
                VanillaChunkPool_->GetJobCount());

            auto cookiesToAbort = VanillaChunkPool_->UpdateJobCount(newJobCount);
            if (!cookiesToAbort.empty()) {
                constexpr int MaxCookiesInLog = 5;
                YT_LOG_DEBUG(
                    "Try to abort jobs by cookies (Count: %v, Cookies: %v)",
                    cookiesToAbort.size(),
                    MakeShrunkFormattableView(cookiesToAbort, TDefaultFormatter(), MaxCookiesInLog));

                VanillaController_->AbortJobsByCookies(
                    this,
                    cookiesToAbort,
                    EAbortReason::JobCountChangedByUserRequest);
            }
            UpdateTask();
        }));

    return configurator;
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
    , Options_(std::move(options))
    , Spec_(std::move(spec))
{ }

void TVanillaController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOperationControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
    PHOENIX_REGISTER_FIELD(3, Tasks_);
    PHOENIX_REGISTER_FIELD(4, TaskOutputTables_);
}

void TVanillaController::CustomMaterialize()
{
    ValidateOperationLimits();

    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        std::vector<TOutputStreamDescriptorPtr> streamDescriptors;
        int taskIndex = Tasks.size();
        TotalTargetJobCount_ += taskSpec->JobCount;
        for (int index = 0; index < std::ssize(TaskOutputTables_[taskIndex]); ++index) {
            auto streamDescriptor = TaskOutputTables_[taskIndex][index]->GetStreamDescriptorTemplate(index)->Clone();
            streamDescriptor->DestinationPool = GetSink();
            streamDescriptor->TargetDescriptor = TDataFlowGraph::SinkDescriptor;
            streamDescriptors.push_back(std::move(streamDescriptor));
        }

        auto task = CreateTask(
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

int TVanillaController::GetTotalTargetJobCount() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return TotalTargetJobCount_;
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
    const TJobletPtr& joblet) const
{
    YT_ASSERT_INVOKER_AFFINITY(GetJobSpecBuildInvoker());

    TOperationControllerBase::InitUserJobSpec(proto, joblet);
}

bool TVanillaController::IsOperationGang() const noexcept
{
    return false;
}

void TVanillaController::ValidateOperationLimits()
{
    if (std::ssize(Spec_->Tasks) > Options_->MaxTaskCount) {
        THROW_ERROR_EXCEPTION(
            "Maximum number of tasks exceeded: %v > %v",
            Spec_->Tasks.size(),
            Options_->MaxTaskCount);
    }

    i64 totalJobCount = 0;
    for (const auto& [taskName, taskSpec] : Spec_->Tasks) {
        totalJobCount += taskSpec->JobCount;
    }
    if (totalJobCount > Options_->MaxTotalJobCount) {
        THROW_ERROR_EXCEPTION(
            "Maximum total job count exceeded: %v > %v",
            totalJobCount,
            Options_->MaxTotalJobCount);
    }
}

TVanillaTaskPtr TVanillaController::CreateTask(
    ITaskHostPtr taskHost,
    TVanillaTaskSpecPtr spec,
    TString name,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
{
    return New<TVanillaTask>(
        std::move(taskHost),
        std::move(spec),
        std::move(name),
        std::move(outputStreamDescriptors),
        std::move(inputStreamDescriptors));
}

TOperationSpecBasePtr TVanillaController::ParseTypedSpec(const INodePtr& spec) const
{
    return ParseOperationSpec<TVanillaOperationSpec>(ConvertToNode(spec));
}

TOperationSpecBaseConfigurator TVanillaController::GetOperationSpecBaseConfigurator() const
{
    auto configurator = TConfigurator<TVanillaOperationSpec>();

    auto& tasksRegistrar = configurator.MapField("tasks", &TVanillaOperationSpec::Tasks)
        .ValidateOnAdded(BIND_NO_PROPAGATE([] (const TString& key, const TVanillaTaskSpecPtr& /*newSpec*/) {
            THROW_ERROR_EXCEPTION("Cannot create a new task")
                << TErrorAttribute("new_task_name", key);
        }))
        .ValidateOnRemoved(BIND_NO_PROPAGATE([] (const TString& key, const TVanillaTaskSpecPtr& /*oldSpec*/) {
            THROW_ERROR_EXCEPTION("Cannot remove a task")
                << TErrorAttribute("old_task_name", key);
        }))
        .OnAdded(BIND_NO_PROPAGATE([] (const TString& /*key*/, const TVanillaTaskSpecPtr& /*newSpec*/) -> TConfigurator<TVanillaTaskSpec> {
            YT_ABORT();
        }))
        .OnRemoved(BIND_NO_PROPAGATE([] (const TString& /*key*/, const TVanillaTaskSpecPtr& /*oldSpec*/) {
            YT_ABORT();
        }));

    for (const auto& task : Tasks_) {
        tasksRegistrar.ConfigureChild(task->GetName(), task->ConfigureUpdate());
    }

    return configurator;
}

void TVanillaController::AbortJobsByCookies(
    TTask* task,
    const std::vector<TOutputCookie>& cookiesVector,
    EAbortReason abortReason)
{
    THashSet<TOutputCookie> cookies(cookiesVector.begin(), cookiesVector.end());

    for (const auto& [_, allocation] : AllocationMap_) {
        if (cookies.empty()) {
            break;
        }

        if (!allocation.Joblet || allocation.Joblet->Task != task) {
            continue;
        }

        auto cookieIt = cookies.find(allocation.Joblet->OutputCookie);
        if (cookieIt != cookies.end()) {
            DoAbortJob(
                allocation.Joblet,
                abortReason,
                /*requestJobTrackerJobAbortion*/ true,
                /*force*/ true);
            cookies.erase(cookieIt);
        }
    }

    YT_VERIFY(cookies.empty());
}

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EOperationIncarnationSwitchReason,
    (JobAborted)
    (JobFailed)
    (JobInterrupted)
    (JobLackAfterRevival)
);

TEnumIndexedArray<EOperationIncarnationSwitchReason, NProfiling::TCounter> OperationIncarnationSwitchCounters;

NProfiling::TCounter GangOperationStartedCounter;

} // namespace

class TGangOperationController;

////////////////////////////////////////////////////////////////////////////////

struct TLastGangJobInfo
    : public TAllocation::TLastJobInfo
{
    NChunkPools::IChunkPoolOutput::TCookie OutputCookie;

    std::optional<TJobMonitoringDescriptor> MonitoringDescriptor;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TLastGangJobInfo, 0x2201d8d6);
};

class TGangJoblet
    : public TJoblet
{
public:
    using TJoblet::TJoblet;

    TOperationIncarnation OperationIncarnation;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TGangJoblet, 0x99fa99b3);
};

PHOENIX_DEFINE_TYPE(TGangJoblet);
DEFINE_REFCOUNTED_TYPE(TGangJoblet)
DECLARE_REFCOUNTED_CLASS(TGangJoblet)

////////////////////////////////////////////////////////////////////////////////

class TGangTask
    : public TVanillaTask
{
public:
    using TVanillaTask::TVanillaTask;

    void TrySwitchToNewOperationIncarnation(
        const TGangJobletPtr& joblet,
        bool operationIsReviving,
        EOperationIncarnationSwitchReason reason);

private:
    TNewJobConstraints GetNewJobConstraints(const TAllocation& allocation) const final;

    IChunkPoolOutput::TCookie ExtractCookieForAllocation(
        const TAllocation& allocation,
        const TNewJobConstraints& newJobConstraints) final;

    THashMap<TString, TString> BuildJobEnvironment() const final;

    TGangOperationController& GetOperationController() const;

    void StoreLastJobInfo(TAllocation& allocation, const TJobletPtr& joblet) const final;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TGangTask, 0x99fa90be);
};

PHOENIX_DEFINE_TYPE(TGangTask);
DEFINE_REFCOUNTED_TYPE(TGangTask)
DECLARE_REFCOUNTED_CLASS(TGangTask)

////////////////////////////////////////////////////////////////////////////////

class TGangOperationController
    : public TVanillaController
{
public:
    TGangOperationController(
        TVanillaOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TVanillaOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation);

    void TrySwitchToNewOperationIncarnation(
        const TGangJobletPtr& joblet,
        bool operationIsReviving,
        EOperationIncarnationSwitchReason reason);

    void OnOperationIncarnationChanged(bool operationIsReviving, EOperationIncarnationSwitchReason reason);

    bool OnJobCompleted(
        TJobletPtr joblet,
        std::unique_ptr<TCompletedJobSummary> jobSummary) final;

    bool OnJobFailed(
        TJobletPtr joblet,
        std::unique_ptr<TFailedJobSummary> jobSummary) final;

    bool OnJobAborted(
        TJobletPtr joblet,
        std::unique_ptr<TAbortedJobSummary> jobSummary) final;

    TJobletPtr CreateJoblet(
        TTask* task,
        TJobId jobId,
        TString treeId,
        int taskJobIndex,
        std::optional<TString> poolPath,
        bool treeIsTentative) final;

    bool IsOperationGang() const noexcept final;

    void SwitchToNewOperationIncarnation(bool operationIsReviving, EOperationIncarnationSwitchReason reason);

private:
    TOperationIncarnation Incarnation_;

    std::optional<EOperationIncarnationSwitchReason> ShouldRestartJobsOnRevival() const;

    void TrySwitchToNewOperationIncarnation(bool operationIsReviving, EOperationIncarnationSwitchReason reason);

    void VerifyJobsIncarnationsEqual() const;

    void OnOperationRevived() final;
    void BuildControllerInfoYson(NYTree::TFluentMap fluent) const final;

    void RestartAllRunningJobsPreservingAllocations(bool operationIsReviving);

    TOperationIncarnation GenerateNewIncarnation();

    TVanillaTaskPtr CreateTask(
        ITaskHostPtr taskHost,
        TVanillaTaskSpecPtr spec,
        TString name,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors) final;

    void ReportOperationIncarnationToArchive(const TGangJobletPtr& joblet) const;

    void OnJobStarted(const TJobletPtr& joblet) final;

    void InitUserJobSpec(
        NControllerAgent::NProto::TUserJobSpec* jobSpec,
        const TJobletPtr& joblet) const final;

    void EnrichJobInfo(NYTree::TFluentMap fluent, const TJobletPtr& joblet) const final;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TGangOperationController, 0x99fa99be);
};

PHOENIX_DEFINE_TYPE(TGangOperationController);

////////////////////////////////////////////////////////////////////////////////

void TLastGangJobInfo::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TAllocation::TLastJobInfo>();

    PHOENIX_REGISTER_FIELD(1, OutputCookie);
    PHOENIX_REGISTER_FIELD(2, MonitoringDescriptor);
}

PHOENIX_DEFINE_TYPE(TLastGangJobInfo);

void TGangJoblet::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJoblet>();

    PHOENIX_REGISTER_FIELD(1, OperationIncarnation);
}

////////////////////////////////////////////////////////////////////////////////

void TGangTask::TrySwitchToNewOperationIncarnation(
    const TGangJobletPtr& joblet,
    bool operationIsReviving,
    EOperationIncarnationSwitchReason reason)
{
    if (IsJobRestartingEnabled()) {
        YT_LOG_DEBUG("Trying to switch operation to new incarnation");

        GetOperationController().TrySwitchToNewOperationIncarnation(joblet, operationIsReviving, reason);
    } else {
        YT_LOG_DEBUG("Operation incarnation switch skipped due to task gang options");
    }
}

TVanillaTask::TNewJobConstraints TGangTask::GetNewJobConstraints(const TAllocation& allocation) const
{
    auto result = TTask::GetNewJobConstraints(allocation);
    if (auto lastJobInfo = static_cast<const TLastGangJobInfo*>(allocation.LastJobInfo.get())) {
        result.OutputCookie = lastJobInfo->OutputCookie;
        result.MonitoringDescriptor = lastJobInfo->MonitoringDescriptor;
        if (!result.MonitoringDescriptor) {
            // NB(pogorelov): If it would be just unset optional,
            // we could get descriptor intended for another allocation.
            result.MonitoringDescriptor = NullMonitoringDescriptor;
        }
    }

    return result;
}

IChunkPoolOutput::TCookie TGangTask::ExtractCookieForAllocation(
    const TAllocation& allocation,
    const TNewJobConstraints& newJobConstraints)
{
    if (newJobConstraints.OutputCookie) {
        VanillaChunkPool_->Extract(*newJobConstraints.OutputCookie);

        return *newJobConstraints.OutputCookie;
    }

    return VanillaChunkPool_->Extract(NodeIdFromAllocationId(allocation.Id));
}

THashMap<TString, TString> TGangTask::BuildJobEnvironment() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobEnvironment = TVanillaTask::BuildJobEnvironment();

    jobEnvironment.emplace("YT_TASK_JOB_COUNT", ToString(GetTargetJobCount()));
    jobEnvironment.emplace("YT_JOB_COUNT", ToString(GetOperationController().GetTotalTargetJobCount()));

    return jobEnvironment;
}

TGangOperationController& TGangTask::GetOperationController() const
{
    return static_cast<TGangOperationController&>(*VanillaController_);
}

void TGangTask::StoreLastJobInfo(TAllocation& allocation, const TJobletPtr& joblet) const
{
    auto lastJobInfo = std::make_unique<TLastGangJobInfo>();
    lastJobInfo->JobId = joblet->JobId;
    lastJobInfo->CompetitionType = joblet->CompetitionType;
    lastJobInfo->OutputCookie = joblet->OutputCookie;
    lastJobInfo->MonitoringDescriptor = joblet->UserJobMonitoringDescriptor;

    allocation.LastJobInfo = std::move(lastJobInfo);
}

void TGangTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TVanillaTask>();
}

////////////////////////////////////////////////////////////////////////////////

TGangOperationController::TGangOperationController(
    TVanillaOperationSpecPtr spec,
    TControllerAgentConfigPtr config,
    TVanillaOperationOptionsPtr options,
    IOperationControllerHostPtr host,
    TOperation* operation)
    : TVanillaController(
        std::move(spec),
        std::move(config),
        std::move(options),
        std::move(host),
        operation)
    , Incarnation_(GenerateNewIncarnation())
{
    YT_LOG_DEBUG("Gang operation controller created (Incarnation: %v)", Incarnation_);
    GangOperationStartedCounter.Increment();
}

void TGangOperationController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TVanillaController>();

    PHOENIX_REGISTER_FIELD(1, Incarnation_);
}

bool TGangOperationController::OnJobCompleted(
    TJobletPtr joblet,
    std::unique_ptr<TCompletedJobSummary> jobSummary)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    auto interruptionReason = jobSummary->InterruptionReason;

    if (!TOperationControllerBase::OnJobCompleted(joblet, std::move(jobSummary))) {
        return false;
    }

    if (joblet->JobType == EJobType::Vanilla && interruptionReason != EInterruptionReason::None) {
        static_cast<TGangTask*>(joblet->Task)->TrySwitchToNewOperationIncarnation(
            StaticPointerCast<TGangJoblet>(std::move(joblet)),
            /*operationIsReviving*/ false,
            EOperationIncarnationSwitchReason::JobInterrupted);
    }

    return true;
}

bool TGangOperationController::OnJobFailed(
    TJobletPtr joblet,
    std::unique_ptr<TFailedJobSummary> jobSummary)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    if (!TOperationControllerBase::OnJobFailed(joblet, std::move(jobSummary))) {
        return false;
    }

    if (joblet->JobType == EJobType::Vanilla) {
        static_cast<TGangTask*>(joblet->Task)->TrySwitchToNewOperationIncarnation(
            StaticPointerCast<TGangJoblet>(std::move(joblet)),
            /*operationIsReviving*/ false,
            EOperationIncarnationSwitchReason::JobFailed);
    }

    return true;
}

bool TGangOperationController::OnJobAborted(
    TJobletPtr joblet,
    std::unique_ptr<TAbortedJobSummary> jobSummary)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    if (!TOperationControllerBase::OnJobAborted(joblet, std::move(jobSummary))) {
        return false;
    }

    if (joblet->JobType == EJobType::Vanilla) {
        static_cast<TGangTask*>(joblet->Task)->TrySwitchToNewOperationIncarnation(
            StaticPointerCast<TGangJoblet>(std::move(joblet)),
            /*operationIsReviving*/ false,
            EOperationIncarnationSwitchReason::JobAborted);
    }

    return true;
}

TJobletPtr TGangOperationController::CreateJoblet(
    TTask* task,
    TJobId jobId,
    TString treeId,
    int taskJobIndex,
    std::optional<TString> poolPath,
    bool treeIsTentative)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(CancelableInvokerPool);

    auto joblet = New<TGangJoblet>(
        task,
        NextJobIndex(),
        taskJobIndex,
        std::move(treeId),
        treeIsTentative);

    joblet->StartTime = TInstant::Now();
    joblet->JobId = jobId;
    joblet->PoolPath = std::move(poolPath);

    joblet->OperationIncarnation = Incarnation_;

    return joblet;
}

void TGangOperationController::TrySwitchToNewOperationIncarnation(
    const TGangJobletPtr& joblet,
    bool operationIsReviving,
    EOperationIncarnationSwitchReason reason)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    if (auto state = State.load(); state != EControllerState::Running) {
        YT_LOG_INFO("Operation is not running, skip switching to new incarnation (State: %v)", state);
        return;
    }

    if (joblet->OperationIncarnation == Incarnation_) {
        SwitchToNewOperationIncarnation(operationIsReviving, reason);
    }
}

// NB(pogorelov): In case of restarting job during operation revival, we do not know the latest job id, started on node in the allocation,
// so we can not create new job immediately, and we do not force new job started in the same allocation.
// We could avoid this problem if node will know operation incarnation of jobs, but I'm not sure that this case is important.
void TGangOperationController::RestartAllRunningJobsPreservingAllocations(bool operationIsReviving)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    auto abortReason = EAbortReason::OperationIncarnationChanged;

    std::vector<TNonNullPtr<TAllocation>> allocationsToRestartJobs;
    allocationsToRestartJobs.reserve(size(AllocationMap_));

    // NB(pogorelov): OnJobAborted may produce context switches only if operation has been finished, in such cases we break the loop.
    for (auto& [allocationId, allocation] : AllocationMap_) {
        if (const auto& joblet = allocation.Joblet) {
            auto jobId = joblet->JobId;
            bool contextSwitchesDetected = false;
            TOneShotContextSwitchGuard guard([&] {
                contextSwitchesDetected = true;
            });

            allocationsToRestartJobs.push_back(GetPtr(allocation));

            // NB(pogorelov): We abort job with requestNewJob even if we aren't able to schedule new job since:
            // 1) Job aborting causes immediate job releasing.
            // 2) Job tracker doesn't expect running job releasing (intentionally).
            // 3) We can't schedule new job before aborting old one.
            // TODO(pogorelov): It could be fixed by defering job releasing untill the end of method, think about it.
            Host->AbortJob(jobId, abortReason, /*requestNewJob*/ true);

            if ([[maybe_unused]] auto operationFinished = !OnJobAborted(joblet, std::make_unique<TAbortedJobSummary>(jobId, abortReason))) {
                YT_LOG_DEBUG("Operation finished during restarting jobs (JobId: %v)", jobId);
                return;
            }

            YT_LOG_FATAL_IF(
                contextSwitchesDetected,
                "Unexpected context switches detected during restarting job (JobId: %v)",
                jobId);
        }
    }

    // Currently gang operation may not contain not vanilla tasks at all.
    for (const auto& task : Tasks_) {
        YT_VERIFY(task->GetJobType() == EJobType::Vanilla);

        auto chunkPool = task->GetVanillaChunkPool();
        chunkPool->LostAll();
    }

    if (operationIsReviving) {
        YT_LOG_DEBUG("No later job absence guaranteed, do not create new jobs immediately");

        UpdateAllTasks();

        // NB(pogorelov): We can not just do nothing here with job constraints
        // because current and new allocation can conflict over job cookie, for example.
        // We could preserve allocation alive resetting job constraints.
        // But the occurrence is not so frequent.
        // And to not violate some convenient invariants (like preserving job cookie for allocation),
        // we just finish current allocations.
        for (auto& [allocationId, allocation] : AllocationMap_) {
            allocation.NewJobsForbiddenReason = EScheduleFailReason::AllocationFinishRequested;
        }

        return;
    }

    for (auto allocation : allocationsToRestartJobs) {
        auto failReason = TryScheduleNextJob(*allocation, allocation->LastJobInfo->JobId);
        if (failReason) {
            YT_VERIFY(allocation->LastJobInfo);

            YT_LOG_DEBUG(
                "Failed to restart job, just aborting it (AllocationId: %v, CurrentJobId: %v, ScheduleNewJobFailReason: %v)",
                allocation->Id,
                allocation->LastJobInfo->JobId,
                failReason);

            allocation->NewJobsForbiddenReason = failReason;
        }
    }

    UpdateAllTasks();
}

TOperationIncarnation TGangOperationController::GenerateNewIncarnation()
{
    return TOperationIncarnation(ToString(TGuid::Create()));
}

TVanillaTaskPtr TGangOperationController::CreateTask(
    ITaskHostPtr taskHost,
    TVanillaTaskSpecPtr spec,
    TString name,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
{
    return New<TGangTask>(
        std::move(taskHost),
        std::move(spec),
        std::move(name),
        std::move(outputStreamDescriptors),
        std::move(inputStreamDescriptors));
}

void TGangOperationController::ReportOperationIncarnationToArchive(const TGangJobletPtr& joblet) const
{
    HandleJobReport(joblet, TControllerJobReport()
        .OperationIncarnation(static_cast<const std::string&>(joblet->OperationIncarnation)));
}

void TGangOperationController::OnJobStarted(const TJobletPtr& joblet)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    TOperationControllerBase::OnJobStarted(joblet);
    ReportOperationIncarnationToArchive(StaticPointerCast<TGangJoblet>(joblet));
}

void TGangOperationController::InitUserJobSpec(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const TJobletPtr& joblet) const
{
    YT_ASSERT_INVOKER_AFFINITY(JobSpecBuildInvoker_);

    TOperationControllerBase::InitUserJobSpec(jobSpec, joblet);

    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);

    auto fillEnvironment = [&] (const auto& setEnvironmentVariable) {
        setEnvironmentVariable("YT_OPERATION_INCARNATION", ToString(gangJoblet.OperationIncarnation));
    };

    fillEnvironment([&jobSpec] (TStringBuf key, TStringBuf value) {
        jobSpec->add_environment(Format("%v=%v", key, value));
    });

    if (const auto& options = Options_->GpuCheck;
        options->UseSeparateRootVolume && jobSpec->has_gpu_check_binary_path())
    {
        auto* protoEnvironment = jobSpec->mutable_gpu_check_environment();
        fillEnvironment([protoEnvironment] (TStringBuf key, TStringBuf value) {
            (*protoEnvironment)[key] = value;
        });
    }
}

void TGangOperationController::EnrichJobInfo(NYTree::TFluentMap fluent, const TJobletPtr& joblet) const
{
    TOperationControllerBase::EnrichJobInfo(fluent, joblet);

    fluent
        .Item("operation_incarnation").Value(static_cast<const TGangJoblet&>(*joblet).OperationIncarnation);
}

void TGangOperationController::OnOperationIncarnationChanged(bool operationIsReviving, EOperationIncarnationSwitchReason reason)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    TForbidContextSwitchGuard guard;

    OperationIncarnationSwitchCounters[reason].Increment();

    RestartAllRunningJobsPreservingAllocations(operationIsReviving);
}

bool TGangOperationController::IsOperationGang() const noexcept
{
    return true;
}

void TGangOperationController::SwitchToNewOperationIncarnation(bool operationIsReviving, EOperationIncarnationSwitchReason reason)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config->JobEventsControllerQueue));

    auto oldIncarnation = std::exchange(Incarnation_, GenerateNewIncarnation());

    YT_LOG_INFO(
        "Switching operation to new incarnation (From: %v, To: %v, Reason: %v)",
        oldIncarnation,
        Incarnation_,
        reason);

    OnOperationIncarnationChanged(operationIsReviving, reason);
}

void TGangOperationController::VerifyJobsIncarnationsEqual() const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    if (empty(AllocationMap_)) {
        return;
    }

    const TGangJoblet* joblet = nullptr;
    for (const auto& [id, allocation] : AllocationMap_) {
        if (allocation.Joblet) {
            if (!joblet) {
                joblet = static_cast<const TGangJoblet*>(allocation.Joblet.Get());
                continue;
            }
            // NB(pogorelov): The situation where incarnations differ after revival should be impossible.
            YT_VERIFY(static_cast<const TGangJoblet*>(allocation.Joblet.Get())->OperationIncarnation == joblet->OperationIncarnation);
            YT_VERIFY(static_cast<const TGangJoblet*>(allocation.Joblet.Get())->OperationIncarnation == Incarnation_);
        }
    }
}

std::optional<EOperationIncarnationSwitchReason> TGangOperationController::ShouldRestartJobsOnRevival() const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    VerifyJobsIncarnationsEqual();

    THashMap<TTask*, int> jobCountByTasks;
    for (const auto& [id, allocation] : AllocationMap_) {
        if (allocation.Joblet && allocation.Joblet->JobType == EJobType::Vanilla) {
            ++jobCountByTasks[allocation.Joblet->Task];
        }
    }

    for (const auto& task : Tasks_) {
        if (!task->IsJobRestartingEnabled()) {
            continue;
        }

        auto jobCountIt = jobCountByTasks.find(static_cast<TTask*>(task.Get()));
        if (jobCountIt == end(jobCountByTasks)) {
            YT_LOG_DEBUG(
                "No jobs started in task, switching to new incarnation (TaskName: %v)",
                task->GetTitle());

            return EOperationIncarnationSwitchReason::JobLackAfterRevival;
        }

        if (auto jobCount = jobCountIt->second;
            jobCount != task->GetTargetJobCount())
        {
            YT_LOG_DEBUG(
                "Not all jobs started in task, switching to new incarnation (TaskName: %v, RevivedJobCount: %v, TargetJobCount: %v)",
                task->GetTitle(),
                jobCount,
                task->GetTargetJobCount());

            return EOperationIncarnationSwitchReason::JobLackAfterRevival;
        }
    }

    return std::nullopt;
}

void TGangOperationController::OnOperationRevived()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    TOperationControllerBase::OnOperationRevived();

    if (auto maybeIncarnationSwitchReason = ShouldRestartJobsOnRevival()) {
        YT_LOG_DEBUG(
            "Switching to new operation incarnation during revival (Reason: %v)",
            *maybeIncarnationSwitchReason);

        SwitchToNewOperationIncarnation(/*operationIsReviving*/ true, *maybeIncarnationSwitchReason);
    }
}

void TGangOperationController::BuildControllerInfoYson(TFluentMap fluent) const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool);

    TOperationControllerBase::BuildControllerInfoYson(fluent);

    fluent.Item("operation_incarnation").Value(Incarnation_);
}

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateVanillaController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = CreateOperationOptions(config->VanillaOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TVanillaOperationSpec>(
        UpdateSpec(options->SpecTemplate, operation->GetSpec()));

    for (const auto& [taskName, taskSpec] : spec->Tasks) {
        if (taskSpec->GangOptions) {
            return New<TGangOperationController>(
                std::move(spec),
                std::move(config),
                std::move(options),
                std::move(host),
                operation);
        }
    }
    return New<TVanillaController>(
        std::move(spec),
        std::move(config),
        std::move(options),
        std::move(host),
        operation);
}

////////////////////////////////////////////////////////////////////////////////

void InitVanillaProfilers(const NProfiling::TProfiler& profiler)
{
    for (auto reason : TEnumTraits<EOperationIncarnationSwitchReason>::GetDomainValues()) {
        OperationIncarnationSwitchCounters[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/gang_operations/incarnation_switch_count");
    }

    GangOperationStartedCounter = profiler.Counter("/gang_operations/started_count");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
