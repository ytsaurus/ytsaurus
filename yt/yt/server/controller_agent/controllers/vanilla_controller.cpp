#include "vanilla_controller.h"

#include "job_info.h"
#include "operation_controller_detail.h"
#include "table.h"
#include "task.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/server/lib/chunk_pools/vanilla_chunk_pool.h>

#include <yt/yt/server/lib/misc/operation_events_reporter.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/controller_agent/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NConcurrency;
using namespace NControllerAgent::NProto;
using namespace NScheduler::NProto;
using namespace NScheduler;
using namespace NServer;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NLogging;
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

    int GetTargetJobCount() const noexcept;

    IVanillaChunkPoolOutputPtr GetVanillaChunkPool() const noexcept;

    TConfigurator<TVanillaTaskSpec> ConfigureUpdate();

protected:
    TVanillaController* VanillaController_ = nullptr;

    TVanillaTaskSpecPtr Spec_;
    TString Name_;

    TSerializableLogger Logger;

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

    i64 GetJobProxyMemoryIOSize(const TJobIOConfigPtr& jobIO, bool useEstimatedBufferSize) const;

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
    , Logger(TTask::Logger.WithTag("TaskName: %v", Name_))
    , VanillaChunkPool_(CreateVanillaChunkPool(TVanillaChunkPoolOptions{
        .JobCount = Spec_->JobCount,
        .RestartCompletedJobs = Spec_->RestartCompletedJobs,
        .Logger = Logger,
    }))
{ }

void TVanillaTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Name_);
    PHOENIX_REGISTER_FIELD(3, Logger);
    PHOENIX_REGISTER_FIELD(4, VanillaChunkPool_);
    PHOENIX_REGISTER_FIELD(5, JobSpecTemplate_);

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
    result.SetCpu(std::max(Spec_->CpuLimit, TaskHost_->GetOptions()->MinCpuLimit));

    auto jobProxyMemory = VanillaController_->GetJobProxyMemoryIOSize(
        Spec_->JobIO,
        /*useEstimatedBufferSize*/ true);
    auto jobProxyMemoryWithFixedWriteBufferSize = VanillaController_->GetJobProxyMemoryIOSize(
        Spec_->JobIO,
        /*useEstimatedBufferSize*/ false);

    result.SetJobProxyMemory(jobProxyMemory);
    result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);
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

    jobSpecExt->set_io_config(ToProto(ConvertToYsonString(Spec_->JobIO)));

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
        int taskIndex = Tasks_.size();
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

        TotalTargetJobCount_ += task->GetTargetJobCount();

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
            << TErrorAttribute("operation_type", OperationType_)
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

i64 TVanillaController::GetJobProxyMemoryIOSize(const TJobIOConfigPtr& jobIO, bool useEstimatedBufferSize) const
{
    return GetFinalIOMemorySize(jobIO, useEstimatedBufferSize, TChunkStripeStatisticsVector());
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TEnumIndexedArray<EOperationIncarnationSwitchReason, NProfiling::TCounter> OperationIncarnationSwitchCounters;

NProfiling::TCounter GangOperationStartedCounter;

} // namespace

class TGangOperationController;

////////////////////////////////////////////////////////////////////////////////

struct TLastGangJobInfo
    : public TAllocation::TLastJobInfo
{
    NChunkPools::IChunkPoolOutput::TCookie OutputCookie;

    TJobMonitoringDescriptor MonitoringDescriptor;

    std::optional<int> Rank;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TLastGangJobInfo, 0x2201d8d6);
};

class TGangJoblet
    : public TJoblet
{
public:
    using TJoblet::TJoblet;

    TOperationIncarnation OperationIncarnation;

    std::optional<int> Rank;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TGangJoblet, 0x99fa99b3);
};

PHOENIX_DEFINE_TYPE(TGangJoblet);
DEFINE_REFCOUNTED_TYPE(TGangJoblet)
DECLARE_REFCOUNTED_CLASS(TGangJoblet)

////////////////////////////////////////////////////////////////////////////////

class TGangRankPool
{
public:
    // NB(pogorelov): This is needed for deserialization.
    TGangRankPool() = default;

    TGangRankPool(int gangSize, TLogger logger);
    TGangRankPool(TGangRankPool&& other) = default;

    TGangRankPool& operator=(TGangRankPool&& other) = default;

    void Fill();

    std::optional<int> AcquireRank(std::optional<int> requiredRank = std::nullopt);
    void ReleaseRank(int rank);

    void VerifyEmpty() const;

    bool IsCompleted() const;

    void OnRankCompleted(int rank);

private:
    int GangSize_;
    TSerializableLogger Logger;

    THashSet<int> RankPool_;
    // Intentionally not persistent.
    int CompletedRankCount_ = 0;

    PHOENIX_DECLARE_TYPE(TGangRankPool, 0x99c08734);
};

PHOENIX_DEFINE_TYPE(TGangRankPool);

class TGangTask
    : public TVanillaTask
{
public:
    // NB(pogorelov): This is needed for deserialization.
    TGangTask() = default;
    TGangTask(
        ITaskHostPtr taskHost,
        TVanillaTaskSpecPtr spec,
        TString name,
        std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
        std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors);

    void TrySwitchToNewOperationIncarnation(
        const TGangJobletPtr& joblet,
        bool operationIsReviving,
        EOperationIncarnationSwitchReason reason);

    int GetGangSize() const;

    void VerifyRankPoolEmpty() const;

    bool IsGangPolicyEnabled() const noexcept;

    bool IsCompleted() const final;

    void Restart();

    void CustomizeJoblet(TNonNullPtr<TGangJoblet> joblet, const TLastGangJobInfo* lastJobInfo);

private:
    TGangRankPool RankPool_;

    IChunkPoolOutput::TCookie ExtractCookieForAllocation(
        const TAllocation& allocation) final;

    THashMap<TString, TString> BuildJobEnvironment() const final;

    TGangOperationController& GetOperationController() const;

    void StoreLastJobInfo(TAllocation& allocation, const TJobletPtr& joblet) const final;

    TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) final;
    TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) final;
    TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) final;

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

    void OnOperationIncarnationChanged(bool operationIsReviving, TIncarnationSwitchData data);

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

    void SwitchToNewOperationIncarnation(bool operationIsReviving, TIncarnationSwitchData data);

private:
    TOperationIncarnation Incarnation_;

    int TotalGangSize_ = 0;

    std::optional<TIncarnationSwitchData> ShouldRestartJobsOnRevival() const;

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
    void ReportOperationIncarnationStartedEventToArchive(TIncarnationSwitchData data) const;

    const TOperationEventReporterPtr& GetOperationEventReporter() const;

    void ReportGangRankToArchive(const TGangJobletPtr& joblet) const;

    void OnJobStarted(const TJobletPtr& joblet) final;

    void InitUserJobSpec(
        NControllerAgent::NProto::TUserJobSpec* jobSpec,
        const TJobletPtr& joblet) const final;

    void EnrichJobInfo(NYTree::TFluentMap fluent, const TJobletPtr& joblet) const final;

    std::optional<TJobMonitoringDescriptor> AcquireMonitoringDescriptorForJob(
        TJobId jobId,
        const TAllocation& allocation) final;

    void CustomizeJoblet(const TJobletPtr& joblet, const TAllocation& allocation) final;

    void CustomMaterialize() final;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TGangOperationController, 0x99fa99be);
};

PHOENIX_DEFINE_TYPE(TGangOperationController);

////////////////////////////////////////////////////////////////////////////////

void TLastGangJobInfo::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TAllocation::TLastJobInfo>();

    PHOENIX_REGISTER_FIELD(1, OutputCookie);
    PHOENIX_REGISTER_FIELD(2, MonitoringDescriptor);
    PHOENIX_REGISTER_FIELD(3, Rank);
}

PHOENIX_DEFINE_TYPE(TLastGangJobInfo);

void TGangJoblet::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJoblet>();

    PHOENIX_REGISTER_FIELD(1, OperationIncarnation);
    PHOENIX_REGISTER_FIELD(2, Rank);
}

////////////////////////////////////////////////////////////////////////////////

TGangRankPool::TGangRankPool(int gangSize, TLogger logger)
    : GangSize_(gangSize)
    , Logger(std::move(logger))
{
    Fill();
}

void TGangRankPool::Fill()
{
    YT_LOG_DEBUG("Filling rank pool (GangSize: %v)", GangSize_);

    CompletedRankCount_ = 0;

    RankPool_.clear();
    RankPool_.reserve(GangSize_);

    for (int i = 0; i < GangSize_; ++i) {
        RankPool_.insert(i);
    }
}

std::optional<int> TGangRankPool::AcquireRank(std::optional<int> requiredRank)
{
    YT_VERIFY(!IsCompleted());

    if (requiredRank) {
        EraseOrCrash(RankPool_, *requiredRank);
        return requiredRank;
    }

    if (empty(RankPool_)) {
        return std::nullopt;
    }

    auto rank = *begin(RankPool_);
    RankPool_.erase(begin(RankPool_));

    return rank;
}

void TGangRankPool::ReleaseRank(int rank)
{
    InsertOrCrash(RankPool_, rank);
}

bool TGangRankPool::IsCompleted() const
{
    return CompletedRankCount_ == GangSize_;
}

void TGangRankPool::OnRankCompleted(int rank)
{
    ++CompletedRankCount_;
    YT_LOG_DEBUG(
        "Rank completed (Rank: %v, CompletedRankCount: %v)",
        rank,
        CompletedRankCount_);
}

void TGangRankPool::VerifyEmpty() const
{
    if (!empty(RankPool_)) {
        std::vector<int> ranks(begin(RankPool_), end(RankPool_));
        std::ranges::sort(ranks);
        YT_LOG_FATAL(
            "Rank pool is not empty (AvailableRanks: %v)",
            ranks);
    }
}

void TGangRankPool::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, GangSize_);
    PHOENIX_REGISTER_FIELD(2, Logger);
    PHOENIX_REGISTER_FIELD(4, RankPool_);
}

TGangTask::TGangTask(
    ITaskHostPtr taskHost,
    TVanillaTaskSpecPtr spec,
    TString name,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
    : TVanillaTask(
        std::move(taskHost),
        std::move(spec),
        std::move(name),
        std::move(outputStreamDescriptors),
        std::move(inputStreamDescriptors))
    , RankPool_(GetGangSize(), Logger)
{ }

void TGangTask::TrySwitchToNewOperationIncarnation(
    const TGangJobletPtr& joblet,
    bool operationIsReviving,
    EOperationIncarnationSwitchReason reason)
{
    if (IsGangPolicyEnabled()) {
        YT_LOG_DEBUG("Trying to switch operation to new incarnation");

        GetOperationController().TrySwitchToNewOperationIncarnation(joblet, operationIsReviving, reason);
    } else {
        YT_LOG_DEBUG("Operation incarnation switch skipped due to task gang options");
    }
}

int TGangTask::GetGangSize() const
{
    return (Spec_->GangOptions && Spec_->GangOptions->Size) ? *Spec_->GangOptions->Size : Spec_->JobCount;
}

void TGangTask::VerifyRankPoolEmpty() const
{
    RankPool_.VerifyEmpty();
}

bool TGangTask::IsGangPolicyEnabled() const noexcept
{
    return static_cast<bool>(Spec_->GangOptions);
}

bool TGangTask::IsCompleted() const
{
    if (RankPool_.IsCompleted()) {
        return true;
    }

    YT_VERIFY(!VanillaChunkPool_->IsCompleted());

    return false;
}

void TGangTask::Restart()
{
    VanillaChunkPool_->LostAll();

    RankPool_.Fill();
}

void TGangTask::CustomizeJoblet(TNonNullPtr<TGangJoblet> joblet, const TLastGangJobInfo* lastJobInfo)
{
    YT_LOG_DEBUG(
        "Trying to acquire rank for gang job (JobId: %v, PreviousRank: %v)",
        joblet->JobId,
        lastJobInfo ? lastJobInfo->Rank : std::nullopt);

    if (lastJobInfo && lastJobInfo->Rank) {
        joblet->Rank = RankPool_.AcquireRank(lastJobInfo->Rank);
    } else {
        joblet->Rank = RankPool_.AcquireRank();
    }

    YT_LOG_DEBUG(
        "Rank for gang job acquired (JobId: %v, Rank: %v)",
        joblet->JobId,
        joblet->Rank);
}

IChunkPoolOutput::TCookie TGangTask::ExtractCookieForAllocation(
    const TAllocation& allocation)
{
    if (auto lastJobInfo = static_cast<const TLastGangJobInfo*>(allocation.LastJobInfo.get())) {
        VanillaChunkPool_->Extract(lastJobInfo->OutputCookie);
        return lastJobInfo->OutputCookie;
    }

    return VanillaChunkPool_->Extract(NodeIdFromAllocationId(allocation.Id));
}

THashMap<TString, TString> TGangTask::BuildJobEnvironment() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto jobEnvironment = TVanillaTask::BuildJobEnvironment();

    jobEnvironment.emplace("YT_TASK_JOB_COUNT", ToString(GetTargetJobCount()));
    jobEnvironment.emplace("YT_TASK_GANG_SIZE", ToString(GetGangSize()));
    return jobEnvironment;
}

TGangOperationController& TGangTask::GetOperationController() const
{
    return static_cast<TGangOperationController&>(*VanillaController_);
}

void TGangTask::StoreLastJobInfo(TAllocation& allocation, const TJobletPtr& joblet) const
{
    auto& gangJoblet = static_cast<TGangJoblet&>(*joblet);

    auto lastJobInfo = std::make_unique<TLastGangJobInfo>();
    lastJobInfo->JobId = joblet->JobId;
    lastJobInfo->CompetitionType = joblet->CompetitionType;
    lastJobInfo->OutputCookie = joblet->OutputCookie;
    if (!joblet->UserJobMonitoringDescriptor) {
        // NB(pogorelov): If it would be just unset optional,
        // we could get descriptor intended for another allocation.
        lastJobInfo->MonitoringDescriptor = NullMonitoringDescriptor;
    } else {
        lastJobInfo->MonitoringDescriptor = *joblet->UserJobMonitoringDescriptor;
    }
    lastJobInfo->Rank = gangJoblet.Rank;

    allocation.LastJobInfo = std::move(lastJobInfo);
}

TJobFinishedResult TGangTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);

    auto result = TTask::OnJobCompleted(joblet, jobSummary);

    if (auto rank = gangJoblet.Rank) {
        if (jobSummary.InterruptionReason == EInterruptionReason::None) {
            RankPool_.OnRankCompleted(*rank);
        } else {
            YT_LOG_DEBUG(
                "Returning rank to pool due to job interruption (JobId: %v, Rank: %v, InterruptionReason: %v)",
                joblet->JobId,
                *rank,
                jobSummary.InterruptionReason);

            RankPool_.ReleaseRank(*rank);
        }
    }

    return result;
}

TJobFinishedResult TGangTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);
    auto rank = gangJoblet.Rank;

    auto result = TVanillaTask::OnJobFailed(joblet, jobSummary);

    if (rank) {
        YT_LOG_DEBUG(
            "Returning rank to pool due to job failure (JobId: %v, Rank: %v)",
            joblet->JobId,
            *rank);

        RankPool_.ReleaseRank(*rank);
    }

    return result;
}

TJobFinishedResult TGangTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);
    auto rank = gangJoblet.Rank;

    auto result = TVanillaTask::OnJobAborted(joblet, jobSummary);

    if (rank) {
        YT_LOG_DEBUG(
            "Returning rank to pool due to job abortion (JobId: %v, Rank: %v)",
            joblet->JobId,
            *rank);

        RankPool_.ReleaseRank(*rank);
    }

    return result;
}

void TGangTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TVanillaTask>();

    PHOENIX_REGISTER_FIELD(1, RankPool_);
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

    ReportOperationIncarnationStartedEventToArchive(TIncarnationSwitchData{});
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
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);
    auto rank = gangJoblet.Rank;

    // TODO(pogorelov): Move it from here and TOperationControllerBase::OnJobCompleted to TCompletedJobSummary parsing.
    if (const auto& jobResultExt = jobSummary->GetJobResultExt();
        !jobResultExt.restart_needed() && jobSummary->InterruptionReason != EInterruptionReason::None)
    {
        YT_LOG_DEBUG(
            "Overriding job interrupt reason due to unneeded restart (JobId: %v, InterruptionReason: %v)",
            joblet->JobId,
            jobSummary->InterruptionReason);
        jobSummary->InterruptionReason = EInterruptionReason::None;
    }

    auto interruptionReason = jobSummary->InterruptionReason;

    YT_LOG_DEBUG(
        "Gang job completed (JobId: %v, Rank: %v, InterruptionReason: %v)",
        joblet->JobId,
        rank,
        interruptionReason);

    if (!TOperationControllerBase::OnJobCompleted(joblet, std::move(jobSummary))) {
        return false;
    }

    if (auto& gangTask = static_cast<TGangTask&>(*joblet->Task);
        rank && interruptionReason != EInterruptionReason::None)
    {
        gangTask.TrySwitchToNewOperationIncarnation(
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
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);
    auto rank = gangJoblet.Rank;

    if (!TOperationControllerBase::OnJobFailed(joblet, std::move(jobSummary))) {
        return false;
    }

    if (rank) {
        auto& gangTask = static_cast<TGangTask&>(*joblet->Task);
        gangTask.TrySwitchToNewOperationIncarnation(
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
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);

    if (!TOperationControllerBase::OnJobAborted(joblet, std::move(jobSummary))) {
        return false;
    }

    if (gangJoblet.Rank) {
        auto& gangTask = static_cast<TGangTask&>(*joblet->Task);
        gangTask.TrySwitchToNewOperationIncarnation(
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
    YT_ASSERT_INVOKER_POOL_AFFINITY(CancelableInvokerPool_);

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
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

    if (auto state = State_.load(); state != EControllerState::Running) {
        YT_LOG_INFO("Operation is not running, skip switching to new incarnation (State: %v)", state);
        return;
    }

    if (joblet->OperationIncarnation == Incarnation_) {
        TIncarnationSwitchData data{
            .IncarnationSwitchReason = reason,
            .IncarnationSwitchInfo{
                .TriggerJobId = joblet->JobId,
            }
        };
        SwitchToNewOperationIncarnation(operationIsReviving, std::move(data));
    }
}

// NB(pogorelov): In case of restarting job during operation revival, we do not know the latest job id, started on node in the allocation,
// so we can not create new job immediately, and we do not force new job started in the same allocation.
// We could avoid this problem if node will know operation incarnation of jobs, but I'm not sure that this case is important.
void TGangOperationController::RestartAllRunningJobsPreservingAllocations(bool operationIsReviving)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

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
            Host_->AbortJob(jobId, abortReason, /*requestNewJob*/ true);

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
        auto& gangTask = static_cast<TGangTask&>(*task);
        YT_VERIFY(task->GetJobType() == EJobType::Vanilla);

        gangTask.Restart();
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

    std::vector<TJobId> restartedJobIds;
    restartedJobIds.reserve(allocationsToRestartJobs.size());

    // We want to restart jobs with ranks first.
    std::partition(
        begin(allocationsToRestartJobs),
        end(allocationsToRestartJobs),
        [] (TNonNullPtr<TAllocation> allocation) {
            return static_cast<TLastGangJobInfo*>(allocation->LastJobInfo.get())->Rank;
        });

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
        } else {
            restartedJobIds.push_back(allocation->Joblet->JobId);
        }
    }

    TDelayedExecutor::Submit(
        BIND([weakThis = MakeWeak(this), restartedJobIds = std::move(restartedJobIds), this] {
            auto strongThis = weakThis.Lock();
            if (!strongThis) {
                return;
            }

            for (auto jobId : restartedJobIds) {
                auto allocationId = AllocationIdFromJobId(jobId);
                if (auto allocation = FindAllocation(allocationId); allocation && allocation->Joblet && allocation->Joblet->JobId == jobId) {
                    if (allocation->Joblet->JobState && allocation->Joblet->JobState != EJobState::None) {
                        continue;
                    }

                    YT_LOG_WARNING("Waiting for node to settle new job timed out; aborting job (JobId: %v, Timeout: %d)", jobId, Options_->GangManager->JobReincarnationTimeout);

                    allocation->NewJobsForbiddenReason = EScheduleFailReason::Timeout;
                    AbortJob(jobId, EAbortReason::WaitingTimeout);

                    UpdateAllTasks();
                    return;
                }
            }
       }),
       Options_->GangManager->JobReincarnationTimeout,
       GetCancelableInvoker(Config_->JobEventsControllerQueue));

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

void TGangOperationController::ReportOperationIncarnationStartedEventToArchive(TIncarnationSwitchData data) const
{
    auto event = TOperationEventReport{
        .OperationId = GetOperationId(),
        .Timestamp = TInstant::Now(),
        .EventType = NApi::EOperationEventType::IncarnationStarted,
        .Incarnation = Incarnation_.Underlying(),
        .IncarnationSwitchReason = data.IncarnationSwitchReason,
        .IncarnationSwitchInfo = std::move(data.IncarnationSwitchInfo),
    };
    GetOperationEventReporter()->ReportEvent(std::move(event));
}

const TOperationEventReporterPtr& TGangOperationController::GetOperationEventReporter() const
{
    return Host_->GetOperationEventReporter();
}

void TGangOperationController::ReportGangRankToArchive(const TGangJobletPtr& joblet) const
{
    HandleJobReport(joblet, TControllerJobReport()
        .GangRank(joblet->Rank));
}

void TGangOperationController::OnJobStarted(const TJobletPtr& joblet)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    TOperationControllerBase::OnJobStarted(joblet);
    ReportGangRankToArchive(StaticPointerCast<TGangJoblet>(joblet));
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
        setEnvironmentVariable("YT_JOB_COUNT", ToString(GetTotalTargetJobCount()));
        setEnvironmentVariable("YT_GANG_SIZE", ToString(TotalGangSize_));
        if (gangJoblet.Rank) {
            setEnvironmentVariable("YT_GANG_RANK", ToString(*gangJoblet.Rank));
        }
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

    const auto& gangJoblet = static_cast<const TGangJoblet&>(*joblet);

    fluent
        .Item("operation_incarnation").Value(gangJoblet.OperationIncarnation)
        .OptionalItem("gang_rank", gangJoblet.Rank);
}

std::optional<TJobMonitoringDescriptor> TGangOperationController::AcquireMonitoringDescriptorForJob(
    TJobId jobId,
    const TAllocation& allocation)
{
    TLastGangJobInfo* lastGangJobInfo = static_cast<TLastGangJobInfo*>(allocation.LastJobInfo.get());

    if (!lastGangJobInfo) {
        return TOperationControllerBase::AcquireMonitoringDescriptorForJob(jobId, allocation);
    }

    YT_LOG_DEBUG(
        "Trying to acquire monitoring descriptor for gang job (JobId: %v, PreviousMonitoringDescriptor: %v)",
        jobId,
        lastGangJobInfo->MonitoringDescriptor);

    if (lastGangJobInfo->MonitoringDescriptor == NullMonitoringDescriptor) {
        return std::nullopt;
    }

    EraseOrCrash(MonitoringDescriptorPool_, lastGangJobInfo->MonitoringDescriptor);
    ++MonitoredUserJobCount_;
    YT_LOG_DEBUG(
        "Monitoring descriptor reused for job (JobId: %v, MonitoringDescriptor: %v)",
        jobId,
        lastGangJobInfo->MonitoringDescriptor);

    return lastGangJobInfo->MonitoringDescriptor;
}

void TGangOperationController::CustomizeJoblet(const TJobletPtr& joblet, const TAllocation& allocation)
{
    TOperationControllerBase::CustomizeJoblet(joblet, allocation);

    auto& gangJoblet = static_cast<TGangJoblet&>(*joblet);
    auto* lastGangJobInfo = static_cast<TLastGangJobInfo*>(allocation.LastJobInfo.get());

    static_cast<TGangTask*>(joblet->Task)->CustomizeJoblet(
        GetPtr(gangJoblet),
        lastGangJobInfo);
}

void TGangOperationController::CustomMaterialize()
{
    TVanillaController::CustomMaterialize();

    for (const auto& task : Tasks_) {
        TotalGangSize_ += static_cast<const TGangTask*>(task.Get())->GetGangSize();
    }
}

void TGangOperationController::OnOperationIncarnationChanged(bool operationIsReviving, TIncarnationSwitchData data)
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    TForbidContextSwitchGuard guard;

    OperationIncarnationSwitchCounters[data.GetSwitchReason()].Increment();

    ReportOperationIncarnationStartedEventToArchive(std::move(data));

    RestartAllRunningJobsPreservingAllocations(operationIsReviving);
}

bool TGangOperationController::IsOperationGang() const noexcept
{
    return true;
}

void TGangOperationController::SwitchToNewOperationIncarnation(bool operationIsReviving, TIncarnationSwitchData data)
{
    YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker(Config_->JobEventsControllerQueue));

    auto oldIncarnation = std::exchange(Incarnation_, GenerateNewIncarnation());

    YT_LOG_INFO(
        "Switching operation to new incarnation (From: %v, To: %v, Reason: %v)",
        oldIncarnation,
        Incarnation_,
        data.GetSwitchReason());

    OnOperationIncarnationChanged(operationIsReviving, std::move(data));
}

void TGangOperationController::VerifyJobsIncarnationsEqual() const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

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

std::optional<TIncarnationSwitchData> TGangOperationController::ShouldRestartJobsOnRevival() const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    VerifyJobsIncarnationsEqual();

    THashMap<TTask*, int> rankCountByTasks;
    for (const auto& [id, allocation] : AllocationMap_) {
        if (allocation.Joblet) {
            YT_VERIFY(allocation.Joblet->JobType == EJobType::Vanilla);
            if (const auto& gangJoblet = static_cast<const TGangJoblet&>(*allocation.Joblet);
                gangJoblet.Rank)
            {
                ++rankCountByTasks[gangJoblet.Task];
            } else {
                YT_LOG_DEBUG(
                    "Job with no rank revived (JobId: %v)",
                    gangJoblet.JobId);
            }
        }
    }

    for (const auto& task : Tasks_) {
        auto* gangTask = static_cast<TGangTask*>(task.Get());
        if (!gangTask->IsGangPolicyEnabled()) {
            continue;
        }

        auto rankCountIt = rankCountByTasks.find(gangTask);
        if (rankCountIt == end(rankCountByTasks)) {
            YT_LOG_DEBUG("No jobs started in task, switching to new incarnation");

            return TIncarnationSwitchData{
                .IncarnationSwitchReason = EOperationIncarnationSwitchReason::JobLackAfterRevival,
                .IncarnationSwitchInfo{
                    .ExpectedJobCount = gangTask->GetGangSize(),
                    .ActualJobCount = 0,
                    .TaskName = gangTask->GetName(),
                }
            };
        }

        if (auto rankCount = rankCountIt->second;
            rankCount != gangTask->GetGangSize())
        {
            YT_LOG_DEBUG(
                "Not all jobs with ranks started in task, switching to new incarnation (RevivedJobCountWithRanks: %v, TargetJobCount: %v)",
                rankCount,
                gangTask->GetGangSize());

            return TIncarnationSwitchData{
                .IncarnationSwitchReason = EOperationIncarnationSwitchReason::JobLackAfterRevival,
                .IncarnationSwitchInfo{
                    .ExpectedJobCount = gangTask->GetGangSize(),
                    .ActualJobCount = rankCount,
                    .TaskName = gangTask->GetName(),
                }
            };
        }

        gangTask->VerifyRankPoolEmpty();
    }

    return std::nullopt;
}

void TGangOperationController::OnOperationRevived()
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

    TOperationControllerBase::OnOperationRevived();

    if (auto maybeIncarnationSwitchInfo = ShouldRestartJobsOnRevival()) {
        YT_LOG_DEBUG(
            "Switching to new operation incarnation during revival (Reason: %v)",
            maybeIncarnationSwitchInfo->IncarnationSwitchReason);

        SwitchToNewOperationIncarnation(/*operationIsReviving*/ true, std::move(*maybeIncarnationSwitchInfo));
    }
}

void TGangOperationController::BuildControllerInfoYson(TFluentMap fluent) const
{
    YT_ASSERT_INVOKER_POOL_AFFINITY(InvokerPool_);

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
