#include "unordered_controller.h"
#include "auto_merge_task.h"
#include "chunk_list_pool.h"
#include "helpers.h"
#include "job_info.h"
#include "job_memory.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "operation.h"
#include "config.h"

#include <yt/server/chunk_pools/unordered_chunk_pool.h>
#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/query_client/query.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkServer;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/unordered");

////////////////////////////////////////////////////////////////////////////////

class TUnorderedControllerBase
    : public TOperationControllerBase
{
public:
    class TUnorderedTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TUnorderedTaskBase()
            : Controller(nullptr)
        { }

        TUnorderedTaskBase(TUnorderedControllerBase* controller, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller(controller)
        {
            auto options = Controller->GetUnorderedChunkPoolOptions();
            options.Task = GetTitle();

            ChunkPool_ = CreateUnorderedChunkPool(std::move(options), Controller->GetInputStreamDirectory());
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->UnorderedTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->IsLocalityEnabled()
                ? Controller->Spec->LocalityTimeout
                : TDuration::Zero();
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller->GetUnorderedOperationResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool_.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool_.get();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, ChunkPool_);
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetUserJobSpec();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetJobType();
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            return result;
        }

        virtual bool SupportsInputPathYson() const override
        {
            return true;
        }

    private:
        TUnorderedControllerBase* Controller;

        std::unique_ptr<IChunkPool> ChunkPool_;

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller->GetUnorderedOperationResources(ChunkPool_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }
    };

    INHERIT_DYNAMIC_PHOENIX_TYPE(TUnorderedTaskBase, TUnorderedTask, 0x8ab75ee7);
    INHERIT_DYNAMIC_PHOENIX_TYPE_TEMPLATED(TAutoMergeableOutputMixin, TAutoMergeableUnorderedTask, 0x9a9bcee3, TUnorderedTaskBase);

    typedef TIntrusivePtr<TUnorderedTaskBase> TUnorderedTaskPtr;

    TUnorderedControllerBase(
        TUnorderedOperationSpecBasePtr spec,
        TControllerAgentConfigPtr config,
        TSimpleOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOperationControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec(spec)
        , Options(options)
    { }

    // Persistence.
    virtual void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec);
        Persist(context, JobIOConfig);
        Persist(context, JobSpecTemplate);
        Persist(context, JobSizeConstraints_);
        Persist(context, UnorderedTask_);
        Persist(context, UnorderedTaskGroup);
    }

protected:
    TUnorderedOperationSpecBasePtr Spec;
    TSimpleOperationOptionsPtr Options;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    TUnorderedTaskPtr UnorderedTask_;
    TTaskGroupPtr UnorderedTaskGroup;

    // Custom bits of preparation pipeline.
    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        UnorderedTaskGroup = New<TTaskGroup>();
        UnorderedTaskGroup->MinNeededResources.SetCpu(GetCpuLimit());
        RegisterTaskGroup(UnorderedTaskGroup);
    }

    virtual bool IsCompleted() const override
    {
        // Unordered task may be null, if all chunks were teleported.
        return TOperationControllerBase::IsCompleted() && (!UnorderedTask_ || UnorderedTask_->IsCompleted());
    }

    void InitTeleportableInputTables()
    {
        if (GetJobType() == EJobType::UnorderedMerge && !Spec->InputQuery) {
            for (int index = 0; index < InputTables.size(); ++index) {
                if (!InputTables[index].IsDynamic && !InputTables[index].Path.GetColumns()) {
                    InputTables[index].IsTeleportable = ValidateTableSchemaCompatibility(
                        InputTables[index].Schema,
                        OutputTables_[0].TableUploadOptions.TableSchema,
                        false /* ignoreSortOrder */).IsOK();
                }
            }
        }
    }

    virtual i64 MinTeleportChunkSize() const = 0;

    void InitJobSizeConstraints()
    {
        switch (OperationType) {
            case EOperationType::Merge:
                JobSizeConstraints_ = CreateMergeJobSizeConstraints(
                    Spec,
                    Options,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    DataWeightRatio,
                    InputCompressionRatio);
                break;

            case EOperationType::Map:
                JobSizeConstraints_ = CreateUserJobSizeConstraints(
                    Spec,
                    Options,
                    GetOutputTablePaths().size(),
                    DataWeightRatio,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    TotalEstimatedInputRowCount);
                break;

            default:
                Y_UNREACHABLE();
        }

        LOG_INFO(
            "Calculated operation parameters (JobCount: %v, DataWeightPerJob: %v, MaxDataWeightPerJob: %v, "
            "InputSliceDataWeight: %v, IsExplicitJobCount: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->IsExplicitJobCount());
    }

    virtual TUnorderedChunkPoolOptions GetUnorderedChunkPoolOptions() const
    {
        TUnorderedChunkPoolOptions options;
        options.MinTeleportChunkSize = MinTeleportChunkSize();
        options.OperationId = OperationId;
        options.JobSizeConstraints = JobSizeConstraints_;
        options.SliceErasureChunksByParts = Spec->SliceErasureChunksByParts;

        return options;
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            TPeriodicYielder yielder(PrepareYieldPeriod);

            InitTeleportableInputTables();

            int unversionedSlices = 0;
            int versionedSlices = 0;
            for (auto& chunk : CollectPrimaryUnversionedChunks()) {
                const auto& slice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
                UnorderedTask_->AddInput(New<TChunkStripe>(std::move(slice)));
                ++unversionedSlices;
                yielder.TryYield();
            }
            for (auto& slice : CollectPrimaryVersionedDataSlices(JobSizeConstraints_->GetInputSliceDataWeight())) {
                UnorderedTask_->AddInput(New<TChunkStripe>(std::move(slice)));
                ++versionedSlices;
                yielder.TryYield();
            }

            LOG_INFO("Processed inputs (UnversionedSlices: %v, VersionedSlices: %v)",
                unversionedSlices,
                versionedSlices);
        }
    }

    virtual void CustomPrepare() override
    {
        InitTeleportableInputTables();

        InitJobSizeConstraints();

        auto autoMergeEnabled = TryInitAutoMerge(JobSizeConstraints_->GetJobCount(), DataWeightRatio);

        auto edgeDescriptors = GetStandardEdgeDescriptors();
        if (GetAutoMergeDirector()) {
            YCHECK(AutoMergeTasks.size() == edgeDescriptors.size());
            for (int index = 0; index < edgeDescriptors.size(); ++index) {
                if (AutoMergeTasks[index]) {
                    edgeDescriptors[index].DestinationPool = AutoMergeTasks[index]->GetChunkPoolInput();
                    edgeDescriptors[index].ChunkMapping = AutoMergeTasks[index]->GetChunkMapping();
                    edgeDescriptors[index].ImmediatelyUnstageChunkLists = true;
                    edgeDescriptors[index].RequiresRecoveryInfo = true;
                    edgeDescriptors[index].IsFinalOutput = false;
                }
            }
        }

        if (autoMergeEnabled) {
            UnorderedTask_ = New<TAutoMergeableUnorderedTask>(this, std::move(edgeDescriptors));
        } else {
            UnorderedTask_ = New<TUnorderedTask>(this, GetStandardEdgeDescriptors());
        }

        RegisterTask(UnorderedTask_);

        ProcessInputs();

        FinishTaskInput(UnorderedTask_);
        for (int index = 0; index < AutoMergeTasks.size(); ++index) {
            if (AutoMergeTasks[index]) {
                AutoMergeTasks[index]->FinishInput(UnorderedTask_->GetVertexDescriptor());
            }
        }

        auto teleportChunks = UnorderedTask_->GetChunkPoolOutput()->GetTeleportChunks();
        if (!teleportChunks.empty()) {
            YCHECK(GetJobType() == EJobType::UnorderedMerge);
            for (const auto& chunk : teleportChunks) {
                RegisterTeleportChunk(chunk, 0 /* key */, 0 /* tableIndex */);
            }
        }

        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    // Resource management.
    TExtendedJobResources GetUnorderedOperationResources(
        const TChunkStripeStatisticsVector& statistics) const
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(Spec->JobIO, AggregateStatistics(statistics)));
        return result;
    }

    virtual void OnChunksReleased(int chunkCount) override
    {
        TOperationControllerBase::OnChunksReleased(chunkCount);

        if (const auto& autoMergeDirector = GetAutoMergeDirector()) {
            autoMergeDirector->OnMergeJobFinished(chunkCount /* unregisteredIntermediateChunkCount */);
        }
    }

    virtual EIntermediateChunkUnstageMode GetIntermediateChunkUnstageMode() const override
    {
        auto mapperSpec = GetUserJobSpec();
        // We could get here only if this is an unordered map and auto-merge is enabled.
        YCHECK(mapperSpec);
        YCHECK(Spec->AutoMerge->Mode != EAutoMergeMode::Disabled);

        if (Spec->AutoMerge->Mode != EAutoMergeMode::Relaxed && mapperSpec->Deterministic) {
            return EIntermediateChunkUnstageMode::OnJobCompleted;
        } else {
            return EIntermediateChunkUnstageMode::OnSnapshotCompleted;
        }
    }

    // Unsorted helpers.
    virtual EJobType GetJobType() const = 0;

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return nullptr;
    }

    virtual TCpuResource GetCpuLimit() const
    {
        return 1;
    }

    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            ParseInputQuery(*Spec->InputQuery, Spec->InputSchema);
        }
    }

    virtual void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(static_cast<int>(GetJobType()));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->JobIO)).GetData());

        SetInputDataSources(schedulerJobSpecExt);

        if (Spec->InputQuery) {
            WriteInputQueryToJobSpec(schedulerJobSpecExt);
        }

        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).GetData());
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedControllerBase::TUnorderedTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedControllerBase::TAutoMergeableUnorderedTask);

////////////////////////////////////////////////////////////////////////////////

class TMapController
    : public TUnorderedControllerBase
{
public:
    TMapController(
        TMapOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TMapOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TUnorderedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec(spec)
        , Options(options)
    { }

    virtual void BuildBriefSpec(TFluentMap fluent) const override
    {
        TUnorderedControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("mapper").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
            .EndMap();
    }

    // Persistence.
    virtual void Persist(const TPersistenceContext& context) override
    {
        TUnorderedControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec);
        Persist(context, StartRowIndex);
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return AsStringBuf("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::Map};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapController, 0xbac5fd82);

    TMapOperationSpecPtr Spec;
    TMapOperationOptionsPtr Options;

    i64 StartRowIndex = 0;

    // Custom bits of preparation pipeline.

    virtual EJobType GetJobType() const override
    {
        return EJobType::Map;
    }

    virtual TUnorderedChunkPoolOptions GetUnorderedChunkPoolOptions() const override
    {
        auto options = TUnorderedControllerBase::GetUnorderedChunkPoolOptions();
        if (Config->EnableMapJobSizeAdjustment) {
            options.JobSizeAdjusterConfig = Options->JobSizeAdjuster;
        }

        return options;
    }

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
    {
        return
            IsJobInterruptible() &&
            Config->EnableJobSplitting &&
            Spec->EnableJobSplitting &&
            InputTables.size() <= Options->JobSplitter->MaxInputTableCount
            ? Options->JobSplitter
            : nullptr;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec->Mapper;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual TNullable<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriterConfig;
    }

    virtual TNullable<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriterConfig;
    }

    virtual std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        return {Spec->Mapper};
    }

    virtual void DoInitialize() override
    {
        TUnorderedControllerBase::DoInitialize();

        ValidateUserFileCount(Spec->Mapper, "mapper");
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return Spec->EnableLegacyLivePreview;
    }

    // Unsorted helpers.
    virtual TCpuResource GetCpuLimit() const override
    {
        return Spec->Mapper->CpuLimit;
    }

    virtual void InitJobSpecTemplate() override
    {
        TUnorderedControllerBase::InitJobSpecTemplate();
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec->Mapper,
            UserJobFiles_[Spec->Mapper],
            Spec->JobNodeAccount);
    }

    virtual void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual i64 MinTeleportChunkSize() const override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    virtual bool IsJobInterruptible() const override
    {
        // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
        return
            !JobSizeConstraints_->IsExplicitJobCount() &&
            2 * Options->MaxOutputTablesTimesJobsCount > JobCounter->GetTotal() * GetOutputTablePaths().size() &&
            2 * Options->MaxJobCount > JobCounter->GetTotal() &&
            TOperationControllerBase::IsJobInterruptible();
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMapController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMapController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->MapOperationOptions;
    auto spec = ParseOperationSpec<TMapOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TMapController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeController
    : public TUnorderedControllerBase
{
public:
    TUnorderedMergeController(
        TUnorderedMergeOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TUnorderedMergeOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TUnorderedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec(spec)
    { }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return AsStringBuf("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::UnorderedMerge};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController, 0x9a17a41f);

    TUnorderedMergeOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.
    virtual EJobType GetJobType() const override
    {
        return EJobType::UnorderedMerge;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    // Unsorted helpers.
    virtual bool IsRowCountPreserved() const override
    {
        return !Spec->InputQuery;
    }

    virtual i64 MinTeleportChunkSize() const override
    {
        if (Spec->ForceTransform) {
            return std::numeric_limits<i64>::max();
        }
        if (!Spec->CombineChunks) {
            return 0;
        }
        return Spec->JobIO->TableWriter->DesiredChunkSize;
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            ParseInputQuery(*Spec->InputQuery, Spec->InputSchema);
        }
    }

    virtual void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        auto validateOutputNotSorted = [&] () {
            if (table.TableUploadOptions.TableSchema.IsSorted()) {
                THROW_ERROR_EXCEPTION("Cannot perform unordered merge into a sorted table in a \"strong\" schema mode")
                    << TErrorAttribute("schema", table.TableUploadOptions.TableSchema);
            }
        };

        auto inferFromInput = [&] () {
            if (Spec->InputQuery) {
                table.TableUploadOptions.TableSchema = InputQuery->Query->GetTableSchema();
            } else {
                InferSchemaFromInput();
            }
        };

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    inferFromInput();
                } else {
                    validateOutputNotSorted();

                    if (!Spec->InputQuery) {
                        ValidateOutputSchemaCompatibility(true);
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                inferFromInput();
                break;

            case ESchemaInferenceMode::FromOutput:
                validateOutputNotSorted();
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->UnorderedMergeOperationOptions;
    auto spec = ParseOperationSpec<TUnorderedMergeOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TUnorderedMergeController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

