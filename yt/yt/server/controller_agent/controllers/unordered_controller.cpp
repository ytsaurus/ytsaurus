#include "unordered_controller.h"

#include "auto_merge_task.h"
#include "job_info.h"
#include "job_memory.h"
#include "helpers.h"
#include "operation_controller_detail.h"
#include "task.h"

#include <yt/yt/server/controller_agent/chunk_list_pool.h>
#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient;
using namespace NControllerAgent::NProto;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NScheduler;

using NYT::ToProto;

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
            : Controller_(nullptr)
        { }

        TUnorderedTaskBase(
            TUnorderedControllerBase* controller,
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
        {
            auto options = Controller_->GetUnorderedChunkPoolOptions();

            ChunkPool_ = CreateUnorderedChunkPool(std::move(options), Controller_->GetInputStreamDirectory());
            ChunkPool_->SubscribeChunkTeleported(BIND(&TUnorderedTaskBase::OnChunkTeleported, MakeWeak(this)));
        }

        TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec->LocalityTimeout
                : TDuration::Zero();
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetUnorderedOperationResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPool_;
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetUserJobSpec();
        }

        EJobType GetJobType() const override
        {
            return Controller_->GetJobType();
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);
            if (jobSummary.TotalOutputDataStatistics) {
                TotalOutputRowCount_ += jobSummary.TotalOutputDataStatistics->row_count();
            }

            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

            return result;
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (Controller_->AutoMergeTask_) {
                Controller_->AutoMergeTask_->FinishInput();
            }
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            if (Controller_->GetOperationType() == EOperationType::Map) {
                config->EnableJobSplitting &=
                    (IsJobInterruptible() &&
                    std::ssize(Controller_->InputManager->GetInputTables()) <= Controller_->Options->JobSplitter->MaxInputTableCount);
            } else {
                YT_VERIFY(Controller_->GetOperationType() == EOperationType::Merge);
                // TODO(gritukan): YT-13646.
                config->EnableJobSplitting = false;
            }

            return config;
        }

        bool IsJobInterruptible() const override
        {
            if (!TTask::IsJobInterruptible()) {
                return false;
            }

            // TODO(gritukan): YT-13646.
            if (Controller_->GetOperationType() == EOperationType::Merge) {
                return false;
            }
            YT_VERIFY(Controller_->GetOperationType() == EOperationType::Map);

            if (Controller_->JobSizeConstraints_->ForceAllowJobInterruption()) {
                return true;
            }

            // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
            auto totalJobCount = Controller_->GetTotalJobCounter()->GetTotal();
            return
                !(Controller_->AutoMergeTask_ && CanLoseJobs()) &&
                !Controller_->JobSizeConstraints_->IsExplicitJobCount() &&
                2 * Controller_->Options->MaxOutputTablesTimesJobsCount > totalJobCount * std::ssize(Controller_->GetOutputTablePaths()) &&
                2 * Controller_->Options->MaxJobCount > totalJobCount;
        }

        i64 GetTotalOutputRowCount() const
        {
            return TotalOutputRowCount_;
        }

    private:
        TUnorderedControllerBase* Controller_;

        IPersistentChunkPoolPtr ChunkPool_;

        i64 TotalOutputRowCount_ = 0;

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller_->GetUnorderedOperationResources(ChunkPool_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->JobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void OnChunkTeleported(TInputChunkPtr teleportChunk, std::any tag) override
        {
            TTask::OnChunkTeleported(teleportChunk, tag);

            YT_VERIFY(GetJobType() == EJobType::UnorderedMerge);
            Controller_->RegisterTeleportChunk(std::move(teleportChunk), /*key*/ 0, /*tableIndex*/ 0);
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUnorderedTaskBase, 0x38f1471a);
    };

    PHOENIX_INHERIT_POLYMORPHIC_TYPE(TUnorderedTaskBase, TUnorderedTask, 0x8ab75ee7);
    PHOENIX_INHERIT_POLYMORPHIC_TEMPLATE_TYPE(TAutoMergeableOutputMixin, TAutoMergeableUnorderedTask, 0x9a9bcee3, TUnorderedTaskBase);

    using TUnorderedTaskPtr = TIntrusivePtr<TUnorderedTaskBase>;

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

protected:
    TUnorderedOperationSpecBasePtr Spec;
    TSimpleOperationOptionsPtr Options;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    TUnorderedTaskPtr UnorderedTask_;

    // Custom bits of preparation pipeline.
    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    bool IsCompleted() const override
    {
        // Unordered task may be null, if all chunks were teleported.
        return TOperationControllerBase::IsCompleted() && (!UnorderedTask_ || UnorderedTask_->IsCompleted());
    }

    void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        InitTeleportableInputTables();
    }

    void InitTeleportableInputTables()
    {
        if (GetJobType() == EJobType::UnorderedMerge && !Spec->InputQuery) {
            for (int index = 0; index < std::ssize(InputManager->GetInputTables()); ++index) {
                if (InputManager->GetInputTables()[index]->SupportsTeleportation() && OutputTables_[0]->SupportsTeleportation()) {
                    InputManager->GetInputTables()[index]->Teleportable = CheckTableSchemaCompatibility(
                        *InputManager->GetInputTables()[index]->Schema,
                        *OutputTables_[0]->TableUploadOptions.TableSchema.Get(),
                        {}).first == ESchemaCompatibility::FullyCompatible;
                }
            }
        }
    }

    virtual i64 GetMinTeleportChunkSize() const = 0;

    void InitJobSizeConstraints()
    {
        switch (OperationType) {
            case EOperationType::Merge:
                JobSizeConstraints_ = CreateMergeJobSizeConstraints(
                    Spec,
                    Options,
                    Logger,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    DataWeightRatio,
                    InputCompressionRatio);
                break;

            case EOperationType::Map:
                JobSizeConstraints_ = CreateUserJobSizeConstraints(
                    Spec,
                    Options,
                    Logger,
                    GetOutputTablePaths().size(),
                    DataWeightRatio,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    TotalEstimatedInputRowCount);
                break;

            default:
                YT_ABORT();
        }

        YT_LOG_INFO(
            "Calculated operation parameters (JobCount: %v, DataWeightPerJob: %v, MaxDataWeightPerJob: %v, "
            "InputSliceDataWeight: %v, InputSliceRowCount: %v, IsExplicitJobCount: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->IsExplicitJobCount());
    }

    virtual TUnorderedChunkPoolOptions GetUnorderedChunkPoolOptions() const
    {
        TUnorderedChunkPoolOptions options;
        options.RowBuffer = RowBuffer;
        options.MinTeleportChunkSize = GetMinTeleportChunkSize();
        options.MinTeleportChunkDataWeight = options.MinTeleportChunkSize;
        options.JobSizeConstraints = JobSizeConstraints_;
        options.SliceErasureChunksByParts = Spec->SliceErasureChunksByParts;
        options.Logger = Logger().WithTag("Name: Root");

        return options;
    }

    void ProcessInputs()
    {
        YT_PROFILE_TIMING("/operations/unordered/input_processing_time") {
            YT_LOG_INFO("Processing inputs");

            TPeriodicYielder yielder(PrepareYieldPeriod);

            UnorderedTask_->SetIsInput(true);

            int unversionedSlices = 0;
            int versionedSlices = 0;
            // TODO(max42): use CollectPrimaryInputDataSlices() here?
            for (auto& chunk : InputManager->CollectPrimaryUnversionedChunks()) {
                const auto& comparator = InputManager->GetInputTables()[chunk->GetTableIndex()]->Comparator;

                const auto& dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
                dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

                if (comparator) {
                    dataSlice->TransformToNew(RowBuffer, comparator.GetLength());
                    InferLimitsFromBoundaryKeys(dataSlice, RowBuffer, comparator);
                } else {
                    dataSlice->TransformToNewKeyless();
                }

                UnorderedTask_->AddInput(New<TChunkStripe>(std::move(dataSlice)));
                ++unversionedSlices;
                yielder.TryYield();
            }
            for (auto& slice : CollectPrimaryVersionedDataSlices(JobSizeConstraints_->GetInputSliceDataWeight())) {
                UnorderedTask_->AddInput(New<TChunkStripe>(std::move(slice)));
                ++versionedSlices;
                yielder.TryYield();
            }

            YT_LOG_INFO("Processed inputs (UnversionedSlices: %v, VersionedSlices: %v)",
                unversionedSlices,
                versionedSlices);
        }
    }

    void CustomMaterialize() override
    {
        InitJobSizeConstraints();

        auto autoMergeEnabled = TryInitAutoMerge(JobSizeConstraints_->GetJobCount());

        if (autoMergeEnabled) {
            UnorderedTask_ = New<TAutoMergeableUnorderedTask>(
                this,
                GetAutoMergeStreamDescriptors(),
                std::vector<TInputStreamDescriptorPtr>{});
            AutoMergeTask_->SetInputStreamDescriptors(
                BuildInputStreamDescriptorsFromOutputStreamDescriptors(UnorderedTask_->GetOutputStreamDescriptors()));
        } else {
            UnorderedTask_ = New<TUnorderedTask>(
                this,
                GetStandardStreamDescriptors(),
                std::vector<TInputStreamDescriptorPtr>{});
        }

        RegisterTask(UnorderedTask_);

        ProcessInputs();

        FinishTaskInput(UnorderedTask_);
        if (AutoMergeTask_) {
            AutoMergeTask_->RegisterInGraph(UnorderedTask_->GetVertexDescriptor());
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

        auto jobProxyMemory = GetFinalIOMemorySize(
            Spec->JobIO,
            /*useEstimatedBufferSize*/ true,
            AggregateStatistics(statistics));
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            Spec->JobIO,
            /*useEstimatedBufferSize*/ false,
            AggregateStatistics(statistics));

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);
        return result;
    }

    void OnChunksReleased(int chunkCount) override
    {
        TOperationControllerBase::OnChunksReleased(chunkCount);

        if (const auto& autoMergeDirector = GetAutoMergeDirector()) {
            autoMergeDirector->OnMergeJobFinished(chunkCount /*unregisteredIntermediateChunkCount*/);
        }
    }

    EIntermediateChunkUnstageMode GetIntermediateChunkUnstageMode() const override
    {
        auto mapperSpec = GetUserJobSpec();
        // We could get here only if this is an unordered map and auto-merge is enabled.
        YT_VERIFY(mapperSpec);
        YT_VERIFY(Spec->AutoMerge->Mode != EAutoMergeMode::Disabled);

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
        JobIOConfig = CloneYsonStruct(Spec->JobIO);
    }

    void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            if (Spec->InputQueryOptions->UseSystemColumns) {
                InputManager->AdjustSchemas(ControlAttributesToColumnOptions(*Spec->JobIO->ControlAttributes));
            }
            ParseInputQuery(
                *Spec->InputQuery,
                Spec->InputSchema,
                Spec->InputQueryFilterOptions);
        }
    }

    virtual void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(ToProto(GetJobType()));
        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->JobIO)).ToString());

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryWithAutoMerge(
                OutputTables_,
                AutoMergeEnabled_,
                GetSpec()->AutoMerge->UseIntermediateDataAccount
                    ? std::make_optional(GetSpec()->IntermediateDataAccount)
                    : std::nullopt));

        if (Spec->InputQuery) {
            WriteInputQueryToJobSpec(jobSpecExt);
        }

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).ToString());
    }

    TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const override
    {
        return UnorderedTask_->GetVertexDescriptor();
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
    }

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUnorderedControllerBase, 0xf95cf935);
};

void TUnorderedControllerBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOperationControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec);
    PHOENIX_REGISTER_FIELD(2, JobIOConfig);
    PHOENIX_REGISTER_FIELD(3, JobSpecTemplate);
    PHOENIX_REGISTER_FIELD(4, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(5, UnorderedTask_);
}

PHOENIX_DEFINE_TYPE(TUnorderedControllerBase);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedControllerBase::TUnorderedTaskBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, ChunkPool_);
    PHOENIX_REGISTER_FIELD(3, TotalOutputRowCount_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->ChunkPool_->SubscribeChunkTeleported(BIND(&TUnorderedTaskBase::OnChunkTeleported, MakeWeak(this_)));
    });
}

PHOENIX_DEFINE_TYPE(TUnorderedControllerBase::TUnorderedTaskBase);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedControllerBase::TUnorderedTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TUnorderedTaskBase>();
}

PHOENIX_DEFINE_TYPE(TUnorderedControllerBase::TUnorderedTask);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedControllerBase::TAutoMergeableUnorderedTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TAutoMergeableOutputMixin>();
}

PHOENIX_DEFINE_TYPE(TUnorderedControllerBase::TAutoMergeableUnorderedTask);

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

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TUnorderedControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("mapper").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
            .EndMap();
    }

protected:
    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::Map};
    }

private:
    TMapOperationSpecPtr Spec;
    TMapOperationOptionsPtr Options;

    i64 StartRowIndex = 0;

    // Custom bits of preparation pipeline.

    EJobType GetJobType() const override
    {
        return EJobType::Map;
    }

    TUnorderedChunkPoolOptions GetUnorderedChunkPoolOptions() const override
    {
        auto options = TUnorderedControllerBase::GetUnorderedChunkPoolOptions();
        if (Config->EnableMapJobSizeAdjustment) {
            options.JobSizeAdjusterConfig = Options->JobSizeAdjuster;
        }

        return options;
    }

    TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec->Mapper;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriter;
    }

    std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriter;
    }

    bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec->EnableCudaGpuCoreDump;
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        return {Spec->Mapper};
    }

    void DoInitialize() override
    {
        TUnorderedControllerBase::DoInitialize();

        ValidateUserFileCount(Spec->Mapper, "mapper");
    }

    ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec->EnableLegacyLivePreview);
    }

    // Unsorted helpers.
    TCpuResource GetCpuLimit() const override
    {
        return TCpuResource(Spec->Mapper->CpuLimit);
    }

    void InitJobSpecTemplate() override
    {
        TUnorderedControllerBase::InitJobSpecTemplate();
        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
        InitUserJobSpecTemplate(
            jobSpecExt->mutable_user_job_spec(),
            Spec->Mapper,
            UserJobFiles_[Spec->Mapper],
            Spec->DebugArtifactsAccount);
    }

    void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    i64 GetMinTeleportChunkSize() const override
    {
        return std::numeric_limits<i64>::max() / 4;
    }

    bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TMapOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TMapOperationSpec>();
    }

    TError GetAutoMergeError() const override
    {
        return TError();
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TMapController, 0xbac5fd82);
};

void TMapController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TUnorderedControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec);
    PHOENIX_REGISTER_FIELD(2, StartRowIndex);
}

PHOENIX_DEFINE_TYPE(TMapController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMapController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->MapOperationOptions;
    auto spec = ParseOperationSpec<TMapOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
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
    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::UnorderedMerge};
    }

    TUnorderedChunkPoolOptions GetUnorderedChunkPoolOptions() const override
    {
        auto options = TUnorderedControllerBase::GetUnorderedChunkPoolOptions();
        if (Spec->ForceTransform) {
            options.SingleChunkTeleportStrategy = ESingleChunkTeleportStrategy::Disabled;
        } else {
            options.SingleChunkTeleportStrategy = Spec->SingleChunkTeleportStrategy;
        }
        return options;
    }

private:
    TUnorderedMergeOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.
    EJobType GetJobType() const override
    {
        return EJobType::UnorderedMerge;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    // Unsorted helpers.
    bool IsRowCountPreserved() const override
    {
        return !Spec->InputQuery &&
            !Spec->Sampling->SamplingRate &&
            !Spec->JobIO->TableReader->SamplingRate;
    }

    i64 GetMinTeleportChunkSize() const override
    {
        if (Spec->ForceTransform) {
            return std::numeric_limits<i64>::max() / 4;
        }
        if (!Spec->CombineChunks) {
            return 0;
        }
        return Spec->JobIO->TableWriter->DesiredChunkSize;
    }

    void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            if (Spec->InputQueryOptions->UseSystemColumns) {
                InputManager->AdjustSchemas(ControlAttributesToColumnOptions(*Spec->JobIO->ControlAttributes));
            }
            ParseInputQuery(
                *Spec->InputQuery,
                Spec->InputSchema,
                Spec->InputQueryFilterOptions);
        }
    }

    void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        ValidateSchemaInferenceMode(Spec->SchemaInferenceMode);

        auto validateOutputNotSorted = [&] {
            if (table->TableUploadOptions.TableSchema->IsSorted()) {
                THROW_ERROR_EXCEPTION("Cannot perform unordered merge into a sorted table in a \"strong\" schema mode")
                    << TErrorAttribute("schema", *table->TableUploadOptions.TableSchema);
            }
        };

        auto inferFromInput = [&] {
            if (Spec->InputQuery) {
                table->TableUploadOptions.TableSchema = InputQuery->Query->GetTableSchema();
            } else {
                InferSchemaFromInput();
            }
        };

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    inferFromInput();
                } else {
                    validateOutputNotSorted();

                    if (!Spec->InputQuery) {
                        ValidateOutputSchemaCompatibility({
                            .ForbidExtraComputedColumns = false,
                            .IgnoreStableNamesDifference = true,
                            .AllowTimestampColumns = table->TableUploadOptions.VersionedWriteOptions.WriteMode ==
                                EVersionedIOMode::LatestTimestamp,
                        });
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
                YT_ABORT();
        }
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TUnorderedMergeOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TUnorderedMergeOperationSpec>();
    }

    void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables() || InputHasDynamicStores();
            if (!isNontrivialInput && IsRowCountPreserved()) {
                YT_LOG_ERROR_IF(TotalEstimatedInputRowCount != TeleportedOutputRowCount + UnorderedTask_->GetTotalOutputRowCount(),
                    "Input/output row count mismatch in unordered merge operation (TotalEstimatedInputRowCount: %v, TotalOutputRowCount: %v, TeleportedOutputRowCount: %v)",
                    TotalEstimatedInputRowCount,
                    UnorderedTask_->GetTotalOutputRowCount(),
                    TeleportedOutputRowCount);
                YT_VERIFY(TotalEstimatedInputRowCount == TeleportedOutputRowCount + UnorderedTask_->GetTotalOutputRowCount());
                if (Spec->ForceTransform) {
                    YT_VERIFY(TeleportedOutputRowCount == 0);
                }
            }
        }

        TUnorderedControllerBase::OnOperationCompleted(interrupted);
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUnorderedMergeController, 0x9a17a41f);
};

void TUnorderedMergeController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TUnorderedControllerBase>();
}

PHOENIX_DEFINE_TYPE(TUnorderedMergeController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->UnorderedMergeOperationOptions;
    auto spec = ParseOperationSpec<TUnorderedMergeOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TUnorderedMergeController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
