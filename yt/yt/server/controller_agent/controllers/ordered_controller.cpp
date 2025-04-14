#include "ordered_controller.h"

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

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>
#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_upload_options.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NApi;
using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient;
using namespace NControllerAgent::NProto;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NScheduler;

using NYT::FromProto;
using NYT::ToProto;

using NChunkClient::TReadRange;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TOrderedControllerBase
    : public TOperationControllerBase
{
public:
    TOrderedControllerBase(
        TSimpleOperationSpecBasePtr spec,
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
        , Spec_(spec)
        , Options_(options)
    { }

protected:
    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig_;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate_;

    class TOrderedTask
        : public TTask
    {
    public:
        //! For persistence only.
        TOrderedTask()
            : Controller_(nullptr)
        { }

        TOrderedTask(TOrderedControllerBase* controller)
            : TTask(controller, controller->GetStandardStreamDescriptors(), /*inputStreamDescriptors*/ {})
            , Controller_(controller)
        {
            auto options = controller->GetOrderedChunkPoolOptions();
            ChunkPool_ = CreateOrderedChunkPool(options, controller->GetInputStreamDirectory());
            ChunkPool_->SubscribeChunkTeleported(BIND(&TOrderedTask::OnChunkTeleported, MakeWeak(this)));
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPool_;
        }

        i64 GetTotalOutputRowCount() const
        {
            return TotalOutputRowCount_;
        }

    protected:
        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);
            if (jobSummary.TotalOutputDataStatistics) {
                TotalOutputRowCount_ += jobSummary.TotalOutputDataStatistics->row_count();
            }

            TChunkStripeKey key = 0;
            if (Controller_->OrderedOutputRequired_) {
                key = TOutputOrder::TEntry(joblet->OutputCookie);
            }

            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet, key, /*processEmptyStripes*/ true);

            return result;
        }

    private:
        TOrderedControllerBase* Controller_;

        IPersistentChunkPoolPtr ChunkPool_;

        i64 TotalOutputRowCount_ = 0;

        TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec_->LocalityTimeout
                : TDuration::Zero();
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            return GetMergeResources(joblet->InputStripeList->GetStatistics());
        }

        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            return GetMergeResources(ChunkPool_->GetApproximateStripeStatistics());
        }

        TExtendedJobResources GetMergeResources(
            const TChunkStripeStatisticsVector& statistics) const
        {
            TExtendedJobResources result;
            result.SetUserSlots(1);
            result.SetCpu(Controller_->GetCpuLimit());

            auto jobProxyMemory = Controller_->GetFinalIOMemorySize(
                Controller_->Spec_->JobIO,
                /*useEstimatedBufferSize*/ true,
                statistics);
            auto jobProxyMemoryWithFixedWriteBufferSize = Controller_->GetFinalIOMemorySize(
                Controller_->Spec_->JobIO,
                /*useEstimatedBufferSize*/ false,
                statistics);

            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

            AddFootprintAndUserJobResources(result);
            return result;
        }

        EJobType GetJobType() const override
        {
            return Controller_->GetJobType();
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetUserJobSpec();
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->JobSpecTemplate_);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            return TTask::OnJobAborted(joblet, jobSummary);
        }

        void OnChunkTeleported(TInputChunkPtr teleportChunk, std::any tag) override
        {
            TTask::OnChunkTeleported(teleportChunk, tag);

            if (Controller_->OrderedOutputRequired_) {
                Controller_->RegisterTeleportChunk(teleportChunk, /*key*/ TOutputOrder::TEntry(teleportChunk), /*tableIndex*/ 0);
            } else {
                Controller_->RegisterTeleportChunk(std::move(teleportChunk), /*key*/ 0, /*tableIndex*/ 0);
            }
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            if (!IsJobInterruptible()) {
                config->EnableJobSplitting = false;
            }

            return config;
        }

        bool IsJobInterruptible() const override
        {
            if (!TTask::IsJobInterruptible()) {
                return false;
            }

            // Setting batch_row_count disables interrupts since they make it much harder to fullfil the divisibility conditions.
            if (Controller_->JobSizeConstraints_->GetBatchRowCount()) {
                return false;
            }

            if (Controller_->JobSizeConstraints_->ForceAllowJobInterruption()) {
                return true;
            }

            // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
            auto totalJobCount = Controller_->GetTotalJobCounter()->GetTotal();
            return
                !Controller_->IsExplicitJobCount_ &&
                2 * Controller_->Options_->MaxOutputTablesTimesJobsCount > totalJobCount * std::ssize(Controller_->GetOutputTablePaths()) &&
                2 * Controller_->Options_->MaxJobCount > totalJobCount;
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOrderedTask, 0xaba78384);
    };

    using TOrderedTaskPtr = TIntrusivePtr<TOrderedTask>;

    TOrderedTaskPtr OrderedTask_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    i64 InputSliceDataWeight_;

    bool OrderedOutputRequired_ = false;

    bool IsExplicitJobCount_ = false;

    virtual EJobType GetJobType() const = 0;

    virtual void InitJobSpecTemplate() = 0;

    virtual bool IsTeleportationSupported() const = 0;

    virtual i64 GetMinTeleportChunkSize() = 0;

    virtual void ValidateInputDataSlice(const TLegacyDataSlicePtr& /*dataSlice*/)
    { }

    virtual TCpuResource GetCpuLimit() const
    {
        return 1;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return nullptr;
    }

    bool IsCompleted() const override
    {
        return OrderedTask_ && OrderedTask_->IsCompleted();
    }

    void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        InitTeleportableInputTables();
    }

    void CalculateSizes()
    {
        switch (OperationType) {
            case EOperationType::Merge:
            case EOperationType::Erase:
            case EOperationType::Map:
                JobSizeConstraints_ = CreateUserJobSizeConstraints(
                    Spec_,
                    Options_,
                    Logger,
                    OutputTables_.size(),
                    DataWeightRatio,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    PrimaryInputCompressedDataSize);
                break;

            default:
                YT_ABORT();
        }

        IsExplicitJobCount_ = JobSizeConstraints_->IsExplicitJobCount();

        InputSliceDataWeight_ = JobSizeConstraints_->GetInputSliceDataWeight();

        YT_LOG_INFO("Calculated operation parameters (JobCount: %v, MaxDataWeightPerJob: %v, InputSliceDataWeight: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            InputSliceDataWeight_);
    }

    // XXX(max42): this helper seems redundant.
    TChunkStripePtr CreateChunkStripe(TLegacyDataSlicePtr dataSlice)
    {
        TChunkStripePtr chunkStripe = New<TChunkStripe>(false /*foreign*/);
        chunkStripe->DataSlices.emplace_back(std::move(dataSlice));
        return chunkStripe;
    }

    void ProcessInputs()
    {
        YT_PROFILE_TIMING("/operations/ordered/input_processing_time") {
            YT_LOG_INFO("Processing inputs");

            OrderedTask_->SetIsInput(true);

            int sliceCount = 0;
            TPeriodicYielder yielder(PrepareYieldPeriod);
            for (auto& slice : CollectPrimaryInputDataSlices(InputSliceDataWeight_)) {
                ValidateInputDataSlice(slice);
                OrderedTask_->AddInput(CreateChunkStripe(std::move(slice)));
                ++sliceCount;
                yielder.TryYield();
            }

            YT_LOG_INFO("Processed inputs (Slices: %v)", sliceCount);
        }
    }

    void FinishPreparation()
    {
        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig_ = CloneYsonStruct(Spec_->JobIO);
    }

    void InitTeleportableInputTables()
    {
        if (IsTeleportationSupported()) {
            const auto& inputTables = InputManager->GetInputTables();
            for (int index = 0; index < std::ssize(inputTables); ++index) {
                if (inputTables[index]->SupportsTeleportation() && OutputTables_[0]->SupportsTeleportation()) {
                    inputTables[index]->Teleportable = CheckTableSchemaCompatibility(
                        *inputTables[index]->Schema,
                        *OutputTables_[0]->TableUploadOptions.TableSchema.Get(),
                        {}).first == ESchemaCompatibility::FullyCompatible;
                }
            }
        }
    }

    TOutputOrderPtr GetOutputOrder() const override
    {
        return OrderedTask_->GetChunkPoolOutput()->GetOutputOrder();
    }

    void CustomMaterialize() override
    {
        // NB: Base member is not called intentionally.

        CalculateSizes();

        if (!ShouldVerifySortedOutput()) {
            OrderedOutputRequired_ = true;
        }

        for (const auto& table : OutputTables_) {
            if (!table->TableUploadOptions.TableSchema->IsSorted()) {
                OrderedOutputRequired_ = true;
            }
        }

        OrderedTask_ = New<TOrderedTask>(this);
        RegisterTask(OrderedTask_);

        ProcessInputs();

        FinishTaskInput(OrderedTask_);

        FinishPreparation();
    }

    TOrderedChunkPoolOptions GetOrderedChunkPoolOptions()
    {
        TOrderedChunkPoolOptions chunkPoolOptions;
        chunkPoolOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        chunkPoolOptions.EnablePeriodicYielder = true;
        chunkPoolOptions.MinTeleportChunkSize = GetMinTeleportChunkSize();
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.BuildOutputOrder = OrderedOutputRequired_;
        chunkPoolOptions.ShouldSliceByRowIndices = true;
        chunkPoolOptions.UseNewSlicingImplementation = GetSpec()->UseNewSlicingImplementationInOrderedPool;
        chunkPoolOptions.Logger = Logger().WithTag("Name: Root");
        return chunkPoolOptions;
    }

    TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const override
    {
        return OrderedTask_->GetVertexDescriptor();
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
    }

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOrderedControllerBase, 0xfefb7805);
};

void TOrderedControllerBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOperationControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
    PHOENIX_REGISTER_FIELD(3, JobIOConfig_);
    PHOENIX_REGISTER_FIELD(4, JobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(5, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(6, InputSliceDataWeight_);
    PHOENIX_REGISTER_FIELD(7, OrderedTask_);
    PHOENIX_REGISTER_FIELD(8, OrderedOutputRequired_);
    PHOENIX_REGISTER_FIELD(9, IsExplicitJobCount_);
}

PHOENIX_DEFINE_TYPE(TOrderedControllerBase);

////////////////////////////////////////////////////////////////////////////////

void TOrderedControllerBase::TOrderedTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, ChunkPool_);
    PHOENIX_REGISTER_FIELD(3, TotalOutputRowCount_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->ChunkPool_->SubscribeChunkTeleported(BIND(&TOrderedTask::OnChunkTeleported, MakeWeak(this_)));
    });
}

PHOENIX_DEFINE_TYPE(TOrderedControllerBase::TOrderedTask);

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeController
    : public TOrderedControllerBase
{
public:
    TOrderedMergeController(
        TOrderedMergeOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TSimpleOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOrderedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    { }

private:
    TOrderedMergeOperationSpecPtr Spec_;

    bool IsRowCountPreserved() const override
    {
        return !Spec_->InputQuery &&
            !Spec_->Sampling->SamplingRate &&
            !Spec_->JobIO->TableReader->SamplingRate;
    }

    i64 GetMinTeleportChunkSize() override
    {
        if (Spec_->ForceTransform || Spec_->InputQuery) {
            return std::numeric_limits<i64>::max() / 4;
        }
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    EJobType GetJobType() const override
    {
        return EJobType::OrderedMerge;
    }

    void PrepareInputQuery() override
    {
        if (Spec_->InputQuery) {
            if (Spec_->InputQueryOptions->UseSystemColumns) {
                InputManager->AdjustSchemas(ControlAttributesToColumnOptions(*Spec_->JobIO->ControlAttributes));
            }
            ParseInputQuery(
                *Spec_->InputQuery,
                Spec_->InputSchema,
                Spec_->InputQueryFilterOptions);
        }
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    bool IsBoundaryKeysFetchEnabled() const override
    {
        // Required for chunk teleporting in case of sorted output.
        return OutputTables_[0]->TableUploadOptions.TableSchema->IsSorted();
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(ToProto(EJobType::OrderedMerge));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).ToString());

        if (Spec_->InputQuery) {
            WriteInputQueryToJobSpec(jobSpecExt);
        }

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));
        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());
    }

    bool IsTeleportationSupported() const override
    {
        return true;
    }

    void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        auto inferFromInput = [&] {
            if (Spec_->InputQuery) {
                table->TableUploadOptions.TableSchema = InputQuery->Query->GetTableSchema();
            } else {
                InferSchemaFromInputOrdered();
            }
        };

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    inferFromInput();
                } else {
                    ValidateOutputSchemaOrdered();
                    if (!Spec_->InputQuery) {
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
                break;

            default:
                YT_ABORT();
        }
    }

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::OrderedMerge};
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TOrderedMergeOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TOrderedMergeOperationSpec>();
    }

    void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables() || InputHasDynamicStores();
            if (!isNontrivialInput && IsRowCountPreserved() && Spec_->ForceTransform) {
                YT_LOG_ERROR_IF(TotalEstimatedInputRowCount != OrderedTask_->GetTotalOutputRowCount(),
                    "Input/output row count mismatch in ordered merge operation (TotalEstimatedInputRowCount: %v, TotalOutputRowCount: %v)",
                    TotalEstimatedInputRowCount,
                    OrderedTask_->GetTotalOutputRowCount());
                YT_VERIFY(TotalEstimatedInputRowCount == OrderedTask_->GetTotalOutputRowCount());
            }
        }

        TOrderedControllerBase::OnOperationCompleted(interrupted);
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOrderedMergeController, 0xe7098bca);
};

void TOrderedMergeController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOrderedControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
}

PHOENIX_DEFINE_TYPE(TOrderedMergeController);

IOperationControllerPtr CreateOrderedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = CreateOperationOptions(config->OrderedMergeOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TOrderedMergeOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TOrderedMergeController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedMapController
    : public TOrderedControllerBase
{
public:
    TOrderedMapController(
        TMapOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TMapOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOrderedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
        , Options_(options)
    { }

private:
    i64 StartRowIndex_ = 0;

    TMapOperationSpecPtr Spec_;
    TMapOperationOptionsPtr Options_;

    bool IsRowCountPreserved() const override
    {
        return false;
    }

    TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec_->Mapper;
    }

    i64 GetMinTeleportChunkSize() override
    {
        return std::numeric_limits<i64>::max() / 4;
    }

    EJobType GetJobType() const override
    {
        return EJobType::OrderedMap;
    }

    TCpuResource GetCpuLimit() const override
    {
        return TCpuResource(Spec_->Mapper->CpuLimit);
    }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TOperationControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("mapper").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec_->Mapper->Command))
            .EndMap();
    }

    void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex_;
        StartRowIndex_ += joblet->InputStripeList->TotalRowCount;
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec_->OutputTablePaths;
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

    void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(ToProto(EJobType::OrderedMap));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).ToString());

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));

        if (Spec_->InputQuery) {
            WriteInputQueryToJobSpec(jobSpecExt);
        }

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());

        InitUserJobSpecTemplate(
            jobSpecExt->mutable_user_job_spec(),
            Spec_->Mapper,
            UserJobFiles_[Spec_->Mapper],
            Spec_->DebugArtifactsAccount);
    }

    bool IsTeleportationSupported() const override
    {
        return false;
    }

    void PrepareInputQuery() override
    {
        if (Spec_->InputQuery) {
            if (Spec_->InputQueryOptions->UseSystemColumns) {
                InputManager->AdjustSchemas(ControlAttributesToColumnOptions(*Spec_->JobIO->ControlAttributes));
            }
            ParseInputQuery(
                *Spec_->InputQuery,
                Spec_->InputSchema,
                Spec_->InputQueryFilterOptions);
        }
    }

    ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec_->EnableLegacyLivePreview);
    }

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        return TStringBuf("data_weight_per_job");
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::OrderedMap};
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        return {Spec_->Mapper};
    }

    void DoInitialize() override
    {
        TOrderedControllerBase::DoInitialize();

        ValidateUserFileCount(Spec_->Mapper, "mapper");
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TMapOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TMapOperationSpec>();
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOrderedMapController, 0x3be901ca);
};

void TOrderedMapController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOrderedControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
    PHOENIX_REGISTER_FIELD(3, StartRowIndex_);
}

PHOENIX_DEFINE_TYPE(TOrderedMapController);

IOperationControllerPtr CreateOrderedMapController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = CreateOperationOptions(config->MapOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TMapOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TOrderedMapController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TEraseController
    : public TOrderedControllerBase
{
public:
    TEraseController(
        TEraseOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TSimpleOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOrderedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    { }

private:
    TEraseOperationSpecPtr Spec_;

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        YT_ABORT();
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {};
    }

    bool IsTeleportationSupported() const override
    {
        return true;
    }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TOrderedControllerBase::BuildBriefSpec(fluent);
        fluent
            // In addition to "input_table_paths" and "output_table_paths".
            // Quite messy, only needed for consistency with the regular spec.
            .Item("table_path").Value(Spec_->TablePath);
    }

    bool IsRowCountPreserved() const override
    {
        return false;
    }

    i64 GetMinTeleportChunkSize() override
    {
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return {Spec_->TablePath};
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->TablePath};
    }

    void CustomPrepare() override
    {
        TOrderedControllerBase::CustomPrepare();

        auto& path = InputManager->GetInputTables()[0]->Path;
        auto ranges = path.GetNewRanges(InputManager->GetInputTables()[0]->Comparator, InputManager->GetInputTables()[0]->Schema->GetKeyColumnTypes());
        if (ranges.size() > 1) {
            THROW_ERROR_EXCEPTION("Erase operation does not support tables with multiple ranges");
        }
        if (path.GetColumns()) {
            THROW_ERROR_EXCEPTION("Erase operation does not support column filtering");
        }

        if (ranges.size() == 1) {
            std::vector<TReadRange> complementaryRanges;
            const auto& range = ranges[0];
            if (range.LowerLimit().HasIndependentSelectors() || range.UpperLimit().HasIndependentSelectors()) {
                // NB: Without this check we may erase wider range than requested by user.
                THROW_ERROR_EXCEPTION("Erase operation does not support read limits with several independent selectors");
            }

            if (!range.LowerLimit().IsTrivial()) {
                complementaryRanges.push_back(TReadRange(TReadLimit(), range.LowerLimit().Invert()));
            }
            if (!range.UpperLimit().IsTrivial()) {
                complementaryRanges.push_back(TReadRange(range.UpperLimit().Invert(), TReadLimit()));
            }
            path.SetRanges(complementaryRanges);
        } else {
            path.SetRanges(std::vector<TReadRange>{});
        }
    }

    bool IsBoundaryKeysFetchEnabled() const override
    {
        // Required for chunk teleporting in case of sorted output.
        return OutputTables_[0]->TableUploadOptions.TableSchema->IsSorted();
    }

    void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        if (auto writeMode = table->TableUploadOptions.VersionedWriteOptions.WriteMode; writeMode != EVersionedIOMode::Default) {
            THROW_ERROR_EXCEPTION("Versioned write mode %Qlv is not supported for erase operation",
                writeMode);
        }

        table->TableUploadOptions.UpdateMode = EUpdateMode::Overwrite;
        table->TableUploadOptions.LockMode = ELockMode::Exclusive;

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        // Erase output MUST be sorted.
        if (Spec_->SchemaInferenceMode != ESchemaInferenceMode::FromOutput) {
            table->TableWriterOptions->ExplodeOnValidationError = true;
        }

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInputOrdered();
                } else {
                    if (InputManager->GetInputTables()[0]->SchemaMode == ETableSchemaMode::Strong) {
                        const auto& [compatibility, error] = CheckTableSchemaCompatibility(
                            *InputManager->GetInputTables()[0]->Schema,
                            *table->TableUploadOptions.TableSchema.Get(),
                            {.IgnoreStableNamesDifference = true});

                        if (compatibility != ESchemaCompatibility::FullyCompatible) {
                            THROW_ERROR_EXCEPTION(error);
                        }
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInputOrdered();
                break;

            case ESchemaInferenceMode::FromOutput:
                break;

            default:
                YT_ABORT();
        }
    }

    void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(ToProto(EJobType::OrderedMerge));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).ToString());

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());

        auto* mergejobSpecExt = JobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        const auto& table = OutputTables_[0];
        if (table->TableUploadOptions.TableSchema->IsSorted()) {
            ToProto(mergejobSpecExt->mutable_key_columns(), table->TableUploadOptions.TableSchema->GetKeyColumns());
        }
    }

    EJobType GetJobType() const override
    {
        return EJobType::OrderedMerge;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TEraseOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TEraseOperationSpec>();
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TEraseController, 0xfbb39ac0);
};

void TEraseController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOrderedControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
}

PHOENIX_DEFINE_TYPE(TEraseController);

IOperationControllerPtr CreateEraseController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = CreateOperationOptions(config->EraseOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TEraseOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TEraseController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
