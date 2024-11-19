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

    // Persistence.

    void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, JobIOConfig_);
        Persist(context, JobSpecTemplate_);
        Persist(context, JobSizeConstraints_);
        Persist(context, InputSliceDataWeight_);
        // COMPAT(alexelexa)
        if (context.GetVersion() >= ESnapshotVersion::MultipleOrderedTasks) {
            Persist(context, OrderedTasks_);
        } else {
            YT_VERIFY(context.IsLoad());
            TOrderedTaskPtr task;
            Persist(context, task);
            OrderedTasks_.emplace_back(std::move(task));
        }
        Persist(context, OrderedOutputRequired_);
        Persist(context, IsExplicitJobCount_);
    }

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

        void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, ChunkPool_);
            Persist(context, TotalOutputRowCount_);

            ChunkPool_->SubscribeChunkTeleported(BIND(&TOrderedTask::OnChunkTeleported, MakeWeak(this)));
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
        DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedTask, 0xaba78384);

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
            result.SetJobProxyMemory(Controller_->GetFinalIOMemorySize(Controller_->Spec_->JobIO, statistics));
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

            config->EnableJobSplitting &=
                (IsJobInterruptible() &&
                std::ssize(Controller_->InputManager->GetInputTables()) <= Controller_->Options_->JobSplitter->MaxInputTableCount);

            return config;
        }

        bool IsJobInterruptible() const override
        {
            if (!TTask::IsJobInterruptible()) {
                return false;
            }

            // Remote copy jobs works with chunks as blobs and therefore are unsplittable.
            if (Controller_->GetOperationType() == EOperationType::RemoteCopy) {
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
    };

    using TOrderedTaskPtr = TIntrusivePtr<TOrderedTask>;

    std::vector<TOrderedTaskPtr> OrderedTasks_;

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
        return !OrderedTasks_.empty() &&
            std::all_of(OrderedTasks_.begin(), OrderedTasks_.end(), [] (const auto& task) {
                return task->IsCompleted();
            });
    }

    void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        InitTeleportableInputTables();
    }

    void CalculateSizes()
    {
        Spec_->Sampling->MaxTotalSliceCount = Spec_->Sampling->MaxTotalSliceCount.value_or(Config->MaxTotalSliceCount);

        switch (OperationType) {
            case EOperationType::Merge:
            case EOperationType::Erase:
            case EOperationType::RemoteCopy:
                JobSizeConstraints_ = CreateMergeJobSizeConstraints(
                    Spec_,
                    Options_,
                    Logger,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight,
                    DataWeightRatio,
                    InputCompressionRatio);
                break;

            case EOperationType::Map:
                JobSizeConstraints_ = CreateUserJobSizeConstraints(
                    Spec_,
                    Options_,
                    Logger,
                    OutputTables_.size(),
                    DataWeightRatio,
                    TotalEstimatedInputChunkCount,
                    PrimaryInputDataWeight);
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

    virtual int AddInputSlices()
    {
        int sliceCount = 0;
        TPeriodicYielder yielder(PrepareYieldPeriod);
        for (auto& slice : CollectPrimaryInputDataSlices(InputSliceDataWeight_)) {
            ValidateInputDataSlice(slice);
            GetSingleOrderedTask()->AddInput(CreateChunkStripe(std::move(slice)));
            ++sliceCount;
            yielder.TryYield();
        }

        return sliceCount;
    }

    void ProcessInputs()
    {
        YT_PROFILE_TIMING("/operations/merge/input_processing_time") {
            YT_LOG_INFO("Processing inputs");

            for (auto& task : OrderedTasks_) {
                task->SetIsInput(true);
            }

            auto sliceCount = AddInputSlices();
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
        return GetSingleOrderedTask()->GetChunkPoolOutput()->GetOutputOrder();
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

        CreateTasks();
        for (const auto& task : OrderedTasks_) {
            RegisterTask(task);
        }

        ProcessInputs();

        for (const auto& task : OrderedTasks_) {
            FinishTaskInput(task);
        }

        FinishPreparation();
    }

    virtual void CreateTasks()
    {
        OrderedTasks_.emplace_back(New<TOrderedTask>(this));
    }

    TOrderedChunkPoolOptions GetOrderedChunkPoolOptions()
    {
        TOrderedChunkPoolOptions chunkPoolOptions;
        chunkPoolOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        chunkPoolOptions.EnablePeriodicYielder = true;
        chunkPoolOptions.MinTeleportChunkSize = GetMinTeleportChunkSize();
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.KeepOutputOrder = OrderedOutputRequired_;
        chunkPoolOptions.ShouldSliceByRowIndices = GetJobType() != EJobType::RemoteCopy;
        chunkPoolOptions.Logger = Logger().WithTag("Name: Root");
        return chunkPoolOptions;
    }

    TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const override
    {
        return GetSingleOrderedTask()->GetVertexDescriptor();
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
    }

    const TOrderedTaskPtr& GetSingleOrderedTask() const
    {
        YT_VERIFY(std::ssize(OrderedTasks_) == 1);
        return OrderedTasks_[0];
    }

    PHOENIX_DECLARE_FRIEND();
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedControllerBase::TOrderedTask);

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

    void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController, 0xe7098bca);

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

    void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables() || InputHasDynamicStores();
            if (!isNontrivialInput && IsRowCountPreserved() && Spec_->ForceTransform) {
                const auto& task = GetSingleOrderedTask();
                YT_LOG_ERROR_IF(TotalEstimatedInputRowCount != task->GetTotalOutputRowCount(),
                    "Input/output row count mismatch in ordered merge operation (TotalEstimatedInputRowCount: %v, TotalOutputRowCount: %v)",
                    TotalEstimatedInputRowCount,
                    task->GetTotalOutputRowCount());
                YT_VERIFY(TotalEstimatedInputRowCount == task->GetTotalOutputRowCount());
            }
        }

        TOrderedControllerBase::OnOperationCompleted(interrupted);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController);

IOperationControllerPtr CreateOrderedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->OrderedMergeOperationOptions;
    auto spec = ParseOperationSpec<TOrderedMergeOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
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

    void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, StartRowIndex_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMapController, 0x3be901ca);

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
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMapController);

IOperationControllerPtr CreateOrderedMapController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->MapOperationOptions;
    auto spec = ParseOperationSpec<TMapOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
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

    void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseController, 0xfbb39ac0);

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
                // NB: without this check we may erase wider range than requested by user.
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
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseController);

IOperationControllerPtr CreateEraseController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->EraseOperationOptions;
    auto spec = ParseOperationSpec<TEraseOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TEraseController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyController
    : public TOrderedControllerBase
{
public:
    TRemoteCopyController(
        TRemoteCopyOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TRemoteCopyOperationOptionsPtr options,
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
        , Networks_(Spec_->Networks)
    {
        if (!Networks_ && Options_->Networks) {
            Networks_ = Options_->Networks;
        }

        if (!Spec_->AllowClusterConnection) {
            THROW_ERROR_EXCEPTION_IF(
                Spec_->ClusterConnection,
                "\"cluster_connection\" is not allowed in remote copy operation spec");

            THROW_ERROR_EXCEPTION_UNLESS(
                Spec_->ClusterName,
                "\"cluster_name\" is not set in remote copy operation spec");
        }
    }

    void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);
        using NYT::Persist;

        Persist(context, Spec_);
        Persist(context, Options_);
        Persist<TAttributeDictionarySerializer>(context, InputTableAttributes_);
        Persist(context, Networks_);
        // COMPAT(alexelexa)
        if (context.GetVersion() >= ESnapshotVersion::RemoteCopyDynamicTableWithHunks) {
            Persist(context, HunkChunkIdMapping_);
        }
    }

protected:
    class TRemoteCopyTaskBase;
    using TRemoteCopyTaskBasePtr = TIntrusivePtr<TRemoteCopyTaskBase>;
    using TRemoteCopyTaskBaseWeakPtr = TWeakPtr<TRemoteCopyTaskBase>;

    class TRemoteCopyTaskBase
        : public TOrderedTask
    {
    public:
        TRemoteCopyTaskBase()
            : TOrderedTask()
            , Controller_(nullptr)
            , IsInitializationCompleted_(false)
        { }

        TRemoteCopyTaskBase(TRemoteCopyController* controller)
            : TOrderedTask(controller)
            , Controller_(controller)
            , IsInitializationCompleted_(false)
        { }

        void Persist(const TPersistenceContext& context) override
        {
            TOrderedTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, Dependencies_);
            Persist(context, Dependents_);
            Persist(context, IsInitializationCompleted_);
        }

        void FinishInitialization()
        {
            IsInitializationCompleted_ = true;
        }

        void AddDependency(TRemoteCopyTaskBasePtr dependency)
        {
            YT_VERIFY(!IsInitializationCompleted_);
            dependency->AddDependent(MakeStrong(this));
            Dependencies_.emplace_back(std::move(dependency));
        }

    protected:
        TRemoteCopyController* Controller_;

    private:
        // On which it depends.
        std::vector<TRemoteCopyTaskBaseWeakPtr> Dependencies_;
        // Which depends on it.
        std::vector<TRemoteCopyTaskBasePtr> Dependents_;
        bool IsInitializationCompleted_;

        void AddDependent(TRemoteCopyTaskBasePtr dependent)
        {
            YT_VERIFY(!IsInitializationCompleted_);
            Dependents_.emplace_back(std::move(dependent));
        }

        void RemoveDependency(TRemoteCopyTaskBaseWeakPtr dependency)
        {
            YT_VERIFY(IsInitializationCompleted_);
            auto dependencyIt = std::find(Dependencies_.begin(), Dependencies_.end(), dependency);
            YT_VERIFY(dependencyIt != Dependencies_.end());
            Dependencies_.erase(dependencyIt);

            UpdateState();
        }

        TCompositePendingJobCount GetPendingJobCount() const override
        {
            YT_VERIFY(IsInitializationCompleted_);

            if (std::ssize(Dependencies_) > 0) {
                return {};
            }
            return TOrderedTask::GetPendingJobCount();
        }

        void UpdateState()
        {
            Controller_->UpdateTask(this);

            if (Dependencies_.empty()) {
                Controller_->FillJobSpecHunkChunkIdMapping();
            }
        }

        void RemoveDependents()
        {
            YT_VERIFY(IsInitializationCompleted_);
            for (auto& dependant : Dependents_) {
                dependant->RemoveDependency(MakeWeak(this));
            }
        }

        void OnTaskCompleted() override
        {
            ValidateAllDataHaveBeenCopied();
            RemoveDependents();
        }

        virtual void ValidateAllDataHaveBeenCopied()
        { }
    };

    class TRemoteCopyTask
        : public TRemoteCopyTaskBase
    {
    public:
        TRemoteCopyTask()
            : TRemoteCopyTaskBase()
        { }

        TRemoteCopyTask(TRemoteCopyController* controller)
            : TRemoteCopyTaskBase(controller)
        { }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyTask, 0xaba78385);
    };

    using TRemoteCopyTaskPtr = TIntrusivePtr<TRemoteCopyTask>;

    class TRemoteCopyHunkTask
        : public TRemoteCopyTaskBase
    {
    public:
        TRemoteCopyHunkTask()
            : TRemoteCopyTaskBase()
        { }

        TRemoteCopyHunkTask(TRemoteCopyController* controller)
            : TRemoteCopyTaskBase(controller)
        { }

        TString GetTitle() const override
        {
            return "HunkRemoteCopy";
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto& jobResultExt = jobSummary.GetJobResultExt();
            THashMap<TChunkId, TChunkId> newHunkChunkIdMapping;
            for (const auto& mapping : jobResultExt.hunk_chunk_id_mapping()) {
                EmplaceOrCrash(
                    newHunkChunkIdMapping,
                    FromProto<TChunkId>(mapping.input_hunk_chunk_id()),
                    FromProto<TChunkId>(mapping.output_hunk_chunk_id()));
            }
            Controller_->UpdateHunkChunkIdMapping(newHunkChunkIdMapping);

            return TOrderedTask::OnJobCompleted(joblet, jobSummary);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyHunkTask, 0xaba78386);

        void ValidateAllDataHaveBeenCopied() override
        {
            Controller_->ValidateHunkChunksConsistency();
        }
    };

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController, 0xaa8829a9);

    TRemoteCopyOperationSpecPtr Spec_;
    TRemoteCopyOperationOptionsPtr Options_;
    std::optional<NNodeTrackerClient::TNetworkPreferenceList> Networks_;

    IAttributeDictionaryPtr InputTableAttributes_;

    THashMap<TChunkId, TChunkId> HunkChunkIdMapping_;

    void ValidateTableType(auto table) const
    {
        if (table->Type != EObjectType::Table && table->Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Only files and tables are allowed, but %v has type %Qlv",
                table->GetPath(),
                table->Type);
        }
    }

    void ValidateInputTablesTypes() const override
    {
        // NB(coteeq): remote_copy always has one input table.
        YT_VERIFY(InputManager->GetInputTables().size() == 1);
        ValidateTableType(InputManager->GetInputTables()[0]);
    }

    void ValidateUpdatingTablesTypes() const override
    {
        // NB(coteeq): remote_copy always has one input table.
        YT_VERIFY(OutputTables_.size() == 1);
        ValidateTableType(OutputTables_[0]);

        const auto& inputTables = InputManager->GetInputTables();

        THROW_ERROR_EXCEPTION_UNLESS(
            OutputTables_[0]->Type == inputTables[0]->Type,
            "Output object type does not match that of the input object %v: expected %Qlv, found %Qlv",
            inputTables[0]->GetPath(),
            inputTables[0]->Type,
            OutputTables_[0]->Type);

        YT_VERIFY(!StderrTable_ && !CoreTable_);
    }

    EObjectType GetOutputTableDesiredType() const override
    {
        YT_VERIFY(InputManager->GetInputTables().size() == 1);
        return InputManager->GetInputTables()[0]->Type;
    }

    TStringBuf GetDataWeightParameterNameForJob(EJobType /*jobType*/) const override
    {
        YT_ABORT();
    }

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {};
    }

    bool ShouldVerifySortedOutput() const override
    {
        return false;
    }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TOperationControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("cluster_name").Value(Spec_->ClusterName)
            .Item("networks").Value(Networks_);
    }

    // Custom bits of preparation pipeline.
    TTransactionId GetInputTransactionParentId() override
    {
        return {};
    }

    void InitializeClients() override
    {
        TOperationControllerBase::InitializeClients();

        InputClient = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(AuthenticatedUser));
        SchedulerInputClient = GetRemoteConnection()->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::SchedulerUserName));
        InputManager->InitializeClients(InputClient);
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const override
    {
        return OrderedTasks_.back()->GetVertexDescriptor();
    }

    void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInputOrdered();
                    break;
                }
                // We intentionally fall into next clause.

            case ESchemaInferenceMode::FromOutput:
                ValidateOutputSchemaOrdered();

                // Since remote copy doesn't unpack blocks and validate schema, we must ensure
                // that schemas are identical.
                for (const auto& inputTable : InputManager->GetInputTables()) {
                    if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Strong &&
                        *inputTable->Schema->ToCanonical() != *table->TableUploadOptions.TableSchema->ToCanonical())
                    {
                        THROW_ERROR_EXCEPTION("Cannot make remote copy into table with \"strong\" schema since "
                            "input table schema differs from output table schema")
                            << TErrorAttribute("input_table_schema", inputTable->Schema)
                            << TErrorAttribute("output_table_schema", *table->TableUploadOptions.TableSchema);
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInputOrdered();
                break;
        }
    }

    void ValidateInputDataSlice(const TLegacyDataSlicePtr& dataSlice) override
    {
        auto errorCode = NChunkClient::EErrorCode::InvalidInputChunk;
        if (!dataSlice->IsTrivial()) {
            THROW_ERROR_EXCEPTION(errorCode, "Remote copy operation does not support versioned data slices");
        }
        const auto& chunk = dataSlice->GetSingleUnversionedChunk();
        YT_VERIFY(!chunk->IsDynamicStore());
        if ((chunk->LowerLimit() && !IsTrivial(*chunk->LowerLimit())) ||
            (chunk->UpperLimit() && !IsTrivial(*chunk->UpperLimit())))
        {
            THROW_ERROR_EXCEPTION(
                errorCode,
                "Remote copy operation does not support non-trivial table limits%v",
                MakeFormatterWrapper([&] (auto* builder) {
                    if (InputManager->GetInputTables()[0]->Dynamic) {
                        FormatValue(builder, " and chunks crossing tablet boundaries", "v");
                    }
                }));
        }
    }

    void CustomPrepare() override
    {
        TOrderedControllerBase::CustomPrepare();

        static const auto allowedAttributes = [] {
            const auto& wellKnown = GetWellKnownRichYPathAttributes();
            return THashSet<TString>(wellKnown.begin(), wellKnown.end());
        }();

        for (const auto& attributeName : Spec_->OutputTablePath.Attributes().ListKeys()) {
            if (!allowedAttributes.contains(attributeName)) {
                THROW_ERROR_EXCEPTION("Found unexpected attribute %Qv in Rich YPath", attributeName)
                    << TErrorAttribute("path", Spec_->OutputTablePath);
            }
        }
    }

    void CustomMaterialize() override
    {
        if (Spec_->CopyAttributes) {
            if (InputManager->GetInputTables().size() != 1) {
                THROW_ERROR_EXCEPTION("Attributes can be copied only in case of one input table");
            }

            const auto& table = InputManager->GetInputTables()[0];

            auto proxy = CreateObjectServiceReadProxy(InputClient, EMasterChannelKind::Follower);

            auto req = TObjectYPathProxy::Get(table->GetObjectIdPath() + "/@");
            SetTransactionId(req, *table->TransactionId);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting attributes of input table %v",
                table->GetPath());

            const auto& rsp = rspOrError.Value();
            InputTableAttributes_ = ConvertToAttributes(TYsonString(rsp->value()));
        }

        bool hasDynamicInputTable = false;
        bool hasDynamicOutputTable = false;
        bool hasStaticTableWithHunkChunks = false;
        for (const auto& table : InputManager->GetInputTables()) {
            hasDynamicInputTable |= table->Dynamic;
            hasStaticTableWithHunkChunks |= !table->Dynamic && !table->HunkChunks.empty();
        }
        for (const auto& table : OutputTables_) {
            hasDynamicOutputTable |= table->Dynamic;
        }

        if (hasStaticTableWithHunkChunks) {
            THROW_ERROR_EXCEPTION("Static table with hunk chunks cannot be copied");
        }

        if (hasDynamicInputTable) {
            if (!hasDynamicOutputTable) {
                THROW_ERROR_EXCEPTION("Dynamic table can be copied only to another dynamic table");
            }
            if (InputManager->GetInputTables().size() != 1 || OutputTables_.size() != 1) {
                THROW_ERROR_EXCEPTION("Only one dynamic table can be copied at a time");
            }
            if (OutputTables_[0]->TableUploadOptions.UpdateMode != EUpdateMode::Overwrite) {
                THROW_ERROR_EXCEPTION("Remote copy of dynamic tables can only be done in overwrite mode");
            }
        } else if (hasDynamicOutputTable) {
            THROW_ERROR_EXCEPTION("Static table cannot be copied into a dynamic table");
        }

        TOrderedControllerBase::CustomMaterialize();
    }

    void CreateTasks() override
    {
        bool hasDynamicTableWithHunkChunks = InputManager->HasDynamicTableWithHunkChunks();

        // Tasks order in OrderedTasks matters.
        // 1. The task of copying hunk chunks (if any);
        // 2. The task of copying regular chunks.

        auto task = New<TRemoteCopyTask>(this);
        if (hasDynamicTableWithHunkChunks) {
            auto hunkTask = New<TRemoteCopyHunkTask>(this);
            task->AddDependency(hunkTask);
            hunkTask->FinishInitialization();
            OrderedTasks_.emplace_back(std::move(hunkTask));
        }

        task->FinishInitialization();
        OrderedTasks_.emplace_back(std::move(task));
    }

    int AddInputSlices() override
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        auto slices = [&] (std::vector<TLegacyDataSlicePtr>&& slices) -> std::vector<std::vector<TLegacyDataSlicePtr>> {
            if (!HasHunkChunks()) {
                return {std::move(slices)};
            }

            std::vector<TLegacyDataSlicePtr> hunkChunkSlices;
            std::vector<TLegacyDataSlicePtr> chunkSlices;
            for (auto& slice : slices) {
                ValidateInputDataSlice(slice);
                if (slice->GetSingleUnversionedChunk()->IsHunk()) {
                    hunkChunkSlices.emplace_back(std::move(slice));
                } else {
                    chunkSlices.emplace_back(std::move(slice));
                }
            }
            return {hunkChunkSlices, chunkSlices};
        } (CollectPrimaryInputDataSlices(InputSliceDataWeight_));

        YT_VERIFY(std::ssize(slices) == std::ssize(OrderedTasks_));

        int sliceCount = 0;
        for (int index = 0; index < std::ssize(OrderedTasks_); ++index) {
            sliceCount += std::ssize(slices[index]);
            for (auto& slice : slices[index]) {
                OrderedTasks_[index]->AddInput(CreateChunkStripe(std::move(slice)));
                yielder.TryYield();
            }
        }
        return sliceCount;
    }

    bool HasHunkChunks() const
    {
        // If there are any hunk chunks than the task of copying hunk chunks will be created,
        // as well as the task of copying regular chunks, which is always created.
        // Then there will be at least two tasks.
        return std::ssize(OrderedTasks_) > 1;
    }

    void CustomCommit() override
    {
        TOperationControllerBase::CustomCommit();

        if (Spec_->CopyAttributes) {
            const auto& path = Spec_->OutputTablePath.GetPath();

            auto proxy = CreateObjectServiceWriteProxy(OutputClient);

            auto userAttributeKeys = InputTableAttributes_->Get<std::vector<TString>>("user_attribute_keys");
            auto attributeKeys = Spec_->AttributeKeys.value_or(userAttributeKeys);

            auto batchReq = proxy.ExecuteBatch();
            auto req = TYPathProxy::MultisetAttributes(path + "/@");
            SetTransactionId(req, OutputCompletionTransaction->GetId());

            for (const auto& attribute : attributeKeys) {
                auto* subrequest = req->add_subrequests();
                subrequest->set_attribute(attribute);
                auto value = InputTableAttributes_->GetYson(attribute);
                ValidateYson(value, GetYsonNestingLevelLimit());
                subrequest->set_value(value.ToString());
            }

            batchReq->AddRequest(req);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error setting attributes for output table %v",
                path);
        }
    }

    void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(ToProto(EJobType::RemoteCopy));
        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(
            TJobSpecExt::job_spec_ext);

        jobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).ToString());
        jobSpecExt->set_table_reader_options("");
        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            BuildDataSinkDirectoryFromOutputTables(OutputTables_));

        auto connectionConfig = GetRemoteConnectionConfig()->Clone();
        if (Networks_) {
            connectionConfig->Static->Networks = *Networks_;
        }

        auto masterCacheAddresses = GetRemoteMasterCacheAddresses();
        if (masterCacheAddresses.empty()) {
            YT_LOG_DEBUG("Not using remote master caches for remote copy operation");
        } else {
            connectionConfig->Static->OverrideMasterAddresses(masterCacheAddresses);

            YT_LOG_DEBUG("Using remote master caches for remote copy operation (Addresses: %v)",
                masterCacheAddresses);
        }

        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);
        auto connectionNode = ConvertToNode(connectionConfig)->AsMap();
        // COMPAT(max42): remove this after 23.1 is everywhere.
        if (connectionNode->FindChild("yql_agent")) {
            connectionNode->RemoveChild("yql_agent");
        }
        remoteCopyJobSpecExt->set_connection_config(ConvertToYsonString(connectionNode).ToString());
        remoteCopyJobSpecExt->set_concurrency(Spec_->Concurrency);
        remoteCopyJobSpecExt->set_block_buffer_size(Spec_->BlockBufferSize);
        remoteCopyJobSpecExt->set_delay_in_copy_chunk(ToProto(Spec_->DelayInCopyChunk));
        remoteCopyJobSpecExt->set_erasure_chunk_repair_delay(ToProto(Spec_->ErasureChunkRepairDelay));
        remoteCopyJobSpecExt->set_repair_erasure_chunks(Spec_->RepairErasureChunks);

        // TODO(yuryalekseev): Prohibit ClusterConnection in Spec_.
        if (Spec_->ClusterName) {
            remoteCopyJobSpecExt->set_remote_cluster_name(*Spec_->ClusterName);
        }
    }

    NNative::IConnectionPtr GetRemoteConnection() const
    {
        if (Spec_->ClusterConnection) {
            NNative::TConnectionOptions connectionOptions;
            connectionOptions.ConnectionInvoker = Host->GetConnectionInvoker();
            return NApi::NNative::CreateConnection(
                Spec_->ClusterConnection,
                std::move(connectionOptions));
        } else if (Spec_->ClusterName) {
            return Host
                ->GetClient()
                ->GetNativeConnection()
                ->GetClusterDirectory()
                ->GetConnectionOrThrow(*Spec_->ClusterName);
        } else {
            THROW_ERROR_EXCEPTION("No remote cluster is specified");
        }
    }

    NApi::NNative::TConnectionCompoundConfigPtr GetRemoteConnectionConfig() const
    {
        return GetRemoteConnection()->GetCompoundConfig();
    }

    std::vector<std::string> GetRemoteMasterCacheAddresses() const
    {
        try {
            return GuardedGetRemoteMasterCacheAddresses();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to get remote master cache addresses")
                << ex;
        }
    }

    std::vector<std::string> GuardedGetRemoteMasterCacheAddresses() const
    {
        if (!Spec_->UseRemoteMasterCaches) {
            return {};
        }

        TGetClusterMetaOptions options{
            .PopulateMasterCacheNodeAddresses = true,
        };
        auto clusterMeta = WaitFor(InputClient->GetClusterMeta(options))
            .ValueOrThrow();
        return clusterMeta.MasterCacheNodeAddresses;
    }

    EChunkAvailabilityPolicy GetChunkAvailabilityPolicy() const override
    {
        // If repair in remote copy is disabled, all parts are required.
        if (!Spec_->RepairErasureChunks) {
            return EChunkAvailabilityPolicy::AllPartsAvailable;
        }

        return Spec_->ChunkAvailabilityPolicy;
    }

    EJobType GetJobType() const override
    {
        return EJobType::RemoteCopy;
    }

    bool IsTeleportationSupported() const override
    {
        return false;
    }

    i64 GetMinTeleportChunkSize() override
    {
        return std::numeric_limits<i64>::max() / 4;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TCpuResource GetCpuLimit() const override
    {
        return Options_->CpuLimit;
    }

    void UpdateHunkChunkIdMapping(const THashMap<TChunkId, TChunkId>& newMapping)
    {
        for (const auto& mapping : newMapping) {
            EmplaceOrCrash(
                HunkChunkIdMapping_,
                mapping.first,
                mapping.second);
        }
    }

    void ValidateHunkChunksConsistency() const
    {
        YT_VERIFY(HunkChunkIdMapping_.size() == InputManager->GetInputTables()[0]->HunkChunks.size());
    }

    void FillJobSpecHunkChunkIdMapping()
    {
        auto* remoteCopyJobSpecExt = JobSpecTemplate_.MutableExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);

        for (const auto& mapping : HunkChunkIdMapping_) {
            auto* protoMapping = remoteCopyJobSpecExt->add_hunk_chunk_id_mapping();
            ToProto(protoMapping->mutable_input_hunk_chunk_id(), mapping.first);
            ToProto(protoMapping->mutable_output_hunk_chunk_id(), mapping.second);
        }
    }

    PHOENIX_DECLARE_FRIEND();
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController);
DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController::TRemoteCopyTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TRemoteCopyController::TRemoteCopyHunkTask);

IOperationControllerPtr CreateRemoteCopyController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->RemoteCopyOperationOptions;
    auto spec = ParseOperationSpec<TRemoteCopyOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TRemoteCopyController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
