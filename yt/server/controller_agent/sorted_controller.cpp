#include "sorted_controller.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "helpers.h"
#include "job_info.h"
#include "job_memory.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "operation.h"

#include <yt/server/chunk_pools/chunk_pool.h>
#include <yt/server/chunk_pools/sorted_chunk_pool.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NScheduler;

using NChunkClient::TReadRange;
using NChunkClient::TReadLimit;
using NTableClient::TKey;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): support Config->MaxTotalSliceCount
// TODO(max42): reorder virtual methods in public section.

class TSortedControllerBase
    : public TOperationControllerBase
{
public:
    TSortedControllerBase(
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

    virtual void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, JobIOConfig_);
        Persist(context, JobSpecTemplate_);
        Persist(context, JobSizeConstraints_);
        Persist(context, InputSliceDataWeight_);
        Persist(context, SortedTaskGroup_);
        Persist(context, SortedTask_);
        Persist(context, PrimaryKeyColumns_);
        Persist(context, ForeignKeyColumns_);
    }

protected:
    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig_;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate_;

    class TSortedTask
        : public TTask
    {
    public:
        //! For persistence only.
        TSortedTask()
            : Controller_(nullptr)
        { }

        TSortedTask(TSortedControllerBase* controller)
            : TTask(controller)
            , Controller_(controller)
        {
            auto options = controller->GetSortedChunkPoolOptions();
            options.Task = GetId();
            ChunkPool_ = CreateSortedChunkPool(
                options,
                controller->CreateChunkSliceFetcherFactory(),
                controller->GetInputStreamDirectory());

        }

        virtual TString GetId() const override
        {
            return Format("Sorted");
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->SortedTaskGroup_;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec_->LocalityTimeout
                : TDuration::Zero();
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            return GetMergeResources(joblet->InputStripeList->GetStatistics());
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
            Persist(context, Controller_);
            Persist(context, ChunkPool_);
        }

        virtual bool SupportsInputPathYson() const override
        {
            return true;
        }

    protected:
        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedTask, 0xf881be2a);

        TSortedControllerBase* Controller_;

        //! Initialized in descendandt tasks.
        std::unique_ptr<IChunkPool> ChunkPool_;

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
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

        virtual EJobType GetJobType() const override
        {
            return Controller_->GetJobType();
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetUserJobSpec();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->JobSpecTemplate_);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);
        }

        virtual void SetupCallbacks() override
        {
            TTask::SetupCallbacks();

            ChunkPool_->SubscribePoolOutputInvalidated(BIND([&] (const TError& error) {
                YCHECK(false && "Error during resuming stripe in sorted task");
            }));
        }
    };

    typedef TIntrusivePtr<TSortedTask> TSortedTaskPtr;

    TTaskGroupPtr SortedTaskGroup_;

    TSortedTaskPtr SortedTask_;

    //! The (adjusted) key columns that define the sort order inside sorted chunk pool.
    std::vector<TString> PrimaryKeyColumns_;
    std::vector<TString> ForeignKeyColumns_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    i64 InputSliceDataWeight_;

    IFetcherChunkScraperPtr FetcherChunkScraper_;

    // Custom bits of preparation pipeline.

    TInputStreamDirectory GetInputStreamDirectory()
    {
        std::vector<TInputStreamDescriptor> inputStreams;
        inputStreams.reserve(InputTables.size());
        for (const auto& inputTable : InputTables) {
            inputStreams.emplace_back(inputTable.IsTeleportable, inputTable.IsPrimary(), inputTable.IsDynamic /* isVersioned */);
        }
        return TInputStreamDirectory(inputStreams);
    }

    virtual bool IsCompleted() const override
    {
        return SortedTask_->IsCompleted();
    }

    virtual i64 GetUnavailableInputChunkCount() const override
    {
        if (FetcherChunkScraper_ && State == EControllerState::Preparing) {
            return FetcherChunkScraper_->GetUnavailableChunkCount();
        }

        return TOperationControllerBase::GetUnavailableInputChunkCount();
    };

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        SortedTaskGroup_ = New<TTaskGroup>();
        SortedTaskGroup_->MinNeededResources.SetCpu(GetCpuLimit());

        RegisterTaskGroup(SortedTaskGroup_);
    }

    void CalculateSizes()
    {
        auto createJobSizeConstraints = [&] () -> IJobSizeConstraintsPtr {
            switch (OperationType) {
                case EOperationType::Merge:
                    return CreateMergeJobSizeConstraints(
                        Spec_,
                        Options_,
                        PrimaryInputDataWeight,
                        DataWeightRatio,
                        InputCompressionRatio);

                default:
                    return CreateUserJobSizeConstraints(
                        Spec_,
                        Options_,
                        OutputTables_.size(),
                        DataWeightRatio,
                        PrimaryInputDataWeight,
                        std::numeric_limits<i64>::max() /* InputRowCount */, // It is not important in sorted operations.
                        GetForeignInputDataWeight());
            }
        };

        JobSizeConstraints_ = createJobSizeConstraints();
        InputSliceDataWeight_ = JobSizeConstraints_->GetInputSliceDataWeight();

        LOG_INFO(
            "Calculated operation parameters (JobCount: %v, MaxDataWeightPerJob: %v, InputSliceDataWeight: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            InputSliceDataWeight_);
    }

    TChunkStripePtr CreateChunkStripe(TInputDataSlicePtr dataSlice)
    {
        TChunkStripePtr chunkStripe = New<TChunkStripe>(InputTables[dataSlice->GetTableIndex()].IsForeign());
        chunkStripe->DataSlices.emplace_back(std::move(dataSlice));
        return chunkStripe;
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            TPeriodicYielder yielder(PrepareYieldPeriod);

            InitTeleportableInputTables();

            int primaryUnversionedSlices = 0;
            int primaryVersionedSlices = 0;
            int foreignSlices = 0;
            for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
                const auto& slice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
                InferLimitsFromBoundaryKeys(slice, RowBuffer);
                RegisterInputStripe(CreateChunkStripe(slice), SortedTask_);
                ++primaryUnversionedSlices;
                yielder.TryYield();
            }
            for (const auto& slice : CollectPrimaryVersionedDataSlices(InputSliceDataWeight_)) {
                RegisterInputStripe(CreateChunkStripe(slice), SortedTask_);
                ++primaryVersionedSlices;
                yielder.TryYield();
            }
            for (const auto& tableSlices : CollectForeignInputDataSlices(ForeignKeyColumns_.size())) {
                for (const auto& slice : tableSlices) {
                    RegisterInputStripe(CreateChunkStripe(slice), SortedTask_);
                    ++foreignSlices;
                    yielder.TryYield();
                }
            }

            LOG_INFO("Processed inputs (PrimaryUnversionedSlices: %v, PrimaryVersionedSlices: %v, ForeignSlices: %v)",
                primaryUnversionedSlices,
                primaryVersionedSlices,
                foreignSlices);
        }
    }

    void FinishPreparation()
    {
        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    // Progress reporting.

    virtual TString GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, I: %v}, "
                "UnavailableInputChunks: %v",
            JobCounter->GetTotal(),
            JobCounter->GetRunning(),
            JobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter->GetFailed(),
            JobCounter->GetAbortedTotal(),
            JobCounter->GetInterruptedTotal(),
            GetUnavailableInputChunkCount());
    }

    virtual TNullable<int> GetOutputTeleportTableIndex() const = 0;

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig_ = CloneYsonSerializable(Spec_->JobIO);
    }

    virtual bool IsKeyGuaranteeEnabled() = 0;

    virtual EJobType GetJobType() const = 0;

    virtual TCpuResource GetCpuLimit() const = 0;

    virtual void InitJobSpecTemplate() = 0;

    void InitTeleportableInputTables()
    {
        auto tableIndex = GetOutputTeleportTableIndex();
        if (tableIndex) {
            for (int index = 0; index < InputTables.size(); ++index) {
                if (!InputTables[index].IsDynamic && !InputTables[index].Path.GetColumns()) {
                    InputTables[index].IsTeleportable = ValidateTableSchemaCompatibility(
                        InputTables[index].Schema,
                        OutputTables_[*tableIndex].TableUploadOptions.TableSchema,
                        false /* ignoreSortOrder */).IsOK();
                    if (GetJobType() == EJobType::SortedReduce) {
                        InputTables[index].IsTeleportable &= InputTables[index].Path.GetTeleport();
                    }
                }
            }
        }
    }

    virtual bool ShouldSlicePrimaryTableByKeys() const
    {
        return true;
    }

    virtual i64 MinTeleportChunkSize()  = 0;

    virtual void AdjustKeyColumns() = 0;

    virtual i64 GetUserJobMemoryReserve() const = 0;

    virtual i64 GetForeignInputDataWeight() const = 0;

    virtual void PrepareOutputTables() override
    {
        // NB: we need to do this after locking input tables but before preparing ouput tables.
        AdjustKeyColumns();
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const = 0;

    virtual void CustomPrepare() override
    {
        // NB: Base member is not called intentionally.
        // TODO(max42): But why?

        CalculateSizes();

        InitTeleportableInputTables();

        SortedTask_ = New<TSortedTask>(this);
        RegisterTask(SortedTask_);

        ProcessInputs();

        FinishTaskInput(SortedTask_);

        for (const auto& teleportChunk : SortedTask_->GetChunkPoolOutput()->GetTeleportChunks()) {
            // If teleport chunks were found, then teleport table index should be non-Null.
            RegisterTeleportChunk(teleportChunk, 0, *GetOutputTeleportTableIndex());
        }

        FinishPreparation();
    }

    virtual bool IsBoundaryKeysFetchEnabled() const override
    {
        return true;
    }

    class TChunkSliceFetcherFactory
        : public IChunkSliceFetcherFactory
    {
    public:
        //! Used only for persistence.
        TChunkSliceFetcherFactory() = default;

        TChunkSliceFetcherFactory(TSortedControllerBase* controller)
            : Controller_(controller)
        { }

        virtual IChunkSliceFetcherPtr CreateChunkSliceFetcher() override
        {
            return Controller_->CreateChunkSliceFetcher();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            using NYT::Persist;

            Persist(context, Controller_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TChunkSliceFetcherFactory, 0x23cad49e);

        TSortedControllerBase* Controller_;
    };

    virtual IChunkSliceFetcherFactoryPtr CreateChunkSliceFetcherFactory()
    {
        return New<TChunkSliceFetcherFactory>(this /* controller */);
    }

    virtual TSortedChunkPoolOptions GetSortedChunkPoolOptions()
    {
        TSortedChunkPoolOptions chunkPoolOptions;
        TSortedJobOptions jobOptions;
        jobOptions.EnableKeyGuarantee = IsKeyGuaranteeEnabled();
        jobOptions.PrimaryPrefixLength = PrimaryKeyColumns_.size();
        jobOptions.ForeignPrefixLength = ForeignKeyColumns_.size();
        jobOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        jobOptions.MaxDataWeightPerJob = Spec_->MaxDataWeightPerJob;
        jobOptions.EnablePeriodicYielder = true;

        if (Spec_->NightlyOptions) {
            auto useNewEndpointKeys = Spec_->NightlyOptions->FindChild("use_new_endpoint_keys");
            if (useNewEndpointKeys && useNewEndpointKeys->GetType() == ENodeType::Boolean) {
                jobOptions.UseNewEndpointKeys = useNewEndpointKeys->AsBoolean()->GetValue();
            }
        }

        chunkPoolOptions.SortedJobOptions = jobOptions;
        chunkPoolOptions.MinTeleportChunkSize = MinTeleportChunkSize();
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.OperationId = OperationId;
        chunkPoolOptions.ExtractionOrder = Spec_->StripeListExtractionOrder;
        return chunkPoolOptions;
    }

private:
    IChunkSliceFetcherPtr CreateChunkSliceFetcher()
    {
        if (Spec_->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
            FetcherChunkScraper_ = CreateFetcherChunkScraper(
                Config->ChunkScraper,
                GetCancelableInvoker(),
                Host->GetChunkLocationThrottlerManager(),
                InputClient,
                InputNodeDirectory_,
                Logger);
        }

        return NTableClient::CreateChunkSliceFetcher(
            Config->Fetcher,
            InputSliceDataWeight_,
            PrimaryKeyColumns_,
            ShouldSlicePrimaryTableByKeys(),
            InputNodeDirectory_,
            GetCancelableInvoker(),
            FetcherChunkScraper_,
            Host->GetClient(),
            RowBuffer,
            Logger);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedControllerBase::TSortedTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedControllerBase::TChunkSliceFetcherFactory);

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeController
    : public TSortedControllerBase
{
public:
    TSortedMergeController(
        TSortedMergeOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TSortedMergeOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    {
        RegisterJobProxyMemoryDigest(EJobType::SortedMerge, spec->JobProxyMemoryDigest);
    }

    virtual bool ShouldSlicePrimaryTableByKeys() const override
    {
        return true;
    }

    virtual bool IsRowCountPreserved() const override
    {
        return true;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return nullptr;
    }

    virtual i64 MinTeleportChunkSize() override
    {
        if (Spec_->ForceTransform) {
            return std::numeric_limits<i64>::max();
        }
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    virtual void AdjustKeyColumns() override
    {
        const auto& specKeyColumns = Spec_->MergeBy;
        LOG_INFO("Spec key columns are %v", specKeyColumns);

        PrimaryKeyColumns_ = CheckInputTablesSorted(specKeyColumns);
        LOG_INFO("Adjusted key columns are %v", PrimaryKeyColumns_);
    }

    virtual bool IsKeyGuaranteeEnabled() override
    {
        return false;
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::SortedMerge;
    }

    virtual TCpuResource GetCpuLimit() const override
    {
        return 1;
    }

    virtual i64 GetUserJobMemoryReserve() const override
    {
        return 0;
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::SortedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        auto* mergeJobSpecExt = JobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).GetData());

        SetInputDataSources(schedulerJobSpecExt);
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).GetData());

        ToProto(mergeJobSpecExt->mutable_key_columns(), PrimaryKeyColumns_);
    }

    virtual TNullable<int> GetOutputTeleportTableIndex() const override
    {
        return MakeNullable(0);
    }

    virtual void PrepareOutputTables() override
    {
        // Check that all input tables are sorted by the same key columns.
        TSortedControllerBase::PrepareOutputTables();

        auto& table = OutputTables_[0];
        table.TableUploadOptions.LockMode = ELockMode::Exclusive;

        auto prepareOutputKeyColumns = [&] () {
            if (table.TableUploadOptions.TableSchema.IsSorted()) {
                if (table.TableUploadOptions.TableSchema.GetKeyColumns() != PrimaryKeyColumns_) {
                    THROW_ERROR_EXCEPTION("Merge key columns do not match output table schema in \"strong\" schema mode")
                            << TErrorAttribute("output_schema", table.TableUploadOptions.TableSchema)
                            << TErrorAttribute("merge_by", PrimaryKeyColumns_)
                            << TErrorAttribute("schema_inference_mode", Spec_->SchemaInferenceMode);
                }
            } else {
                table.TableUploadOptions.TableSchema =
                    table.TableUploadOptions.TableSchema.ToSorted(PrimaryKeyColumns_);
            }
        };

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(PrimaryKeyColumns_);
                } else {
                    prepareOutputKeyColumns();
                    ValidateOutputSchemaCompatibility(true);
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(PrimaryKeyColumns_);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table.TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(PrimaryKeyColumns_);
                } else {
                    prepareOutputKeyColumns();
                }
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual i64 GetForeignInputDataWeight() const override
    {
        return 0;
    }

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::SortedMerge};
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec_;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController, 0xf3b791ca);

    TSortedMergeOperationSpecPtr Spec_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController);

IOperationControllerPtr CreateSortedMergeController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TSortedMergeOperationSpec>(operation->GetSpec());
    return New<TSortedMergeController>(spec, config, config->SortedMergeOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TSortedReduceControllerBase
    : public TSortedControllerBase
{
public:
    TSortedReduceControllerBase(
        TReduceOperationSpecBasePtr spec,
        TControllerAgentConfigPtr config,
        TReduceOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
        , Options_(options)
    { }

    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    virtual bool AreForeignTablesSupported() const override
    {
        return true;
    }

    virtual TCpuResource GetCpuLimit() const override
    {
        return Spec_->Reducer->CpuLimit;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return Spec_->Reducer;
    }

    virtual i64 GetUserJobMemoryReserve() const override
    {
        return ComputeUserJobMemoryReserve(GetJobType(), Spec_->Reducer);
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec_->OutputTablePaths;
    }

    virtual TNullable<int> GetOutputTeleportTableIndex() const override
    {
        return OutputTeleportTableIndex_;
    }

    virtual i64 MinTeleportChunkSize() override
    {
        return 0;
    }

    virtual void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex_;
        StartRowIndex_ += joblet->InputStripeList->TotalRowCount;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        for (const auto& path : Spec_->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual std::vector<TPathWithStage> GetLayerPaths() const override
    {
        std::vector<TPathWithStage> result;
        for (const auto& path : Spec_->Reducer->LayerPaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual void InitJobSpecTemplate() override
    {
        YCHECK(!PrimaryKeyColumns_.empty());

        JobSpecTemplate_.set_type(static_cast<int>(GetJobType()));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).GetData());

        SetInputDataSources(schedulerJobSpecExt);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).GetData());

        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec_->Reducer,
            Files,
            Spec_->JobNodeAccount);

        auto* reduceJobSpecExt = JobSpecTemplate_.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        ToProto(reduceJobSpecExt->mutable_key_columns(), SortKeyColumns_);
        reduceJobSpecExt->set_reduce_key_column_count(PrimaryKeyColumns_.size());
        reduceJobSpecExt->set_join_key_column_count(ForeignKeyColumns_.size());
    }

    virtual void CustomizeJobSpec(const TJobletPtr& joblet, TJobSpec* jobSpec) override
    {
        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet);
    }

    virtual void DoInitialize() override
    {
        TSortedControllerBase::DoInitialize();

        int teleportOutputCount = 0;
        for (int i = 0; i < static_cast<int>(OutputTables_.size()); ++i) {
            if (OutputTables_[i].Path.GetTeleport()) {
                ++teleportOutputCount;
                OutputTeleportTableIndex_ = i;
            }
        }

        if (teleportOutputCount > 1) {
            THROW_ERROR_EXCEPTION("Too many teleport output tables: maximum allowed 1, actual %v",
                teleportOutputCount);
        }

        ValidateUserFileCount(Spec_->Reducer, "reducer");
    }

    virtual void BuildBriefSpec(TFluentMap fluent) const override
    {
        TSortedControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("reducer").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec_->Reducer->Command))
            .EndMap();
    }

    virtual bool IsJobInterruptible() const override
    {
        // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
        return
            2 * Options_->MaxOutputTablesTimesJobsCount > JobCounter->GetTotal() * GetOutputTablePaths().size() &&
            2 * Options_->MaxJobCount > JobCounter->GetTotal() &&
            TOperationControllerBase::IsJobInterruptible();
    }

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
    {
        return IsJobInterruptible() && Config->EnableJobSplitting && Spec_->EnableJobSplitting
            ? Options_->JobSplitter
            : nullptr;
    }

    virtual bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    virtual TNullable<TRichYPath> GetStderrTablePath() const override
    {
        return Spec_->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec_->StderrTableWriterConfig;
    }

    virtual TNullable<TRichYPath> GetCoreTablePath() const override
    {
        return Spec_->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec_->CoreTableWriterConfig;
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    virtual i64 GetForeignInputDataWeight() const override
    {
        return Spec_->ConsiderOnlyPrimarySize ? 0 : ForeignInputDataWeight;
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec_;
    }

protected:
    std::vector<TString> SortKeyColumns_;

private:
    TReduceOperationSpecBasePtr Spec_;
    TReduceOperationOptionsPtr Options_;

    i64 StartRowIndex_ = 0;

    TNullable<int> OutputTeleportTableIndex_;
};

class TSortedReduceController
    : public TSortedReduceControllerBase
{
public:
    TSortedReduceController(
        TReduceOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TReduceOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedReduceControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    {
        RegisterJobProxyMemoryDigest(EJobType::SortedReduce, spec->JobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::SortedReduce, spec->Reducer->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);
    }

    virtual bool ShouldSlicePrimaryTableByKeys() const override
    {
        return true;
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::SortedReduce;
    }

    virtual bool IsKeyGuaranteeEnabled() override
    {
        return true;
    }

    virtual void AdjustKeyColumns() override
    {
        auto specKeyColumns = Spec_->SortBy.empty() ? Spec_->ReduceBy : Spec_->SortBy;
        LOG_INFO("Spec key columns are %v", specKeyColumns);

        SortKeyColumns_ = CheckInputTablesSorted(specKeyColumns, &TInputTable::IsPrimary);

        if (SortKeyColumns_.size() < Spec_->ReduceBy.size() ||
            !CheckKeyColumnsCompatible(SortKeyColumns_, Spec_->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce key columns %v are not compatible with sort key columns %v",
                Spec_->ReduceBy,
                SortKeyColumns_);
        }

        PrimaryKeyColumns_ = Spec_->ReduceBy;
        ForeignKeyColumns_ = Spec_->JoinBy;
        if (!ForeignKeyColumns_.empty()) {
            LOG_INFO("Foreign key columns are %v", ForeignKeyColumns_);

            CheckInputTablesSorted(ForeignKeyColumns_, &TInputTable::IsForeign);

            if (Spec_->ReduceBy.size() < ForeignKeyColumns_.size() ||
                !CheckKeyColumnsCompatible(Spec_->ReduceBy, ForeignKeyColumns_))
            {
                THROW_ERROR_EXCEPTION("Join key columns %v are not compatible with reduce key columns %v",
                    ForeignKeyColumns_,
                    Spec_->ReduceBy);
            }
        }
    }

    virtual void DoInitialize() override
    {
        TSortedReduceControllerBase::DoInitialize();

        int foreignInputCount = 0;
        for (auto& table : InputTables) {
            if (table.Path.GetForeign()) {
                if (table.Path.GetTeleport()) {
                    THROW_ERROR_EXCEPTION("Foreign table can not be specified as teleport");
                }
                if (table.Path.GetRanges().size() > 1) {
                    THROW_ERROR_EXCEPTION("Reduce operation does not support foreign tables with multiple ranges");
                }
                ++foreignInputCount;
            }
        }

        if (foreignInputCount == InputTables.size()) {
            THROW_ERROR_EXCEPTION("At least one non-foreign input table is required");
        }

        if (foreignInputCount == 0 && !Spec_->JoinBy.empty()) {
            THROW_ERROR_EXCEPTION("At least one foreign input table is required");
        }

        if (foreignInputCount != 0 && Spec_->JoinBy.empty()) {
            THROW_ERROR_EXCEPTION("Join key columns are required");
        }

        if (!Spec_->PivotKeys.empty()) {
            TKey previousKey;
            for (const auto& key : Spec_->PivotKeys) {
                if (key < previousKey) {
                    THROW_ERROR_EXCEPTION("Pivot keys should be sorted")
                        << TErrorAttribute("lhs", previousKey)
                        << TErrorAttribute("rhs", key);
                }
                previousKey = key;
                if (key.GetCount() > Spec_->ReduceBy.size()) {
                    THROW_ERROR_EXCEPTION("Pivot key can't be longer than reduce key column count")
                        << TErrorAttribute("key", key)
                        << TErrorAttribute("reduce_by", Spec_->ReduceBy);
                }
            }
            for (auto& table : InputTables) {
                if (table.Path.GetTeleport()) {
                    THROW_ERROR_EXCEPTION("Chunk teleportation is not supported when pivot keys are specified");
                }
            }
        }
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::SortedReduce};
    }

    virtual bool IsJobInterruptible() const override
    {
        return Spec_->PivotKeys.empty() && TSortedControllerBase::IsJobInterruptible();
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedReduceController, 0x761aad8e);

    TReduceOperationSpecPtr Spec_;

    virtual IChunkSliceFetcherFactoryPtr CreateChunkSliceFetcherFactory() override
    {
        if (Spec_->PivotKeys.empty()) {
            return TSortedControllerBase::CreateChunkSliceFetcherFactory();
        } else {
            return nullptr;
        }
    }

    virtual TSortedChunkPoolOptions GetSortedChunkPoolOptions() override
    {
        auto options = TSortedControllerBase::GetSortedChunkPoolOptions();
        options.SortedJobOptions.PivotKeys = std::vector<TKey>(Spec_->PivotKeys.begin(), Spec_->PivotKeys.end());
        return options;
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec_;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedReduceController);

IOperationControllerPtr CreateSortedReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TReduceOperationSpec>(operation->GetSpec());
    return New<TSortedReduceController>(spec, config, config->ReduceOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TJoinReduceController
    : public TSortedReduceControllerBase
{
public:
    TJoinReduceController(
        TJoinReduceOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TReduceOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortedReduceControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec_(spec)
    {
        RegisterJobProxyMemoryDigest(EJobType::JoinReduce, spec->JobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::JoinReduce, spec->Reducer->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);
    }

    virtual bool ShouldSlicePrimaryTableByKeys() const override
    {
        return false;
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::JoinReduce;
    }

    virtual bool IsKeyGuaranteeEnabled() override
    {
        return false;
    }

    virtual void AdjustKeyColumns() override
    {
        LOG_INFO("Spec key columns are %v", Spec_->JoinBy);
        SortKeyColumns_ = ForeignKeyColumns_ = PrimaryKeyColumns_ = CheckInputTablesSorted(Spec_->JoinBy);
    }

    virtual void DoInitialize() override
    {
        TSortedReduceControllerBase::DoInitialize();

        if (InputTables.size() < 2) {
            THROW_ERROR_EXCEPTION("At least two input tables are required");
        }

        int primaryInputCount = 0;
        for (const auto& inputTable : InputTables) {
            if (!inputTable.Path.GetForeign()) {
                ++primaryInputCount;
            }
            if (inputTable.Path.GetTeleport()) {
                THROW_ERROR_EXCEPTION("Teleport tables are not supported in join-reduce");
            }
        }

        if (primaryInputCount != 1) {
            THROW_ERROR_EXCEPTION("You must specify exactly one non-foreign (primary) input table (%v specified)",
                                  primaryInputCount);
        }

        // For join reduce tables with multiple ranges are not supported.
        for (const auto& inputTable : InputTables) {
            auto& path = inputTable.Path;
            auto ranges = path.GetRanges();
            if (ranges.size() > 1) {
                THROW_ERROR_EXCEPTION("Join reduce operation does not support tables with multiple ranges");
            }
        }

        // Forbid teleport attribute for output tables.
        if (GetOutputTeleportTableIndex()) {
            THROW_ERROR_EXCEPTION("Teleport tables are not supported in join-reduce");
        }
    }

    virtual i64 GetForeignInputDataWeight() const override
    {
        return Spec_->ConsiderOnlyPrimarySize ? 0 : ForeignInputDataWeight;
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec_;
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::JoinReduce};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TJoinReduceController, 0x1120ca9f);

    TJoinReduceOperationSpecPtr Spec_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJoinReduceController);

IOperationControllerPtr CreateJoinReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TJoinReduceOperationSpec>(operation->GetSpec());
    return New<TJoinReduceController>(spec, config, config->ReduceOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
