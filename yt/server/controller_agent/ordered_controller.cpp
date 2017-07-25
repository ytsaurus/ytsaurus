#include "ordered_controller.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "helpers.h"
#include "job_memory.h"
#include "operation_controller_detail.h"

#include <yt/server/chunk_pools/chunk_pool.h>
#include <yt/server/chunk_pools/ordered_chunk_pool.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
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

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////////////////

class TOrderedControllerBase
    : public TOperationControllerBase
{
public:
    TOrderedControllerBase(
        TSchedulerConfigPtr config,
        TSimpleOperationSpecBasePtr spec,
        TSimpleOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, options, host, operation)
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
        Persist(context, OrderedTaskGroup_);
        Persist(context, OrderedTask_);
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
            : TTask(controller)
            , Controller_(controller)
            , ChunkPool_(CreateOrderedChunkPool(controller->GetOrderedChunkPoolOptions(), controller->GetInputStreamDirectory()))
        { }

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

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedTask, 0xaba78384);

        TOrderedControllerBase* Controller_;

        std::unique_ptr<IChunkPool> ChunkPool_;

        virtual TString GetId() const override
        {
            return Format("Ordered");
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->OrderedTaskGroup_;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller_->Spec_->LocalityTimeout;
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            return GetMergeResources(joblet->InputStripeList->GetStatistics());
        }

        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet);
            AddFinalOutputSpecs(jobSpec, joblet);
        }

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

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            if (Controller_->OrderedOutputRequired_) {
                RegisterOutput(joblet, TOutputOrder::TEntry(joblet->OutputCookie), jobSummary);
            } else {
                RegisterOutput(joblet, 0 /* key */, jobSummary);
            }
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);
        }
    };

    typedef TIntrusivePtr<TOrderedTask> TOrderedTaskPtr;

    TTaskGroupPtr OrderedTaskGroup_;

    TOrderedTaskPtr OrderedTask_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    i64 InputSliceDataWeight_;

    bool OrderedOutputRequired_ = false;

    bool IsExplicitJobCount_ = false;

    virtual EJobType GetJobType() const = 0;

    virtual TCpuResource GetCpuLimit() const = 0;

    virtual void InitJobSpecTemplate() = 0;

    virtual bool IsTeleportationSupported() const = 0;

    virtual i64 GetMinTeleportChunkSize() = 0;

    virtual i64 GetUserJobMemoryReserve() const = 0;

    virtual TUserJobSpecPtr GetUserJobSpec() const = 0;

    // Custom bits of preparation pipeline.

    TInputStreamDirectory GetInputStreamDirectory() const
    {
        std::vector<TInputStreamDescriptor> inputStreams;
        inputStreams.reserve(InputTables.size());
        for (const auto& inputTable : InputTables) {
            inputStreams.emplace_back(inputTable.IsTeleportable, true /* isPrimary */, inputTable.IsDynamic /* isVersioned */);
        }
        return TInputStreamDirectory(inputStreams);
    }

    virtual bool IsCompleted() const override
    {
        return OrderedTask_->IsCompleted();
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        OrderedTaskGroup_ = New<TTaskGroup>();
        OrderedTaskGroup_->MinNeededResources.SetCpu(GetCpuLimit());

        RegisterTaskGroup(OrderedTaskGroup_);
    }

    void CalculateSizes()
    {
        JobSizeConstraints_ = CreateSimpleJobSizeConstraints(
            Spec_,
            Options_,
            OutputTables.size(),
            PrimaryInputDataSize + ForeignInputDataSize);

        IsExplicitJobCount_ = JobSizeConstraints_->IsExplicitJobCount();

        InputSliceDataWeight_ = JobSizeConstraints_->GetInputSliceDataWeight();

        LOG_INFO("Calculated operation parameters (JobCount: %v, MaxDataWeightPerJob: %v, InputSliceDataWeight: %v)",
            JobSizeConstraints_->GetJobCount(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            InputSliceDataWeight_);
    }

    TChunkStripePtr CreateChunkStripe(TInputDataSlicePtr dataSlice)
    {
        TChunkStripePtr chunkStripe = New<TChunkStripe>(false /* foreign */);
        chunkStripe->DataSlices.emplace_back(std::move(dataSlice));
        return chunkStripe;
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            TPeriodicYielder yielder(PrepareYieldPeriod);

            InitTeleportableInputTables();

            int sliceCount = 0;
            for (auto& slice : CollectPrimaryInputDataSlices(InputSliceDataWeight_)) {
                RegisterInputStripe(CreateChunkStripe(std::move(slice)), OrderedTask_);
                ++sliceCount;
                yielder.TryYield();
            }

            LOG_INFO("Processed inputs (Slices: %v)", sliceCount);
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
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAbortedTotal(),
            JobCounter.GetInterruptedTotal(),
            GetUnavailableInputChunkCount());
    }

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig_ = CloneYsonSerializable(Spec_->JobIO);
        InitFinalOutputConfig(JobIOConfig_);
    }

    void InitTeleportableInputTables()
    {
        if (IsTeleportationSupported()) {
            for (int index = 0; index < InputTables.size(); ++index) {
                if (!InputTables[index].IsDynamic && !InputTables[index].Path.GetColumns()) {
                    InputTables[index].IsTeleportable = ValidateTableSchemaCompatibility(
                        InputTables[index].Schema,
                        OutputTables[0].TableUploadOptions.TableSchema,
                        false /* ignoreSortOrder */).IsOK();
                }
            }
        }
    }

    virtual TOutputOrderPtr GetOutputOrder() const override
    {
        return OrderedTask_->GetChunkPoolOutput()->GetOutputOrder();
    }

    virtual void CustomPrepare() override
    {
        // NB: Base member is not called intentionally.

        CalculateSizes();

        InitTeleportableInputTables();

        for (const auto& table : InputTables) {
            if (!table.Schema.IsSorted()) {
                OrderedOutputRequired_ = true;
            }
        }

        OrderedTask_ = New<TOrderedTask>(this);

        ProcessInputs();

        OrderedTask_->FinishInput();

        for (const auto& teleportChunk : OrderedTask_->GetChunkPoolOutput()->GetTeleportChunks()) {
            if (OrderedOutputRequired_) {
                RegisterTeleportChunk(
                    teleportChunk,
                    TOutputOrder::TEntry(teleportChunk) /* key */,
                    0 /* tableIndex */);
            } else {
                RegisterTeleportChunk(
                    teleportChunk,
                    0 /* key */,
                    0 /* tableIndex */);
            }
        }

        RegisterTask(OrderedTask_);

        FinishPreparation();
    }

    virtual TOrderedChunkPoolOptions GetOrderedChunkPoolOptions()
    {
        TOrderedChunkPoolOptions chunkPoolOptions;
        chunkPoolOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
        chunkPoolOptions.EnablePeriodicYielder = true;
        chunkPoolOptions.MinTeleportChunkSize = GetMinTeleportChunkSize();
        chunkPoolOptions.JobSizeConstraints = JobSizeConstraints_;
        chunkPoolOptions.OperationId = OperationId;
        chunkPoolOptions.KeepOutputOrder = OrderedOutputRequired_;
        return chunkPoolOptions;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedControllerBase::TOrderedTask);

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeController
    : public TOrderedControllerBase
{
public:
    TOrderedMergeController(
        TSchedulerConfigPtr config,
        TOrderedMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedControllerBase(config, spec, config->OrderedMergeOperationOptions, host, operation)
        , Spec_(spec)
    {
        RegisterJobProxyMemoryDigest(EJobType::OrderedMerge, spec->JobProxyMemoryDigest);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController, 0xe7098bca);

    TOrderedMergeOperationSpecPtr Spec_;

    virtual bool IsRowCountPreserved() const override
    {
        return !Spec_->InputQuery;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const override
    {
        return nullptr;
    }

    virtual i64 GetMinTeleportChunkSize() override
    {
        if (Spec_->ForceTransform || Spec_->InputQuery) {
            return std::numeric_limits<i64>::max();
        }
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::OrderedMerge;
    }

    virtual TCpuResource GetCpuLimit() const override
    {
        return 1;
    }

    virtual i64 GetUserJobMemoryReserve() const override
    {
        return 0;
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec_->InputQuery) {
            ParseInputQuery(*Spec_->InputQuery, Spec_->InputSchema);
        }
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    virtual bool IsBoundaryKeysFetchEnabled() const override
    {
        // Required for chunk teleporting in case of sorted output.
        return OutputTables[0].TableUploadOptions.TableSchema.IsSorted();
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->OutputTablePath};
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::OrderedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).GetData());

        if (Spec_->InputQuery) {
            WriteInputQueryToJobSpec(schedulerJobSpecExt);
        }

        ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).GetData());
    }

    virtual bool IsTeleportationSupported() const override
    {
        return true;
    }

    virtual void PrepareOutputTables() override
    {
        auto& table = OutputTables[0];

        auto inferFromInput = [&] () {
            if (Spec_->InputQuery) {
                table.TableUploadOptions.TableSchema = InputQuery->Query->GetTableSchema();
            } else {
                InferSchemaFromInputOrdered();
            }
        };

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    inferFromInput();
                } else {
                    ValidateOutputSchemaOrdered();
                    if (!Spec_->InputQuery) {
                        ValidateOutputSchemaCompatibility(false);
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                inferFromInput();
                break;

            case ESchemaInferenceMode::FromOutput:
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::OrderedMerge};
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController);

IOperationControllerPtr CreateOrderedMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TOrderedMergeOperationSpec>(operation->GetSpec());
    return New<TOrderedMergeController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedMapController
    : public TOrderedControllerBase
{
public:
    TOrderedMapController(
        TSchedulerConfigPtr config,
        TMapOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedControllerBase(config, spec, config->MapOperationOptions, host, operation)
        , Spec_(spec)
        , Options_(config->MapOperationOptions)
    {
        RegisterJobProxyMemoryDigest(EJobType::OrderedMap, spec->JobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::OrderedMap, spec->Mapper->MemoryReserveFactor);
    }

    virtual void Persist(const TPersistenceContext& context) override
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

    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return Spec_->Mapper;
    }

    virtual i64 GetMinTeleportChunkSize() override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::OrderedMap;
    }

    virtual TCpuResource GetCpuLimit() const override
    {
        return Spec_->Mapper->CpuLimit;
    }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOperationControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .Item("mapper").BeginMap()
            .Item("command").Value(TrimCommandForBriefSpec(Spec_->Mapper->Command))
            .EndMap();
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex_;
        StartRowIndex_ += joblet->InputStripeList->TotalRowCount;
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet);
    }

    virtual i64 GetUserJobMemoryReserve() const override
    {
        return ComputeUserJobMemoryReserve(EJobType::OrderedMap, Spec_->Mapper);
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec_->OutputTablePaths;
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

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::OrderedMap));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).GetData());

        ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());

        if (Spec_->InputQuery) {
            WriteInputQueryToJobSpec(schedulerJobSpecExt);
        }

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).GetData());

        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec_->Mapper,
            Files,
            Spec_->JobNodeAccount);
    }

    virtual bool IsTeleportationSupported() const override
    {
        return false;
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec_->InputQuery) {
            ParseInputQuery(*Spec_->InputQuery, Spec_->InputSchema);
        }
    }

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
    {
        return IsJobInterruptible() && Config->EnableJobSplitting && Spec_->EnableJobSplitting
            ? Options_->JobSplitter
            : nullptr;
    }

    virtual bool IsJobInterruptible() const override
    {
        // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
        return !IsExplicitJobCount_ &&
               2 * Options_->MaxOutputTablesTimesJobsCount > JobCounter.GetTotal() * GetOutputTablePaths().size();
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::OrderedMap};
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        for (const auto& path : Spec_->Mapper->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Map));
        }
        return result;
    }

    virtual void DoInitialize() override
    {
        TOrderedControllerBase::DoInitialize();

        ValidateUserFileCount(Spec_->Mapper, "mapper");
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMapController);

IOperationControllerPtr CreateOrderedMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
    return New<TOrderedMapController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TEraseController
    : public TOrderedControllerBase
{
public:
    TEraseController(
        TSchedulerConfigPtr config,
        TEraseOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedControllerBase(config, spec, config->EraseOperationOptions, host, operation)
        , Spec_(spec)
    {
        RegisterJobProxyMemoryDigest(EJobType::OrderedMerge, spec->JobProxyMemoryDigest);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TOrderedControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Spec_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseController, 0xfbb39ac0);

    TEraseOperationSpecPtr Spec_;

    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        Y_UNREACHABLE();
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {};
    }

    bool IsTeleportationSupported() const override
    {
        return true;
    }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOrderedControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            // In addition to "input_table_paths" and "output_table_paths".
            // Quite messy, only needed for consistency with the regular spec.
            .Item("table_path").Value(Spec_->TablePath);
    }

    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const override
    {
        return nullptr;
    }

    virtual i64 GetMinTeleportChunkSize() override
    {
        if (!Spec_->CombineChunks) {
            return 0;
        }
        return Spec_->JobIO
            ->TableWriter
            ->DesiredChunkSize;
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return {Spec_->TablePath};
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return {Spec_->TablePath};
    }

    virtual TCpuResource GetCpuLimit() const override
    {
        return 1;
    }

    virtual i64 GetUserJobMemoryReserve() const override
    {
        return 0;
    }

    virtual void DoInitialize() override
    {
        TOrderedControllerBase::DoInitialize();

        auto& path = InputTables[0].Path;
        auto ranges = path.GetRanges();
        if (ranges.size() > 1) {
            THROW_ERROR_EXCEPTION("Erase operation does not support tables with multiple ranges");
        }
        if (path.GetColumns()) {
            THROW_ERROR_EXCEPTION("Erase operation does not support column filtering");
        }

        if (ranges.size() == 1) {
            std::vector<TReadRange> complementaryRanges;
            const auto& range = ranges[0];
            if (!range.LowerLimit().IsTrivial()) {
                complementaryRanges.push_back(TReadRange(TReadLimit(), range.LowerLimit()));
            }
            if (!range.UpperLimit().IsTrivial()) {
                complementaryRanges.push_back(TReadRange(range.UpperLimit(), TReadLimit()));
            }
            path.SetRanges(complementaryRanges);
        } else {
            path.SetRanges(std::vector<TReadRange>());
        }
    }

    virtual bool IsBoundaryKeysFetchEnabled() const override
    {
        // Required for chunk teleporting in case of sorted output.
        return OutputTables[0].TableUploadOptions.TableSchema.IsSorted();
    }

    virtual void PrepareOutputTables() override
    {
        auto& table = OutputTables[0];
        table.TableUploadOptions.UpdateMode = EUpdateMode::Overwrite;
        table.TableUploadOptions.LockMode = ELockMode::Exclusive;

        // Erase output MUST be sorted.
        if (Spec_->SchemaInferenceMode != ESchemaInferenceMode::FromOutput) {
            table.Options->ExplodeOnValidationError = true;
        }

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInputOrdered();
                } else {
                    if (InputTables[0].SchemaMode == ETableSchemaMode::Strong) {
                        ValidateTableSchemaCompatibility(
                            InputTables[0].Schema,
                            table.TableUploadOptions.TableSchema,
                            /* ignoreSortOrder */ false)
                            .ThrowOnError();
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInputOrdered();
                break;

            case ESchemaInferenceMode::FromOutput:
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate_.set_type(static_cast<int>(EJobType::OrderedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate_.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec_->JobIO)).GetData());

        ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig_).GetData());

        auto* jobSpecExt = JobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        const auto& table = OutputTables[0];
        if (table.TableUploadOptions.TableSchema.IsSorted()) {
            ToProto(jobSpecExt->mutable_key_columns(), table.TableUploadOptions.TableSchema.GetKeyColumns());
        }
    }

    virtual EJobType GetJobType() const override
    {
        return EJobType::OrderedMerge;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseController);

IOperationControllerPtr CreateEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TEraseOperationSpec>(operation->GetSpec());
    return New<TEraseController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
