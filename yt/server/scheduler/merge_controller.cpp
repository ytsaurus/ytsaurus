#include "stdafx.h"
#include "merge_controller.h"
#include "private.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "map_controller.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"
#include "helpers.h"

#include <ytlib/chunk_client/chunk_scraper.h>
#include <ytlib/chunk_client/chunk_slice.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/chunk_slices_fetcher.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NTableClient;

using NChunkClient::TReadRange;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////

class TMergeControllerBase
    : public TOperationControllerBase
{
public:
    TMergeControllerBase(
        TSchedulerConfigPtr config,
        TSimpleOperationSpecBasePtr spec,
        TSimpleOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, host, operation)
        , Spec(spec)
        , Options(options)
        , TotalChunkCount(0)
        , TotalDataSize(0)
        , CurrentTaskDataSize(0)
        , CurrentChunkCount(0)
        , CurrentPartitionIndex(0)
        , MaxDataSizePerJob(0)
        , ChunkSliceSize(0)
    { }

    // Persistence.

    virtual void Persist(TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, TotalChunkCount);
        Persist(context, TotalDataSize);
        Persist(context, JobIOConfig);
        Persist(context, JobSpecTemplate);
        Persist(context, MaxDataSizePerJob);
        Persist(context, ChunkSliceSize);
        Persist(context, MergeTaskGroup);
    }

protected:
    TSimpleOperationSpecBasePtr Spec;
    TSimpleOperationOptionsPtr Options;

    //! The total number of chunks for processing.
    int TotalChunkCount;

    //! The total data size for processing.
    i64 TotalDataSize;

    //! For each input table, the corresponding entry holds the stripe
    //! containing the chunks collected so far.
    //! Not serialized.
    /*!
     *  Empty stripes are never stored explicitly and are denoted by |nullptr|.
     */
    std::vector<TChunkStripePtr> CurrentTaskStripes;

    //! The total data size accumulated in #CurrentTaskStripes.
    //! Not serialized.
    i64 CurrentTaskDataSize;

    //! The total number of chunks in #CurrentTaskStripes.
    //! Not serialized.
    int CurrentChunkCount;

    //! The number of output partitions generated so far.
    //! Not serialized.
    /*!
     *  Each partition either corresponds to a merge task or to a teleport chunk.
     *  Partition index is used as a key when calling #TOperationControllerBase::RegisterOutputChunkTree.
     */
    int CurrentPartitionIndex;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;


    //! Overrides the spec limit to satisfy global job count limit.
    i64 MaxDataSizePerJob;
    i64 ChunkSliceSize;


    class TMergeTask
        : public TTask
    {
    public:
        //! For persistence only.
        TMergeTask()
            : Controller(nullptr)
            , TaskIndex(-1)
            , PartitionIndex(-1)
        { }

        TMergeTask(
            TMergeControllerBase* controller,
            int taskIndex,
            int partitionIndex = -1)
            : TTask(controller)
            , Controller(controller)
            , ChunkPool(CreateAtomicChunkPool())
            , TaskIndex(taskIndex)
            , PartitionIndex(partitionIndex)
        { }

        virtual Stroka GetId() const override
        {
            return
                PartitionIndex < 0
                ? Format("Merge(%v)", TaskIndex)
                : Format("Merge(%v,%v)", TaskIndex, PartitionIndex);
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->MergeTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->LocalityTimeout;
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto result = GetMinNeededResources();
            result.set_memory(
                Controller->GetFinalIOMemorySize(
                    Controller->Spec->JobIO,
                    UpdateChunkStripeStatistics(joblet->InputStripeList->GetStatistics())) +
                GetFootprintMemorySize() +
                Controller->GetAdditionalMemorySize(joblet->MemoryReserveEnabled));
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool.get();
        }

        virtual void Persist(TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, ChunkPool);
            Persist(context, TaskIndex);
            Persist(context, PartitionIndex);
        }

    protected:
        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet);
            AddFinalOutputSpecs(jobSpec, joblet);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TMergeTask, 0x72736bac);

        TMergeControllerBase* Controller;

        std::unique_ptr<IChunkPool> ChunkPool;

        //! The position in #TMergeControllerBase::Tasks.
        int TaskIndex;

        //! Key for #TOutputTable::OutputChunkTreeIds.
        int PartitionIndex;

        virtual bool IsMemoryReserveEnabled() const override
        {
            return Controller->IsMemoryReserveEnabled(Controller->JobCounter);
        }

        virtual TNodeResources GetMinNeededResourcesHeavy() const override
        {
            TNodeResources result;

            result.set_user_slots(1);
            result.set_cpu(Controller->GetCpuLimit());
            result.set_memory(
                Controller->GetFinalIOMemorySize(
                    Controller->Spec->JobIO,
                    UpdateChunkStripeStatistics(ChunkPool->GetApproximateStripeStatistics())) +
                GetFootprintMemorySize() +
                Controller->GetAdditionalMemorySize(IsMemoryReserveEnabled()));
            return result;
        }

        TChunkStripeStatisticsVector UpdateChunkStripeStatistics(
            const TChunkStripeStatisticsVector& statistics) const
        {
            if (Controller->IsSingleStripeInput()) {
                return AggregateStatistics(statistics);
            } else {
                return statistics;
            }
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->JobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(joblet, PartitionIndex, jobSummary);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);
            Controller->UpdateAllTasksIfNeeded(Controller->JobCounter);
        }

    };

    typedef TIntrusivePtr<TMergeTask> TMergeTaskPtr;

    TTaskGroupPtr MergeTaskGroup;

    virtual bool IsRowCountPreserved() const override
    {
        return true;
    }

    //! Resizes #CurrentTaskStripes appropriately and sets all its entries to |NULL|.
    void ClearCurrentTaskStripes()
    {
        CurrentTaskStripes.clear();
        CurrentTaskStripes.resize(InputTables.size());
    }

    void EndTask(TMergeTaskPtr task)
    {
        YCHECK(HasActiveTask());

        if (CurrentChunkCount > Config->MaxChunkStripesPerJob) {
            OnOperationFailed(TError("Maximum number of chunk per job exceeded: %v > %v",
                CurrentChunkCount,
                Config->MaxChunkStripesPerJob));
        }

        task->AddInput(CurrentTaskStripes);
        task->FinishInput();
        RegisterTask(task);

        ++CurrentPartitionIndex;

        LOG_DEBUG("Task finished (Id: %v, TaskDataSize: %v, TaskChunkCount: %v)",
            task->GetId(),
            CurrentTaskDataSize,
            CurrentChunkCount);

        CurrentTaskDataSize = 0;
        CurrentChunkCount = 0;
        ClearCurrentTaskStripes();
    }

    //! Finishes the current task.
    void EndTask()
    {
        if (!HasActiveTask())
            return;

        auto task = New<TMergeTask>(
            this,
            static_cast<int>(Tasks.size()),
            CurrentPartitionIndex);
        task->Initialize();

        EndTask(task);
    }

    //! Finishes the current task if the size is large enough.
    void EndTaskIfLarge()
    {
        if (HasLargeActiveTask()) {
            EndTask();
        }
    }

    //! Returns True if some stripes are currently queued.
    bool HasActiveTask()
    {
        return CurrentTaskDataSize > 0;
    }

    //! Returns True if the total data size of currently queued stripes exceeds the pre-configured limit
    //! or number of stripes greater than pre-configured limit.
    bool HasLargeActiveTask()
    {
        YCHECK(MaxDataSizePerJob > 0);
        return CurrentTaskDataSize >= MaxDataSizePerJob || CurrentChunkCount >= Config->MaxChunkStripesPerJob;
    }

    //! Add chunk to the current task's pool.
    void AddPendingChunk(TChunkSlicePtr chunkSlice)
    {
        auto stripe = CurrentTaskStripes[chunkSlice->GetChunkSpec()->table_index()];
        if (!stripe) {
            stripe = CurrentTaskStripes[chunkSlice->GetChunkSpec()->table_index()] = New<TChunkStripe>();
        }

        i64 chunkDataSize = chunkSlice->GetDataSize();
        TotalDataSize += chunkDataSize;
        ++TotalChunkCount;

        CurrentTaskDataSize += chunkDataSize;
        ++CurrentChunkCount;
        stripe->ChunkSlices.push_back(chunkSlice);
    }

    //! Add chunk directly to the output.
    void AddTeleportChunk(TRefCountedChunkSpecPtr chunkSpec)
    {
        auto tableIndex = GetTeleportTableIndex();
        if (tableIndex) {
            LOG_TRACE("Teleport chunk added (ChunkId: %v, Partition: %v)",
                FromProto<TChunkId>(chunkSpec->chunk_id()),
                CurrentPartitionIndex);

            // Place the chunk directly to the output table.
            RegisterOutput(chunkSpec, CurrentPartitionIndex, 0);
            ++CurrentPartitionIndex;
        }
    }


    // Custom bits of preparation pipeline.

    virtual bool IsCompleted() const override
    {
        return Tasks.size() == JobCounter.GetCompleted();
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        MergeTaskGroup = New<TTaskGroup>();
        RegisterTaskGroup(MergeTaskGroup);
    }

    virtual void CustomPrepare() override
    {
        CalculateSizes();
        ProcessInputs();
        EndInputChunks();
        FinishPreparation();
    }

    void CalculateSizes()
    {
        auto jobCount = SuggestJobCount(
            TotalEstimatedInputDataSize,
            Spec->DataSizePerJob,
            Spec->JobCount,
            Options->MaxJobCount);

        MaxDataSizePerJob = (TotalEstimatedInputDataSize + jobCount - 1) / jobCount;
        ChunkSliceSize = static_cast<int>(Clamp(MaxDataSizePerJob, 1, Options->JobMaxSliceDataSize));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            ClearCurrentTaskStripes();
            for (auto chunk : CollectInputChunks()) {
                ProcessInputChunk(chunk);
            }
        }
    }

    void FinishPreparation()
    {
        InitJobIOConfig();
        InitJobSpecTemplate();

        LOG_INFO("Inputs processed (DataSize: %v, ChunkCount: %v, JobCount: %v)",
            TotalDataSize,
            TotalChunkCount,
            Tasks.size());
    }

    //! Called for each input chunk.
    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) = 0;

    //! Called at the end of input chunks scan.
    void EndInputChunks()
    {
        // Close the last task, if any.
        if (CurrentTaskDataSize > 0) {
            EndTask();
        }
    }

    // Progress reporting.

    virtual Stroka GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v}, "
            "UnavailableInputChunks: %v",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            UnavailableInputChunkCount);
    }


    // Unsorted helpers.
    virtual i32 GetCpuLimit() const
    {
        return 1;
    }

    virtual bool IsSingleStripeInput() const
    {
        return true;
    }

    virtual TNullable<int> GetTeleportTableIndex() const
    {
        return MakeNullable(0);
    }

    //! Returns True if the chunk can be included into the output as-is.
    virtual bool IsTeleportChunk(const TChunkSpec& chunkSpec) const = 0;

    virtual i64 GetAdditionalMemorySize(bool memoryReserveEnabled) const
    {
        UNUSED(memoryReserveEnabled);
        return 0;
    }

    //! A typical implementation of #IsTeleportChunk that depends on whether chunks must be combined or not.
    bool IsTeleportChunkImpl(const TChunkSpec& chunkSpec, bool combineChunks) const
    {
        return combineChunks
            ? IsLargeCompleteChunk(chunkSpec, Spec->JobIO->TableWriter->DesiredChunkSize)
            : IsCompleteChunk(chunkSpec);
    }

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
        InitFinalOutputConfig(JobIOConfig);
    }

    //! Initializes #JobSpecTemplate.
    virtual void InitJobSpecTemplate() = 0;

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeControllerBase::TMergeTask);

////////////////////////////////////////////////////////////////////

//! Handles ordered merge and (sic!) erase operations.
class TOrderedMergeControllerBase
    : public TMergeControllerBase
{
public:
    TOrderedMergeControllerBase(
        TSchedulerConfigPtr config,
        TSimpleOperationSpecBasePtr spec,
        TSimpleOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, options, host, operation)
    { }

private:
    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        if (IsTeleportChunk(*chunkSpec)) {
            // Merge is not needed. Copy the chunk directly to the output.
            if (HasActiveTask()) {
                EndTask();
            }
            AddTeleportChunk(chunkSpec);
            return;
        }

        // NB: During ordered merge all chunks go to a single chunk stripe.
        for (const auto& slice : SliceChunkByRowIndexes(chunkSpec, ChunkSliceSize)) {
            AddPendingChunk(slice);
            EndTaskIfLarge();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TOrderedMapController
    : public TOrderedMergeControllerBase
{
public:
    TOrderedMapController(
        TSchedulerConfigPtr config,
        TMapOperationSpecPtr spec,
        TMapOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedMergeControllerBase(config, spec, options, host, operation)
        , Spec(spec)
    { }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOrderedMergeControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .Item("mapper").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
            .EndMap();
    }

    // Persistence.
    virtual void Persist(TPersistenceContext& context) override
    {
        TOrderedMergeControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, StartRowIndex);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMapController, 0x1e5a7e32);

    TMapOperationSpecPtr Spec;

    i64 StartRowIndex = 0;


    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual TNullable<int> GetTeleportTableIndex() const override
    {
        YUNREACHABLE();
    }

    virtual bool IsTeleportChunk(const TChunkSpec& chunkSpec) const override
    {
        return false;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        for (const auto& path : Spec->Mapper->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Map));
        }
        return result;
    }

    virtual void DoInitialize() override
    {
        TOrderedMergeControllerBase::DoInitialize();

        if (Spec->Mapper->FilePaths.size() > Config->MaxUserFileCount) {
            THROW_ERROR_EXCEPTION("Too many user files in mapper: maximum allowed %v, actual %v",
                Config->MaxUserFileCount,
                Spec->Mapper->FilePaths.size());
        }
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }


    // Unsorted helpers.
    virtual i32 GetCpuLimit() const override
    {
        return Spec->Mapper->CpuLimit;
    }

    virtual i64 GetAdditionalMemorySize(bool memoryReserveEnabled) const override
    {
        return GetMemoryReserve(memoryReserveEnabled, Spec->Mapper);
    }

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(static_cast<int>(EJobType::OrderedMap));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (Spec->InputQuery) {
            InitQuerySpec(schedulerJobSpecExt, Spec->InputQuery.Get(), Spec->InputSchema.Get());
        }

        AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec->Mapper,
            Files);
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet,
            GetAdditionalMemorySize(joblet->MemoryReserveEnabled));
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMapController);

////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateOrderedMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
    return New<TOrderedMapController>(config, spec, config->MapOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////

class TOrderedMergeController
    : public TOrderedMergeControllerBase
{
public:
    TOrderedMergeController(
        TSchedulerConfigPtr config,
        TOrderedMergeOperationSpecPtr spec,
        TOrderedMergeOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedMergeControllerBase(config, spec, options, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController, 0x1f748c56);

    TOrderedMergeOperationSpecPtr Spec;

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual bool IsTeleportChunk(const TChunkSpec& chunkSpec) const override
    {
        if (Spec->ForceTransform)
            return false;

        return IsTeleportChunkImpl(chunkSpec, Spec->CombineChunks);
    }

    virtual bool IsRowCountPreserved() const override
    {
        return Spec->InputQuery ? false : TMergeControllerBase::IsRowCountPreserved();
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(static_cast<int>(EJobType::OrderedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        if (Spec->InputQuery) {
            InitQuerySpec(schedulerJobSpecExt, Spec->InputQuery.Get(), Spec->InputSchema.Get());
        }

        AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedMergeController);

////////////////////////////////////////////////////////////////////

class TEraseController
    : public TOrderedMergeControllerBase
{
public:
    TEraseController(
        TSchedulerConfigPtr config,
        TEraseOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedMergeControllerBase(config, spec, config->EraseOperationOptions, host, operation)
        , Spec(spec)
    { }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOrderedMergeControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            // In addition to "input_table_paths" and "output_table_paths".
            // Quite messy, only needed for consistency with the regular spec.
            .Item("table_path").Value(Spec->TablePath);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseController, 0x1cc6ba39);

    TEraseOperationSpecPtr Spec;

    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->TablePath);
        return result;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->TablePath);
        return result;
    }

    virtual bool IsTeleportChunk(const TChunkSpec& chunkSpec) const override
    {
        return IsTeleportChunkImpl(chunkSpec, Spec->CombineChunks);
    }

    virtual void DoInitialize() override
    {
        TOrderedMergeControllerBase::DoInitialize();

        // For erase operation the rowset specified by the user must actually be negated.
        {
            auto& path = InputTables[0].Path;
            auto ranges = path.GetRanges();
            if (ranges.size() > 1) {
                THROW_ERROR_EXCEPTION("Erase operation does not support tables with multiple ranges");
            } else if (ranges.size() == 1) {
                std::vector<TReadRange> complementaryRanges;
                const auto& range = ranges[0];
                if (!range.LowerLimit().IsTrivial()) {
                    complementaryRanges.push_back(TReadRange(TReadLimit(), range.LowerLimit()));
                }
                if (!range.UpperLimit().IsTrivial()) {
                    complementaryRanges.push_back(TReadRange(range.UpperLimit(), TReadLimit()));
                }
                path.Attributes().Set("ranges", complementaryRanges);
                path.Attributes().Remove("lower_limit");
                path.Attributes().Remove("upper_limit");
            } else {
                path.Attributes().Set("ranges", std::vector<TReadRange>());
            }
        }

        // ...and the output table must be cleared (regardless of requested "append" attribute).
        {
            auto& table = OutputTables[0];
            table.UpdateMode = EUpdateMode::Overwrite;
            table.LockMode = ELockMode::Exclusive;
            table.AppendRequested = false;
        }
    }

    virtual void CustomPrepare() override
    {
        TOrderedMergeControllerBase::CustomPrepare();

        // If the input is sorted then the output chunk tree must also be marked as sorted.
        const auto& inputTable = InputTables[0];
        auto& outputTable = OutputTables[0];
        if (!inputTable.KeyColumns.empty()) {
            outputTable.KeyColumns = inputTable.KeyColumns;
        }
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(static_cast<int>(EJobType::OrderedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        // If the input is sorted then the output must also be sorted.
        // To produce sorted output a job needs key columns.
        const auto& table = InputTables[0];
        if (!table.KeyColumns.empty()) {
            ToProto(jobSpecExt->mutable_key_columns(), table.KeyColumns);
        }
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

////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEndpointType,
    (Left)
    (Right)
);

//! Handles sorted merge and reduce operations.
class TSortedMergeControllerBase
    : public TMergeControllerBase
{
public:
    TSortedMergeControllerBase(
        TSchedulerConfigPtr config,
        TSimpleOperationSpecBasePtr spec,
        TSortedMergeOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, options, host, operation)
    { }

    // Persistence.
    virtual void Persist(TPersistenceContext& context) override
    {
        TMergeControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, Endpoints);
        Persist(context, KeyColumns);
        Persist(context, ManiacJobSpecTemplate);
    }

protected:
    class TManiacTask
        : public TMergeTask
    {
    public:
        //! For persistence only.
        TManiacTask()
            : Controller(nullptr)
        { }

        TManiacTask(
            TSortedMergeControllerBase* controller,
            int taskIndex,
            int partitionIndex)
            : TMergeTask(controller, taskIndex, partitionIndex)
            , Controller(controller)
        { }

        virtual void Persist(TPersistenceContext& context) override
        {
            TMergeTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TManiacTask, 0xb3ed19a2);

        TSortedMergeControllerBase* Controller;

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->ManiacJobSpecTemplate);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

    };

    struct TKeyEndpoint
    {
        EEndpointType Type;
        TChunkSlicePtr ChunkSlice;
        TOwningKey MinBoundaryKey;
        TOwningKey MaxBoundaryKey;
        bool IsTeleport;

        void Persist(TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Type);
            Persist(context, ChunkSlice);
            Persist(context, MinBoundaryKey);
            Persist(context, MaxBoundaryKey);
            Persist(context, IsTeleport);
        }

        const TOwningKey& GetKey() const
        {
            return Type == EEndpointType::Left
                ? MinBoundaryKey
                : MaxBoundaryKey;
        }
    };

    std::vector<TKeyEndpoint> Endpoints;

    //! The actual (adjusted) key columns.
    std::vector<Stroka> KeyColumns;

    TChunkSlicesFetcherPtr ChunkSlicesFetcher;

    TJobSpec ManiacJobSpecTemplate;


    virtual TKeyColumns GetSpecKeyColumns() = 0;

    virtual bool IsTeleportChunk(const TChunkSpec& chunkSpec) const override
    {
        YUNREACHABLE();
    }

    virtual bool IsSingleStripeInput() const override
    {
        return false;
    }

    virtual void CustomPrepare() override
    {
        // NB: Base member is not called intentionally.

        auto specKeyColumns = GetSpecKeyColumns();
        LOG_INFO("Spec key columns are %v",
            !specKeyColumns.empty() ? ConvertToYsonString(specKeyColumns, EYsonFormat::Text).Data() : "<Null>");

        KeyColumns = CheckInputTablesSorted(specKeyColumns);
        LOG_INFO("Adjusted key columns are %v",
            ConvertToYsonString(KeyColumns, EYsonFormat::Text).Data());

        bool sliceByKeys = true;

        CalculateSizes();

        TScrapeChunksCallback scraperCallback;
        if (Spec->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
            scraperCallback = CreateScrapeChunksSessionCallback(
                Config,
                Host->GetBackgroundInvoker(),
                Host->GetChunkLocationThrottlerManager(),
                AuthenticatedInputMasterClient,
                InputNodeDirectory,
                Logger);
        }

        ChunkSlicesFetcher = New<TChunkSlicesFetcher>(
            Config->Fetcher,
            ChunkSliceSize,
            KeyColumns,
            sliceByKeys,
            InputNodeDirectory,
            Host->GetBackgroundInvoker(),
            scraperCallback,
            Logger);

        ProcessInputs();

        {
            auto result = WaitFor(ChunkSlicesFetcher->Fetch());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        CollectEndpoints();

        LOG_INFO("Sorting %v endpoints", static_cast<int>(Endpoints.size()));
        SortEndpoints();

        FindTeleportChunks();
        BuildTasks();

        FinishPreparation();
    }

    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        ChunkSlicesFetcher->AddChunk(chunkSpec);
    }

    virtual void SortEndpoints() = 0;
    virtual void FindTeleportChunks() = 0;
    virtual void BuildTasks() = 0;

    void CollectEndpoints()
    {
        const auto& slices = ChunkSlicesFetcher->GetChunkSlices();
        for (const auto& slice : slices) {
            TKeyEndpoint leftEndpoint;
            leftEndpoint.Type = EEndpointType::Left;
            leftEndpoint.ChunkSlice = slice;
            YCHECK(slice->LowerLimit().HasKey());
            leftEndpoint.MinBoundaryKey = slice->LowerLimit().GetKey();
            YCHECK(slice->UpperLimit().HasKey());
            leftEndpoint.MaxBoundaryKey = slice->UpperLimit().GetKey();

            try {
                ValidateKey(leftEndpoint.MinBoundaryKey);
                ValidateKey(leftEndpoint.MaxBoundaryKey);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION(
                    "Error validating sample key in input table %v",
                    GetInputTablePaths()[slice->GetChunkSpec()->table_index()])
                    << ex;
            }

            leftEndpoint.IsTeleport = false;
            Endpoints.push_back(leftEndpoint);

            TKeyEndpoint rightEndpoint = leftEndpoint;
            rightEndpoint.Type = EEndpointType::Right;
            Endpoints.push_back(rightEndpoint);
        }
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeControllerBase::TManiacTask);

////////////////////////////////////////////////////////////////////

class TSortedMergeController
    : public TSortedMergeControllerBase
{
public:
    TSortedMergeController(
        TSchedulerConfigPtr config,
        TSortedMergeOperationSpecPtr spec,
        TSortedMergeOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation)
        : TSortedMergeControllerBase(config, spec, options, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController, 0xbc6daa18);

    TSortedMergeOperationSpecPtr Spec;
    TSortedMergeOperationOptionsPtr Options;

    bool IsLargeEnoughToTeleport(const TChunkSpec& chunkSpec)
    {
        if (!Spec->CombineChunks)
            return true;

        return IsLargeCompleteChunk(chunkSpec, Spec->JobIO->TableWriter->DesiredChunkSize);
    }

    virtual void SortEndpoints() override
    {
        int prefixLength = static_cast<int>(KeyColumns.size());
        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                int cmpResult = CompareRows(lhs.GetKey(), rhs.GetKey(), prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                cmpResult = CompareRows(lhs.MinBoundaryKey, rhs.MinBoundaryKey, prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                cmpResult = CompareRows(lhs.MaxBoundaryKey, rhs.MaxBoundaryKey, prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                // ChunkSpec address is used to identify the slices of one chunk.
                auto cmpPtr = reinterpret_cast<intptr_t>(lhs.ChunkSlice->GetChunkSpec().Get())
                    - reinterpret_cast<intptr_t>(rhs.ChunkSlice->GetChunkSpec().Get());
                if (cmpPtr != 0) {
                    return cmpPtr < 0;
                }

                return lhs.Type < rhs.Type;
            });
    }

    virtual void FindTeleportChunks() override
    {
        if (Spec->ForceTransform) {
            return;
        }

        int openedSlicesCount = 0;
        TRefCountedChunkSpecPtr currentChunkSpec = nullptr;
        int startTeleportIndex = -1;
        for (int i = 0; i < Endpoints.size(); ++i) {
            auto& endpoint = Endpoints[i];
            auto& chunkSlice = endpoint.ChunkSlice;

            openedSlicesCount += endpoint.Type == EEndpointType::Left ? 1 : -1;

            TOwningKey minKey, maxKey;
            YCHECK(TryGetBoundaryKeys(chunkSlice->GetChunkSpec()->chunk_meta(), &minKey, &maxKey));

            if (currentChunkSpec) {
                if (chunkSlice->GetChunkSpec() == currentChunkSpec) {
                    if (endpoint.Type == EEndpointType::Right && CompareRows(maxKey, endpoint.MaxBoundaryKey, KeyColumns.size()) == 0) {
                        // The last slice of a full chunk.
                        currentChunkSpec = nullptr;
                        auto completeChunk = chunkSlice->GetChunkSpec();

                        bool isManiacTeleport = CompareRows(
                            Endpoints[startTeleportIndex].GetKey(),
                            endpoint.GetKey(),
                            KeyColumns.size()) == 0;

                        if (IsLargeEnoughToTeleport(*completeChunk) &&
                            (openedSlicesCount == 0 || isManiacTeleport)) {
                            for (int j = startTeleportIndex; j <= i; ++j) {
                                Endpoints[j].IsTeleport = true;
                            }
                        }
                    }

                    continue;
                } else {
                    currentChunkSpec = nullptr;
                }
            }

            // No current Teleport candidate.
            if (endpoint.Type == EEndpointType::Left && CompareRows(minKey, endpoint.MinBoundaryKey, KeyColumns.size()) == 0) {
                // The first slice of a full chunk.
                currentChunkSpec = chunkSlice->GetChunkSpec();
                startTeleportIndex = i;
            }
        }
    }

    virtual void BuildTasks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        yhash_set<TChunkSlicePtr> globalOpenedSlices;
        TNullable<TOwningKey> lastBreakpoint = Null;

        int startIndex = 0;
        while (startIndex < Endpoints.size()) {
            auto& key = Endpoints[startIndex].GetKey();

            yhash_set<TRefCountedChunkSpecPtr> teleportChunks;
            yhash_set<TChunkSlicePtr> localOpenedSlices;

            // Slices with equal left and right boundaries.
            std::vector<TChunkSlicePtr> maniacs;

            int currentIndex = startIndex;
            while (currentIndex < Endpoints.size()) {
                // Iterate over endpoints with equal keys.
                auto& endpoint = Endpoints[currentIndex];
                auto& currentKey = endpoint.GetKey();

                if (CompareRows(key, currentKey, prefixLength) != 0) {
                    // This key is over.
                    break;
                }

                if (endpoint.IsTeleport) {
                    auto chunkSpec = endpoint.ChunkSlice->GetChunkSpec();
                    YCHECK(teleportChunks.insert(chunkSpec).second);
                    while (currentIndex < Endpoints.size() &&
                        Endpoints[currentIndex].IsTeleport &&
                        Endpoints[currentIndex].ChunkSlice->GetChunkSpec() == chunkSpec)
                    {
                        ++currentIndex;
                    }
                    continue;
                }

                if (endpoint.Type == EEndpointType::Left) {
                    YCHECK(localOpenedSlices.insert(endpoint.ChunkSlice).second);
                    ++currentIndex;
                    continue;
                }

                // Right non-Teleport endpoint.
                {
                    auto it = globalOpenedSlices.find(endpoint.ChunkSlice);
                    if (it != globalOpenedSlices.end()) {
                        AddPendingChunk(CreateChunkSlice((*it)->GetChunkSpec(), lastBreakpoint));
                        globalOpenedSlices.erase(it);
                        ++currentIndex;
                        continue;
                    }
                }
                {
                    auto it = localOpenedSlices.find(endpoint.ChunkSlice);
                    YCHECK(it != localOpenedSlices.end());
                    maniacs.push_back(*it);
                    localOpenedSlices.erase(it);
                    ++currentIndex;
                    continue;
                }

                YUNREACHABLE();
            }

            globalOpenedSlices.insert(localOpenedSlices.begin(), localOpenedSlices.end());

            auto endTask = [&] () {
                if (lastBreakpoint && CompareRows(key, *lastBreakpoint) == 0) {
                    // Already flushed at this key.
                    return;
                }

                auto nextBreakpoint = GetKeyPrefixSuccessor(key.Get(), prefixLength);
                LOG_TRACE("Finish current task, flushing %v chunks at key %v",
                    globalOpenedSlices.size(),
                    nextBreakpoint);

                for (const auto& chunkSlice : globalOpenedSlices) {
                    this->AddPendingChunk(CreateChunkSlice(
                        chunkSlice->GetChunkSpec(),
                        lastBreakpoint,
                        nextBreakpoint));
                }
                lastBreakpoint = nextBreakpoint;

                EndTask();
            };

            while (!HasLargeActiveTask() && !maniacs.empty()) {
                AddPendingChunk(maniacs.back());
                maniacs.pop_back();
            }

            if (!maniacs.empty()) {
                endTask();

                for (auto& chunkSlice : maniacs) {
                    AddPendingChunk(chunkSlice);
                    if (HasLargeActiveTask()) {
                        EndManiacTask();
                    }
                }
                EndManiacTask();
            }

            if (!teleportChunks.empty()) {
                endTask();

                for (auto& chunkSpec : teleportChunks) {
                    AddTeleportChunk(chunkSpec);
                }
            }

            if (HasLargeActiveTask()) {
                endTask();
            }

            startIndex = currentIndex;
        }

        YCHECK(globalOpenedSlices.empty());
        if (HasActiveTask()) {
            EndTask();
        }
    }

    void EndManiacTask()
    {
        if (!HasActiveTask())
            return;

        auto task = New<TManiacTask>(
            this,
            static_cast<int>(Tasks.size()),
            CurrentPartitionIndex);
        task->Initialize();

        EndTask(task);
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual void DoInitialize() override
    {
        TSortedMergeControllerBase::DoInitialize();

        auto& table = OutputTables[0];
        table.UpdateMode = EUpdateMode::Overwrite;
        table.LockMode = ELockMode::Exclusive;
    }

    virtual TKeyColumns GetSpecKeyColumns() override
    {
        return Spec->MergeBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(static_cast<int>(EJobType::SortedMerge));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        auto* mergeJobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ToProto(mergeJobSpecExt->mutable_key_columns(), KeyColumns);

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
        ManiacJobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
    }

    virtual void CustomPrepare() override
    {
        TSortedMergeControllerBase::CustomPrepare();

        OutputTables[0].KeyColumns = KeyColumns;
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController);

////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = operation->GetSpec();
    auto baseSpec = ParseOperationSpec<TMergeOperationSpec>(spec);
    switch (baseSpec->Mode) {
        case EMergeMode::Unordered: {
            return CreateUnorderedMergeController(config, host, operation);
        }
        case EMergeMode::Ordered: {
            return New<TOrderedMergeController>(
                config,
                ParseOperationSpec<TOrderedMergeOperationSpec>(spec),
                config->OrderedMergeOperationOptions,
                host,
                operation);
        }
        case EMergeMode::Sorted: {
            return New<TSortedMergeController>(
                config,
                ParseOperationSpec<TSortedMergeOperationSpec>(spec),
                config->SortedMergeOperationOptions,
                host,
                operation);
        }
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////

class TReduceController
    : public TSortedMergeControllerBase
{
public:
    TReduceController(
        TSchedulerConfigPtr config,
        TReduceOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TSortedMergeControllerBase(config, spec, config->ReduceOperationOptions, host, operation)
        , Spec(spec)
        , StartRowIndex(0)
        , TeleportOutputTable(Null)
    { }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TSortedMergeControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .Item("reducer").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Reducer->Command))
            .EndMap();
    }

    // Persistence.
    virtual void Persist(TPersistenceContext& context) override
    {
        TSortedMergeControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, StartRowIndex);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TReduceController, 0xacd16dbc);

    TReduceOperationSpecPtr Spec;

    i64 StartRowIndex;
    TNullable<int> TeleportOutputTable;


    virtual void DoInitialize() override
    {
        TSortedMergeControllerBase::DoInitialize();

        if (Spec->Reducer && Spec->Reducer->FilePaths.size() > Config->MaxUserFileCount) {
            THROW_ERROR_EXCEPTION("Too many user files in reducer: maximum allowed %v, actual %v",
                Config->MaxUserFileCount,
                Spec->Reducer->FilePaths.size());
        }
    }

    virtual bool IsRowCountPreserved() const override
    {
        return false;
    }

    bool IsTeleportInputTable(int tableIndex) const
    {
        return InputTables[tableIndex].Path.Attributes().Get<bool>("teleport", false);
    }

    virtual void SortEndpoints() override
    {
        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                int cmpResult = CompareRows(lhs.GetKey(), rhs.GetKey());
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                if (lhs.Type < rhs.Type) {
                    return true;
                } else {
                    return (reinterpret_cast<intptr_t>(lhs.ChunkSlice->GetChunkSpec().Get())
                        - reinterpret_cast<intptr_t>(rhs.ChunkSlice->GetChunkSpec().Get())) < 0;
                }
            });
    }

    virtual void FindTeleportChunks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        {
            int teleportOutputCount = 0;
            for (int i = 0; i < OutputTables.size(); ++i) {
                if (OutputTables[i].Path.Attributes().Get<bool>("teleport", false)) {
                    ++teleportOutputCount;
                    TeleportOutputTable = i;
                }
            }

            if (teleportOutputCount > 1) {
                THROW_ERROR_EXCEPTION("Too many teleport output tables: maximum allowed 1, actual %v", teleportOutputCount);
            }
        }

        TRefCountedChunkSpecPtr currentChunkSpec = nullptr;
        int startTeleportIndex = -1;

        int openedSlicesCount = 0;
        auto previousKey = EmptyKey();

        for (int i = 0; i < Endpoints.size(); ++i) {
            auto& endpoint = Endpoints[i];
            auto& key = endpoint.GetKey();

            openedSlicesCount += endpoint.Type == EEndpointType::Left ? 1 : -1;

            if (currentChunkSpec &&
                endpoint.ChunkSlice->GetChunkSpec() == currentChunkSpec)
            {
                previousKey = key;
                continue;
            }

            if (CompareRows(key, previousKey, prefixLength) == 0) {
                currentChunkSpec = nullptr;
                // Don't update previous key - it's equal to current.
                continue;
            }

            if (currentChunkSpec) {
                auto& previousEndpoint = Endpoints[i - 1];
                const auto& chunkSpec = previousEndpoint.ChunkSlice->GetChunkSpec();

                TOwningKey maxKey, minKey;
                YCHECK(TryGetBoundaryKeys(chunkSpec->chunk_meta(), &minKey, &maxKey));
                if (previousEndpoint.Type == EEndpointType::Right
                    && CompareRows(maxKey, previousEndpoint.GetKey(), prefixLength) == 0)
                {
                    for (int j = startTeleportIndex; j < i; ++j) {
                        Endpoints[j].IsTeleport = true;
                    }
                }
            }

            currentChunkSpec = nullptr;
            previousKey = key;

            // No current Teleport candidate.
            const auto& chunkSpec = endpoint.ChunkSlice->GetChunkSpec();
            TOwningKey maxKey, minKey;
            YCHECK(TryGetBoundaryKeys(chunkSpec->chunk_meta(), &minKey, &maxKey));
            if (endpoint.Type == EEndpointType::Left &&
                CompareRows(minKey, endpoint.GetKey(), prefixLength) == 0 &&
                IsTeleportInputTable(chunkSpec->table_index()) &&
                openedSlicesCount == 1)
            {
                currentChunkSpec = endpoint.ChunkSlice->GetChunkSpec();
                startTeleportIndex = i;
            }
        }

        if (currentChunkSpec) {
            // Last Teleport candidate.
            auto& previousEndpoint = Endpoints.back();
            const auto& chunkSpec = previousEndpoint.ChunkSlice->GetChunkSpec();
            YCHECK(previousEndpoint.Type == EEndpointType::Right);
            TOwningKey maxKey, minKey;
            YCHECK(TryGetBoundaryKeys(chunkSpec->chunk_meta(), &minKey, &maxKey));
            if (CompareRows(maxKey, previousEndpoint.GetKey(), prefixLength) == 0) {
                for (int j = startTeleportIndex; j < Endpoints.size(); ++j) {
                    Endpoints[j].IsTeleport = true;
                }
            }
        }
    }

    virtual void BuildTasks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        yhash_set<TChunkSlicePtr> openedSlices;
        TNullable<TOwningKey> lastBreakpoint = Null;

        int startIndex = 0;
        while (startIndex < Endpoints.size()) {
            auto& key = Endpoints[startIndex].GetKey();

            int currentIndex = startIndex;
            while (currentIndex < Endpoints.size()) {
                // Iterate over endpoints with equal keys.
                auto& endpoint = Endpoints[currentIndex];
                auto& currentKey = endpoint.GetKey();

                if (CompareRows(key, currentKey, prefixLength) != 0) {
                    // This key is over.
                    break;
                }

                if (endpoint.IsTeleport) {
                    YCHECK(openedSlices.empty());
                    EndTask();

                    auto chunkSpec = endpoint.ChunkSlice->GetChunkSpec();
                    AddTeleportChunk(chunkSpec);

                    while (currentIndex < Endpoints.size() &&
                        Endpoints[currentIndex].IsTeleport &&
                        Endpoints[currentIndex].ChunkSlice->GetChunkSpec() == chunkSpec)
                    {
                        ++currentIndex;
                    }
                    continue;
                }

                if (endpoint.Type == EEndpointType::Left) {
                    YCHECK(openedSlices.insert(endpoint.ChunkSlice).second);
                    ++currentIndex;
                    continue;
                }

                // Right non-Teleport endpoint.
                YCHECK(endpoint.Type == EEndpointType::Right);

                auto it = openedSlices.find(endpoint.ChunkSlice);
                YCHECK(it != openedSlices.end());
                AddPendingChunk(CreateChunkSlice((*it)->GetChunkSpec(), lastBreakpoint));
                openedSlices.erase(it);
                ++currentIndex;
            }

            if (HasLargeActiveTask()) {
                YCHECK(!lastBreakpoint || CompareRows(key, *lastBreakpoint, prefixLength) != 0);

                auto nextBreakpoint = GetKeyPrefixSuccessor(key.Get(), prefixLength);
                LOG_TRACE("Finish current task, flushing %v chunks at key %v",
                    openedSlices.size(),
                    nextBreakpoint);

                for (const auto& chunkSlice : openedSlices) {
                    this->AddPendingChunk(CreateChunkSlice(
                        chunkSlice->GetChunkSpec(),
                        lastBreakpoint,
                        nextBreakpoint));
                }
                lastBreakpoint = nextBreakpoint;

                EndTask();
            }

            startIndex = currentIndex;
        }

        YCHECK(openedSlices.empty());
        if (HasActiveTask()) {
            EndTask();
        }
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual TNullable<int> GetTeleportTableIndex() const override
    {
        return TeleportOutputTable;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        for (const auto& path : Spec->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    virtual i32 GetCpuLimit() const override
    {
        return Spec->Reducer->CpuLimit;
    }

    virtual i64 GetAdditionalMemorySize(bool memoryReserveEnabled) const override
    {
        return GetMemoryReserve(memoryReserveEnabled, Spec->Reducer);
    }

    virtual TKeyColumns GetSpecKeyColumns() override
    {
        return Spec->ReduceBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        YCHECK(!KeyColumns.empty());

        JobSpecTemplate.set_type(static_cast<int>(EJobType::SortedReduce));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec->Reducer,
            Files);

        auto* reduceJobSpecExt = JobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        ToProto(reduceJobSpecExt->mutable_key_columns(), GetSortingKeyColumns());
        reduceJobSpecExt->set_reduce_key_column_count(KeyColumns.size());

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet,
            GetAdditionalMemorySize(joblet->MemoryReserveEnabled));
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        for (const auto& inputTable : InputTables) {
            if (inputTable.Path.Attributes().Get<bool>("teleport", false)) {
                return false;
            }
        }
        return true;
    }

    TKeyColumns GetSortingKeyColumns()
    {
        auto sortBy = InputTables[0].KeyColumns;
        for (const auto& table : InputTables) {
            if (table.KeyColumns.size() < sortBy.size()) {
                sortBy.erase(sortBy.begin() + table.KeyColumns.size(), sortBy.end());
            }

            int i = 0;
            while (i < sortBy.size() && sortBy[i] == table.KeyColumns[i]) {
              	++i;
            }
            sortBy.erase(sortBy.begin() + i, sortBy.end());
        }
        YCHECK(sortBy.size() >= KeyColumns.size());
        return sortBy;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceController);

IOperationControllerPtr CreateReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TReduceOperationSpec>(operation->GetSpec());
    return New<TReduceController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

