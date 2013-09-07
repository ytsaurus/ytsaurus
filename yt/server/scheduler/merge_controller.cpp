#include "stdafx.h"
#include "merge_controller.h"
#include "private.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"
#include "chunk_splits_fetcher.h"
#include "chunk_info_collector.h"

#include <ytlib/concurrency/fiber.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

static auto& Logger = OperationLogger;
static NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////

class TMergeControllerBase
    : public TOperationControllerBase
{
public:
    TMergeControllerBase(
        TSchedulerConfigPtr config,
        TMergeOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, host, operation)
        , Spec(spec)
        , TotalChunkCount(0)
        , TotalDataSize(0)
        , CurrentTaskDataSize(0)
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
    TMergeOperationSpecBasePtr Spec;

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

    //! The number of output partitions generated so far.
    //! Not serialized.
    /*!
     *  Each partition either corresponds to a merge task or to a pass-through chunk.
     *  Partition index is used as a key when calling #TOperationControllerBase::RegisterOutputChunkTree.
     *
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

        explicit TMergeTask(
            TMergeControllerBase* controller,
            int taskIndex,
            int partitionIndex = -1)
            : TTask(controller)
            , Controller(controller)
            , TaskIndex(taskIndex)
            , PartitionIndex(partitionIndex)
        {
            ChunkPool = CreateAtomicChunkPool(Controller->NodeDirectory);
        }

        virtual Stroka GetId() const override
        {
            return
                PartitionIndex < 0
                ? Sprintf("Merge(%d)", TaskIndex)
                : Sprintf("Merge(%d,%d)", TaskIndex, PartitionIndex);
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
                Controller->GetAdditionalMemorySize());
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
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
            AddParallelInputSpec(jobSpec, joblet, Controller->IsTableIndexEnabled());
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


        virtual TNodeResources GetMinNeededResourcesHeavy() const override
        {
            TNodeResources result;

            result.set_user_slots(1);
            result.set_cpu(1);
            result.set_memory(
                Controller->GetFinalIOMemorySize(
                Controller->Spec->JobIO,
                UpdateChunkStripeStatistics(ChunkPool->GetApproximateStripeStatistics())) +
                GetFootprintMemorySize() +
                Controller->GetAdditionalMemorySize());
            return result;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->OutputTables.size();
        }

        TChunkStripeStatisticsVector UpdateChunkStripeStatistics(
            const TChunkStripeStatisticsVector& statistics) const
        {
            if (Controller->JobSpecTemplate.type() == EJobType::SortedMerge ||
                Controller->JobSpecTemplate.type() == EJobType::SortedReduce)
            {
                return statistics;
            } else {
                return AggregateStatistics(statistics);
            }
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

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            RegisterOutput(joblet, PartitionIndex);
        }

    };

    typedef TIntrusivePtr<TMergeTask> TMergeTaskPtr;

    TTaskGroupPtr MergeTaskGroup;

    //! Resizes #CurrentTaskStripes appropriately and sets all its entries to |NULL|.
    void ClearCurrentTaskStripes()
    {
        CurrentTaskStripes.clear();
        CurrentTaskStripes.resize(InputTables.size());
    }

    void EndTask(TMergeTaskPtr task)
    {
        YCHECK(HasActiveTask());

        task->AddInput(CurrentTaskStripes);
        task->FinishInput();
        RegisterTask(task);

        ++CurrentPartitionIndex;

        LOG_DEBUG("Task finished (Id: %s, TaskDataSize: %" PRId64 ")",
            ~task->GetId(),
            CurrentTaskDataSize);

        CurrentTaskDataSize = 0;
        ClearCurrentTaskStripes();
    }

    //! Finishes the current task.
    void EndTask()
    {
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

    //! Returns True if the total data size of currently queued stripes exceeds the pre-configured limit.
    bool HasLargeActiveTask()
    {
        YCHECK(MaxDataSizePerJob > 0);
        return CurrentTaskDataSize >= MaxDataSizePerJob;
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
        stripe->ChunkSlices.push_back(chunkSlice);
    }

    //! Add chunk directly to the output.
    void AddPassthroughChunk(TRefCountedChunkSpecPtr chunkSpec)
    {
        auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
        LOG_DEBUG("Passthrough chunk added (ChunkId: %s, Partition: %d)",
            ~ToString(chunkId),
            CurrentPartitionIndex);

        // Place the chunk directly to the output table.
        RegisterOutput(chunkId, CurrentPartitionIndex, 0);
        ++CurrentPartitionIndex;
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
        ProcessInputs();
        EndInputChunks();
        FinishPreparation();
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            ClearCurrentTaskStripes();

            auto jobCount = SuggestJobCount(
                TotalInputDataSize,
                Spec->DataSizePerJob,
                Spec->JobCount);

            MaxDataSizePerJob = 1 + TotalInputDataSize / jobCount;
            ChunkSliceSize = std::min(Config->MergeJobMaxSliceDataSize, MaxDataSizePerJob);

            FOREACH (auto chunk, CollectInputChunks()) {
                ProcessInputChunk(chunk);
            }
        }
    }

    void FinishPreparation()
    {
        InitJobIOConfig();
        InitJobSpecTemplate();

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %d, JobCount: %" PRISZT ")",
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

    virtual Stroka GetLoggingProgress() override
    {
        return Sprintf(
            "Jobs = {T: %" PRId64 ", R: %" PRId64 ", C: %" PRId64 ", P: %d, F: %" PRId64 ", A: %" PRId64 "}, "
            "UnavailableInputChunks: %d",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            UnavailableInputChunkCount);
    }


    // Unsorted helpers.

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(const TChunkSpec& chunkSpec)
    {
        return IsStartingSlice(chunkSpec) && IsEndingSlice(chunkSpec);
    }

    static bool IsStartingSlice(const TChunkSpec& chunkSpec)
    {
        return !chunkSpec.start_limit().has_key() &&
               !chunkSpec.start_limit().has_row_index();
    }

    static bool IsEndingSlice(const TChunkSpec& chunkSpec)
    {
        return !chunkSpec.end_limit().has_key() &&
               !chunkSpec.end_limit().has_row_index();
    }

    //! Returns True if the chunk can be included into the output as-is.
    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) = 0;

    virtual i64 GetAdditionalMemorySize() const
    {
        return 0;
    }

    //! Returns True iff the chunk is complete and is large enough.
    bool IsLargeCompleteChunk(const TChunkSpec& chunkSpec)
    {
        if (!IsCompleteChunk(chunkSpec)) {
            return false;
        }

        return IsLargeChunk(chunkSpec);
    }

    bool IsLargeChunk(const TChunkSpec& chunkSpec)
    {
        i64 chunkDataSize;
        NChunkClient::GetStatistics(chunkSpec, &chunkDataSize);

        // ChunkSequenceWriter may actually produce a chunk a bit smaller than DesiredChunkSize,
        // so we have to be more flexible here.
        if (0.9 * chunkDataSize >= Spec->JobIO->TableWriter->DesiredChunkSize) {
            return true;
        }

        return false;
    }

    //! A typical implementation of #IsPassthroughChunk that depends on whether chunks must be combined or not.
    bool IsPassthroughChunkImpl(const TChunkSpec& chunkSpec, bool combineChunks)
    {
        return combineChunks ? IsLargeCompleteChunk(chunkSpec) : IsCompleteChunk(chunkSpec);
    }

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
        InitFinalOutputConfig(JobIOConfig);
    }

    //! Initializes #JobSpecTemplate.
    virtual void InitJobSpecTemplate() = 0;

    virtual bool IsTableIndexEnabled() const
    {
        return false;
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeControllerBase::TMergeTask);

////////////////////////////////////////////////////////////////////

//! Handles unordered merge operation.
class TUnorderedMergeController
    : public TMergeControllerBase
{
public:
    TUnorderedMergeController(
        TSchedulerConfigPtr config,
        TUnorderedMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController, 0x6acdae46);

    TUnorderedMergeOperationSpecPtr Spec;


    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        if (Spec->ForceTransform)
            return false;

        return IsPassthroughChunkImpl(chunkSpec, Spec->CombineChunks);
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

    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        if (IsPassthroughChunk(*chunkSpec)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(chunkSpec);
            return;
        }

        // NB: During unordered merge all chunks go to a single chunk stripe.
        FOREACH(const auto& slice, CreateChunkSlice(chunkSpec)->SliceEvenly(ChunkSliceSize)) {
            AddPendingChunk(slice);
            EndTaskIfLarge();
        }
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::UnorderedMerge);
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController);

////////////////////////////////////////////////////////////////////

//! Handles ordered merge and (sic!) erase operations.
class TOrderedMergeControllerBase
    : public TMergeControllerBase
{
public:
    TOrderedMergeControllerBase(
        TSchedulerConfigPtr config,
        TMergeOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
    { }

private:
    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        if (IsPassthroughChunk(*chunkSpec)) {
            // Merge is not needed. Copy the chunk directly to the output.
            if (HasActiveTask()) {
                EndTask();
            }
            AddPassthroughChunk(chunkSpec);
            return;
        }

        // NB: During ordered merge all chunks go to a single chunk stripe.
        FOREACH(const auto& slice, CreateChunkSlice(chunkSpec)->SliceEvenly(ChunkSliceSize)) {
            AddPendingChunk(slice);
            EndTaskIfLarge();
        }
    }

};

////////////////////////////////////////////////////////////////////

class TOrderedMergeController
    : public TOrderedMergeControllerBase
{
public:
    TOrderedMergeController(
        TSchedulerConfigPtr config,
        TOrderedMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedMergeControllerBase(config, spec, host, operation)
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

    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        if (Spec->ForceTransform)
            return false;

        return IsPassthroughChunkImpl(chunkSpec, Spec->CombineChunks);
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::OrderedMerge);
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());
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
        : TOrderedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TEraseController, 0x1cc6ba39);

    TEraseOperationSpecPtr Spec;


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

    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        return IsPassthroughChunkImpl(chunkSpec, Spec->CombineChunks);
    }

    virtual void DoInitialize() override
    {
        TOrderedMergeControllerBase::DoInitialize();

        // For erase operation the rowset specified by the user must actually be negated.
        {
            auto& table = InputTables[0];
            table.ComplementFetch = true;
        }
        // ...and the output table must be cleared (regardless of "overwrite" attribute).
        {
            auto& table = OutputTables[0];
            table.Clear = true;
            table.Overwrite = true;
            table.LockMode = ELockMode::Exclusive;
        }
    }

    virtual void CustomPrepare() override
    {
        TOrderedMergeControllerBase::CustomPrepare();

        // If the input is sorted then the output chunk tree must also be marked as sorted.
        const auto& inputTable = InputTables[0];
        auto& outputTable = OutputTables[0];
        if (inputTable.KeyColumns) {
            outputTable.Options->KeyColumns = inputTable.KeyColumns;
        }
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::OrderedMerge);
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        // If the input is sorted then the output must also be sorted.
        // For this, the job needs key columns.
        const auto& table = InputTables[0];
        if (table.KeyColumns) {
            ToProto(jobSpecExt->mutable_key_columns(), *table.KeyColumns);
        }
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TEraseController);

IOperationControllerPtr CreateEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TEraseOperationSpec>(
        operation,
        config->EraseOperationSpec);
    return New<TEraseController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

//! Handles sorted merge and reduce operations.
class TSortedMergeControllerBase
    : public TMergeControllerBase
{
public:
    TSortedMergeControllerBase(
        TSchedulerConfigPtr config,
        TMergeOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
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

    DECLARE_ENUM(EEndpointType,
        (Left)
        (Maniac)
        (Right)
    );

    struct TKeyEndpoint
    {
        EEndpointType Type;
        NChunkClient::NProto::TKey Key;
        TRefCountedChunkSpecPtr ChunkSpec;

        void Persist(TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Type);
            Persist(context, Key);
            Persist(context, ChunkSpec);
        }
    };

    std::vector<TKeyEndpoint> Endpoints;

    //! The actual (adjusted) key columns.
    std::vector<Stroka> KeyColumns;

    TChunkSplitsFetcherPtr ChunkSplitsFetcher;
    TChunkSplitsCollectorPtr ChunkSplitsCollector;

    TJobSpec ManiacJobSpecTemplate;


    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() = 0;

    virtual void CustomPrepare() override
    {
        // NB: Base member is not called intentionally.

        auto specKeyColumns = GetSpecKeyColumns();
        LOG_INFO("Spec key columns are %s",
            specKeyColumns ? ~ConvertToYsonString(*specKeyColumns, EYsonFormat::Text).Data() : "<Null>");

        KeyColumns = CheckInputTablesSorted(GetSpecKeyColumns());
        LOG_INFO("Adjusted key columns are %s",
            ~ConvertToYsonString(KeyColumns, EYsonFormat::Text).Data());

        ChunkSplitsFetcher = New<TChunkSplitsFetcher>(
            Config,
            Spec,
            Operation->GetOperationId(),
            KeyColumns);

        ChunkSplitsCollector = New<TChunkSplitsCollector>(
            NodeDirectory,
            ChunkSplitsFetcher,
            Host->GetBackgroundInvoker());

        ProcessInputs();

        {
            auto asyncCollectorResult = ChunkSplitsCollector->Run();
            auto collectorResult = WaitFor(asyncCollectorResult);
            THROW_ERROR_EXCEPTION_IF_FAILED(collectorResult);
        }

        ProcessChunkSplits();
        FinishPreparation();
    }

    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        ChunkSplitsCollector->AddChunk(chunkSpec);
    }

    virtual bool IsLargeEnoughToPassthrough(const TChunkSpec& chunkSpec) = 0;

    void ProcessChunkSplits()
    {
        int prefixLength = static_cast<int>(KeyColumns.size());
        const auto& chunks = ChunkSplitsFetcher->GetChunkSplits();
        FOREACH (const auto& chunk, chunks) {
            auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunk->extensions());
            if (CompareKeys(boundaryKeysExt.start(), boundaryKeysExt.end(), prefixLength) == 0) {
                // Maniac chunk
                TKeyEndpoint endpoint;
                endpoint.Type = EEndpointType::Maniac;
                endpoint.Key = boundaryKeysExt.start();
                endpoint.ChunkSpec = chunk;
                Endpoints.push_back(endpoint);
            } else {
                {
                    TKeyEndpoint endpoint;
                    endpoint.Type = EEndpointType::Left;
                    endpoint.Key = boundaryKeysExt.start();
                    endpoint.ChunkSpec = chunk;
                    Endpoints.push_back(endpoint);
                } {
                    TKeyEndpoint endpoint;
                    endpoint.Type = EEndpointType::Right;
                    endpoint.Key = boundaryKeysExt.end();
                    endpoint.ChunkSpec = chunk;
                    Endpoints.push_back(endpoint);
                }
            }
        }

        // Sort earlier collected endpoints to figure out overlapping chunks.
        // Sort endpoints by keys, in case of a tie left endpoints go first.
        LOG_INFO("Sorting %d endpoints", static_cast<int>(Endpoints.size()));

        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                auto keysResult = CompareKeys(lhs.Key, rhs.Key, prefixLength);
                if (keysResult != 0) {
                    return keysResult < 0;
                }
                return lhs.Type < rhs.Type;
            });

        BuildTasks();
    }

    void BuildTasks()
    {
        // Compute components consisting of overlapping chunks.
        // Combine small tasks, if requested so.
        LOG_INFO("Building tasks");
        yhash_set<TRefCountedChunkSpecPtr> openedChunks;

        int currentIndex = 0;
        TNullable<NChunkClient::NProto::TKey> lastBreakpoint;

        int endpointsCount = static_cast<int>(Endpoints.size());
        int prefixLength = static_cast<int>(KeyColumns.size());

        auto flushOpenedChunks = [&] () {
            const auto& endpoint = Endpoints[currentIndex];
            auto nextBreakpoint = GetKeyPrefixSuccessor(endpoint.Key, prefixLength);
            LOG_DEBUG("Finish current task, flushing %" PRISZT " chunks at key %s",
                openedChunks.size(),
                ~ToString(nextBreakpoint));

            FOREACH (const auto& chunkSpec, openedChunks) {
                this->AddPendingChunk(CreateChunkSlice(
                    chunkSpec,
                    lastBreakpoint,
                    nextBreakpoint));
            }
            lastBreakpoint = nextBreakpoint;
        };

        while (currentIndex < endpointsCount) {
            const auto& endpoint = Endpoints[currentIndex];

            switch (endpoint.Type) {
                case EEndpointType::Left:
                    if (openedChunks.empty() &&
                        IsStartingSlice(*endpoint.ChunkSpec) &&
                        AllowPassthroughChunks())
                    {
                        // Trying to reconstruct passthrough chunk from chunk slices.
                        auto chunkId = FromProto<TChunkId>(endpoint.ChunkSpec->chunk_id());
                        auto tableIndex = endpoint.ChunkSpec->table_index();
                        auto nextIndex = currentIndex + 1;
                        for (; nextIndex < endpointsCount; ++nextIndex) {
                            auto nextChunkId = FromProto<TChunkId>(Endpoints[nextIndex].ChunkSpec->chunk_id());
                            auto nextTableIndex = Endpoints[nextIndex].ChunkSpec->table_index();
                            if (nextChunkId != chunkId || tableIndex != nextTableIndex) {
                                break;
                            }
                        }

                        const auto& lastEndpoint = Endpoints[nextIndex - 1];
                        if (lastEndpoint.Type == EEndpointType::Right && IsEndingSlice(*lastEndpoint.ChunkSpec)) {
                            auto chunk = CreateCompleteChunk(endpoint.ChunkSpec);
                            if (IsLargeEnoughToPassthrough(*chunk)) {
                                if (HasActiveTask()) {
                                   EndTask();
                                }
                                AddPassthroughChunk(chunk);
                                currentIndex = nextIndex;
                                break;
                            }
                        }
                    }

                    YCHECK(openedChunks.insert(endpoint.ChunkSpec).second);
                    ++currentIndex;
                    break;

                case EEndpointType::Right:
                    AddPendingChunk(CreateChunkSlice(endpoint.ChunkSpec, lastBreakpoint, Null));
                    YCHECK(openedChunks.erase(endpoint.ChunkSpec) == 1);

                    if (!openedChunks.empty() &&
                        HasLargeActiveTask() &&
                        CompareKeys(endpoint.Key, Endpoints[currentIndex + 1].Key, prefixLength) < 0)
                    {
                        flushOpenedChunks();
                        EndTask();
                    }

                    if (openedChunks.empty()) {
                        EndTaskIfLarge();
                    }
                    ++currentIndex;
                    break;

                case EEndpointType::Maniac: {
                    auto nextIndex = currentIndex;
                    i64 partialManiacSize = 0;
                    i64 completeLargeManiacSize = 0;
                    std::vector<TRefCountedChunkSpecPtr> completeLargeChunks;
                    std::vector<TRefCountedChunkSpecPtr> partialChunks;
                    do {
                        const auto& nextEndpoint = Endpoints[nextIndex];

                        if (nextEndpoint.Type == EEndpointType::Maniac &&
                            CompareKeys(nextEndpoint.Key, endpoint.Key, prefixLength) == 0)
                        {
                            i64 dataSize;
                            GetStatistics(*nextEndpoint.ChunkSpec, &dataSize);
                            if (IsLargeCompleteChunk(*nextEndpoint.ChunkSpec)) {
                                completeLargeManiacSize += dataSize;
                                completeLargeChunks.push_back(nextEndpoint.ChunkSpec);
                            } else {
                                partialManiacSize += dataSize;
                                partialChunks.push_back(nextEndpoint.ChunkSpec);
                            }
                        } else {
                            break;
                        }
                        ++nextIndex;
                    } while (nextIndex != endpointsCount);


                    flushOpenedChunks();
                    if (AllowPassthroughChunks()) {
                        bool hasManiacTask = partialManiacSize > MaxDataSizePerJob;
                        bool hasPassthroughManiacs = completeLargeManiacSize > 0;

                        if (!hasManiacTask) {
                            FOREACH (const auto& chunk, partialChunks) {
                                AddPendingChunk(CreateChunkSlice(chunk));
                            }
                        }

                        if (hasManiacTask || hasPassthroughManiacs) {
                            if (HasActiveTask()) {
                               EndTask();
                            }
                        }

                        if (hasManiacTask) {
                            YCHECK(!HasActiveTask());
                            // Create special maniac task.
                            FOREACH (const auto& chunk, partialChunks) {
                                AddPendingChunk(CreateChunkSlice(chunk));
                                if (HasLargeActiveTask()) {
                                    EndManiacTask();
                                }
                            }

                            if (HasActiveTask()) {
                               EndManiacTask();
                            }
                        }

                        if (hasPassthroughManiacs) {
                            YCHECK(!HasActiveTask());
                            FOREACH (const auto& chunk, completeLargeChunks) {
                                // Add passthrough maniacs.
                                AddPassthroughChunk(chunk);
                            }
                        }
                    } else {
                        bool hasManiacTask = partialManiacSize + completeLargeManiacSize > MaxDataSizePerJob;
                        if (hasManiacTask) {
                            // Complete current task
                            if (HasActiveTask()) {
                               EndTask();
                            }
                        }

                        FOREACH (const auto& chunk, partialChunks) {
                            AddPendingChunk(CreateChunkSlice(chunk));
                        }

                        FOREACH (const auto& chunk, completeLargeChunks) {
                            AddPendingChunk(CreateChunkSlice(chunk));
                        }

                        if (hasManiacTask) {
                            EndManiacTask();
                        }
                    }

                    currentIndex = nextIndex;
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }

        if (HasActiveTask()) {
            EndTask();
        }
    }

    void EndManiacTask()
    {
        auto task = New<TManiacTask>(
            this,
            static_cast<int>(Tasks.size()),
            CurrentPartitionIndex);
        task->Initialize();

        EndTask(task);
    }

    virtual bool AllowPassthroughChunks() = 0;

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
        IOperationHost* host,
        TOperation* operation)
        : TSortedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController, 0xbc6daa18);

    TSortedMergeOperationSpecPtr Spec;


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

    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        if (Spec->ForceTransform)
            return false;

        return IsPassthroughChunkImpl(chunkSpec, Spec->CombineChunks);
    }

    virtual void DoInitialize() override
    {
        TSortedMergeControllerBase::DoInitialize();

        auto& table = OutputTables[0];
        table.Clear = true;
        table.LockMode = ELockMode::Exclusive;
    }

    virtual bool AllowPassthroughChunks() override
    {
        return !Spec->ForceTransform;
    }

    virtual bool IsLargeEnoughToPassthrough(const TChunkSpec& chunkSpec) override
    {
        if (!Spec->CombineChunks)
            return true;

        return IsLargeChunk(chunkSpec);
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->MergeBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedMerge);
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        auto* mergeJobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ToProto(mergeJobSpecExt->mutable_key_columns(), KeyColumns);

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
        ManiacJobSpecTemplate.set_type(EJobType::UnorderedMerge);
    }

    virtual void CustomPrepare() override
    {
        TSortedMergeControllerBase::CustomPrepare();

        OutputTables[0].Options->KeyColumns = KeyColumns;
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController);

////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto baseSpec = ParseOperationSpec<TMergeOperationSpec>(
        operation,
        NYTree::GetEphemeralNodeFactory()->CreateMap());

    switch (baseSpec->Mode) {
        case EMergeMode::Unordered: {
            auto spec = ParseOperationSpec<TUnorderedMergeOperationSpec>(
                operation,
                config->UnorderedMergeOperationSpec);
            return New<TUnorderedMergeController>(config, spec, host, operation);
        }
        case EMergeMode::Ordered: {
            auto spec = ParseOperationSpec<TOrderedMergeOperationSpec>(
                operation,
                config->OrderedMergeOperationSpec);
            return New<TOrderedMergeController>(config, spec, host, operation);
        }
        case EMergeMode::Sorted: {
            auto spec = ParseOperationSpec<TSortedMergeOperationSpec>(
                operation,
                config->SortedMergeOperationSpec);
            return New<TSortedMergeController>(config, spec, host, operation);
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
        : TSortedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
        , StartRowIndex(0)
    { }

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


    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        FOREACH (const auto& path, Spec->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        YUNREACHABLE();
    }

    virtual bool AllowPassthroughChunks() override
    {
        return false;
    }

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    virtual i64 GetAdditionalMemorySize() const override
    {
        return Spec->Reducer->MemoryLimit;
    }

    virtual bool IsLargeEnoughToPassthrough(const TChunkSpec& chunkSpec) override
    {
        UNUSED(chunkSpec);
        YUNREACHABLE();
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->ReduceBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedReduce);
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        auto* reduceJobSpecExt = JobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());
        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ToProto(reduceJobSpecExt->mutable_key_columns(), KeyColumns);

        InitUserJobSpec(
            reduceJobSpecExt->mutable_reducer_spec(),
            Spec->Reducer,
            RegularFiles,
            TableFiles);

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        auto* jobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        AddUserJobEnvironment(jobSpecExt->mutable_reducer_spec(), joblet);
    }

    virtual bool IsTableIndexEnabled() const override
    {
        return Spec->Reducer->EnableTableIndex;
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TReduceController);

IOperationControllerPtr CreateReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TReduceOperationSpec>(
        operation,
        config->ReduceOperationSpec);
    return New<TReduceController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

