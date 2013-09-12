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

#include <ytlib/fibers/fiber.h>

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

    //! Returns True if the total data size of currently queued stripes exceeds the pre-configured limit.
    bool HasLargeActiveTask()
    {
        YCHECK(MaxDataSizePerJob > 0);
        return CurrentTaskDataSize >= MaxDataSizePerJob;
    }

    //! Add chunk to the current task's pool.
    void AddPendingChunk(TChunkSlicePtr chunkSlice)
    {
        chunkSlice->GetChunkSpec()->clear_partition_tag();
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
        chunkSpec->clear_partition_tag();
        LOG_DEBUG("Passthrough chunk added (ChunkId: %s, Partition: %d)",
            ~ToString(FromProto<TChunkId>(chunkSpec->chunk_id())),
            CurrentPartitionIndex);

        // Place the chunk directly to the output table.
        RegisterOutput(chunkSpec, CurrentPartitionIndex, 0);
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
        YCHECK(!IsNontrivial(chunkSpec.start_limit()));
        YCHECK(!IsNontrivial(chunkSpec.end_limit()));

        auto miscExt = GetProtoExtension<TMiscExt>(chunkSpec.extensions());

        // ChunkSequenceWriter may actually produce a chunk a bit smaller than DesiredChunkSize,
        // so we have to be more flexible here.
        if (0.9 * miscExt.compressed_data_size() >= Spec->JobIO->TableWriter->DesiredChunkSize) {
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

static const int DefaultPartitionTag = -1;

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
        (Right)
    );

    struct TKeyEndpoint
    {
        EEndpointType Type;
        TRefCountedChunkSpecPtr ChunkSpec;
        TBoundaryKeysExt BoundaryKeys;
        bool IsPassthrough;

        void Persist(TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Type);
            Persist(context, ChunkSpec);
            Persist(context, BoundaryKeys);
            Persist(context, IsPassthrough);
        }

        const NChunkClient::NProto::TKey& GetKey() const
        {
            return Type == EEndpointType::Left
                ? BoundaryKeys.start()
                : BoundaryKeys.end();
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

        CollectEndpoints();

        LOG_INFO("Sorting %d endpoints", static_cast<int>(Endpoints.size()));
        SortEndpoints();

        FindPassthroughChunks();
        BuildTasks();
        FinishPreparation();
    }

    virtual void ProcessInputChunk(TRefCountedChunkSpecPtr chunkSpec) override
    {
        ChunkSplitsCollector->AddChunk(chunkSpec);
    }

    virtual bool IsPassthroughChunk(const TChunkSpec& chunkSpec) override
    {
        // ToDo(psushin): refactor.
        YUNREACHABLE();
    }

    virtual void SortEndpoints() = 0;
    virtual void FindPassthroughChunks() = 0;
    virtual void BuildTasks() = 0;

    void CollectEndpoints()
    {
        const auto& chunks = ChunkSplitsFetcher->GetChunkSplits();
        FOREACH (const auto& chunk, chunks) {
            auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunk->extensions());
            {
                TKeyEndpoint endpoint;
                endpoint.Type = EEndpointType::Left;
                endpoint.ChunkSpec = chunk;
                endpoint.BoundaryKeys = boundaryKeysExt;
                endpoint.IsPassthrough = false;
                Endpoints.push_back(endpoint);
            } {
                TKeyEndpoint endpoint;
                endpoint.Type = EEndpointType::Right;
                endpoint.ChunkSpec = chunk;
                endpoint.BoundaryKeys = boundaryKeysExt;
                endpoint.IsPassthrough = false;
                Endpoints.push_back(endpoint);
            }
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
        IOperationHost* host,
        TOperation* operation)
        : TSortedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeController, 0xbc6daa18);

    TSortedMergeOperationSpecPtr Spec;

    bool CanPassthrough()
    {
        return !Spec->ForceTransform;
    }

    bool IsLargeEnoughToPassthrough(const TChunkSpec& chunkSpec)
    {
        if (!Spec->CombineChunks)
            return true;

        return IsLargeChunk(chunkSpec);
    }

    virtual void SortEndpoints() override
    {
        int prefixLength = static_cast<int>(KeyColumns.size());

        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                int cmpResult = CompareKeys(lhs.GetKey(), rhs.GetKey(), prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                cmpResult = CompareKeys(lhs.BoundaryKeys.start(), rhs.BoundaryKeys.start(), prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                cmpResult = CompareKeys(lhs.BoundaryKeys.end(), rhs.BoundaryKeys.end(), prefixLength);
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                // Partition tag is used to identify the splits of one chunk.
                cmpResult = lhs.ChunkSpec->partition_tag() - rhs.ChunkSpec->partition_tag();
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                return lhs.Type < rhs.Type;
            });
    }

    virtual void FindPassthroughChunks() override
    {
        if (!CanPassthrough()) {
            return;
        }

        int openedSlicesCount = 0;
        int currentPartitionTag = DefaultPartitionTag;
        int startPassthroughIndex = -1;
        for (int i = 0; i < Endpoints.size(); ++i) {
            auto& endpoint = Endpoints[i];
            auto& chunkSpec = endpoint.ChunkSpec;

            openedSlicesCount += endpoint.Type == EEndpointType::Left ? 1 : -1;

            if (currentPartitionTag != DefaultPartitionTag) {
                if (chunkSpec->partition_tag() == currentPartitionTag) {
                    if (endpoint.Type == EEndpointType::Right && IsTrivial(chunkSpec->end_limit())) {
                        currentPartitionTag = DefaultPartitionTag;
                        auto completeChunk = CreateCompleteChunk(chunkSpec);

                        bool isManiacPassthrough = CompareKeys(
                            Endpoints[startPassthroughIndex].GetKey(),
                            endpoint.GetKey(),
                            KeyColumns.size()) == 0;

                        if (IsLargeEnoughToPassthrough(*completeChunk) &&
                            (openedSlicesCount == 0 || isManiacPassthrough)) {
                            for (int j = startPassthroughIndex; j <= i; ++j) {
                                Endpoints[j].IsPassthrough = true;
                            }
                        }
                    }

                    continue;
                } else {
                    currentPartitionTag = DefaultPartitionTag;
                }
            }

            // No current passthrough candidate.
            if (endpoint.Type == EEndpointType::Left &&
                IsTrivial(chunkSpec->start_limit()))
            {
                currentPartitionTag = chunkSpec->partition_tag();
                startPassthroughIndex = i;
            }
        }
    }

    virtual void BuildTasks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        yhash_set<TRefCountedChunkSpecPtr> globalOpenedSlices;
        TNullable<NChunkClient::NProto::TKey> lastBreakpoint = Null;

        int startIndex = 0;
        while (startIndex < Endpoints.size()) {
            auto& key = Endpoints[startIndex].GetKey();

            yhash_set<TRefCountedChunkSpecPtr> passthroughChunks;
            yhash_set<TRefCountedChunkSpecPtr> localOpenedSlices;

            // Slices with equal left and right boundaries.
            std::vector<TRefCountedChunkSpecPtr> maniacs;

            int currentIndex = startIndex;
            while (currentIndex < Endpoints.size()) {
                // Iterate over endpoints with equal keys.
                auto& endpoint = Endpoints[currentIndex];
                auto& currentKey = endpoint.GetKey();

                if (CompareKeys(key, currentKey, prefixLength) != 0) {
                    // This key is over.
                    break;
                }

                if (endpoint.IsPassthrough) {
                    auto partitionTag = endpoint.ChunkSpec->partition_tag();
                    auto chunkSpec = CreateCompleteChunk(endpoint.ChunkSpec);
                    YCHECK(passthroughChunks.insert(chunkSpec).second);
                    while (currentIndex < Endpoints.size() &&
                        Endpoints[currentIndex].IsPassthrough &&
                        Endpoints[currentIndex].ChunkSpec->partition_tag() == partitionTag)
                    {
                        ++currentIndex;
                    }
                    continue;
                }

                if (endpoint.Type == EEndpointType::Left) {
                    YCHECK(localOpenedSlices.insert(endpoint.ChunkSpec).second);
                    ++currentIndex;
                    continue;
                }

                // Right non-passthrough endpoint.
                {
                    auto it = globalOpenedSlices.find(endpoint.ChunkSpec);
                    if (it != globalOpenedSlices.end()) {
                        AddPendingChunk(CreateChunkSlice(*it, lastBreakpoint));
                        globalOpenedSlices.erase(it);
                        ++currentIndex;
                        continue;
                    }
                }
                {
                    auto it = localOpenedSlices.find(endpoint.ChunkSpec);
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
                if (lastBreakpoint && CompareKeys(key, *lastBreakpoint) == 0) {
                    // Already flushed at this key.
                    return;
                }

                auto nextBreakpoint = GetKeyPrefixSuccessor(key, prefixLength);
                LOG_DEBUG("Finish current task, flushing %" PRISZT " chunks at key %s",
                    globalOpenedSlices.size(),
                    ~ToString(nextBreakpoint));

                FOREACH (const auto& chunkSpec, globalOpenedSlices) {
                    this->AddPendingChunk(CreateChunkSlice(
                        chunkSpec,
                        lastBreakpoint,
                        nextBreakpoint));
                }
                lastBreakpoint = nextBreakpoint;

                EndTask();
            };

            while (!HasLargeActiveTask() && !maniacs.empty()) {
                AddPendingChunk(CreateChunkSlice(maniacs.back()));
                maniacs.pop_back();
            }

            if (!maniacs.empty()) {
                endTask();

                FOREACH(auto& chunkSpec, maniacs) {
                    AddPendingChunk(CreateChunkSlice(chunkSpec));
                    if (HasLargeActiveTask()) {
                        EndManiacTask();
                    }
                }
                EndManiacTask();
            }

            if (!passthroughChunks.empty()) {
                endTask();

                FOREACH(auto& chunkSpec, passthroughChunks) {
                    AddPassthroughChunk(chunkSpec);
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
        table.Clear = true;
        table.LockMode = ELockMode::Exclusive;
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

    bool IsPrimaryTable(int tableIndex) const
    {
        return InputTables[tableIndex].Path.Attributes().Get<bool>("primary", false);
    }

    virtual void SortEndpoints() override
    {
        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                int cmpResult = CompareKeys(lhs.GetKey(), rhs.GetKey());
                if (cmpResult != 0) {
                    return cmpResult < 0;
                }

                return lhs.Type < rhs.Type;
            });
    }

    virtual void FindPassthroughChunks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        // For update task.
        int currentPartitionTag = DefaultPartitionTag;
        int startPassthroughIndex = -1;

        int openedSlicesCount = 0;
        NChunkClient::NProto::TKey previosKey;
        previosKey = GetKeySuccessor(previosKey);

        for (int i = 0; i < Endpoints.size(); ++i) {
            auto& endpoint = Endpoints[i];
            auto& key = endpoint.GetKey();

            openedSlicesCount += endpoint.Type == EEndpointType::Left ? 1 : -1;

            if (currentPartitionTag != DefaultPartitionTag &&
                endpoint.ChunkSpec->partition_tag() == currentPartitionTag)
            {
                previosKey = key;
                continue;
            }

            if (CompareKeys(key, previosKey, prefixLength) == 0) {
                currentPartitionTag = DefaultPartitionTag;
                // Don't update previous key - it's equal to current.
                continue;
            }

            if (currentPartitionTag != DefaultPartitionTag) {
                auto& previousEndpoint = Endpoints[i - 1];
                auto& chunkSpec = previousEndpoint.ChunkSpec;
                if (previousEndpoint.Type == EEndpointType::Right && IsTrivial(chunkSpec->end_limit())) {
                    for (int j = startPassthroughIndex; j < i; ++j) {
                        Endpoints[j].IsPassthrough = true;
                    }
                }
            }

            currentPartitionTag = DefaultPartitionTag;
            previosKey = key;

            // No current passthrough candidate.
            auto& chunkSpec = endpoint.ChunkSpec;
            if (endpoint.Type == EEndpointType::Left &&
                IsTrivial(chunkSpec->start_limit()) &&
                IsPrimaryTable(chunkSpec->table_index()) &&
                openedSlicesCount == 1)
            {
                currentPartitionTag = chunkSpec->partition_tag();
                startPassthroughIndex = i;
            }
        }

        if (currentPartitionTag != DefaultPartitionTag) {
            // Last passthrough candidate.
            auto& previousEndpoint = Endpoints.back();
            auto& chunkSpec = previousEndpoint.ChunkSpec;
            YCHECK(previousEndpoint.Type == EEndpointType::Right);
            if (IsTrivial(chunkSpec->end_limit())) {
                for (int j = startPassthroughIndex; j < Endpoints.size(); ++j) {
                    Endpoints[j].IsPassthrough = true;
                }
            }
        }
    }

    virtual void BuildTasks() override
    {
        const int prefixLength = static_cast<int>(KeyColumns.size());

        yhash_set<TRefCountedChunkSpecPtr> openedSlices;
        TNullable<NChunkClient::NProto::TKey> lastBreakpoint = Null;

        int startIndex = 0;
        while (startIndex < Endpoints.size()) {
            auto& key = Endpoints[startIndex].GetKey();

            int currentIndex = startIndex;
            while (currentIndex < Endpoints.size()) {
                // Iterate over endpoints with equal keys.
                auto& endpoint = Endpoints[currentIndex];
                auto& currentKey = endpoint.GetKey();

                if (CompareKeys(key, currentKey, prefixLength) != 0) {
                    // This key is over.
                    break;
                }

                if (endpoint.IsPassthrough) {
                    YCHECK(openedSlices.empty());
                    EndTask();

                    auto partitionTag = endpoint.ChunkSpec->partition_tag();
                    auto chunkSpec = CreateCompleteChunk(endpoint.ChunkSpec);
                    AddPassthroughChunk(chunkSpec);

                    while (currentIndex < Endpoints.size() &&
                        Endpoints[currentIndex].IsPassthrough &&
                        Endpoints[currentIndex].ChunkSpec->partition_tag() == partitionTag)
                    {
                        ++currentIndex;
                    }
                    continue;
                }

                if (endpoint.Type == EEndpointType::Left) {
                    YCHECK(openedSlices.insert(endpoint.ChunkSpec).second);
                    ++currentIndex;
                    continue;
                }

                // Right non-passthrough endpoint.
                YCHECK(endpoint.Type == EEndpointType::Right);

                auto it = openedSlices.find(endpoint.ChunkSpec);
                YCHECK(it != openedSlices.end());
                AddPendingChunk(CreateChunkSlice(*it, lastBreakpoint));
                openedSlices.erase(it);
                ++currentIndex;
            }

            if (HasLargeActiveTask()) {
                YCHECK(!lastBreakpoint || CompareKeys(key, *lastBreakpoint, prefixLength) != 0);

                auto nextBreakpoint = GetKeyPrefixSuccessor(key, prefixLength);
                LOG_DEBUG("Finish current task, flushing %" PRISZT " chunks at key %s",
                    openedSlices.size(),
                    ~ToString(nextBreakpoint));

                FOREACH (const auto& chunkSpec, openedSlices) {
                    this->AddPendingChunk(CreateChunkSlice(
                        chunkSpec,
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

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        FOREACH (const auto& path, Spec->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    virtual i64 GetAdditionalMemorySize() const override
    {
        return Spec->Reducer->MemoryLimit;
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

        InitUserJobSpecTemplate(
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
        InitUserJobSpec(jobSpecExt->mutable_reducer_spec(), joblet);
    }

    virtual bool IsTableIndexEnabled() const override
    {
        return Spec->Reducer->EnableTableIndex;
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        FOREACH(const auto& inputTable, InputTables) {
            if (inputTable.Path.Attributes().Get<bool>("primary", false)) {
                return false;
            }
        }
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

