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

#include <ytlib/ytree/fluent.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/table_client/helpers.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationLogger);
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
        : TOperationControllerBase(config, host, operation)
        , SpecBase(spec)
        , TotalChunkCount(0)
        , TotalDataSize(0)
        , CurrentTaskDataSize(0)
        , PartitionCount(0)
        , MaxDataSizePerJob(0)
    { }

    virtual TNodeResources GetMinNeededResources() override
    {
        return Tasks.empty()
            ? InfiniteNodeResources()
            : Tasks[0]->GetMinNeededResources();
    }

protected:
    TMergeOperationSpecBasePtr SpecBase;

    //! For each input table, the corresponding entry holds the stripe
    //! containing the chunks collected so far. Empty stripes are never stored explicitly
    //! and are denoted by |NULL|.
    std::vector<TChunkStripePtr> CurrentTaskStripes;

    //! The total number of chunks for processing.
    int TotalChunkCount;

    //! The total data size for processing.
    i64 TotalDataSize;

    //! The total data size accumulated in #CurrentTaskStripes.
    i64 CurrentTaskDataSize;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    //! The number of output partitions generated so far.
    /*!
     *  Each partition either corresponds to a merge task or to a pass-through chunk.
     *  Partition index is used as a key when calling #TOperationControllerBase::RegisterOutputChunkTree.
     */
    int PartitionCount;

    //! Overrides the spec limit to satisfy global job count limit.
    i64 MaxDataSizePerJob;

    class TMergeTask
        : public TTask
    {
    public:
        explicit TMergeTask(
            TMergeControllerBase* controller,
            int taskIndex,
            int partitionIndex = -1)
            : TTask(controller)
            , Controller(controller)
            , TaskIndex(taskIndex)
            , PartitionIndex(partitionIndex)
        {
            ChunkPool = CreateAtomicChunkPool();
        }

        virtual Stroka GetId() const override
        {
            return
                PartitionIndex < 0
                ? Sprintf("Merge(%d)", TaskIndex)
                : Sprintf("Merge(%d,%d)", TaskIndex, PartitionIndex);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->SpecBase->LocalityTimeout;
        }

        virtual NProto::TNodeResources GetMinNeededResources() const override
        {
            TNodeResources result;
            result.set_slots(1);
            result.set_cpu(1);
            result.set_memory(
                GetIOMemorySize(
                    Controller->SpecBase->JobIO,
                    Controller->GetInputTablePaths().size(),
                    1) +
                GetFootprintMemorySize());
            return result;
        }

    protected:
        void BuildInputOutputJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, joblet, Controller->EnableTableIndex());
            AddFinalOutputSpecs(jobSpec, joblet);
            Controller->CustomizeJobSpec(joblet, jobSpec);
        }

    private:
        TMergeControllerBase* Controller;

        TAutoPtr<IChunkPool> ChunkPool;

        //! The position in #TMergeControllerBase::Tasks.
        int TaskIndex;

        //! Key for #TOutputTable::OutputChunkTreeIds.
        int PartitionIndex;


        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->OutputTables.size();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            BuildInputOutputJobSpec(joblet, jobSpec);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            const TUserJobResult* userJobResult = NULL;
            if (joblet->Job->Result().HasExtension(TReduceJobResultExt::reduce_job_result_ext)) {
                userJobResult = &joblet->Job->Result()
                    .GetExtension(TReduceJobResultExt::reduce_job_result_ext)
                    .reducer_result();
            }

            Controller->RegisterOutputChunkTrees(joblet, PartitionIndex, userJobResult);
        }
    };

    typedef TIntrusivePtr<TMergeTask> TMergeTaskPtr;

    std::vector<TMergeTaskPtr> Tasks;

    //! Resizes #CurrentTaskStripes appropriately and sets all its entries to |NULL|.
    void ClearCurrentTaskStripes()
    {
        CurrentTaskStripes.clear();
        CurrentTaskStripes.resize(InputTables.size());
    }

    void EndTask(TMergeTaskPtr task)
    {
        YCHECK(HasActiveTask());

        FOREACH (auto stripe, CurrentTaskStripes) {
            if (stripe) {
                task->AddInput(stripe);
            }
        }
        task->FinishInput();

        ++PartitionCount;
        Tasks.push_back(task);

        LOG_DEBUG("Task finished (Task: %d, TaskDataSize: %" PRId64 ")",
            static_cast<int>(Tasks.size()) - 1,
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
            PartitionCount);

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
    void AddPendingChunk(TRefCountedInputChunkPtr inputChunk)
    {
        auto stripe = CurrentTaskStripes[inputChunk->table_index()];
        if (!stripe) {
            stripe = CurrentTaskStripes[inputChunk->table_index()] = New<TChunkStripe>();
        }

        i64 chunkDataSize;
        GetStatistics(*inputChunk, &chunkDataSize);

        TotalDataSize += chunkDataSize;
        ++TotalChunkCount;

        CurrentTaskDataSize += chunkDataSize;
        stripe->Chunks.push_back(inputChunk);

        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        LOG_DEBUG("Pending chunk added (ChunkId: %s, Partition: %d, Task: %d, TableIndex: %d, DataSize: %" PRId64 ")",
            ~chunkId.ToString(),
            PartitionCount,
            static_cast<int>(Tasks.size()),
            inputChunk->table_index(),
            chunkDataSize);
    }

    //! Add chunk directly to the output.
    void AddPassthroughChunk(TRefCountedInputChunkPtr inputChunk)
    {
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        LOG_DEBUG("Passthrough chunk added (ChunkId: %s, Partition: %d)",
            ~chunkId.ToString(),
            PartitionCount);

        // Place the chunk directly to the output table.
        RegisterOutputChunkTree(chunkId, PartitionCount, 0);
        ++PartitionCount;
    }


    // Custom bits of preparation pipeline.

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline
            ->Add(BIND(&TMergeControllerBase::ProcessInputs, MakeStrong(this)))
            ->Add(BIND(&TMergeControllerBase::EndInputChunks, MakeStrong(this)))
            ->Add(BIND(&TMergeControllerBase::FinishPreparation, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            ClearCurrentTaskStripes();

            std::vector<TRefCountedInputChunkPtr> chunks;
            i64 totalDataSize = 0;
            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                FOREACH (const auto& inputChunk, *table.FetchResponse->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());

                    i64 chunkDataSize;
                    NTableClient::GetStatistics(inputChunk, &chunkDataSize);

                    auto rcInputChunk = New<TRefCountedInputChunk>(inputChunk, tableIndex);
                    chunks.push_back(rcInputChunk);

                    totalDataSize += chunkDataSize;

                    LOG_DEBUG("Processing chunk (ChunkId: %s, DataSize: %" PRId64 ", TableIndex: %d)",
                        ~chunkId.ToString(),
                        chunkDataSize,
                        rcInputChunk->table_index());
                }
            }

            MaxDataSizePerJob = std::max(
                SpecBase->MaxDataSizePerJob,
                static_cast<i64>(std::ceil((double) totalDataSize / Config->MaxJobCount)));

            FOREACH (auto chunk, chunks) {
                ProcessInputChunk(chunk);
            }
        }
    }

    void FinishPreparation()
    {
        // Check for trivial inputs.
        if (Tasks.empty()) {
            LOG_INFO("Trivial merge");
            OnOperationCompleted();
            return;
        }

        // Init counters.
        JobCounter.Set(static_cast<int>(Tasks.size()));

        InitJobIOConfig();
        InitJobSpecTemplate();

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %d, JobCount: %" PRId64 ")",
            TotalDataSize,
            TotalChunkCount,
            JobCounter.GetTotal());

        // Kick-start the tasks.
        FOREACH (auto task, Tasks) {
            AddTaskPendingHint(task);
        }
    }


    //! Called for each input chunk.
    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk) = 0;

    //! Called at the end of input chunks scan.
    void EndInputChunks()
    {
        // Close the last task, if any.
        if (CurrentTaskDataSize > 0) {
            EndTask();
        }
    }

    // Progress reporting.

    virtual void LogProgress() override
    {
        LOG_DEBUG("Progress: "
            "Jobs = {T: %" PRId64 ", R: %" PRId64 ", C: %" PRId64 ", P: %d, F: %" PRId64 ", A: %" PRId64 "}",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted());
    }


    // Unsorted helpers.

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(const TInputChunk& inputChunk)
    {
        return IsStartingSlice(inputChunk) && IsEndingSlice(inputChunk);
    }

    static bool IsStartingSlice(const TInputChunk& inputChunk)
    {
        return
            !inputChunk.slice().start_limit().has_key() &&
            !inputChunk.slice().start_limit().has_row_index();
    }

    static bool IsEndingSlice(const TInputChunk& inputChunk)
    {
        return
            !inputChunk.slice().end_limit().has_key() &&
            !inputChunk.slice().end_limit().has_row_index();
    }

    //! Returns True if the chunk can be included into the output as-is.
    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) = 0;

    //! Returns True iff the chunk is complete and is large enough.
    bool IsLargeCompleteChunk(const TInputChunk& inputChunk)
    {
        if (!IsCompleteChunk(inputChunk)) {
            return false;
        }

        return IsLargeChunk(inputChunk);
    }

    bool IsLargeChunk(const TInputChunk& inputChunk)
    {
        i64 chunkDataSize;
        NTableClient::GetStatistics(inputChunk, &chunkDataSize);

        // ChunkSequenceWriter may actually produce a chunk a bit smaller than DesiredChunkSize,
        // so we have to be more flexible here.
        if (0.9 * chunkDataSize >= SpecBase->JobIO->TableWriter->DesiredChunkSize) {
            return true;
        }

        return false;
    }

    //! A typical implementation of #IsPassthroughChunk that depends on whether chunks must be combined or not.
    bool IsPassthroughChunkImpl(const TInputChunk& inputChunk, bool combineChunks)
    {
        return combineChunks ? IsLargeCompleteChunk(inputChunk) : IsCompleteChunk(inputChunk);
    }

    //! Initializes #JobIOConfig.
    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(SpecBase->JobIO);
        InitFinalOutputConfig(JobIOConfig);
    }

    //! Initializes #JobSpecTemplate.
    virtual void InitJobSpecTemplate() = 0;

    virtual void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec)
    { }

    virtual bool EnableTableIndex() const
    {
        return false;
    }

};

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
    TUnorderedMergeOperationSpecPtr Spec;

    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) override
    {
        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
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

    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        if (IsPassthroughChunk(*inputChunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(inputChunk);
            return;
        }

        // NB: During unordered merge all chunks go to a single chunk stripe.
        AddPendingChunk(inputChunk);
        EndTaskIfLarge();
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::UnorderedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }
};

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
    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        if (IsPassthroughChunk(*inputChunk)) {
            // Merge is not needed. Copy the chunk directly to the output.
            if (HasActiveTask()) {
                EndTask();
            }
            AddPassthroughChunk(inputChunk);
            return;
        }

        // NB: During ordered merge all chunks go to a single chunk stripe.
        AddPendingChunk(inputChunk);
        EndTaskIfLarge();
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

    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) override
    {
        if (!Spec->AllowPassthroughChunks)
            return false;

        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::OrderedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

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

    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) override
    {
        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
    }

    virtual void DoInitialize() override
    {
        TOrderedMergeControllerBase::DoInitialize();

        // For erase operation the rowset specified by the user must actually be negated.
        {
            auto& table = InputTables[0];
            table.NegateFetch = true;
        }
        // ...and the output table must be cleared (regardless of "overwrite" attribute).
        {
            auto& table = OutputTables[0];
            table.Clear = true;
            table.Overwrite = true;
            table.LockMode = ELockMode::Exclusive;
        }
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        // If the input is sorted then the output chunk tree must also be marked as sorted.
        const auto& inputTable = InputTables[0];
        auto& outputTable = OutputTables[0];
        if (inputTable.KeyColumns) {
            outputTable.KeyColumns = inputTable.KeyColumns;
        }
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::OrderedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

        // If the input is sorted then the output must also be sorted.
        // For this, the job needs key columns.
        const auto& table = InputTables[0];
        if (table.KeyColumns) {
            ToProto(jobSpecExt->mutable_key_columns(), table.KeyColumns.Get());
        }

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

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

protected:
    class TManiacTask
        : public TMergeTask
    {
    public:
        TManiacTask(
            TSortedMergeControllerBase* controller,
            int taskIndex,
            int partitionIndex)
            : TMergeTask(controller, taskIndex, partitionIndex)
            , Controller(controller)
        { }

    private:
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
        NTableClient::NProto::TKey Key;
        TRefCountedInputChunkPtr InputChunk;
    };

    std::vector<TKeyEndpoint> Endpoints;

    //! The actual (adjusted) key columns.
    std::vector<Stroka> KeyColumns;

    TChunkSplitsFetcherPtr ChunkSplitsFetcher;
    TChunkSplitsCollectorPtr ChunkSplitsCollector;

    TJobSpec ManiacJobSpecTemplate;

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() = 0;

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        auto this_ = MakeStrong(this);
        return pipeline
            ->Add(BIND(&TSortedMergeControllerBase::ProcessInputs, MakeStrong(this)))
            ->Add(BIND( [=] () -> TFuture< TValueOrError<void> > {
                    return this_->ChunkSplitsCollector->Run();
                }))
            ->Add(BIND(&TSortedMergeControllerBase::OnChunkSplitsReceived, MakeStrong(this)))
            ->Add(BIND(&TSortedMergeControllerBase::FinishPreparation, MakeStrong(this)));
    }

    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        ChunkSplitsCollector->AddChunk(inputChunk);
    }

    virtual bool IsLargeEnoughToPassthrough(const TInputChunk& inputChunk) = 0;

    void OnChunkSplitsReceived()
    {
        int prefixLength = static_cast<int>(KeyColumns.size());
        const auto& chunks = ChunkSplitsFetcher->GetChunkSplits();
        FOREACH (const auto& chunk, chunks) {
            auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(chunk->extensions());
            if (CompareKeys(boundaryKeysExt.start(), boundaryKeysExt.end(), prefixLength) == 0) {
                // Maniac chunk
                TKeyEndpoint endpoint;
                endpoint.Type = EEndpointType::Maniac;
                endpoint.Key = boundaryKeysExt.start();
                endpoint.InputChunk = chunk;
                Endpoints.push_back(endpoint);
            } else {
                {
                    TKeyEndpoint endpoint;
                    endpoint.Type = EEndpointType::Left;
                    endpoint.Key = boundaryKeysExt.start();
                    endpoint.InputChunk = chunk;
                    Endpoints.push_back(endpoint);
                } {
                    TKeyEndpoint endpoint;
                    endpoint.Type = EEndpointType::Right;
                    endpoint.Key = boundaryKeysExt.end();
                    endpoint.InputChunk = chunk;
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
        yhash_set<TRefCountedInputChunkPtr> openedChunks;

        int currentIndex = 0;
        TNullable<NTableClient::NProto::TKey> lastBreakpoint;

        int endpointsCount = static_cast<int>(Endpoints.size());

        auto flushOpenedChunks = [&] () {
            const auto& endpoint = Endpoints[currentIndex];
            auto nextBreakpoint = GetSuccessorKey(endpoint.Key);
            LOG_DEBUG("Finish current task, flushing %" PRISZT " chunks at key %s",
                openedChunks.size(),
                ~ToString(nextBreakpoint));

            FOREACH (const auto& inputChunk, openedChunks) {
                this->AddPendingChunk(SliceChunk(inputChunk, lastBreakpoint, nextBreakpoint));
            }
            lastBreakpoint = nextBreakpoint;
        };

        while (currentIndex < endpointsCount) {
            const auto& endpoint = Endpoints[currentIndex];

            switch (endpoint.Type) {
                case EEndpointType::Left:
                    if (openedChunks.empty() &&
                        IsStartingSlice(*endpoint.InputChunk) &&
                        AllowPassthroughChunks())
                    {
                        // Trying to reconstruct passthrough chunk from chunk slices.
                        auto chunkId = TChunkId::FromProto(endpoint.InputChunk->slice().chunk_id());
                        auto tableIndex = endpoint.InputChunk->table_index();
                        auto nextIndex = currentIndex;
                        while (true) {
                            ++nextIndex;
                            if (nextIndex == endpointsCount) {
                                break;
                            }
                            auto nextChunkId = TChunkId::FromProto(Endpoints[nextIndex].InputChunk->slice().chunk_id());
                            auto nextTableIndex = Endpoints[nextIndex].InputChunk->table_index();
                            if (nextChunkId != chunkId || tableIndex != nextTableIndex) {
                                break;
                            }
                        }

                        auto lastEndpoint = Endpoints[nextIndex - 1];
                        if (lastEndpoint.Type == EEndpointType::Right && IsEndingSlice(*lastEndpoint.InputChunk)) {
                            if (IsLargeEnoughToPassthrough(*endpoint.InputChunk)) {
                                auto chunk = CreateCompleteChunk(endpoint.InputChunk);
                                if (HasActiveTask()) {
                                   EndTask();
                                }
                                AddPassthroughChunk(chunk);
                                currentIndex = nextIndex;
                                break;
                            }
                        }
                    }

                    YCHECK(openedChunks.insert(endpoint.InputChunk).second);
                    ++currentIndex;
                    break;

                case EEndpointType::Right:
                    AddPendingChunk(SliceChunk(endpoint.InputChunk, lastBreakpoint, Null));
                    YCHECK(openedChunks.erase(endpoint.InputChunk) == 1);

                    if (!openedChunks.empty() &&
                        HasLargeActiveTask() &&
                        endpoint.Key < Endpoints[currentIndex + 1].Key)
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
                    std::vector<TRefCountedInputChunkPtr> completeLargeChunks;
                    std::vector<TRefCountedInputChunkPtr> partialChunks;
                    do {
                        const auto& nextEndpoint = Endpoints[nextIndex];

                        if (nextEndpoint.Type == EEndpointType::Maniac && nextEndpoint.Key == endpoint.Key) {
                            i64 dataSize;
                            GetStatistics(*nextEndpoint.InputChunk, &dataSize);
                            if (IsLargeCompleteChunk(*nextEndpoint.InputChunk)) {
                                completeLargeManiacSize += dataSize;
                                completeLargeChunks.push_back(nextEndpoint.InputChunk);
                            } else {
                                partialManiacSize += dataSize;
                                partialChunks.push_back(nextEndpoint.InputChunk);
                            }
                        } else {
                            break;
                        }
                        ++nextIndex;
                    } while (nextIndex != endpointsCount);

                    if (AllowPassthroughChunks()) {
                        bool hasManiacTask = partialManiacSize > MaxDataSizePerJob;
                        bool hasPassthroughManiacs = completeLargeManiacSize > 0;

                        if (!hasManiacTask) {
                            FOREACH (const auto& chunk, partialChunks) {
                                AddPendingChunk(chunk);
                            }
                        }

                        if (hasManiacTask || hasPassthroughManiacs) {
                            flushOpenedChunks();

                            if (HasActiveTask()) {
                               EndTask();
                            }
                        }

                        if (hasManiacTask) {
                            YCHECK(!HasActiveTask());
                            // Create special maniac task.
                            FOREACH (const auto& chunk, partialChunks) {
                                AddPendingChunk(chunk);
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
                            flushOpenedChunks();
                            if (HasActiveTask()) {
                               EndTask();
                            }
                        }

                        FOREACH (const auto& chunk, partialChunks) {
                            AddPendingChunk(chunk);
                        }

                        FOREACH (const auto& chunk, completeLargeChunks) {
                            AddPendingChunk(chunk);
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
            PartitionCount);

        EndTask(task);
    }

    virtual bool AllowPassthroughChunks() = 0;

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        auto specKeyColumns = GetSpecKeyColumns();
        LOG_INFO("Spec key columns are %s",
            specKeyColumns ? ~ConvertToYsonString(specKeyColumns.Get(), NYson::EYsonFormat::Text).Data() : "<Null>");

        KeyColumns = CheckInputTablesSorted(GetSpecKeyColumns());
        LOG_INFO("Adjusted key columns are %s",
            ~ConvertToYsonString(KeyColumns, NYson::EYsonFormat::Text).Data());

        ChunkSplitsFetcher = New<TChunkSplitsFetcher>(
            Config,
            SpecBase,
            Operation->GetOperationId(),
            KeyColumns);

        ChunkSplitsCollector = New<TChunkSplitsCollector>(
            ChunkSplitsFetcher,
            ~Host->GetBackgroundInvoker());
    }
};

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

    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) override
    {
        if (!Spec->AllowPassthroughChunks)
            return false;

        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
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
        return Spec->AllowPassthroughChunks;
    }

    virtual bool IsLargeEnoughToPassthrough(const TInputChunk& inputChunk) override
    {
        if (!Spec->CombineChunks)
            return true;

        return IsLargeChunk(inputChunk);
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->MergeBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
        ToProto(jobSpecExt->mutable_key_columns(), KeyColumns);

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
        ManiacJobSpecTemplate.set_type(EJobType::UnorderedMerge);
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        TSortedMergeControllerBase::OnCustomInputsRecieved(batchRsp);

        OutputTables[0].KeyColumns = KeyColumns;
    }

};

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

    virtual NProto::TNodeResources GetMinNeededResources() override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(Spec->Reducer->CpuLimit);
        result.set_memory(
            GetIOMemorySize(Spec->JobIO, Spec->InputTablePaths.size(), Spec->OutputTablePaths.size()) +
            Spec->Reducer->MemoryLimit +
            GetFootprintMemorySize());
        return result;
    }

private:
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

    virtual bool IsPassthroughChunk(const TInputChunk& inputChunk) override
    {
        YUNREACHABLE();
    }

    virtual bool AllowPassthroughChunks() override
    {
        return false;
    }

    virtual bool SupportsSortedOutput() const override
    {
        return true;
    }

    virtual bool IsLargeEnoughToPassthrough(const TInputChunk& inputChunk) override
    {
        UNUSED(inputChunk);
        YUNREACHABLE();
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->ReduceBy;
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedReduce);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        ToProto(jobSpecExt->mutable_key_columns(), KeyColumns);

        InitUserJobSpec(
            jobSpecExt->mutable_reducer_spec(),
            Spec->Reducer,
            Files,
            TableFiles);

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
    }

    void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;

        auto* jobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        AddUserJobEnvironment(jobSpecExt->mutable_reducer_spec(), joblet);
    }

    bool EnableTableIndex() const override
    {
        return Spec->EnableTableIndex;
    }
};

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
        case EMergeMode::Unordered:
        {
            auto spec = ParseOperationSpec<TUnorderedMergeOperationSpec>(
                operation,
                config->UnorderedMergeOperationSpec);
            return New<TUnorderedMergeController>(config, spec, host, operation);
        }
        case EMergeMode::Ordered:
        {
            auto spec = ParseOperationSpec<TOrderedMergeOperationSpec>(
                operation,
                config->OrderedMergeOperationSpec);
            return New<TOrderedMergeController>(config, spec, host, operation);
        }
        case EMergeMode::Sorted:
        {
            auto spec = ParseOperationSpec<TSortedMergeOperationSpec>(
                operation,
                config->SortedMergeOperationSpec);
            return New<TSortedMergeController>(config, spec, host, operation);
        }
        default:
            YUNREACHABLE();
    };
}

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

