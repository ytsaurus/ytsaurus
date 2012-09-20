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

#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NObjectClient;
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
        , TotalJobCount(0)
        , CurrentTaskDataSize(0)
        , PartitionCount(0)
    { }

    virtual TNodeResources GetMinNeededResources() override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            GetIOMemorySize(JobIOConfig, GetInputTablePaths().size(), 1) +
            GetFootprintMemorySize());
        return result;
    }

protected:
    TMergeOperationSpecBasePtr SpecBase;

    // Counters.
    int TotalJobCount;
    TProgressCounter DataSizeCounter;
    TProgressCounter ChunkCounter;

    //! For each input table, the corresponding entry holds the stripe
    //! containing the chunks collected so far. Empty stripes are never stored explicitly
    //! and are denoted by |NULL|.
    std::vector<TChunkStripePtr> CurrentTaskStripes;

    //! The total data size accumulated in #CurrentTaskStripes.
    i64 CurrentTaskDataSize;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    //! Number of output partitions generated so far.
    /*!
     *  Each partition either corresponds to a merge task or to a pass-through chunk.
     *  Partition index is used as a key when calling #TOperationControllerBase::RegisterOutputChunkTree.
     */
    int PartitionCount;

    TJobIOConfigPtr JobIOConfig;

    // Merge task.

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

        virtual int GetPendingJobCount() const override
        {
            return ChunkPool->IsPending() ? 1 : 0;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->SpecBase->LocalityTimeout;
        }
        
        virtual NProto::TNodeResources GetMinNeededResources() const override
        {
            return Controller->GetMinNeededResources();
        }

    protected:
        void BuildSpecInputOutput(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec)
        {
            AddParallelInputSpec(jobSpec, jip);

            for (int i = 0; i < Controller->OutputTables.size(); ++i) {
                // Reduce task can have multiple output tables.
                AddTabularOutputSpec(jobSpec, jip, i);
            }
        }

    private:
        TMergeControllerBase* Controller;

        //! The position in |MergeTasks|. 
        int TaskIndex;

        //! The position in |TOutputTable::PartitionIds| where the 
        //! output of this task must be placed.
        int PartitionIndex;


        virtual int GetChunkListCountPerJob() const override
        {
            return static_cast<int>(Controller->OutputTables.size());
        }

        virtual TNullable<i64> GetJobDataSizeThreshold() const override
        {
            return Null;
        }

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            BuildSpecInputOutput(jip, jobSpec);
        }

        virtual void OnJobStarted(TJobInProgressPtr jip) override
        {
            TTask::OnJobStarted(jip);

            Controller->ChunkCounter.Start(jip->PoolResult->TotalChunkCount);
            Controller->DataSizeCounter.Start(jip->PoolResult->TotalDataSize);
        }

        virtual void OnJobCompleted(TJobInProgressPtr jip) override
        {
            TTask::OnJobCompleted(jip);

            Controller->ChunkCounter.Completed(jip->PoolResult->TotalChunkCount);
            Controller->DataSizeCounter.Completed(jip->PoolResult->TotalDataSize);

            int outputCount = static_cast<int>(Controller->OutputTables.size());
            for (int i = 0; i < outputCount; ++i) {
                Controller->RegisterOutputChunkTree(jip->ChunkListIds[i], PartitionIndex, i);
            }
        }

        virtual void OnJobFailed(TJobInProgressPtr jip) override
        {
            TTask::OnJobFailed(jip);

            Controller->ChunkCounter.Failed(jip->PoolResult->TotalChunkCount);
            Controller->DataSizeCounter.Failed(jip->PoolResult->TotalDataSize);
        }
    };

    typedef TIntrusivePtr<TMergeTask> TMergeTaskPtr;

    std::vector<TMergeTaskPtr> MergeTasks;

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
                task->AddStripe(stripe);
            }
        }

        ++PartitionCount;
        MergeTasks.push_back(task);

        LOG_DEBUG("Finished task (Task: %d, TaskDataSize: %" PRId64 ")",
            static_cast<int>(MergeTasks.size()) - 1,
            CurrentTaskDataSize);

        CurrentTaskDataSize = 0;
        ClearCurrentTaskStripes();
    }
    
    //! Finishes the current task.
    void EndTask()
    {
        auto task = New<TMergeTask>(
            this,
            static_cast<int>(MergeTasks.size()),
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
        return CurrentTaskDataSize >= SpecBase->MaxDataSizePerJob;
    }

    //! Add chunk to the current task's pool.
    void AddPendingChunk(TRefCountedInputChunkPtr inputChunk)
    {
        // Merge is IO-bound, use data size as weight.
        auto miscExt = GetProtoExtension<TMiscExt>(inputChunk->extensions());
        i64 dataSize = miscExt.uncompressed_data_size();
        i64 rowCount = miscExt.row_count();

        auto stripe = CurrentTaskStripes[inputChunk->TableIndex];
        if (!stripe) {
            stripe = CurrentTaskStripes[inputChunk->TableIndex] = New<TChunkStripe>();
        }

        DataSizeCounter.Increment(dataSize);
        ChunkCounter.Increment(1);
        CurrentTaskDataSize += dataSize;
        stripe->AddChunk(inputChunk, dataSize, rowCount);

        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        LOG_DEBUG("Added pending chunk (ChunkId: %s, Partition: %d, Task: %d, TableIndex: %d, DataSize: %" PRId64 ", RowCount: %" PRId64 ")",
            ~chunkId.ToString(),
            PartitionCount,
            static_cast<int>(MergeTasks.size()),
            inputChunk->TableIndex,
            dataSize,
            rowCount);
    }

    //! Add chunk directly to the output.
    void AddPassthroughChunk(TRefCountedInputChunkPtr inputChunk)
    {
        auto& table = OutputTables[0];
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        LOG_DEBUG("Added passthrough chunk (ChunkId: %s, Partition: %d)",
            ~chunkId.ToString(),
            PartitionCount);

        // Place the chunk directly to the output table.
        RegisterOutputChunkTree(chunkId, PartitionCount, 0);
        ++PartitionCount;
    }


    // Init/finish.

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        if (InputTables.empty()) {
            // At least one table is needed for sorted merge to figure out the key columns.
            // To be consistent, we don't allow empty set of input tables in for any merge type.
            THROW_ERROR_EXCEPTION("At least one input table must be given");
        }
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

            InitJobIOConfig();
            ClearCurrentTaskStripes();

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                FOREACH (auto& inputChunk, *table.FetchResponse->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());
                    auto miscExt = GetProtoExtension<TMiscExt>(inputChunk.extensions());
                    i64 dataSize = miscExt.uncompressed_data_size();
                    i64 rowCount = miscExt.row_count();
                    auto chunk = New<TRefCountedInputChunk>(inputChunk, tableIndex);
                    LOG_DEBUG("Processing chunk (ChunkId: %s, DataSize: %" PRId64 ", RowCount: %" PRId64 ", TableIndex: %d)",
                        ~chunkId.ToString(),
                        dataSize,
                        rowCount,
                        chunk->TableIndex);
                    ProcessInputChunk(chunk);
                }
            }
        }
    }

    void FinishPreparation()
    {
        // Check for trivial inputs.
        if (ChunkCounter.GetTotal() == 0) {
            LOG_INFO("Trivial merge");
            OnOperationCompleted();
            return;
        }

        // Init counters.
        TotalJobCount = static_cast<int>(MergeTasks.size());

        InitJobSpecTemplate();

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %" PRId64 ", JobCount: %d)",
            DataSizeCounter.GetTotal(),
            ChunkCounter.GetTotal(),
            TotalJobCount);

        // Kick-start the tasks.
        FOREACH (const auto task, MergeTasks) {
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
            "Jobs = {T: %d, R: %d, C: %d, P: %d, F: %d}",
            TotalJobCount,
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount);
    }


    // Unsorted helpers.

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(TRefCountedInputChunkPtr inputChunk)
    {
        return IsStartingSlice(inputChunk) && IsEndingSlice(inputChunk);
    }

    static bool IsStartingSlice(TRefCountedInputChunkPtr inputChunk)
    {
        return !inputChunk->slice().start_limit().has_key() &&
            !inputChunk->slice().start_limit().has_row_index();
    }

    static bool IsEndingSlice(TRefCountedInputChunkPtr inputChunk)
    {
        return !inputChunk->slice().end_limit().has_key() &&
            !inputChunk->slice().end_limit().has_row_index();
    }

    //! Returns True if the chunk can be included into the output as-is.
    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) = 0;

    //! Returns True iff the chunk is complete and is large enough.
    bool IsLargeCompleteChunk(TRefCountedInputChunkPtr inputChunk)
    {
        if (!IsCompleteChunk(inputChunk)) {
            return false;
        }

        return IsLargeChunk(inputChunk);
    }

    bool IsLargeChunk(TRefCountedInputChunkPtr inputChunk)
    {
        auto miscExt = GetProtoExtension<TMiscExt>(inputChunk->extensions());
        // ChunkSequenceWriter may actually produce a chunk a bit smaller than DesiredChunkSize,
        // so we have to be more flexible here.
        if (0.9 * miscExt.compressed_data_size() >= JobIOConfig->TableWriter->DesiredChunkSize) {
            return true;
        }

        return false;
    }

    //! A typical implementation of #IsPassthroughChunk that depends on whether chunks must be combined or not.
    bool IsPassthroughChunkImpl(TRefCountedInputChunkPtr inputChunk, bool combineChunks)
    {
        return combineChunks ? IsLargeCompleteChunk(inputChunk) : IsCompleteChunk(inputChunk);
    }

    //! Initializes #JobIOConfig.
    virtual void InitJobIOConfig() = 0;

    //! Initializes #JobSpecTemplate.
    virtual void InitJobSpecTemplate() = 0;
};

////////////////////////////////////////////////////////////////////

//! Handles unordered merge operation.
class TUnorderedMergeController
    : public TMergeControllerBase
{
public:
    TUnorderedMergeController(
        TSchedulerConfigPtr config,
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    TMergeOperationSpecPtr Spec;

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
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
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsPassthroughChunk(inputChunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(inputChunk);
            return;
        }

        // NB: During unordered merge all chunks go to a single chunk stripe.
        AddPendingChunk(inputChunk);
        EndTaskIfLarge();
    }

    virtual void InitJobIOConfig() override
    {
        JobIOConfig = BuildJobIOConfig(Config->UnorderedMergeJobIO, Spec->JobIO);
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
        if (IsPassthroughChunk(inputChunk)) {
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
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOrderedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    TMergeOperationSpecPtr Spec;

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

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        if (!Spec->AllowPassthroughChunks)
            return false;

        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
    }

    virtual void InitJobIOConfig() override
    {
        JobIOConfig = BuildJobIOConfig(Config->OrderedMergeJobIO, Spec->JobIO);
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

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        // For erase operation the rowset specified by the user must actually be removed...
        InputTables[0].NegateFetch = true;
        // ...and the output table must be cleared.
        ScheduleClearOutputTables();
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        // If the input is sorted then the output chunk tree must also be marked as sorted.
        const auto& table = InputTables[0];
        if (table.Sorted) {
            ScheduleSetOutputTablesSorted(table.KeyColumns);
        }
    }

    virtual void InitJobIOConfig() override
    {
        JobIOConfig = BuildJobIOConfig(Config->OrderedMergeJobIO, Spec->JobIO);
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::OrderedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(NScheduler::NProto::TMergeJobSpecExt::merge_job_spec_ext);

        // If the input is sorted then the output must also be sorted.
        // For this, the job needs key columns.
        const auto& table = InputTables[0];
        if (table.Sorted) {
            ToProto(jobSpecExt->mutable_key_columns(), table.KeyColumns);
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

        virtual void BuildJobSpec(TJobInProgressPtr jip, NProto::TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->ManiacJobSpecTemplate);
            BuildSpecInputOutput(jip, jobSpec);
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
        int TableIndex;
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

    virtual bool IsLargeEnoughToPassthrough(TRefCountedInputChunkPtr inputChunk) = 0;

    void OnChunkSplitsReceived()
    {
        int prefixLength = static_cast<int>(KeyColumns.size());
        auto& chunks = ChunkSplitsFetcher->GetChunkSplits();
        FOREACH(auto& chunk, chunks) {
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
            auto& endpoint = Endpoints[currentIndex];
            auto nextBreakpoint = GetSuccessorKey(endpoint.Key);
            LOG_DEBUG("Finish current task, flushing %" PRISZT " chunks at key %s",
                openedChunks.size(),
                ~ToString(nextBreakpoint));

            FOREACH (auto& inputChunk, openedChunks) {
                this->AddPendingChunk(SliceChunk(*inputChunk, lastBreakpoint, nextBreakpoint));
            }
            lastBreakpoint = nextBreakpoint;
        };

        while (currentIndex < endpointsCount) {
            auto& endpoint = Endpoints[currentIndex];

            switch (endpoint.Type) {
            case EEndpointType::Left:
                if (openedChunks.empty() && 
                    IsStartingSlice(endpoint.InputChunk) &&
                    AllowPassthroughChunks())
                {
                    // Trying to reconstruct passthrough chunk from chunk slices.
                    auto chunkId = TChunkId::FromProto(endpoint.InputChunk->slice().chunk_id());
                    auto tableIndex = endpoint.InputChunk->TableIndex;
                    auto nextIndex = currentIndex;
                    while (true) {
                        ++nextIndex;
                        if (nextIndex == endpointsCount) {
                            break;
                        }
                        auto nextChunkId = TChunkId::FromProto(Endpoints[nextIndex].InputChunk->slice().chunk_id());
                        auto nextTableIndex = Endpoints[nextIndex].InputChunk->TableIndex;
                        if (nextChunkId != chunkId || tableIndex != nextTableIndex) {
                            break;
                        }
                    }

                    auto lastEndpoint = Endpoints[nextIndex - 1];
                    if (lastEndpoint.Type == EEndpointType::Right && IsEndingSlice(lastEndpoint.InputChunk)) {
                        if (IsLargeEnoughToPassthrough(endpoint.InputChunk)) {
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
                AddPendingChunk(SliceChunk(*endpoint.InputChunk, lastBreakpoint, Null));
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
                    auto& nextEndpoint = Endpoints[nextIndex];

                    if (nextEndpoint.Type == EEndpointType::Maniac && nextEndpoint.Key == endpoint.Key) {
                        if (IsLargeCompleteChunk(nextEndpoint.InputChunk)) {
                            completeLargeManiacSize += nextEndpoint.InputChunk->uncompressed_data_size();
                            completeLargeChunks.push_back(nextEndpoint.InputChunk);
                        } else {
                            partialManiacSize += nextEndpoint.InputChunk->uncompressed_data_size();
                            partialChunks.push_back(nextEndpoint.InputChunk);
                        }
                    } else {
                        break;
                    }
                    ++nextIndex;
                } while (nextIndex != endpointsCount);

                if (AllowPassthroughChunks()) {
                    bool hasManiacTask = partialManiacSize > SpecBase->MaxDataSizePerJob;
                    bool hasPassthroughManiacs = completeLargeManiacSize > 0;

                    if (!hasManiacTask) {
                        FOREACH (auto& chunk, partialChunks) {
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
                        FOREACH (auto& chunk, partialChunks) {
                            AddPendingChunk(chunk);
                            if (HasLargeActiveTask()) {
                                EndManiacTask();
                            }
                        }

                        if (HasActiveTask()) {
                           EndManiacTask();
                        }
                    }

                    YCHECK(!HasActiveTask());
                    FOREACH (auto& chunk, completeLargeChunks) {
                        // Add passthrough maniacs.
                        AddPassthroughChunk(chunk);
                    }
                } else {
                    bool hasManiacTask = partialManiacSize + completeLargeManiacSize > 
                        SpecBase->MaxDataSizePerJob;

                    if (hasManiacTask) {
                        // Complete current task
                        flushOpenedChunks();
                        if (HasActiveTask()) {
                           EndTask();
                        }
                    }

                    FOREACH (auto& chunk, partialChunks) {
                        AddPendingChunk(chunk);
                    }

                    FOREACH (auto& chunk, completeLargeChunks) {
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
            };
        }

        if (HasActiveTask()) {
            EndTask();
        }
    }

    void EndManiacTask()
    {
        auto task = New<TManiacTask>(
            this, 
            static_cast<int>(MergeTasks.size()),
            PartitionCount);

        EndTask(task);
    }

    virtual bool AllowPassthroughChunks() = 0;

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        auto specKeyColumns = GetSpecKeyColumns();
        LOG_INFO("Spec key columns are %s",
            specKeyColumns ? ~ConvertToYsonString(specKeyColumns.Get(), EYsonFormat::Text).Data() : "<Null>");

        KeyColumns = CheckInputTablesSorted(GetSpecKeyColumns());
        LOG_INFO("Adjusted key columns are %s",
            ~ConvertToYsonString(KeyColumns, EYsonFormat::Text).Data());

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
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TSortedMergeControllerBase(config, spec, host, operation)
        , Spec(spec)
    { }

private:
    TMergeOperationSpecPtr Spec;

    virtual void DoInitialize() override
    {
        ScheduleClearOutputTables();
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

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        if (!Spec->AllowPassthroughChunks)
            return false;

        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
    }

    virtual bool AllowPassthroughChunks() override
    {
        return Spec->AllowPassthroughChunks;
    }

    virtual bool IsLargeEnoughToPassthrough(TRefCountedInputChunkPtr inputChunk)
    {
        if (!Spec->CombineChunks)
            return true;

        return IsLargeChunk(inputChunk);
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->MergeBy;
    }

    virtual void InitJobIOConfig() override
    {
        JobIOConfig = BuildJobIOConfig(Config->SortedMergeJobIO, Spec->JobIO);
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedMerge);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(NScheduler::NProto::TMergeJobSpecExt::merge_job_spec_ext);
        ToProto(jobSpecExt->mutable_key_columns(), KeyColumns);

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
        ManiacJobSpecTemplate.set_type(EJobType::UnorderedMerge);
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        TSortedMergeControllerBase::OnCustomInputsRecieved(batchRsp);

        ScheduleSetOutputTablesSorted(KeyColumns);
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
    { }

    virtual NProto::TNodeResources GetMinNeededResources() override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(Spec->Reducer->CpuLimit);
        result.set_memory(
            GetIOMemorySize(JobIOConfig, Spec->InputTablePaths.size(), Spec->OutputTablePaths.size()) +
            Spec->Reducer->MemoryLimit +
            GetFootprintMemorySize());
        return result;
    }

private:
    TReduceOperationSpecPtr Spec;

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TRichYPath> GetFilePaths() const override
    {
        return Spec->Reducer->FilePaths;
    }

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        YUNREACHABLE();
    }

    virtual bool AllowPassthroughChunks() override
    {
        return false;
    }

    virtual bool IsLargeEnoughToPassthrough(TRefCountedInputChunkPtr inputChunk)
    {
        return false;
    }

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() override
    {
        return Spec->ReduceBy;
    }

    virtual void InitJobIOConfig() override
    {
        JobIOConfig = BuildJobIOConfig(Config->SortedReduceJobIO, Spec->JobIO);
    }

    virtual void InitJobSpecTemplate() override
    {
        JobSpecTemplate.set_type(EJobType::SortedReduce);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(NScheduler::NProto::TReduceJobSpecExt::reduce_job_spec_ext);
        ToProto(jobSpecExt->mutable_key_columns(), KeyColumns);

        InitUserJobSpec(
            jobSpecExt->mutable_reducer_spec(),
            Spec->Reducer,
            Files);

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());

        ManiacJobSpecTemplate.CopyFrom(JobSpecTemplate);
    }
};

////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMergeOperationSpec>(operation);
    switch (spec->Mode) {
        case EMergeMode::Unordered:
            return New<TUnorderedMergeController>(config, spec, host, operation);
        case EMergeMode::Ordered:
            return New<TOrderedMergeController>(config, spec, host, operation);
        case EMergeMode::Sorted:
            return New<TSortedMergeController>(config, spec, host, operation);
        default:
            YUNREACHABLE();
    }
}

IOperationControllerPtr CreateEraseController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TEraseOperationSpec>(operation);
    return New<TEraseController>(config, spec, host, operation);
}

IOperationControllerPtr CreateReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TReduceOperationSpec>(operation);
    return New<TReduceController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

