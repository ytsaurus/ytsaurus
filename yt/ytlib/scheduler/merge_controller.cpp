#include "stdafx.h"
#include "merge_controller.h"
#include "private.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"

#include <ytlib/ytree/fluent.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_holder/chunk_meta_extensions.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NJobProxy;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NChunkHolder::NProto;

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
        , Spec(spec)
        , TotalJobCount(0)
        , CurrentTaskDataSize(0)
        , PartitionCount(0)
    { }

private:
    TMergeOperationSpecBasePtr Spec;

protected:
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
            return Controller->Spec->LocalityTimeout;
        }
        
        virtual NProto::TNodeResources GetMinRequestedResources() const override
        {
            return Controller->GetMinRequestedResources();
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
            AddParallelInputSpec(jobSpec, jip);

            for (int i = 0; i < Controller->OutputTables.size(); ++i) {
                // Reduce task can have multiple output tables.
                AddTabularOutputSpec(jobSpec, jip, i);
            }
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
    
    //! Finishes the current task.
    void EndTask()
    {
        YCHECK(HasActiveTask());

        auto& table = OutputTables[0];
        auto task = New<TMergeTask>(
            this,
            static_cast<int>(MergeTasks.size()),
            PartitionCount);

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
        return CurrentTaskDataSize >= Spec->MaxDataSizePerJob;
    }

    //! Add chunk to the current task's pool.
    void AddPendingChunk(TRefCountedInputChunkPtr inputChunk, int tableIndex)
    {
        // Merge is IO-bound, use data size as weight.
        auto miscExt = GetProtoExtension<TMiscExt>(inputChunk->extensions());
        i64 dataSize = miscExt.uncompressed_data_size();
        i64 rowCount = miscExt.row_count();

        auto stripe = CurrentTaskStripes[tableIndex];
        if (!stripe) {
            stripe = CurrentTaskStripes[tableIndex] = New<TChunkStripe>();
        }

        DataSizeCounter.Increment(dataSize);
        ChunkCounter.Increment(1);
        CurrentTaskDataSize += dataSize;
        stripe->AddChunk(inputChunk, dataSize, rowCount);

        auto& table = OutputTables[0];
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        LOG_DEBUG("Added pending chunk (ChunkId: %s, Partition: %d, Task: %d, TableIndex: %d, DataSize: %" PRId64 ", RowCount: %" PRId64 ")",
            ~chunkId.ToString(),
            PartitionCount,
            static_cast<int>(MergeTasks.size()),
            tableIndex,
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
            ythrow yexception() << "At least one input table must be given";
        }
    }

    // Custom bits of preparation pipeline.

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline->Add(BIND(&TMergeControllerBase::ProcessInputs, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            InitJobIOConfig();
            ClearCurrentTaskStripes();
            BeginInputChunks();

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                FOREACH (auto& inputChunk, *table.FetchResponse->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());
                    auto miscExt = GetProtoExtension<TMiscExt>(inputChunk.extensions());
                    i64 dataSize = miscExt.uncompressed_data_size();
                    i64 rowCount = miscExt.row_count();
                    LOG_DEBUG("Processing chunk (ChunkId: %s, DataSize: %" PRId64 ", RowCount: %" PRId64 ")",
                        ~chunkId.ToString(),
                        dataSize,
                        rowCount);
                    ProcessInputChunk(New<TRefCountedInputChunk>(inputChunk), tableIndex);
                }
            }

            EndInputChunks();

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
    }


    //! Called at the beginning of input chunks scan.
    virtual void BeginInputChunks()
    { }

    //! Called for each input chunk.
    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk, int tableIndex) = 0;

    //! Called at the end of input chunks scan.
    virtual void EndInputChunks()
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

    virtual TNodeResources GetMinRequestedResources() const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cores(1);
        result.set_memory(
            GetIOMemorySize(JobIOConfig, GetInputTablePaths().size(), 1) +
            GetFootprintMemorySize());
        return result;
    }

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(TRefCountedInputChunkPtr inputChunk)
    {
        return
            !inputChunk->slice().start_limit().has_row_index() &&
            !inputChunk->slice().start_limit().has_key () &&
            !inputChunk->slice().end_limit().has_row_index() &&
            !inputChunk->slice().end_limit().has_key ();
    }

    //! Returns True if the chunk can be included into the output as-is.
    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) = 0;

    //! Returns True iff the chunk is complete and is large enough.
    bool IsLargeCompleteChunk(TRefCountedInputChunkPtr inputChunk)
    {
        if (!IsCompleteChunk(inputChunk)) {
            return false;
        }

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
        return Spec->CombineChunks ? IsLargeCompleteChunk(inputChunk) : IsCompleteChunk(inputChunk);
    }

    virtual std::vector<TYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() const override
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk, int tableIndex) override
    {
        UNUSED(tableIndex);

        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsPassthroughChunk(inputChunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(inputChunk);
            return;
        }

        // NB: During unordered merge all chunks go to a single chunk stripe.
        AddPendingChunk(inputChunk, 0);
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

        // ToDo(psushin): set larger PrefetchWindow for unordered merge.
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
    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk, int tableIndex) override
    {
        UNUSED(tableIndex);

        if (IsPassthroughChunk(inputChunk)) {
            // Merge is not needed. Copy the chunk directly to the output.
            if (HasActiveTask()) {
                EndTask();
            }
            AddPassthroughChunk(inputChunk);
            return;
        }

        // NB: During ordered merge all chunks go to a single chunk stripe.
        AddPendingChunk(inputChunk, 0);
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

    virtual std::vector<TYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() const override
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
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

    virtual std::vector<TYPath> GetInputTablePaths() const override
    {
        std::vector<TYPath> result;
        result.push_back(Spec->TablePath);
        return result;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() const override
    {
        std::vector<TYPath> result;
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
    //! Either the left or the right endpoint of a chunk.
    struct TKeyEndpoint
    {
        bool Left;
        int TableIndex;
        NTableClient::NProto::TKey Key;
        TRefCountedInputChunkPtr InputChunk;
    };

    std::vector<TKeyEndpoint> Endpoints;
    
    //! The actual (adjusted) key columns.
    std::vector<Stroka> KeyColumns;

    virtual TNullable< std::vector<Stroka> > GetSpecKeyColumns() = 0;

    virtual void ProcessInputChunk(TRefCountedInputChunkPtr inputChunk, int tableIndex) override
    {
        auto miscExt = GetProtoExtension<TMiscExt>(inputChunk->extensions());
        YCHECK(miscExt.sorted());

        // Construct endpoints and place them into the list.
        auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(inputChunk->extensions());
        {
            TKeyEndpoint endpoint;
            endpoint.Left = true;
            endpoint.TableIndex = tableIndex;
            endpoint.Key = boundaryKeysExt.start();
            endpoint.InputChunk = inputChunk;
            Endpoints.push_back(endpoint);
        }
        {
            TKeyEndpoint endpoint;
            endpoint.Left = false;
            endpoint.TableIndex = tableIndex;
            endpoint.Key = boundaryKeysExt.end();
            endpoint.InputChunk = inputChunk;
            Endpoints.push_back(endpoint);
        }
    }

    virtual void EndInputChunks() override
    {
        // Sort earlier collected endpoints to figure out overlapping chunks.
        // Sort endpoints by keys, in case of a tie left endpoints go first.
        LOG_INFO("Sorting chunks");

        int prefixLength = static_cast<int>(KeyColumns.size());
        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [=] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                auto keysResult = CompareKeys(lhs.Key, rhs.Key, prefixLength);
                if (keysResult != 0) {
                    return keysResult < 0;
                }
                return lhs.Left && !rhs.Left;
            });

        // Compute components consisting of overlapping chunks.
        // Combine small tasks, if requested so.
        LOG_INFO("Building tasks");
        int depth = 0;
        int startIndex = 0;
        for (
            int currentIndex = startIndex; 
            currentIndex < static_cast<int>(Endpoints.size()); 
            ++currentIndex) 
        {
            auto& endpoint = Endpoints[currentIndex];
            if (endpoint.Left) {
                ++depth;
            } else {
                --depth;
                if (depth == 0) {
                    ProcessOverlap(startIndex, currentIndex + 1);
                    startIndex = currentIndex + 1;
                }
            }
        }

        TMergeControllerBase::EndInputChunks();
    }

    void ProcessOverlap(int startIndex, int endIndex)
    {
        using NTableClient::ToString;
        using ::ToString;

        // Must be an even number of endpoints.
        YCHECK((endIndex - startIndex) % 2 == 0);

        int chunkCount = (endIndex - startIndex) / 2;
        LOG_DEBUG("Found overlap of %d chunks", chunkCount);

        // Check for trivial components.
        {
            auto inputChunk = Endpoints[startIndex].InputChunk;
            if (chunkCount == 1 && IsPassthroughChunk(inputChunk)) {
                // No merge is needed. Copy the chunk directly to the output.
                if (HasActiveTask()) {
                    EndTask();
                }
                AddPassthroughChunk(inputChunk);
                return;
            }
        }

        TNullable<NTableClient::NProto::TKey> lastBreakpoint;
        yhash_map<TRefCountedInputChunkPtr, TKeyEndpoint*> openedChunks;

        for (int index = startIndex; index < endIndex; ++index) {
            auto& endpoint = Endpoints[index];
            auto chunkId = TChunkId::FromProto(endpoint.InputChunk->slice().chunk_id());
            if (endpoint.Left) {
                YCHECK(openedChunks.insert(std::make_pair(endpoint.InputChunk, &endpoint)).second);
                LOG_DEBUG("Chunk interval opened (ChunkId: %s)", ~chunkId.ToString());
            } else {
                AddPendingChunk(
                    SliceChunk(*endpoint.InputChunk, lastBreakpoint, Null),
                    endpoint.TableIndex);
                YCHECK(openedChunks.erase(endpoint.InputChunk) == 1);
                LOG_DEBUG("Chunk interval closed (ChunkId: %s)", ~chunkId.ToString());
                if (!openedChunks.empty() &&
                    HasLargeActiveTask() &&
                    // Avoid producing empty slices.
                    index < endIndex - 1 &&
                    endpoint.Key < Endpoints[index + 1].Key)
                {
                    auto nextBreakpoint = GetSuccessorKey(endpoint.Key);
                    LOG_DEBUG("Task is too large, flushing %" PRISZT " chunks at key %s",
                        openedChunks.size(),
                        ~ToString(nextBreakpoint));
                    FOREACH (const auto& pair, openedChunks) {
                        AddPendingChunk(
                            SliceChunk(*pair.second->InputChunk, lastBreakpoint, nextBreakpoint),
                            pair.second->TableIndex);
                    }
                    EndTask();
                    LOG_DEBUG("Finished flushing opened chunks");
                    lastBreakpoint = nextBreakpoint;
                }
            }
        }

        YCHECK(openedChunks.empty());
        EndTaskIfLarge();
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        auto specKeyColumns = GetSpecKeyColumns();
        LOG_INFO("Spec key columns are %s",
            specKeyColumns ? ~ConvertToYsonString(specKeyColumns.Get(), EYsonFormat::Text).Data() : "<Null>");

        KeyColumns = CheckInputTablesSorted(GetSpecKeyColumns());
        LOG_INFO("Adjusted key columns are %s",
            ~ConvertToYsonString(KeyColumns, EYsonFormat::Text).Data());
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

    virtual std::vector<TYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() const override
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        return IsPassthroughChunkImpl(inputChunk, Spec->CombineChunks);
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

private:
    TReduceOperationSpecPtr Spec;

    virtual std::vector<TYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TYPath> GetFilePaths() const override
    {
        return Spec->Reducer->FilePaths;
    }

    virtual bool IsPassthroughChunk(TRefCountedInputChunkPtr inputChunk) override
    {
        UNUSED(inputChunk);
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
    }

    virtual NProto::TNodeResources GetMinRequestedResources() const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cores(Spec->Reducer->CoresLimit);
        result.set_memory(
            GetIOMemorySize(JobIOConfig, Spec->InputTablePaths.size(), Spec->OutputTablePaths.size()) +
            Spec->Reducer->MemoryLimit +
            GetFootprintMemorySize());
        return result;
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

