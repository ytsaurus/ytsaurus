#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "private.h"
#include "chunk_list_pool.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/value.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
static NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////

class TMergeControllerBase
    : public TOperationControllerBase
{
public:
    TMergeControllerBase(
        TSchedulerConfigPtr config,
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
        , Spec(spec)
    { }

private:
    typedef TMergeControllerBase TThis;

protected:
    TMergeOperationSpecPtr Spec;

    // Running counters.
    TProgressCounter ChunkCounter;
    TProgressCounter WeightCounter;
    // XXX(babenko): currently this is only initialized but not updated during operation lifetime.
    TProgressCounter RowCounter;


    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;


    // Merge groups.
    struct TMergeGroup
        : public TIntrinsicRefCounted
    {
        TMergeGroup()
            : PartitionIndex(-1)
        { }

        TAutoPtr<IChunkPool> ChunkPool;

        //! The position in |Groups|. 
        int GroupIndex;

        //! The position in |TOutputTable::PartitionIds| where the 
        //! output of this group must be placed.
        int PartitionIndex;
    };

    typedef TIntrusivePtr<TMergeGroup> TMergeGroupPtr;

    std::vector<TMergeGroupPtr> Groups;
    TMergeGroupPtr CurrentGroup;

    //! Start a new group.
    TMergeGroupPtr BeginGroup(TAutoPtr<IChunkPool> chunkPool)
    {
        YASSERT(!CurrentGroup);

        auto& table = OutputTables[0];

        auto group = New<TMergeGroup>();
        group->ChunkPool = chunkPool;
        group->GroupIndex = static_cast<int>(Groups.size());
        group->PartitionIndex = static_cast<int>(table.PartitionTreeIds.size());

        // Reserve a place for this group among partitions.
        table.PartitionTreeIds.push_back(NullChunkListId);

        Groups.push_back(group);
        CurrentGroup = group;

        LOG_DEBUG("Created group %d", group->GroupIndex);
        return group;
    }

    //! Finish the current group.
    void EndGroup()
    {
        YASSERT(CurrentGroup);
        LOG_DEBUG("Finished group %d (DataSize: %" PRId64 ")",
            static_cast<int>(Groups.size()) - 1,
            CurrentGroup->ChunkPool->GetTotalWeight());
        CurrentGroup.Reset();
    }

    //! Finish the current group if the size is large enough.
    void EndGroupIfLarge()
    {
        if (CurrentGroup->ChunkPool->GetTotalWeight() >= Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize) {
            EndGroup();
        }
    }


    // Chunk pools and locality.

    yhash_map<Stroka, yhash_set<TMergeGroupPtr> > AddressToGroupsWithPendingChunks;
    yhash_set<TMergeGroupPtr> GroupsWithPendingChunks;

    void RegisterPendingChunk(TMergeGroupPtr group, TPooledChunkPtr chunk)
    {
        GroupsWithPendingChunks.insert(group);
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            AddressToGroupsWithPendingChunks[address].insert(group);
        }
    }

    //! Add chunk to the current group's pool.
    void AddPooledChunk(TPooledChunkPtr chunk)
    {
        YASSERT(CurrentGroup);

        auto chunkId = TChunkId::FromProto(chunk->InputChunk.slice().chunk_id());
        RowCounter.Increment(chunk->InputChunk.has_approximate_row_count());
        WeightCounter.Increment(chunk->InputChunk.approximate_data_size());
        ChunkCounter.Increment(1);
        CurrentGroup->ChunkPool->Add(chunk);
        RegisterPendingChunk(CurrentGroup, chunk);

        LOG_DEBUG("Added pooled chunk %s in partition %d, group %d",
            ~chunkId.ToString(),
            CurrentGroup->PartitionIndex,
            CurrentGroup->GroupIndex);
    }

    //! Add chunk directly to the output.
    void AddPassthroughChunk(const TInputChunk& chunk)
    {
        auto& table = OutputTables[0];
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        LOG_DEBUG("Added passthrough chunk %s in partition %d",
            ~chunkId.ToString(),
            static_cast<int>(table.PartitionTreeIds.size()));
        // Place the chunk directly to the output table.
        table.PartitionTreeIds.push_back(chunkId);
    }

    IChunkPool::TExtractResultPtr ExtractChunksFromPool(
        TMergeGroupPtr group,
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        auto result = group->ChunkPool->Extract(
            address,
            weightThreshold,
            maxCount,
            needLocal);
        YASSERT(result);

        if (!group->ChunkPool->HasPendingChunks()) {
            YVERIFY(GroupsWithPendingChunks.erase(group) == 1);
        }

        FOREACH (const auto& chunk, result->Chunks) {
            FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
                if (!group->ChunkPool->HasPendingLocalChunksFor(address)) {
                    AddressToGroupsWithPendingChunks[address].erase(group);
                }
            }
        }

        return result;
    }

    void PutChunksBackToPool(TMergeGroupPtr group, IChunkPool::TExtractResultPtr result)
    {
        group->ChunkPool->PutBack(result);
        FOREACH (const auto& chunk, result->Chunks) {
            RegisterPendingChunk(group, chunk);
        }
    }

    TMergeGroupPtr GetGroupFor(const Stroka& address)
    {
        // Try to fetch a group with local chunks.
        auto it = AddressToGroupsWithPendingChunks.find(address);
        if (it != AddressToGroupsWithPendingChunks.end()) {
            const auto& set = it->second;
            if (!set.empty()) {
                return *set.begin();
            }
        }

        // Fetch any group.
        YASSERT(!GroupsWithPendingChunks.empty());
        return *GroupsWithPendingChunks.begin();
    }


    // Init/finish.

    virtual void CustomInitialize()
    {
        if (InputTables.empty()) {
            // At least one table is needed for sorted merge to figure out the key columns.
            // To be consistent, we don't allow empty set of input tables in for any merge type.
            ythrow yexception() << "At least more input table must be given";
        }
    }

    virtual bool HasPendingJobs()
    {
        // Use the chunk counter, not the job counter! The latter may be inaccurate.
        return ChunkCounter.GetPending() > 0;
    }


    // Job scheduling and outcome handling.

    struct TJobInProgress
        : public TIntrinsicRefCounted
    {
        IChunkPool::TExtractResultPtr ExtractResult;
        TChunkListId ChunkListId;
        TMergeGroupPtr Group;
    };

    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(1)) {
            return NULL;
        }

        // We've got a job to do! :)

        // Allocate chunks for the job.
        auto group = GetGroupFor(node->GetAddress());
        i64 weightThreshold = GetJobWeightThreshold(JobCounter.GetPending(), WeightCounter.GetPending());
        auto jip = New<TJobInProgress>();
        jip->Group = group;
        jip->ExtractResult = ExtractChunksFromPool(
            group,
            node->GetAddress(),
            weightThreshold,
            std::numeric_limits<int>::max(),
            false);
        YASSERT(!jip->ExtractResult->Chunks.empty());

        LOG_DEBUG("Extracted %d chunks, %d local for node %s (ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            static_cast<int>(jip->ExtractResult->Chunks.size()),
            jip->ExtractResult->LocalCount,
            ~node->GetAddress(),
            jip->ExtractResult->Weight,
            weightThreshold);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        auto* mergeJobSpec = jobSpec.MutableExtension(TMergeJobSpec::merge_job_spec);
        FOREACH (const auto& chunk, jip->ExtractResult->Chunks) {
            *mergeJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }
        jip->ChunkListId = ChunkListPool->Extract();
        *mergeJobSpec->mutable_output_spec()->mutable_chunk_list_id() = jip->ChunkListId.ToProto();

        // Update running counters.
        ChunkCounter.Start(jip->ExtractResult->Chunks.size());
        WeightCounter.Start(jip->ExtractResult->Weight);

        return CreateJob(
            Operation,
            node,
            jobSpec,
            BIND(&TThis::OnJobCompleted, MakeWeak(this), jip),
            BIND(&TThis::OnJobFailed, MakeWeak(this), jip));
        return NULL;
    }

    virtual void OnJobCompleted(TJobInProgressPtr jip)
    {
        auto group = jip->Group;
        auto& table = OutputTables[0];
        table.PartitionTreeIds[group->PartitionIndex] = jip->ChunkListId;

        ChunkCounter.Completed(jip->ExtractResult->Chunks.size());
        WeightCounter.Completed(jip->ExtractResult->Weight);
    }

    virtual void OnJobFailed(TJobInProgressPtr jip)
    {
        LOG_DEBUG("Returned %d chunks back into the pool", static_cast<int>(jip->ExtractResult->Chunks.size()));
        PutChunksBackToPool(jip->Group, jip->ExtractResult);

        ChunkCounter.Failed(jip->ExtractResult->Chunks.size());
        WeightCounter.Failed(jip->ExtractResult->Weight);

        ReleaseChunkList(jip->ChunkListId);
    }


    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths()
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths()
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual std::vector<TYPath> GetFilePaths()
    {
        return std::vector<TYPath>();
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
    {
        return pipeline->Add(BIND(&TThis::ProcessInputs, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            i64 totalRowCount = 0;
            i64 totalDataSize = 0;
            i64 totalChunkCount = 0;

            BeginInputChunks();

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                auto fetchRsp = table.FetchResponse;
                FOREACH (auto& chunk, *fetchRsp->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                    i64 dataSize = chunk.approximate_data_size();
                    i64 rowCount = chunk.approximate_row_count();
                    LOG_DEBUG("Processing chunk %s (DataSize: %" PRId64 ", RowCount: %" PRId64 ")",
                        ~chunkId.ToString(),
                        dataSize,
                        rowCount);
                    totalRowCount += rowCount;
                    totalDataSize += dataSize;
                    totalChunkCount += 1;
                    ProcessInputChunk(chunk);
                }
            }

            EndInputChunks();

            // Check for empty and trivial inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            if (ChunkCounter.GetPending() == 0) {
                LOG_INFO("Trivial merge");
                FinalizeOperation();
                return;
            }

            // Init counters.
            JobCounter.Set(ComputeJobCount());

            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(JobCounter.GetPending() + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", MergeRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", MergeDataSize: %" PRId64 ", TotalChunkCount: %" PRId64 ", MergeChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
                totalRowCount,
                RowCounter.GetPending(),
                totalDataSize,
                WeightCounter.GetPending(),
                totalChunkCount,
                ChunkCounter.GetPending(),
                JobCounter.GetPending());
        }
    }


    //! Called at the beginning of input chunks scan.
    virtual void BeginInputChunks()
    { }

    //! Called for each input chunk.
    virtual void ProcessInputChunk(const TInputChunk& chunk) = 0;

    //! Called at the end of input chunks scan.
    virtual void EndInputChunks()
    {
        // Close the last group, if any.
        if (CurrentGroup) {
            EndGroup();
        }
    }


    // Job count selection.

    virtual i64 ComputeJobCount() = 0;


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: Jobs = {%s}, Chunks = {%s}, Weight = {%s}",
            ~ToString(JobCounter),
            ~ToString(ChunkCounter),
            ~ToString(WeightCounter));
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("chunks").Do(BIND(&TProgressCounter::ToYson, &ChunkCounter))
            .Item("weight").Do(BIND(&TProgressCounter::ToYson, &WeightCounter));
    }


    // Unsorted helpers.

    //! Increments the counters.

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(const TInputChunk& chunk)
    {
        return
            !chunk.slice().start_limit().has_row_index() &&
            !chunk.slice().start_limit().has_key () &&
            !chunk.slice().end_limit().has_row_index() &&
            !chunk.slice().end_limit().has_key ();
    }

    //! Returns True iff the chunk is complete and is large enough to be included to the output as-is.
    //! When |CombineChunks| is off all complete chunks are considered large.
    bool IsLargeCompleteChunk(const TInputChunk& chunk)
    {
        if (!IsCompleteChunk(chunk)) {
            return true;
        }

        if (chunk.approximate_data_size() >= Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize) {
            return true;
        }

        if (!Spec->CombineChunks) {
            return true;
        }

        return false;
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(
            Spec->Mode == EMergeMode::Sorted
            ? EJobType::SortedMerge
            : EJobType::OrderedMerge);

        TMergeJobSpec mergeJobSpec;
        *mergeJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        mergeJobSpec.mutable_output_spec()->set_schema(OutputTables[0].Schema);
        *JobSpecTemplate.MutableExtension(TMergeJobSpec::merge_job_spec) = mergeJobSpec;

        JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));
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
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
    { }

private:
    virtual void ProcessInputChunk(const TInputChunk& chunk)
    {
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsLargeCompleteChunk(chunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(chunk);
            return;
        }

        // Create a merge group if not exists.
        if (!CurrentGroup) {
            BeginGroup(CreateUnorderedChunkPool());
        }

        // Merge is IO-bound, use data size as weight.
        auto pooledChunk = New<TPooledChunk>(
            chunk,
            chunk.approximate_data_size());
        AddPooledChunk(pooledChunk);
    }

    virtual i64 ComputeJobCount()
    {
        // Choose job count.
        // TODO(babenko): refactor, generalize, and improve.
        i64 jobCount = (i64) std::ceil((double) WeightCounter.GetPending() / Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize);
        if (Spec->JobCount) {
            jobCount = Spec->JobCount.Get();
        }
        jobCount = std::min(jobCount, ChunkCounter.GetPending());
        YASSERT(jobCount > 0);
        return jobCount;
    }
};

////////////////////////////////////////////////////////////////////

//! Handles ordered merge and (sic!) erase operations.
class TOrderedMergeController
    : public TMergeControllerBase
{
public:
    TOrderedMergeController(
        TSchedulerConfigPtr config,
        EOperationType operationType,
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
        , OperationType(operationType)
    { }

private:
    EOperationType OperationType;

    virtual void CustomInitialize()
    {
        if (OperationType == EOperationType::Erase) {
            // For erase operation the rowset specified by the user must actually be removed.
            InputTables[0].NegateFetch = true;
        }
    }

    virtual void OnCustomInputsRecieved(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        UNUSED(batchRsp);

        if (OperationType == EOperationType::Erase) {
            // For erase operation:
            // - the output must be empty
            CheckOutputTablesEmpty();
            // - if the input is sorted then the output is marked as sorted as well
            if (InputTables[0].Sorted) {
                SetOutputTablesSorted(InputTables[0].KeyColumns);
            }
        }
    }

    virtual void ProcessInputChunk(const TInputChunk& chunk)
    {
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsLargeCompleteChunk(chunk) && !CurrentGroup) {
            // Merge is not required and no current group is active.
            // Copy the chunk directly to the output.
            LOG_DEBUG("Chunk %s is large and complete, using as-is in partition %d",
                ~chunkId.ToString(),
                static_cast<int>(table.PartitionTreeIds.size()));
            table.PartitionTreeIds.push_back(chunkId);
            return;
        }

        // Ensure that the current group exists.
        if (!CurrentGroup) {
            BeginGroup(CreateAtomicChunkPool());
        }

        // Merge is IO-bound, use data size as weight.
        auto pooledChunk = New<TPooledChunk>(
            chunk,
            chunk.approximate_data_size());
        AddPooledChunk(pooledChunk);

        EndGroupIfLarge();
    }

    virtual i64 ComputeJobCount()
    {
        // Each group corresponds to a unique job.
        return Groups.size();
    }
};

////////////////////////////////////////////////////////////////////

//! Handles sorted merge operation.
class TSortedMergeController
    : public TMergeControllerBase
{
public:
    TSortedMergeController(
        TSchedulerConfigPtr config,
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
    { }

private:
    // Either the left or the right endpoint of a chunk.
    struct TKeyEndpoint
    {
        bool Left;
        NTableClient::TKey Key;
        const TInputChunk* InputChunk;
    };

    std::vector<TKeyEndpoint> Endpoints;

    virtual void ProcessInputChunk(const TInputChunk& chunk)
    {
        // Deserialize attributes.
        TChunkAttributes chunkAttributes;
        YVERIFY(DeserializeFromProto(&chunkAttributes, TRef::FromString(chunk.chunk_attributes())));
        const auto& tableAttributes = chunkAttributes.GetExtension(TTableChunkAttributes::table_attributes);
        YASSERT(tableAttributes.sorted());

        // Construct endpoints and place it into the list.
        const auto& samples = tableAttributes.key_samples();
        {
            TKeyEndpoint endpoint;
            endpoint.Left = true;
            endpoint.Key = FromProto<Stroka>(samples.Get(0).key().values());
            endpoint.InputChunk = &chunk;
            Endpoints.push_back(endpoint);
        }
        {
            TKeyEndpoint endpoint;
            endpoint.Left = false;
            endpoint.Key = FromProto<Stroka>(samples.Get(samples.size() - 1).key().values());
            endpoint.InputChunk = &chunk;
            Endpoints.push_back(endpoint);
        }
    }

    virtual void EndInputChunks()
    {
        // Sort earlier collected endpoints to figure out overlapping chunks.
        // Sort endpoints by keys, in case of a tie left endpoints go first.
        LOG_INFO("Sorting chunks");
        std::sort(
            Endpoints.begin(),
            Endpoints.end(),
            [] (const TKeyEndpoint& lhs, const TKeyEndpoint& rhs) -> bool {
                auto keysResult = CompareKeys(lhs.Key, rhs.Key);
                if (keysResult != 0) {
                    return keysResult < 0;
                }
                return lhs.Left && !rhs.Left;
            });

        // Compute sets of overlapping chunks.
        // Combine small groups, if requested so.
        LOG_INFO("Building groups");
        int depth = 0;
        std::vector<const TInputChunk*> chunks;
        FOREACH (const auto& endpoint, Endpoints) {
            if (endpoint.Left) {
                ++depth;
                chunks.push_back(endpoint.InputChunk);
            } else {
                --depth;
                if (depth == 0) {
                    BuildGroupIfNeeded(chunks);
                    chunks.clear();
                }
            }
        }

        // Force all output tables to be marked as sorted.
        auto keyColumns = GetInputKeyColumns();
        SetOutputTablesSorted(keyColumns);
    }

    void BuildGroupIfNeeded(const std::vector<const TInputChunk*>& chunks)
    {
        auto& table = OutputTables[0];

        LOG_DEBUG("Found overlap of %d chunks", static_cast<int>(chunks.size()));

        if (chunks.size() == 1 && IsLargeCompleteChunk(*chunks[0]) && !CurrentGroup) {
            const auto& chunk = *chunks[0];
            auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
            // Merge is not required and no current group is active.
            // Copy the chunk directly to the output.
            AddPassthroughChunk(chunk);
            return;
        }

        // Ensure that the current group exists.
        if (!CurrentGroup) {
            // TODO(babenko): use merge pool instead
            BeginGroup(CreateAtomicChunkPool());
        }

        FOREACH (auto chunk, chunks) {
            // Merge is IO-bound, use data size as weight.
            auto pooledChunk = New<TPooledChunk>(
                *chunk,
                chunk->approximate_data_size());
            AddPooledChunk(pooledChunk);
        }

        EndGroupIfLarge();
    }


    virtual void OnCustomInputsRecieved(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        UNUSED(batchRsp);

        CheckInputTablesSorted();
        CheckOutputTablesEmpty();
    }


    virtual i64 ComputeJobCount()
    {
        // TODO(babenko): fixme, for now each group corresponds to a unique job.
        return Groups.size();
    }
};

////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = New<TMergeOperationSpec>();
    try {
        spec->Load(~operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }

    switch (spec->Mode) {
        case EMergeMode::Unordered:
            return New<TUnorderedMergeController>(config, spec, host, operation);
        case EMergeMode::Ordered:
            return New<TOrderedMergeController>(config, EOperationType::Merge, spec, host, operation);
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
    auto eraseSpec = New<TEraseOperationSpec>();
    try {
        eraseSpec->Load(~operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }

    // Create a fake spec for the ordered merge controller.
    auto mergeSpec = New<TMergeOperationSpec>();
    mergeSpec->InputTablePaths.push_back(eraseSpec->InputTablePath);
    mergeSpec->OutputTablePath = eraseSpec->OutputTablePath;
    mergeSpec->Mode = EMergeMode::Ordered;
    mergeSpec->CombineChunks = eraseSpec->CombineChunks;
    return New<TOrderedMergeController>(config, EOperationType::Erase, mergeSpec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

