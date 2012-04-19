#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "private.h"

#include <ytlib/ytree/fluent.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
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

        //! The position in |TOutputTable::PartitionIds| where the 
        //! output of this group must be placed.
        //! -1 indicates that no particular position is preallocated and
        //! the output must be appended to the end.
        int PartitionIndex;
    };

    typedef TIntrusivePtr<TMergeGroup> TMergeGroupPtr;

    std::vector<TMergeGroupPtr> Groups;
    TMergeGroupPtr CurrentGroup;

    TMergeGroupPtr GetCurrentGroup()
    {
        return CurrentGroup;
    }

    TMergeGroupPtr BeginGroup()
    {
        YASSERT(!CurrentGroup);
        auto group = New<TMergeGroup>();
        LOG_DEBUG("Created group %d", static_cast<int>(Groups.size()));
        Groups.push_back(group);
        CurrentGroup = group;
        return group;
    }

    void EndGroup()
    {
        YASSERT(CurrentGroup);
        LOG_DEBUG("Finished group %d (DataSize: %" PRId64 ")",
            static_cast<int>(Groups.size()) - 1,
            CurrentGroup->ChunkPool->GetTotalWeight());
        CurrentGroup.Reset();
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

    void AddChunkToPool(TMergeGroupPtr group, TPooledChunkPtr chunk)
    {
        group->ChunkPool->Add(chunk);
        RegisterPendingChunk(group, chunk);
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

    virtual void DoInitialize()
    { }

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
        if (group->PartitionIndex >= 0) {
            table.PartitionTreeIds[group->PartitionIndex] = jip->ChunkListId;
        } else {
            table.PartitionTreeIds.push_back(jip->ChunkListId);
        }

        ChunkCounter.Completed(jip->ExtractResult->Chunks.size());
        WeightCounter.Completed(jip->ExtractResult->Weight);
    }

    virtual void OnJobFailed(TJobInProgressPtr jip)
    {
        LOG_DEBUG("Returned %d chunks back into the pool", static_cast<int>(jip->ExtractResult->Chunks.size()));
        jip->Group->ChunkPool->PutBack(jip->ExtractResult);

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

    virtual void DoCompletePreparation()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            i64 totalRowCount = 0;
            i64 mergeRowCount = 0;
            i64 totalDataSize = 0;
            i64 mergeDataSize = 0;
            i64 totalChunkCount = 0;
            i64 mergeChunkCount = 0;

            BeginInputChunksProcessing();

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                auto fetchRsp = table.FetchResponse;
                FOREACH (auto& chunk, *fetchRsp->mutable_chunks()) {
                    i64 rowCount = chunk.approximate_row_count();
                    i64 dataSize = chunk.approximate_data_size();

                    totalRowCount += rowCount;
                    totalDataSize += dataSize;
                    totalChunkCount += 1;

                    if (ProcessInputChunk(chunk)) {
                        mergeRowCount += rowCount;
                        mergeDataSize += dataSize;
                        mergeChunkCount += 1;
                    }
                }
            }

            EndInputChunksProcessing();

            // Check for empty and trivial inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            if (mergeChunkCount == 0) {
                LOG_INFO("Trivial merge");
                FinalizeOperation();
                return;
            }

            // Init counters.
            ChunkCounter.Set(mergeChunkCount);
            WeightCounter.Set(mergeDataSize);
            ChooseJobCount();

            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(JobCounter.GetPending() + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", MergeRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", MergeDataSize: %" PRId64 ", TotalChunkCount: %" PRId64 ", MergeChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
                totalRowCount,
                mergeRowCount,
                totalDataSize,
                mergeDataSize,
                totalChunkCount,
                mergeChunkCount,
                JobCounter.GetPending());
        }
    }


    // The following functions return True iff the chunk is pooled.
    virtual void BeginInputChunksProcessing()
    { }

    virtual bool ProcessInputChunk(const TInputChunk& chunk) = 0;

    virtual void EndInputChunksProcessing()
    {
        // Close the last group, if any.
        if (GetCurrentGroup()) {
            EndGroup();
        }
    }


    // Job count selection.

    virtual i64 ChooseJobCount() = 0;


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

        return
            chunk.approximate_data_size() < Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize &&
            Spec->CombineChunks;
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(Spec->Mode == EMergeMode::Sorted ? EJobType::SortedMerge : EJobType::OrderedMerge);

        TMergeJobSpec mergeJobSpec;
        *mergeJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        mergeJobSpec.mutable_output_spec()->set_schema(OutputTables[0].Schema);
        *JobSpecTemplate.MutableExtension(TMergeJobSpec::merge_job_spec) = mergeJobSpec;

        JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));
    }

};

////////////////////////////////////////////////////////////////////

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
    virtual bool ProcessInputChunk(const TInputChunk& chunk)
    {
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto& table = OutputTables[0];

        if (!IsLargeCompleteChunk(chunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            LOG_DEBUG("Chunk %s is large and complete, using as-is", ~chunkId.ToString());
            table.PartitionTreeIds.push_back(chunkId);
            return false;
        }

        // Create a merge group if not exists.
        auto group = GetCurrentGroup();
        if (!group) {
            group = BeginGroup();
            group->ChunkPool = CreateUnorderedChunkPool();
        }

        // Merge is IO-bound, use data size as weight.
        auto pooledChunk = New<TPooledChunk>(
            chunk,
            chunk.approximate_data_size());
        AddChunkToPool(group, pooledChunk);

        LOG_DEBUG("Chunk %s is pooled (DataSize: %" PRId64 ")",
            ~chunkId.ToString(),
            chunk.approximate_data_size());

        return true;
    }

    virtual i64 ChooseJobCount()
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

class TOrderedMergeController
    : public TMergeControllerBase
{
public:
    TOrderedMergeController(
        TSchedulerConfigPtr config,
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TMergeControllerBase(config, spec, host, operation)
    { }

private:
    virtual bool ProcessInputChunk(const TInputChunk& chunk)
    {
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto group = GetCurrentGroup();
        auto& table = OutputTables[0];

        if (IsLargeCompleteChunk(chunk) && !group) {
            // Merge is not required and no current group is active.
            // Copy the chunk directly to the output.
            LOG_DEBUG("Chunk %s is large and complete, using as-is in partition %d",
                ~chunkId.ToString(),
                static_cast<int>(table.PartitionTreeIds.size()));
            table.PartitionTreeIds.push_back(chunkId);
            return false;
        }

        // Ensure that the current group exists.
        if (!group) {
            group = BeginGroup();
            group->ChunkPool = CreateAtomicChunkPool();
            group->PartitionIndex = static_cast<int>(table.PartitionTreeIds.size());
            table.PartitionTreeIds.push_back(NullChunkListId);
        }

        // Merge is IO-bound, use data size as weight.
        auto pooledChunk = New<TPooledChunk>(
            chunk,
            chunk.approximate_data_size());
        AddChunkToPool(group, pooledChunk);

        LOG_DEBUG("Chunk %s is pooled in partition %d (DataSize: %" PRId64 ")",
            ~chunkId.ToString(),
            group->PartitionIndex,
            chunk.approximate_data_size());

        // Finish the group if the size is large enough.
        if (group->ChunkPool->GetTotalWeight() >= Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize) {
            EndGroup();
        }

        return true;
    }

    virtual i64 ChooseJobCount()
    {
        // Each group corresponds to a unique job.
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
            return New<TOrderedMergeController>(config, spec, host, operation);
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

