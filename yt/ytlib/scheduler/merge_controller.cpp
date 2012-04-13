#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "private.h"

#include <ytlib/ytree/fluent.h>

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

class TMergeController
    : public TOperationControllerBase
{
public:
    TMergeController(
        TSchedulerConfigPtr config,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
    { }

private:
    typedef TMergeController TThis;

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
            : Index(-1)
            , OutputChildIndex(-1)
        { }

        //! Sequential index in group list, 0 for unordered merge.
        int Index;

        TAutoPtr<IChunkPool> ChunkPool;

        //! The position in |TOutputTable::OutputChildrenIds| where the 
        //! output of this group must be placed.
        //! -1 indicates that no position was reallocated, so the output
        //! is just appended to the end.
        int OutputChildIndex;
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
        group->Index = static_cast<int>(Groups.size());
        Groups.push_back(group);
        return group;
    }

    void EndGroup()
    {
        YASSERT(CurrentGroup);
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
    {
        Spec = New<TMergeOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
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
        if (group->OutputChildIndex >= 0) {
            table.OutputChildrenIds[group->OutputChildIndex] = jip->ChunkListId;
        } else {
            table.OutputChildrenIds.push_back(jip->ChunkListId);
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

            // Compute statistics and populate the pool.
            i64 totalRowCount = 0;
            i64 mergeRowCount = 0;
            i64 totalDataSize = 0;
            i64 mergeDataSize = 0;
            i64 totalChunkCount = 0;
            i64 mergeChunkCount = 0;

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                auto fetchRsp = table.FetchResponse;
                FOREACH (auto& inputChunk, *fetchRsp->mutable_chunks()) {
                    i64 rowCount = inputChunk.approximate_row_count();
                    i64 dataSize = inputChunk.approximate_data_size();

                    totalRowCount += rowCount;
                    totalDataSize += dataSize;
                    totalChunkCount += 1;

                    if (ProcessInputChunk(inputChunk)) {
                        mergeRowCount += rowCount;
                        mergeDataSize += dataSize;
                        mergeChunkCount += 1;
                    }
                }
            }

            // Check for empty and trivial inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            if (mergeRowCount == 0) {
                LOG_INFO("Trivial merge");
                FinalizeOperation();
                return;
            }

            ChooseJobCount();

            // Init progress counters.
            ChunkCounter.Set(mergeChunkCount);
            WeightCounter.Set(mergeDataSize);

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
    bool ProcessInputChunk(const TInputChunk& inputChunk)
    {
        switch (Spec->Mode) {
            case EMergeMode::Unordered:
                return ProcessInputChunkUnordered(inputChunk);
            case EMergeMode::Ordered:
                return ProcessInputChunkOrdered(inputChunk);
                break;
            default:
                YUNREACHABLE();
        }
    }

    bool ProcessInputChunkUnordered(const TInputChunk& inputChunk)
    {
        auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());

        if (!IsLargeCompleteChunk(inputChunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            OutputTables[0].OutputChildrenIds.push_back(chunkId);
            return false;
        }

        // Create a merge group if none exists.
        auto group = GetCurrentGroup();
        if (!group) {
            group = BeginGroup();
            group->ChunkPool = CreateUnorderedChunkPool();
        }

        // Merge is IO-bound, use data size as weight.
        auto chunk = New<TPooledChunk>(
            inputChunk,
            inputChunk.approximate_data_size());
        AddChunkToPool(group, chunk);
        return true;
    }

    bool ProcessInputChunkOrdered(const TInputChunk& inputChunk)
    {
        YUNREACHABLE();
    }


    // Job count selection.

    void ChooseJobCount()
    {
        switch (Spec->Mode) {
            case EMergeMode::Unordered:
                ChooseJobCountUnordered();
                break;
            case EMergeMode::Ordered:
                ChooseJobCountOrdered();
                break;
            default:
                YUNREACHABLE();
        }
    }

    void ChooseJobCountUnordered()
    {
        // Choose job count.
        // TODO(babenko): refactor, generalize, and improve.
        i64 jobCount = (i64) ceil((double) WeightCounter.GetPending() / Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize);
        if (Spec->JobCount) {
            jobCount = Spec->JobCount.Get();
        }
        jobCount = std::min(jobCount, ChunkCounter.GetPending());
        YASSERT(jobCount > 0);
        JobCounter.Set(jobCount);
    }

    void ChooseJobCountOrdered()
    {

    }


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

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    return New<TMergeController>(config, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

