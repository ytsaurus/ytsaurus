#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "private.h"
#include "chunk_list_pool.h"

#include <ytlib/ytree/fluent.h>
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
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

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
        TMergeOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
        , Spec(spec)
        , TotalJobCount(0)
        , TotalWeight(0)
        , PendingWeight(0)
        , CompletedWeight(0)
        , TotalChunkCount(0)
        , PendingChunkCount(0)
        , CompletedChunkCount(0)
        , CurrentGroupWeight(0)
    { }

private:
    typedef TMergeControllerBase TThis;

protected:
    TMergeOperationSpecPtr Spec;

    // Counters.
    int TotalJobCount;
    i64 TotalWeight;
    i64 PendingWeight;
    i64 CompletedWeight;
    int TotalChunkCount;
    int PendingChunkCount;
    int CompletedChunkCount;

    yhash_map<int, TChunkStripePtr> CurrentGroupStripes;
    i64 CurrentGroupWeight;

    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;


    // Merge groups.
    struct TMergeGroup
        : public TIntrinsicRefCounted
    {
        TMergeGroup(int groupIndex, int partitionIndex = -1)
            : GroupIndex(groupIndex)
            , PartitionIndex(partitionIndex)
        { }

        TAtomicChunkPool ChunkPool;

        //! The position in |Groups|. 
        int GroupIndex;

        //! The position in |TOutputTable::PartitionIds| where the 
        //! output of this group must be placed.
        int PartitionIndex;
    };

    typedef TIntrusivePtr<TMergeGroup> TMergeGroupPtr;

    std::vector<TMergeGroupPtr> Groups;

    //! Finish the current group.
    void EndGroup()
    {
        YCHECK(CurrentGroupWeight > 0);

        auto& table = OutputTables[0];
        auto group = New<TMergeGroup>(
            static_cast<int>(Groups.size()),
            static_cast<int>(table.PartitionTreeIds.size()));

        FOREACH(auto& pair, CurrentGroupStripes) {
            group->ChunkPool.Add(pair.second);
            RegisterPendingStripe(group, pair.second);
        }

        // Reserve a place for this group among partitions.
        table.PartitionTreeIds.push_back(NullChunkListId);

        Groups.push_back(group);

        LOG_DEBUG("Finished group %d (DataSize: %" PRId64 ")",
            static_cast<int>(Groups.size()) - 1,
            CurrentGroupWeight);

        CurrentGroupWeight = 0;
        CurrentGroupStripes.clear();
    }

    //! Finish the current group if the size is large enough.
    void EndGroupIfLarge()
    {
        if (CurrentGroupWeight >= Config->MergeJobIO->ChunkSequenceWriter->DesiredChunkSize) {
            EndGroup();
        }
    }

    // Chunk pools and locality.

    yhash_map<Stroka, yhash_set<TMergeGroupPtr> > AddressToGroupsAwaitingMerge;
    yhash_set<TMergeGroupPtr> GroupsAwatingMerge;

    void RegisterPendingStripe(TMergeGroupPtr group, TChunkStripePtr stripe)
    {
        GroupsAwatingMerge.insert(group);
        FOREACH (const auto& chunk, stripe->InputChunks) {
            FOREACH (const auto& address, chunk.node_addresses()) {
                AddressToGroupsAwaitingMerge[address].insert(group);
            }
        }
    }

    //! Add chunk to the current group's pool.
    void AddPendingChunk(const TInputChunk& chunk, int stripeTag)
    {
        // Merge is IO-bound, use data size as weight.
        auto misc = GetProtoExtension<TMiscExt>(chunk.extensions());
        i64 weight = misc->data_weight();

        TChunkStripePtr stripe;
        auto it = CurrentGroupStripes.find(stripeTag);
        if (it == CurrentGroupStripes.end()) {
            stripe = New<TChunkStripe>();
            YASSERT(CurrentGroupStripes.insert(std::make_pair(stripeTag, stripe)).second);
        } else {
            stripe = it->second;
        }

        TotalWeight += weight;
        CurrentGroupWeight += weight;

        ++TotalChunkCount;
        stripe->AddChunk(chunk, weight);

        auto& table = OutputTables[0];
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        LOG_DEBUG("Added pending chunk %s in partition %d, group %d",
            ~chunkId.ToString(),
            static_cast<int>(table.PartitionTreeIds.size()),
            static_cast<int>(Groups.size()));
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

    TPoolExtractionResultPtr ExtractForMerge(
        TMergeGroupPtr group,
        const Stroka& address,
        bool needLocal)
    {
        auto result = group->ChunkPool.Extract(
            address,
            needLocal);

        YVERIFY(GroupsAwatingMerge.erase(group) == 1);

        FOREACH (const auto& stripe, result->Stripes) {
            FOREACH (const auto& chunk, stripe->InputChunks) {
                FOREACH (const auto& address, chunk.node_addresses()) {
                    if (!group->ChunkPool.HasPendingLocalChunksAt(address)) {
                        AddressToGroupsAwaitingMerge[address].erase(group);
                    }
                }
            }
        }

        return result;
    }

    void ReturnForMerge(TMergeGroupPtr group, TPoolExtractionResultPtr result)
    {
        group->ChunkPool.OnFailed(result);
        FOREACH (const auto& chunk, result->Stripes) {
            RegisterPendingStripe(group, chunk);
        }
    }

    TMergeGroupPtr FindGroupAwaitingMerge(const Stroka& address)
    {
        // Try to fetch a group with local chunks.
        auto it = AddressToGroupsAwaitingMerge.find(address);
        if (it != AddressToGroupsAwaitingMerge.end()) {
            const auto& set = it->second;
            if (!set.empty()) {
                return *set.begin();
            }
        }

        // Fetch any group.
        return GroupsAwatingMerge.empty() ? NULL : *GroupsAwatingMerge.begin();
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

    virtual int GetPendingJobCount()
    {
        return TotalJobCount - RunningJobCount - CompletedJobCount;
    }


    // Job scheduling and outcome handling.

    struct TMergeJobInProgress
        : public TJobInProgress
    {
        TPoolExtractionResultPtr PoolResult;
        TChunkListId ChunkListId;
        TMergeGroupPtr Group;
    };

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(1)) {
            return NULL;
        }

        // We've got a job to do! :)

        auto group = FindGroupAwaitingMerge(node->GetAddress());
        YCHECK(group);

        // Allocate chunks for the job.
        auto jip = New<TMergeJobInProgress>();
        jip->Group = group;
        jip->PoolResult = ExtractForMerge(
            group,
            node->GetAddress(),
            false);

        LOG_DEBUG("Extracted %d chunks for merge at node %s (LocalCount: %d, ExtractedWeight: %" PRId64 ")",
            jip->PoolResult->TotalChunkCount,
            ~node->GetAddress(),
            jip->PoolResult->LocalChunkCount,
            jip->PoolResult->TotalChunkWeight);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        {
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                auto* inputSpec = jobSpec.add_input_specs();
                FOREACH (const auto& chunk, stripe->InputChunks) {
                    *inputSpec->add_chunks() = chunk;
                }
            }
            {
                auto* outputSpec = jobSpec.add_output_specs();
                outputSpec->set_channels(OutputTables[0].Channels);
                jip->ChunkListId = ChunkListPool->Extract();
                *outputSpec->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
            }
        }

        // Update counters.
        PendingChunkCount -= jip->PoolResult->TotalChunkCount;
        PendingWeight -= jip->PoolResult->TotalChunkWeight;

        return CreateJob(
            jip,
            node,
            jobSpec,
            BIND(&TThis::OnJobCompleted, MakeWeak(this)),
            BIND(&TThis::OnJobFailed, MakeWeak(this)));
    }

    virtual void OnJobCompleted(TMergeJobInProgress* jip)
    {
        CompletedChunkCount += jip->PoolResult->TotalChunkCount;
        CompletedWeight += jip->PoolResult->TotalChunkWeight;

        auto group = jip->Group;
        group->ChunkPool.OnCompleted(jip->PoolResult);
        auto& table = OutputTables[0];
        table.PartitionTreeIds[group->PartitionIndex] = jip->ChunkListId;
    }

    virtual void OnJobFailed(TMergeJobInProgress* jip)
    {
        PendingChunkCount += jip->PoolResult->TotalChunkCount;
        PendingWeight += jip->PoolResult->TotalChunkWeight;

        LOG_DEBUG("Returned %d chunks into pool", jip->PoolResult->TotalChunkCount);
        ReturnForMerge(jip->Group, jip->PoolResult);

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

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
    {
        return pipeline->Add(BIND(&TThis::ProcessInputs, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            BeginInputChunks();

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    i64 weight = miscExt->data_weight();
                    i64 rowCount = miscExt->row_count();
                    LOG_DEBUG("Processing chunk %s (DataWeight: %" PRId64 ", RowCount: %" PRId64 ")",
                        ~chunkId.ToString(),
                        weight,
                        rowCount);
                    ProcessInputChunk(chunk, tableIndex);
                }
            }

            EndInputChunks();

            // Check for trivial inputs.
            if (TotalChunkCount == 0) {
                LOG_INFO("Trivial merge");
                FinalizeOperation();
                return;
            }

            // Init counters.
            TotalJobCount = static_cast<int>(Groups.size());
            PendingWeight = TotalWeight;
            PendingChunkCount = TotalChunkCount;

            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(TotalJobCount + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %d, JobCount: %d)",
                TotalWeight,
                TotalChunkCount,
                TotalJobCount);
        }
    }


    //! Called at the beginning of input chunks scan.
    virtual void BeginInputChunks()
    { }

    //! Called for each input chunk.
    virtual void ProcessInputChunk(const TInputChunk& chunk, int tableIndex) = 0;

    //! Called at the end of input chunks scan.
    virtual void EndInputChunks()
    {
        // Close the last group, if any.
        if (CurrentGroupWeight > 0) {
            EndGroup();
        }
    }


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {T: %d, R: %d, C: %d, P: %d, F: %d}, "
            "Chunks = {T: %d, C: %d, P: %d}, "
            "Weight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}",
            TotalJobCount,
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount,
            TotalChunkCount,
            CompletedChunkCount,
            PendingChunkCount,
            TotalWeight,
            CompletedWeight,
            PendingWeight);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("chunks").BeginMap()
                .Item("total").Scalar(TotalChunkCount)
                .Item("completed").Scalar(CompletedChunkCount)
                .Item("pending").Scalar(PendingChunkCount)
            .EndMap()
            .Item("weight").BeginMap()
                .Item("total").Scalar(TotalWeight)
                .Item("completed").Scalar(CompletedWeight)
                .Item("pending").Scalar(PendingWeight)
            .EndMap();
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

        auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
        // ChunkSequenceWriter may actually produce a chunk a bit smaller than DesiredChunkSize,
        // so we have to be more flexible here.
        if (0.9 * miscExt->compressed_data_size() >= Config->MergeJobIO->ChunkSequenceWriter->DesiredChunkSize) {
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

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        // ToDo(psushin): set larger PrefetchWindow for unordered merge.
        JobSpecTemplate.set_io_config(SerializeToYson(Config->MergeJobIO));
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
    virtual void ProcessInputChunk(const TInputChunk& chunk, int tableIndex)
    {
        UNUSED(tableIndex);

        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsLargeCompleteChunk(chunk)) {
            // Chunks not requiring merge go directly to the output chunk list.
            AddPassthroughChunk(chunk);
            return;
        }

        // All chunks go to a single chunk stripe.
        AddPendingChunk(chunk, 0);
        EndGroupIfLarge();
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

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
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

    virtual void ProcessInputChunk(const TInputChunk& chunk, int tableIndex)
    {
        UNUSED(tableIndex);

        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
        auto& table = OutputTables[0];

        if (IsLargeCompleteChunk(chunk) && CurrentGroupWeight > 0) {
            // Merge is not required and no current group is active.
            // Copy the chunk directly to the output.
            LOG_DEBUG("Chunk %s is large and complete, using as-is in partition %d",
                ~chunkId.ToString(),
                static_cast<int>(table.PartitionTreeIds.size()));
            table.PartitionTreeIds.push_back(chunkId);
            return;
        }

        // All chunks go to a single chunk stripe.
        AddPendingChunk(chunk, 0);
        EndGroupIfLarge();
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
        int TableIndex;
        NTableClient::NProto::TKey Key;
        const TInputChunk* InputChunk;
    };

    std::vector<TKeyEndpoint> Endpoints;

    virtual void ProcessInputChunk(const TInputChunk& chunk, int tableIndex)
    {
        auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
        YCHECK(miscExt->sorted());

        // Construct endpoints and place them into the list.
        auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(chunk.extensions());
        {
            TKeyEndpoint endpoint;
            endpoint.Left = true;
            endpoint.TableIndex = tableIndex;
            endpoint.Key = boundaryKeysExt->left();
            endpoint.InputChunk = &chunk;
            Endpoints.push_back(endpoint);
        }
        {
            TKeyEndpoint endpoint;
            endpoint.Left = false;
            endpoint.TableIndex = tableIndex;
            endpoint.Key = boundaryKeysExt->right();
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
                    BuildGroupIfNeeded(startIndex, currentIndex + 1);
                    startIndex = currentIndex + 1;
                }
            }
        }

        // Force all output tables to be marked as sorted.
        auto keyColumns = GetInputKeyColumns();
        SetOutputTablesSorted(keyColumns);
    }

    void BuildGroupIfNeeded(int startIndex, int endIndex)
    {
        // Must be even number of endpoints.
        YASSERT((endIndex - startIndex) % 2 == 0);

        int chunkCount = (endIndex - startIndex) / 2;
        LOG_DEBUG("Found overlap of %d chunks", chunkCount);

        {
            const auto& chunk = *Endpoints[startIndex].InputChunk;
            if (chunkCount == 1 && IsLargeCompleteChunk(chunk) && CurrentGroupWeight > 0) {
                auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                // Merge is not required and no current group is active.
                // Copy the chunk directly to the output.
                AddPassthroughChunk(chunk);
                return;
            }
        }

        for (; startIndex < endIndex; ++startIndex) {
            auto& endpoint = Endpoints[startIndex];
            if (endpoint.Left) {
                AddPendingChunk(*endpoint.InputChunk, endpoint.TableIndex);
            }
        }

        EndGroupIfLarge();
    }


    virtual void OnCustomInputsRecieved(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        UNUSED(batchRsp);

        CheckInputTablesSorted();
        CheckOutputTablesEmpty();
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

