#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"

#include <ytlib/misc/string.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/job_proxy/config.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NObjectServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationLogger);
static NProfiling::TProfiler Profiler("/operations/sort");

////////////////////////////////////////////////////////////////////

class TSortController
    : public TOperationControllerBase
{
public:
    TSortController(
        TSchedulerConfigPtr config,
        TSortOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
        , Config(config)
        , Spec(spec)
        , CompletedPartitionCount(0)
        , TotalPartitionJobCount(0)
        , RunningPartitionJobCount(0)
        , CompletedPartitionJobCount(0)
        , TotalPartitionWeight(0)
        , PendingPartitionWeight(0)
        , CompletedPartitionWeight(0)
        , TotalPartitionChunkCount(0)
        , PendingPartitionChunkCount(0)
        , CompletedPartitionChunkCount(0)
        , MaxSortJobCount(0)
        , RunningSortJobCount(0)
        , CompletedSortJobCount(0)
        , TotalSortWeight(0)
        , PendingSortWeight(0)
        , CompletedSortWeight(0)
        , MaxMergeJobCount(0)
        , RunningMergeJobCount(0)
        , CompletedMergeJobCount(0)
        , SamplesFetcher(New<TSamplesFetcher>(
            Config,
            Spec,
            Host->GetBackgroundInvoker(),
            Operation->GetOperationId()))
    { }

private:
    typedef TSortController TThis;

    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;

    // Partition counters.
    int CompletedPartitionCount;

    // Partition job counters.
    int TotalPartitionJobCount;
    int RunningPartitionJobCount;
    int CompletedPartitionJobCount;
    i64 TotalPartitionWeight;
    i64 PendingPartitionWeight;
    i64 CompletedPartitionWeight;
    int TotalPartitionChunkCount;
    int PendingPartitionChunkCount;
    int CompletedPartitionChunkCount;

    // Sort job counters.
    int MaxSortJobCount;
    int RunningSortJobCount;
    int CompletedSortJobCount;
    i64 TotalSortWeight;
    i64 PendingSortWeight;
    i64 CompletedSortWeight;

    // Merge job counters.
    int MaxMergeJobCount;
    int RunningMergeJobCount;
    int CompletedMergeJobCount;

    // Samples and partitions.
    struct TPartition
        : public TIntrinsicRefCounted
    {
        explicit TPartition(int index)
            : Index(index)
            , Small(false)
            , Completed(false)
        { }

        //! Sequential index (zero based).
        int Index;

        //! Small partitions contain data that fits into a single sort job.
        bool Small;

        //! Is partition completed?
        bool Completed;

        //! Pool containing all chunks awaiting sort.
        TUnorderedChunkPool SortChunkPool;

        //! Pool containing all stripes awaiting merge.
        TAtomicChunkPool MergeChunkPool;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    TSamplesFetcherPtr SamplesFetcher;
    std::vector<const NTableClient::NProto::TKey*> SortedSamples;

    //! |PartitionCount - 1| separating keys.
    std::vector<const NTableClient::NProto::TKey*> PartitionKeys;
    
    //! Pool storing all chunks awaiting partition job.
    TUnorderedChunkPool PartitionChunkPool;

    //! List of all partitions.
    std::vector<TPartitionPtr> Partitions;

    //! Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec SortJobSpecTemplate;
    TJobSpec MergeJobSpecTemplate;

    // Locality management for partition phase.
    yhash_map<Stroka, yhash_set<TPartitionPtr> > AddressToPartitionsAwaitingSort;
    yhash_set<TPartitionPtr> PartitionsAwaitingSort;

    bool IsPartitionAwaitingSort(TPartitionPtr partition)
    {
        return
            IsPartitionPhaseCompleted()
            ? partition->SortChunkPool.HasPendingChunks()
            : partition->SortChunkPool.GetPendingWeight() >= Spec->MaxSortJobDataSize;
    }

    bool IsPartitionAwaitingSortAt(TPartitionPtr partition, const Stroka& address)
    {
        return
            IsPartitionAwaitingSort(partition) &&
            partition->SortChunkPool.HasPendingLocalChunksAt(address);
    }

    void RegisterStripeForSort(TPartitionPtr partition, TChunkStripePtr stripe)
    {
        if (IsPartitionAwaitingSort(partition)) {
            PartitionsAwaitingSort.insert(partition);
            FOREACH (const auto& chunk, stripe->InputChunks) {
                FOREACH (const auto& address, chunk.node_addresses()) {
                    if (IsPartitionAwaitingSortAt(partition, address)) {
                        AddressToPartitionsAwaitingSort[address].insert(partition);
                    }
                }
            }
        }
    }

    void AddChunkForSort(TPartitionPtr partition, const TInputChunk& chunk, i64 weight)
    {
        // TODO(babenko): avoid excessive copying

        auto stripe = New<TChunkStripe>(chunk, weight);
        auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());

        partition->SortChunkPool.Add(stripe);
        RegisterStripeForSort(partition, stripe);

        LOG_DEBUG("Added pending chunk %s for sort (Partition: %d)",
            ~chunkId.ToString(),
            partition->Index);

        if (Partitions.size() > 1 &&
            IsPartitionPhaseCompleted() &&
            partition->SortChunkPool.GetTotalWeight() <= Spec->MaxSortJobDataSize)
        {
            partition->Small = true;
            LOG_DEBUG("Partition is small (Partition: %d, Weight: %" PRId64 ")",
                partition->Index,
                partition->SortChunkPool.GetTotalWeight());
        }
    }

    TPoolExtractionResultPtr ExtractForSort(
        TPartitionPtr partition,
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal)
    {
        auto result = partition->SortChunkPool.Extract(
            address,
            weightThreshold,
            needLocal);

        YVERIFY(PartitionsAwaitingSort.find(partition) != PartitionsAwaitingSort.end());
        if (!IsPartitionAwaitingSort(partition)) {
            YVERIFY(PartitionsAwaitingSort.erase(partition) == 1);
        }

        FOREACH (const auto& stripe, result->Stripes) {
            FOREACH (const auto& chunk, stripe->InputChunks) {
                FOREACH (const auto& address, chunk.node_addresses()) {
                    if (!IsPartitionAwaitingSortAt(partition, address)) {
                        AddressToPartitionsAwaitingSort[address].erase(partition);
                    }
                }
            }
        }

        return result;
    }

    void ReturnForSort(
        TPartitionPtr partition,
        TPoolExtractionResultPtr result)
    {
        partition->SortChunkPool.OnFailed(result);
        FOREACH (const auto& chunk, result->Stripes) {
            RegisterStripeForSort(partition, chunk);
        }
    }

    TPartitionPtr FindPartitionAwaitingSort(const Stroka& address)
    {
        // Try to fetch a partition with local chunks.
        auto it = AddressToPartitionsAwaitingSort.find(address);
        if (it != AddressToPartitionsAwaitingSort.end()) {
            const auto& set = it->second;
            if (!set.empty()) {
                auto partition = *set.begin();
                YVERIFY(PartitionsAwaitingSort.find(partition) != PartitionsAwaitingSort.end());
                return partition;
            }
        }

        // Fetch any partition.
        return PartitionsAwaitingSort.empty() ? NULL : *PartitionsAwaitingSort.begin();
    }


    // Locality management for merge phase.
    std::set<TPartitionPtr> PartitionsAwaitingMerge;
    yhash_map<Stroka, yhash_set<TPartitionPtr> > AddressToPartitionsAwaitingMerge;

    bool IsPartitionAwaitingMerge(TPartitionPtr partition)
    {
        return
            !partition->Small &&
            IsSortPhaseCompleted(partition) &&
            partition->MergeChunkPool.HasPendingChunks();
    }

    bool IsPartitionAwaitingMergeAt(TPartitionPtr partition, const Stroka& address)
    {
        return
            IsPartitionAwaitingMerge(partition) &&
            partition->MergeChunkPool.HasPendingLocalChunksAt(address);
    }

    void RegisterStripeForMerge(TPartitionPtr partition, TChunkStripePtr stripe)
    {
        if (IsPartitionAwaitingMerge(partition)) {
            PartitionsAwaitingMerge.insert(partition);
            FOREACH (const auto& chunk, stripe->InputChunks) {
                FOREACH (const auto& address, chunk.node_addresses()) {
                    if (IsPartitionAwaitingMergeAt(partition, address)) {
                        AddressToPartitionsAwaitingMerge[address].insert(partition);
                    }
                }
            }
        }
    }

    void AddStripeForMerge(TPartitionPtr partition, TChunkStripePtr stripe)
    {
        partition->MergeChunkPool.Add(stripe);
        RegisterStripeForMerge(partition, stripe);

        LOG_DEBUG("Added pending stripe [%s] for merge in partition %d",
            ~JoinToString(stripe->GetChunkIds()),
            partition->Index);
    }

    TPoolExtractionResultPtr ExtractForMerge(
        TPartitionPtr partition,
        const Stroka& address,
        bool needLocal)
    {
        auto result = partition->MergeChunkPool.Extract(
            address,
            needLocal);

        YVERIFY(PartitionsAwaitingMerge.find(partition) != PartitionsAwaitingMerge.end());
        if (!IsPartitionAwaitingMerge(partition)) {
            YVERIFY(PartitionsAwaitingMerge.erase(partition) == 1);
        }

        FOREACH (const auto& stripe, result->Stripes) {
            FOREACH (const auto& chunk, stripe->InputChunks) {
                FOREACH (const auto& address, chunk.node_addresses()) {
                    if (!IsPartitionAwaitingMergeAt(partition, address)) {
                        AddressToPartitionsAwaitingMerge[address].erase(partition);
                    }
                }
            }
        }

        return result;
    }

    void ReturnForMerge(
        TPartitionPtr partition,
        TPoolExtractionResultPtr result)
    {
        partition->MergeChunkPool.OnFailed(result);
        FOREACH (const auto& chunk, result->Stripes) {
            RegisterStripeForMerge(partition, chunk);
        }
    }

    TPartitionPtr FindPartitionAwaitingMerge(const Stroka& address)
    {
        // Try to fetch a partition with local chunks.
        auto it = AddressToPartitionsAwaitingMerge.find(address);
        if (it != AddressToPartitionsAwaitingMerge.end()) {
            const auto& set = it->second;
            if (!set.empty()) {
                auto partition = *set.begin();
                YVERIFY(PartitionsAwaitingSort.find(partition) != PartitionsAwaitingSort.end());
                return partition;
            }
        }

        // Fetch any partition.
        return PartitionsAwaitingMerge.empty() ? NULL : *PartitionsAwaitingMerge.begin();
    }


    // Init/finish.

    bool IsPartitionPhaseCompleted()
    {
        return PartitionChunkPool.IsCompleted();
    }

    bool IsSortPhaseCompleted(TPartitionPtr partition)
    {
        return
            IsPartitionPhaseCompleted() &&
            partition->SortChunkPool.IsCompleted();
    }

    virtual int GetPendingJobCount()
    {
        return
            GetPendingPartitionJobCount() +
            GetPendingSortJobCount() +
            GetPendingMergeJobCount();
    }

    int GetPendingPartitionJobCount()
    {
        return PendingPartitionChunkCount == 0
            ? 0
            : TotalPartitionJobCount - RunningPartitionJobCount - CompletedPartitionJobCount;
    }

    int GetPendingSortJobCount()
    {
        int result = 0;
        FOREACH (auto partition, Partitions) {
            result += GetPendingSortJobCount(partition);
        }
        return result;
    }

    int GetPendingSortJobCount(TPartitionPtr partition)
    {
        i64 weight = partition->SortChunkPool.GetPendingWeight();
        i64 weightPerChunk = Spec->MaxSortJobDataSize;
        double fractionJobCount = (double) weight / weightPerChunk;
        return IsPartitionPhaseCompleted()
            ? static_cast<int>(ceil(fractionJobCount))
            : static_cast<int>(floor(fractionJobCount));
    }

    int GetPendingMergeJobCount()
    {
        if (!IsPartitionPhaseCompleted()) {
            return 0;
        }
        int result = 0;
        FOREACH (auto partition, Partitions) {
            result += GetPendingMergeJobCount(partition);
        }
        return result;
    }

    int GetPendingMergeJobCount(TPartitionPtr partition)
    {
        return IsPartitionAwaitingMerge(partition) ? 1 : 0;
    }

    void CompletePartition(TPartitionPtr partition, const TChunkTreeId& chunkTreeId)
    {
        auto& table = OutputTables[0];
        YCHECK(table.PartitionTreeIds[partition->Index] == NullChunkTreeId);
        table.PartitionTreeIds[partition->Index] = chunkTreeId;
        ++CompletedPartitionCount;
        YCHECK(!partition->Completed);
        partition->Completed = true;
        LOG_INFO("Partition completed (Partition: %d)", partition->Index);
    }


    // Generic job scheduling.

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have at least one free chunk lists in the pool.
        if (!CheckChunkListsPoolSize(1)) {
            return NULL;
        }

        auto partitionJob = TrySchedulePartitionJob(node);
        if (partitionJob) {
            return partitionJob;
        }

        auto sortJob = TryScheduleSortJob(node);
        if (sortJob) {
            return sortJob;
        }

        auto mergeJob = TryScheduleMergeJob(node);
        if (mergeJob) {
            return mergeJob;
        }

        YUNREACHABLE();
    }


    // Job scheduling and outcome handling for partition phase.

    struct TPartitionJobInProgress
        : public TJobInProgress
    {
        TPoolExtractionResultPtr PoolResult;
        TChunkListId ChunkListId;
    };

    TJobPtr TrySchedulePartitionJob(TExecNodePtr node)
    {
        if (Partitions.size() < 2) {
            // Single partition case.
            return NULL;
        }

        if (!PartitionChunkPool.HasPendingChunks()) {
            return NULL;
        }

        // Allocate chunks for the job.
        auto jip = New<TPartitionJobInProgress>();
        i64 weightThreshold = GetJobWeightThreshold(GetPendingPartitionJobCount(), PendingPartitionWeight);
        jip->PoolResult = PartitionChunkPool.Extract(
            node->GetAddress(),
            weightThreshold,
            false);

        LOG_DEBUG("Extracted %d chunks for partition at node %s (LocalCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            jip->PoolResult->TotalChunkCount,
            ~node->GetAddress(),
            jip->PoolResult->LocalChunkCount,
            jip->PoolResult->TotalChunkWeight,
            weightThreshold);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = PartitionJobSpecTemplate;
        {
            auto* inputSpec = jobSpec.add_input_specs();
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                const auto& chunk = stripe->InputChunks[0];
                *inputSpec->add_chunks() = chunk;
            }

            auto* outputSpec = jobSpec.add_output_specs();
            const auto& ouputTable = OutputTables[0];
            jip->ChunkListId = ChunkListPool->Extract();
            *outputSpec->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
            outputSpec->set_channels(ouputTable.Channels);
        }

        // Update counters.
        ++RunningPartitionJobCount;
        PendingPartitionChunkCount -= jip->PoolResult->TotalChunkCount;
        PendingPartitionWeight -= jip->PoolResult->TotalChunkWeight;

        return CreateJob(
            jip,
            node,
            jobSpec,
            BIND(&TThis::OnPartitionJobCompleted, MakeWeak(this)),
            BIND(&TThis::OnPartitionJobFailed, MakeWeak(this)));
    }

    void OnPartitionJobCompleted(TPartitionJobInProgress* jip)
    {
        --RunningPartitionJobCount;
        ++CompletedPartitionJobCount;
        CompletedPartitionChunkCount += jip->PoolResult->TotalChunkCount;
        CompletedPartitionWeight += jip->PoolResult->TotalChunkWeight;
        PartitionChunkPool.OnCompleted(jip->PoolResult);

        auto* resultExt = jip->Job->Result().MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
        FOREACH (auto& partitionChunk, *resultExt->mutable_chunks()) {
            // We're keeping chunk information received from partition jobs to populate sort pools.
            // TPartitionsExt is, however, quite heavy.
            // Deserialize it and then drop its protobuf copy immediately.
            auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.extensions());
            RemoveProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.mutable_extensions());

            YCHECK(partitionsExt->sizes_size() == Partitions.size());
            LOG_DEBUG("Partition sizes are [%s]", ~JoinToString(partitionsExt->sizes()));
            for (int index = 0; index < partitionsExt->sizes_size(); ++index) {
                i64 weight = partitionsExt->sizes(index);
                if (weight > 0) {
                    auto partition = Partitions[index];
                    i64 weight = partitionsExt->sizes(index);
                    AddChunkForSort(partition, partitionChunk, weight);                }
            }
        }
    }

    void OnPartitionJobFailed(TPartitionJobInProgress* jip)
    {
        --RunningPartitionJobCount;
        PendingPartitionChunkCount += jip->PoolResult->TotalChunkCount;
        PendingPartitionWeight  += jip->PoolResult->TotalChunkWeight;

        LOG_DEBUG("Returned %d chunks into partition pool", jip->PoolResult->TotalChunkCount);
        PartitionChunkPool.OnFailed(jip->PoolResult);

        ReleaseChunkList(jip->ChunkListId);
    }


    // Job scheduling and outcome handling for sort phase.

    struct TSortJobInProgress
        : public TJobInProgress
    {
        TPartitionPtr Partition;
        TPoolExtractionResultPtr PoolResult;
        TChunkListId ChunkListId;
    };

    TJobPtr TryScheduleSortJob(TExecNodePtr node)
    {
        // Check for an active partition.
        auto partition = FindPartitionAwaitingSort(node->GetAddress());
        if (!partition) {
            return NULL;
        }

        // Allocate chunks for the job.
        auto jip = New<TSortJobInProgress>();
        jip->Partition = partition;
        i64 weightThreshold = Spec->MaxSortJobDataSize;
        jip->PoolResult = ExtractForSort(
            partition,
            node->GetAddress(),
            weightThreshold,
            false);

        LOG_DEBUG("Extracted %d chunks for sort at node %s (Partition: %d, LocalCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            jip->PoolResult->TotalChunkCount,
            ~node->GetAddress(),
            partition->Index,
            jip->PoolResult->LocalChunkCount,
            jip->PoolResult->TotalChunkWeight,
            weightThreshold);


        // Make a copy of the generic spec and customize it.
        auto jobSpec = SortJobSpecTemplate;
        {
            auto* inputSpec = jobSpec.add_input_specs();
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                const auto& chunk = stripe->InputChunks[0];
                *inputSpec->add_chunks() = chunk;
            }

            const auto& outputTable = OutputTables[0];
            auto* outputSpec = jobSpec.add_output_specs();
            jip->ChunkListId = ChunkListPool->Extract();
            *outputSpec->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
            outputSpec->set_channels(outputTable.Channels);

            // Use output replication to sort jobs in small partitions since their chunks go directly to the output.
            // Don't use replication for sort jobs in large partitions since their chunks will be merged.
            auto ioConfig = PrepareJobIOConfig(Config->SortJobIO, partition->Small);
            jobSpec.set_io_config(SerializeToYson(ioConfig));

            auto* jobSpecExt = jobSpec.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            if (Partitions.size() > 1) {
                jobSpecExt->set_partition_tag(partition->Index);
            }
        }

        // Update counters.
        ++RunningSortJobCount;
        PendingSortWeight -= jip->PoolResult->TotalChunkWeight;

        return CreateJob(
            jip,
            node,
            jobSpec,
            BIND(&TThis::OnSortJobCompleted, MakeWeak(this)),
            BIND(&TThis::OnSortJobFailed, MakeWeak(this)));
    }

    void OnSortJobCompleted(TSortJobInProgress* jip)
    {
        --RunningSortJobCount;
        ++CompletedSortJobCount;
        CompletedSortWeight += jip->PoolResult->TotalChunkWeight;

        auto partition = jip->Partition;
        partition->SortChunkPool.OnCompleted(jip->PoolResult);

        if (partition->SortChunkPool.IsCompleted()) {
            LOG_DEBUG("Partition sorted (Partition: %d)", partition->Index);
        }

        if (partition->Small) {
            // Sort outputs in small partitions go directly to the output table.
            CompletePartition(partition, jip->ChunkListId);
            return;
        } 

        // Sort outputs in large partitions are queued for further merge.

        // Construct a stripe consisting of sorted chunks.
        const auto& resultExt = jip->Job->Result().GetExtension(TSortJobResultExt::sort_job_result_ext);
        auto stripe = New<TChunkStripe>();
        FOREACH (const auto& chunk, resultExt.chunks()) {
            auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
            stripe->InputChunks.push_back(chunk);
            stripe->Weight += miscExt->data_weight();
        }

        // Put the stripe into the pool.
        AddStripeForMerge(partition, stripe);
    }

    void OnSortJobFailed(TSortJobInProgress* jip)
    {
        --RunningSortJobCount;
        PendingSortWeight += jip->PoolResult->TotalChunkWeight;

        LOG_DEBUG("Returned %d chunks into sort pool (Partition: %d)",
            jip->PoolResult->TotalChunkCount,
            jip->Partition->Index);
        ReturnForSort(jip->Partition, jip->PoolResult);

        ReleaseChunkList(jip->ChunkListId);
    }


    // Job scheduling and outcome handling for merge phase.

    struct TMergeJobInProgress
        : public TJobInProgress
    {
        TPartitionPtr Partition;
        TPoolExtractionResultPtr PoolResult;
        TChunkListId ChunkListId;
    };

    TJobPtr TryScheduleMergeJob(TExecNodePtr node)
    {
        auto partition = FindPartitionAwaitingMerge(node->GetAddress());
        if (!partition) {
            return NULL;
        }

        // Allocate chunks for the job.
        auto jip = New<TMergeJobInProgress>();
        jip->Partition = partition;
        jip->PoolResult = ExtractForMerge(
            partition,
            node->GetAddress(),
            false);

        LOG_DEBUG("Extracted %d chunks for merge at node %s (LocalCount: %d, ExtractedWeight: %" PRId64 ")",
            jip->PoolResult->TotalChunkCount,
            ~node->GetAddress(),
            jip->PoolResult->LocalChunkCount,
            jip->PoolResult->TotalChunkWeight);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = MergeJobSpecTemplate;
        {
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                auto* inputSpec = jobSpec.add_input_specs();
                FOREACH (const auto& chunk, stripe->InputChunks) {
                    *inputSpec->add_chunks() = chunk;
                }
            }

            auto* outputSpec = jobSpec.add_output_specs();
            const auto& ouputTable = OutputTables[0];
            jip->ChunkListId = ChunkListPool->Extract();
            *outputSpec->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
            outputSpec->set_channels(ouputTable.Channels);
        }

        // Update counters.
        ++RunningMergeJobCount;

        return CreateJob(
            jip,
            node,
            jobSpec,
            BIND(&TThis::OnMergeJobCompleted, MakeWeak(this)),
            BIND(&TThis::OnMergeJobFailed, MakeWeak(this)));
    }

    void OnMergeJobCompleted(TMergeJobInProgress* jip)
    {
        --RunningMergeJobCount;
        ++CompletedMergeJobCount;

        auto partition = jip->Partition;
        partition->MergeChunkPool.OnCompleted(jip->PoolResult);
        YASSERT(partition->MergeChunkPool.IsCompleted());

        LOG_DEBUG("Partition merged (Partition: %d)", partition->Index);

        CompletePartition(partition, jip->ChunkListId);
    }

    void OnMergeJobFailed(TMergeJobInProgress* jip)
    {
        --RunningMergeJobCount;

        LOG_DEBUG("Returned %d chunks into partition pool (Partition: %d)",
            jip->PoolResult->TotalChunkCount,
            jip->Partition->Index);
        ReturnForMerge(jip->Partition, jip->PoolResult);

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
        return pipeline
            ->Add(BIND(&TThis::RequestSamples, MakeStrong(this)))
            ->Add(BIND(&TThis::OnSamplesReceived, MakeStrong(this)));
    }

    TFuture< TValueOrError<void> > RequestSamples()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            // Compute statistics, populate partition pool, and prepare the fetcher.
            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                auto fetchRsp = table.FetchResponse;
                FOREACH (const auto& chunk, fetchRsp->chunks()) {
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    i64 weight = miscExt->data_weight();

                    TotalPartitionWeight += weight;
                    ++TotalPartitionChunkCount;

                    SamplesFetcher->AddChunk(chunk);

                    auto stripe = New<TChunkStripe>(chunk, weight);
                    PartitionChunkPool.Add(stripe);
                }
            }

            // Check for empty inputs.
            if (TotalPartitionChunkCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return MakeFuture(TValueOrError<void>());
            }

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %d)",
                TotalPartitionWeight,
                TotalPartitionChunkCount);

            return SamplesFetcher->Run();
        }
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
    {
        UNUSED(batchRsp);

        CheckOutputTablesEmpty();
        SetOutputTablesSorted(Spec->KeyColumns);
    }

    void SortSamples()
    {
        const auto& samples = SamplesFetcher->GetSamples();
        int sampleCount = static_cast<int>(samples.size());
        LOG_INFO("Sorting %d samples", sampleCount);

        SortedSamples.reserve(sampleCount);
        FOREACH (const auto& sample, samples) {
            SortedSamples.push_back(&sample);
        }

        std::sort(SortedSamples.begin(), SortedSamples.end(), 
            [] (const NTableClient::NProto::TKey* lhs, const NTableClient::NProto::TKey* rhs) {
                return CompareKeys(*lhs, *rhs) < 0;
            }
        );
    }

    void BuildPartitions()
    {
        FOREACH (const auto& table, InputTables) {
            FOREACH (const auto& chunk, table.FetchResponse->chunks()) {
                auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                TotalSortWeight += miscExt->data_weight();
            }
        }
        PendingSortWeight = TotalSortWeight;

        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = Spec->PartitionCount
            ? Spec->PartitionCount.Get()
            : static_cast<int>(ceil((double) TotalSortWeight / Spec->MinSortPartitionSize));

        // Don't create more partitions than we have samples.
        partitionCount = std::min(partitionCount, static_cast<int>(SortedSamples.size()) + 1);

        YCHECK(partitionCount > 0);

        if (partitionCount == 1) {
            BuildSinglePartition();
        } else {
            BuildMulitplePartitions(partitionCount);
        }

        // Init output trees.
        {
            auto& table = OutputTables[0];
            table.PartitionTreeIds.resize(Partitions.size());
            for (int index = 0; index < static_cast<int>(Partitions.size()); ++index) {
                table.PartitionTreeIds[index] = NullChunkTreeId;
            }
        }
    }

    void BuildSinglePartition()
    {
        // Create a single partition.
        Partitions.resize(1);
        auto partition = Partitions[0] = New<TPartition>(0);
        partition->Small = true;

        // There will be no partition jobs, reset partition counters.
        TotalPartitionChunkCount = 0;
        TotalPartitionWeight = 0;

        // Put all input chunks into this unique partition.
        TotalSortWeight = 0;
        int totalSortChunkCount = 0;
        FOREACH (const auto& table, InputTables) {
            FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                i64 weight = miscExt->uncompressed_data_size();
                AddChunkForSort(partition, chunk, weight);
                TotalSortWeight += weight;
                ++totalSortChunkCount;
            }
        }

        // Init counters.
        PendingSortWeight = TotalSortWeight;
        MaxSortJobCount = GetJobCount(
            TotalSortWeight,
            Spec->MaxSortJobDataSize,
            Spec->SortJobCount,
            totalSortChunkCount);

        LOG_INFO("Sorting without partitioning");
    }

    void BuildMulitplePartitions(int partitionCount)
    {
        // Take partition keys evenly.
        for (int partIndex = 0; partIndex < partitionCount - 1; ++partIndex) {
            int sampleIndex = (partIndex + 1) * (SortedSamples.size() - 1) / partitionCount;
            auto* key = SortedSamples[sampleIndex];
            // Avoid producing same keys.
            if (PartitionKeys.empty() || CompareKeys(*key, *SortedSamples.back()) != 0) {
                PartitionKeys.push_back(key);
            }
        }

        // Do the final adjustments.
        partitionCount = static_cast<int>(PartitionKeys.size()) + 1;

        // Prepare partitions.
        Partitions.resize(partitionCount);
        for (int partIndex = 0; partIndex < static_cast<int>(Partitions.size()); ++partIndex) {
            Partitions[partIndex] = New<TPartition>(partIndex);
        }

        // Init counters.
        TotalPartitionJobCount = GetJobCount(
            TotalPartitionWeight,
            Config->PartitionJobIO->ChunkSequenceWriter->DesiredChunkSize,
            Spec->PartitionJobCount,
            TotalPartitionChunkCount);
        PendingPartitionWeight = TotalPartitionWeight;
        PendingPartitionChunkCount = TotalPartitionChunkCount;

        // Very rough estimates.
        MaxSortJobCount = GetJobCount(
            TotalPartitionWeight,
            Spec->MaxSortJobDataSize,
            Null,
            std::numeric_limits<int>::max()) + partitionCount;
        MaxMergeJobCount = partitionCount;

        LOG_INFO("Sorting with %d partitions", partitionCount);
    }

    void OnSamplesReceived()
    {
        PROFILE_TIMING ("/samples_processing_time") {
            SortSamples();
            BuildPartitions();
           
            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(
                TotalPartitionJobCount +
                MaxSortJobCount +
                MaxMergeJobCount +
                Config->SpareChunkListCount);

            InitJobSpecTemplates();

            LOG_INFO("Samples processed (PartitionJobCount: %d)",
                TotalPartitionJobCount);
        }
    }

    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d}, "
            "Partitions = {T: %d, C: %d}, "
            "PartitionJobs = {T: %d, R: %d, C: %d, P: %d}, "
            "PartitionChunks = {T: %d, C: %d, P: %d}, "
            "PartitionWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}, "
            "SortJobs = {M: %d, R: %d, C: %d}, "
            "SortWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}, "
            "MergeJobs = {M: %d, R: %d, C: %d}",
            // Jobs
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount,
            // Partitions
            static_cast<int>(Partitions.size()),
            CompletedPartitionCount,
            // PartitionJobs
            TotalPartitionJobCount,
            RunningPartitionJobCount,
            CompletedPartitionJobCount,
            GetPendingPartitionJobCount(),
            // PartitionChunks
            TotalPartitionChunkCount,
            CompletedPartitionChunkCount,
            PendingPartitionChunkCount,
            // PartitionWeight
            TotalPartitionWeight,
            CompletedPartitionWeight,
            PendingPartitionWeight,
            // SortJobs
            MaxSortJobCount,
            RunningSortJobCount,
            CompletedSortJobCount,
            // SortWeight
            TotalSortWeight,
            CompletedSortWeight,
            PendingSortWeight,
            // MergeJobs
            MaxMergeJobCount,
            RunningMergeJobCount,
            CompletedMergeJobCount);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("partition_jobs").BeginMap()
                .Item("total").Scalar(TotalPartitionJobCount)
                .Item("completed").Scalar(CompletedPartitionJobCount)
            .EndMap()
            .Item("sort_jobs").BeginMap()
                .Item("max").Scalar(MaxSortJobCount)
                .Item("running").Scalar(RunningSortJobCount)
                .Item("completed").Scalar(CompletedSortJobCount)
            .EndMap()
            .Item("merge_jobs").BeginMap()
                .Item("max").Scalar(MaxMergeJobCount)
                .Item("running").Scalar(RunningMergeJobCount)
                .Item("completed").Scalar(CompletedMergeJobCount)
            .EndMap()
            .Item("partitions").BeginMap()
                .Item("total").Scalar(Partitions.size())
                .Item("completed").Scalar(CompletedPartitionCount)
            .EndMap();
    }


    // Unsorted helpers.

    TJobIOConfigPtr PrepareJobIOConfig(TJobIOConfigPtr config, bool replicateOutput)
    {
        if (replicateOutput) {
            return config;
        } else {
            auto newConfig = CloneConfigurable(config);
            newConfig->ChunkSequenceWriter->ReplicationFactor = 1;
            newConfig->ChunkSequenceWriter->UploadReplicationFactor = 1;
            return newConfig;
        }
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(EJobType::Partition);
            *PartitionJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            FOREACH (const auto* key, PartitionKeys) {
                *specExt->add_partition_keys() = *key;
            }
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);

            // Don't replicate partition chunks.
            PartitionJobSpecTemplate.set_io_config(SerializeToYson(
                PrepareJobIOConfig(Config->PartitionJobIO, false)));
        }
        {
            SortJobSpecTemplate.set_type(EJobType::Sort);
            *SortJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = SortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);          

            // Can't fill io_config right away: some sort jobs need output replication
            // while others don't. Leave this customization to |TryScheduleSortJob|.
        }
        {
            MergeJobSpecTemplate.set_type(EJobType::SortedMerge);
            *MergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            MergeJobSpecTemplate.set_io_config(SerializeToYson(
                PrepareJobIOConfig(Config->MergeJobIO, true)));
        }
    }
};

IOperationControllerPtr CreateSortController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = New<TSortOperationSpec>();
    try {
        spec->Load(~operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }

    return New<TSortController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

