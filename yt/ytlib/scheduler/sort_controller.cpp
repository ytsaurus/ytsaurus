#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
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
        , TotalSortJobCount(0)
        , RunningSortJobCount(0)
        , CompletedSortJobCount(0)
        , TotalSortWeight(0)
        , PendingSortWeight(0)
        , CompletedSortWeight(0)
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
    int TotalSortJobCount;
    int RunningSortJobCount;
    int CompletedSortJobCount;
    i64 TotalSortWeight;
    i64 PendingSortWeight;
    i64 CompletedSortWeight;

    // Samples and partitions.
    struct TPartition
        : public TIntrinsicRefCounted
    {
        explicit TPartition(int index)
            : Index(index)
            , Small(false)
            , SortChunkPool(CreateUnorderedChunkPool())
        { }

        //! Sequential index (zero based).
        int Index;

        //! Small partitions contain data that fits into a single sort job.
        bool Small;

        //! Pool storing all chunks awaiting sort job.
        TAutoPtr<IChunkPool> SortChunkPool;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    TSamplesFetcherPtr SamplesFetcher;
    std::vector<const NTableClient::NProto::TKey*> SortedSamples;

    //! |PartitionCount - 1| separating keys.
    std::vector<const NTableClient::NProto::TKey*> PartitionKeys;
    
    //! Pool storing all chunks awaiting partition job.
    TAutoPtr<IChunkPool> PartitionChunkPool;
    
    //! List of all partitions.
    std::vector<TPartitionPtr> Partitions;

    //! Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec SortJobSpecTemplate;

    //! Locality and active partitions management.
    yhash_map<Stroka, yhash_set<TPartitionPtr> > AddressToActivePartitions;
    yhash_set<TPartitionPtr> ActivePartitions;

    bool IsPartitionActive(TPartitionPtr partition)
    {
        return GetPendingPartitionJobCount() > 0
            ? partition->SortChunkPool->GetTotalWeight() > Config->MaxSortJobDataSize
            : partition->SortChunkPool->HasPendingChunks();
    }

    bool IsPartitionActiveFor(TPartitionPtr partition, const Stroka& address)
    {
        return
            IsPartitionActive(partition) &&
            partition->SortChunkPool->HasPendingLocalChunksFor(address);
    }

    void RegisterPendingChunkForSort(TPartitionPtr partition, TPooledChunkPtr chunk)
    {
        if (IsPartitionActive(partition)) {
            ActivePartitions.insert(partition);
            FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
                if (IsPartitionActiveFor(partition, address)) {
                    AddressToActivePartitions[address].insert(partition);
                }
            }
        }
    }

    void AddPendingChunkForSort(TPartitionPtr partition, TPooledChunkPtr chunk)
    {
        auto chunkId = TChunkId::FromProto(chunk->InputChunk.slice().chunk_id());

        partition->SortChunkPool->Add(chunk);
        RegisterPendingChunkForSort(partition, chunk);

        LOG_DEBUG("Added pending chunk %s for sort in partition %d",
            ~chunkId.ToString(),
            partition->Index);

        if (GetPendingPartitionJobCount() == 0 &&
            partition->SortChunkPool->GetTotalWeight() <= Config->MaxSortJobDataSize)
        {
            partition->Small = true;
            LOG_DEBUG("Partition %d is small (Weight: %" PRId64 ")",
                partition->Index,
                partition->SortChunkPool->GetTotalWeight());
        }
    }

    IChunkPool::TExtractResultPtr ExtractChunksForSort(
        TPartitionPtr partition,
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        auto result = partition->SortChunkPool->Extract(
            address,
            weightThreshold,
            maxCount,
            needLocal);
        YASSERT(result);

        if (!IsPartitionActive(partition)) {
            YVERIFY(ActivePartitions.erase(partition) == 1);
        }

        FOREACH (const auto& chunk, result->Chunks) {
            FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
                if (!IsPartitionActiveFor(partition, address)) {
                    AddressToActivePartitions[address].erase(partition);
                }
            }
        }

        return result;
    }

    void ReturnChunksForSort(
        TPartitionPtr partition,
        IChunkPool::TExtractResultPtr result)
    {
        partition->SortChunkPool->PutBack(result);
        FOREACH (const auto& chunk, result->Chunks) {
            RegisterPendingChunkForSort(partition, chunk);
        }
    }

    TPartitionPtr TryGetActivePartitionForSort(const Stroka& address)
    {
        // Try to fetch a partition with local chunks.
        auto it = AddressToActivePartitions.find(address);
        if (it != AddressToActivePartitions.end()) {
            const auto& set = it->second;
            if (!set.empty()) {
                return *set.begin();
            }
        }

        // Fetch any partition.
        return ActivePartitions.empty() ? NULL : *ActivePartitions.begin();
    }


    // Init/finish.

    virtual int GetPendingJobCount()
    {
        return
            GetPendingPartitionJobCount() +
            GetPendingSortJobCount();
    }

    int GetPendingPartitionJobCount()
    {
        return PendingPartitionChunkCount == 0
            ? 0
            : TotalPartitionJobCount - CompletedPartitionJobCount;
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
        i64 weight = partition->SortChunkPool->GetPendingWeight();
        i64 weightPerChunk = Config->MaxSortJobDataSize;
        return GetPendingPartitionJobCount() > 0
            ? static_cast<int>(floor((double) weight / weightPerChunk))
            : static_cast<int>(ceil((double) weight / weightPerChunk));
    }

    void CompletePartition(TPartitionPtr partition, const TChunkTreeId& chunkTreeId)
    {
        auto& table = OutputTables[0];
        YASSERT(table.PartitionTreeIds[partition->Index] == NullChunkTreeId);
        table.PartitionTreeIds[partition->Index] = chunkTreeId;
        ++CompletedPartitionChunkCount;
        LOG_INFO("Partition %d is complete", partition->Index);
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

        YUNREACHABLE();
    }


    // Job scheduling and outcome handling for partition phase.

    struct TPartitionJobInProgress
        : public TJobInProgress
    {
        IChunkPool::TExtractResultPtr ExtractResult;
        TChunkListId ChunkListId;
    };

    TJobPtr TrySchedulePartitionJob(TExecNodePtr node)
    {
        if (!PartitionChunkPool) {
            // Single partition case.
            return NULL;
        }

        if (!PartitionChunkPool->HasPendingChunks()) {
            return NULL;
        }

        // Allocate chunks for the job.
        auto jip = New<TPartitionJobInProgress>();
        i64 weightThreshold = GetJobWeightThreshold(GetPendingPartitionJobCount(), PendingPartitionWeight);
        jip->ExtractResult = PartitionChunkPool->Extract(
            node->GetAddress(),
            weightThreshold,
            std::numeric_limits<int>::max(),
            false);
        YASSERT(jip->ExtractResult);

        LOG_DEBUG("Extracted %d chunks for partition at node %s (LocalCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            static_cast<int>(jip->ExtractResult->Chunks.size()),
            ~node->GetAddress(),
            jip->ExtractResult->LocalCount,
            jip->ExtractResult->Weight,
            weightThreshold);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = PartitionJobSpecTemplate;
        {
            auto* partitionJobSpec = jobSpec.MutableExtension(TPartitionJobSpec::partition_job_spec);
            FOREACH (const auto& chunk, jip->ExtractResult->Chunks) {
                *partitionJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
            }
            jip->ChunkListId = ChunkListPool->Extract();
            auto* outputSpec = partitionJobSpec->mutable_output_spec();
            const auto& ouputTable = OutputTables[0];
            *outputSpec->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
            outputSpec->set_channels(ouputTable.Channels);
        }

        // Update counters.
        ++RunningPartitionJobCount;
        PendingPartitionChunkCount -= jip->ExtractResult->Chunks.size();
        PendingPartitionWeight -= jip->ExtractResult->Weight;

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
        CompletedPartitionChunkCount += jip->ExtractResult->Chunks.size();
        CompletedPartitionWeight += jip->ExtractResult->Weight;

        auto result = jip->Job->Result().GetExtension(TPartitionJobResult::partition_job_result);
        FOREACH (const auto& partitionChunk, result.chunks()) {
            auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.extensions());
            YASSERT(partitionsExt->sizes_size() == Partitions.size());
            for (int index = 0; index < static_cast<int>(Partitions.size()); ++index) {
                auto partition = Partitions[index];
                i64 weight = partitionsExt->sizes(index);
                // TODO(babenko): avoid excessive copying
                auto pooledChunk = New<TPooledChunk>(partitionChunk, weight);
                AddPendingChunkForSort(partition, pooledChunk);
            }
        }
    }

    void OnPartitionJobFailed(TPartitionJobInProgress* jip)
    {
        --RunningPartitionJobCount;
        PendingPartitionChunkCount += jip->ExtractResult->Chunks.size();
        PendingPartitionWeight  += jip->ExtractResult->Weight;

        LOG_DEBUG("Returned %d chunks into partition pool",
            static_cast<int>(jip->ExtractResult->Chunks.size()));
        PartitionChunkPool->PutBack(jip->ExtractResult);

        ReleaseChunkList(jip->ChunkListId);
    }


    // Job scheduling and outcome handling for sort phase.

    struct TSortJobInProgress
        : public TJobInProgress
    {
        TPartitionPtr Partition;
        IChunkPool::TExtractResultPtr ExtractResult;
        TChunkListId ChunkListId;
    };

    TJobPtr TryScheduleSortJob(TExecNodePtr node)
    {
        // Check for an active partition.
        auto partition = TryGetActivePartitionForSort(node->GetAddress());
        if (!partition) {
            return NULL;
        }

        // Allocate chunks for the job.
        auto jip = New<TSortJobInProgress>();
        jip->Partition = partition;
        i64 weightThreshold = Config->MaxSortJobDataSize;
        jip->ExtractResult = ExtractChunksForSort(
            partition,
            node->GetAddress(),
            weightThreshold,
            std::numeric_limits<int>::max(),
            false);
        YASSERT(!jip->ExtractResult->Chunks.empty());

        LOG_DEBUG("Extracted %d chunks for sort at node %s (LocalCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            static_cast<int>(jip->ExtractResult->Chunks.size()),
            ~node->GetAddress(),
            jip->ExtractResult->LocalCount,
            jip->ExtractResult->Weight,
            weightThreshold);


        // Make a copy of the generic spec and customize it.
        auto jobSpec = SortJobSpecTemplate;
        auto* sortJobSpec = jobSpec.MutableExtension(TSortJobSpec::sort_job_spec);
        FOREACH (const auto& chunk, jip->ExtractResult->Chunks) {
            *sortJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }
        jip->ChunkListId = ChunkListPool->Extract();
        *sortJobSpec->mutable_output_spec()->mutable_chunk_list_id() = jip->ChunkListId.ToProto();
        if (Partitions.size() > 1) {
            sortJobSpec->set_partition_tag(partition->Index);
        }

        // Use output replication to sort jobs in small partitions since their chunks go directly to the output.
        // Don't use replication for sort jobs in large partitions since their chunks will be merged.
        auto ioConfig = GetJobIOCOnfig(partition->Small);
        jobSpec.set_io_config(SerializeToYson(ioConfig));

        // Update counters.
        ++RunningSortJobCount;
        PendingSortWeight -= jip->ExtractResult->Weight;

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
        CompletedSortWeight += jip->ExtractResult->Weight;

        auto partition = jip->Partition;
        if (partition->Small) {
            // Sort outputs in small partitions go directly to the output table.
            CompletePartition(partition, jip->ChunkListId);
        } else {
            // TODO(babenko): handle large partitions
            YUNREACHABLE();
        }
    }

    void OnSortJobFailed(TSortJobInProgress* jip)
    {
        --RunningSortJobCount;
        PendingSortWeight += jip->ExtractResult->Weight;

        LOG_DEBUG("Returned %d chunks into pool",
            static_cast<int>(jip->ExtractResult->Chunks.size()));
        ReturnChunksForSort(jip->Partition, jip->ExtractResult);

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
            PartitionChunkPool = CreateUnorderedChunkPool();
            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                auto fetchRsp = table.FetchResponse;
                FOREACH (const auto& chunk, fetchRsp->chunks()) {
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    i64 weight = miscExt->data_weight();

                    TotalPartitionWeight += weight;
                    ++TotalPartitionChunkCount;

                    SamplesFetcher->AddChunk(chunk);

                    auto pooledChunk = New<TPooledChunk>(chunk, weight);
                    PartitionChunkPool->Add(pooledChunk);
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

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        UNUSED(batchRsp);

        CheckOutputTablesEmpty();
        SetOutputTablesSorted(InputTables[0].KeyColumns);
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

        std::sort(SortedSamples.begin(), SortedSamples.end());
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
            : static_cast<int>(ceil((double) TotalSortWeight / Config->MinSortPartitionSize));

        // Don't create more partitions that we have nodes.
        partitionCount = std::min(partitionCount, ExecNodeCount);
        // Don't create more partitions than we have samples.
        partitionCount = std::min(partitionCount, static_cast<int>(SortedSamples.size()) + 1);

        YASSERT(partitionCount > 0);

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

        // There will be no partition jobs, reset partition counters.
        TotalPartitionChunkCount = 0;
        TotalPartitionWeight = 0;
        PartitionChunkPool.Destroy();

        // Put all input chunks into this unique partition.
        TotalSortWeight = 0;
        int totalSortChunkCount = 0;
        FOREACH (const auto& table, InputTables) {
            FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                i64 weight = miscExt->uncompressed_data_size();
                auto pooledChunk = New<TPooledChunk>(chunk, weight);
                AddPendingChunkForSort(partition, pooledChunk);
                TotalSortWeight += weight;
                ++totalSortChunkCount;
            }
        }

        // Init counters.
        PendingSortWeight = TotalSortWeight;
        TotalSortJobCount = GetJobCount(
            TotalSortWeight,
            Config->MaxSortJobDataSize,
            Null,
            totalSortChunkCount);

        LOG_INFO("Sorting without partitioning");
    }

    void BuildMulitplePartitions(int partitionCount)
    {
        // Take partition keys evenly.
        for (int partIndex = 0; partIndex < partitionCount - 1; ++partIndex) {
            int sampleIndex = (partIndex + 1) * SortedSamples.size() / partitionCount;
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
            Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize,
            Spec->PartitionJobCount,
            TotalPartitionChunkCount);
        PendingPartitionWeight = TotalPartitionWeight;
        PendingPartitionChunkCount = TotalPartitionChunkCount;
        // A very rough estimate.
        TotalSortJobCount = GetJobCount(
            TotalPartitionWeight,
            Config->MaxSortJobDataSize,
            Null,
            std::numeric_limits<int>::max()) + partitionCount;

        LOG_INFO("Sorting with %d partitions", partitionCount);
    }

    void OnSamplesReceived()
    {
        PROFILE_TIMING ("/samples_processing_time") {
            SortSamples();
            BuildPartitions();
           
            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(TotalPartitionJobCount + Config->SpareChunkListCount);

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
            "PartitionJobs = {T: %d, R: %d, C: %d, P: %d}, "
            "PartitionChunks = {T: %d, C: %d, P: %d}, "
            "PartitionWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}, "
            "SortJobs = {T: %d, R: %d, C: %d}, "
            "SortWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}",
            // Jobs
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount,
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
            TotalSortJobCount,
            RunningSortJobCount,
            CompletedSortJobCount,
            // SortWeight
            TotalSortWeight,
            CompletedSortWeight,
            PendingSortWeight);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("partition_jobs").BeginMap()
                .Item("total").Scalar(TotalPartitionJobCount)
                .Item("completed").Scalar(CompletedPartitionJobCount)
            .EndMap()
            .Item("sort_jobs").BeginMap()
                .Item("total").Scalar(TotalSortJobCount)
                .Item("completed").Scalar(CompletedSortJobCount)
            .EndMap()
            .Item("merge_jobs").BeginMap()
                .Item("total").Scalar(0)
                .Item("completed").Scalar(0)
            .EndMap();
    }


    // Unsorted helpers.

    TJobIOConfigPtr GetJobIOCOnfig(bool replicateOutput)
    {
        if (replicateOutput) {
            return Spec->JobIO;
        } else {
            auto config = CloneConfigurable(Spec->JobIO);
            config->ChunkSequenceWriter->ReplicationFactor = 1;
            config->ChunkSequenceWriter->UploadReplicationFactor = 1;
            return config;
        }
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(EJobType::Partition);

            TPartitionJobSpec specExt;
            FOREACH (const auto* key, PartitionKeys) {
                *specExt.add_partition_keys() = *key;
            }
            *specExt.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            ToProto(specExt.mutable_key_columns(), Spec->KeyColumns);
            *PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpec::partition_job_spec) = specExt;

            // Don't replicate partition chunks.
            PartitionJobSpecTemplate.set_io_config(SerializeToYson(GetJobIOCOnfig(false)));
        }
        {
            SortJobSpecTemplate.set_type(EJobType::Sort);

            TSortJobSpec specExt;
            *specExt.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            ToProto(specExt.mutable_key_columns(), Spec->KeyColumns);
            
            *specExt.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            auto& table = OutputTables[0];
            specExt.mutable_output_spec()->set_channels(table.Channels);
            *SortJobSpecTemplate.MutableExtension(TSortJobSpec::sort_job_spec) = specExt;

            // Can't fill in io_config right away: some sort jobs need output replication
            // while others don't. Leave this customization to |TryScheduleSortJob|.
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

