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
    std::vector<const NTableClient::NProto::TKeySample*> SortedSamples;

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

    bool IsPartitionActive(TPartitionPtr partition, const Stroka& address)
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
                if (IsPartitionActive(partition, address)) {
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

        LOG_DEBUG("Added pending %s for sort in partition %d",
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
                if (!IsPartitionActive(partition, address)) {
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
        i64 weight = partition->SortChunkPool->GetTotalWeight();
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

        LOG_DEBUG("Extracted %d chunks for partition, %d local for node %s (ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            static_cast<int>(jip->ExtractResult->Chunks.size()),
            jip->ExtractResult->LocalCount,
            ~node->GetAddress(),
            jip->ExtractResult->Weight,
            weightThreshold);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = PartitionJobSpecTemplate;
        auto* partitionJobSpec = jobSpec.MutableExtension(TPartitionJobSpec::partition_job_spec);
        FOREACH (const auto& chunk, jip->ExtractResult->Chunks) {
            *partitionJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }
        jip->ChunkListId = ChunkListPool->Extract();
        *partitionJobSpec->mutable_output_chunk_list_id() = jip->ChunkListId.ToProto();

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
            YASSERT(partitionChunk.partition_sizes_size() == Partitions.size());
            for (int index = 0; index < static_cast<int>(Partitions.size()); ++index) {
                auto partition = Partitions[index];
                // Plus one is to ensure that weights are positive.
                i64 weight = partitionChunk.partition_sizes(index) + 1;
                // TODO(babenko): avoid excessive copying
                auto pooledChunk = New<TPooledChunk>(partitionChunk.chunk(), weight);
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

        LOG_DEBUG("Extracted %d chunks for sort, %d local for node %s (ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            static_cast<int>(jip->ExtractResult->Chunks.size()),
            jip->ExtractResult->LocalCount,
            ~node->GetAddress(),
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
        sortJobSpec->set_partition_tag(partition->Index);

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
                FOREACH (const auto& chunk, *fetchRsp->mutable_chunks()) {
                    auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(chunk.extensions());
                    i64 dataSize = miscExt->uncompressed_size();

                    // Plus one is to ensure that weights are positive.
                    i64 weight = dataSize + 1;

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

    void SortSamples()
    {
        const auto& samples = SamplesFetcher->GetSamples();
        int sampleCount = static_cast<int>(samples.size());
        LOG_INFO("Sorting %d samples", sampleCount);

        SortedSamples.resize(sampleCount);
        for (int index = 0; index < sampleCount; ++index) {
            SortedSamples[index] = &samples[index];
        }

        std::sort(
            SortedSamples.begin(),
            SortedSamples.end(),
            [] (const NTableClient::NProto::TKeySample* lhs, const NTableClient::NProto::TKeySample* rhs) {
                return CompareKeys(lhs->key(), rhs->key()) < 0;
            });
    }

    void BuildPartitions()
    {
        FOREACH (const auto* sample, SortedSamples) {
            TotalSortWeight += sample->data_size_since_previous() + 1;
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

        // Put all input chunks into this unique partition.
        FOREACH (const auto& table, InputTables) {
            FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(chunk.extensions());
                i64 dataSize = miscExt->uncompressed_size();
                // Plus one is to ensure that weights are positive.
                i64 weight = dataSize + 1;
                auto pooledChunk = New<TPooledChunk>(chunk, weight);
                AddPendingChunkForSort(partition, pooledChunk);
                TotalSortWeight += weight;
            }
        }

        PendingSortWeight = TotalSortWeight;

        LOG_INFO("Sorting without partitioning");
    }

    void BuildMulitplePartitions(int partitionCount)
    {
        // Take partition keys evenly.
        int samplesRemaining = partitionCount - 1;
        i64 weightRemaining = TotalSortWeight;
        i64 weightCurrent = 0;
        i64 weightMin = std::numeric_limits<i64>::max();
        i64 weightMax = std::numeric_limits<i64>::min();
        int index = 0;
        while (index < static_cast<int>(SortedSamples.size())) {
            // Check current weight against the threshold.
            i64 weightThreshold = weightRemaining / (samplesRemaining + 1);
            if (weightCurrent >= weightThreshold) {
                PartitionKeys.push_back(&SortedSamples[index]->key());
                weightMin = std::min(weightMin, weightCurrent);
                weightMax = std::max(weightMax, weightCurrent);
                weightRemaining -= weightCurrent;
                weightCurrent = 0;
                --samplesRemaining;
                if (samplesRemaining == 0) {
                    break;
                }
            }
            // Handle range of equal samples.
            do {
                weightCurrent += SortedSamples[index]->data_size_since_previous() + 1;
                ++index;
            } while (
                index < static_cast<int>(SortedSamples.size()) &&
                CompareKeys(SortedSamples[index]->key(), SortedSamples[index - 1]->key()) == 0);
        }
        // Handle the final partition.
        weightMin = std::min(weightMin, weightRemaining);
        weightMax = std::max(weightMax, weightRemaining);

        // Do the final adjustments.
        partitionCount = static_cast<int>(PartitionKeys.size()) + 1;

        // Prepare partitions.
        Partitions.resize(partitionCount);
        for (int index = 0; index < static_cast<int>(Partitions.size()); ++index) {
            Partitions[index] = New<TPartition>(index);
        }

        LOG_INFO("Sorting with %d partitions (MinWeight: %" PRId64 ", MaxWeight: %" PRId64 ")",
            partitionCount,
            weightMin,
            weightMax);
    }

    void OnSamplesReceived()
    {
        PROFILE_TIMING ("/samples_processing_time") {
            SortSamples();
            BuildPartitions();
           
            // Init counters.
            ChooseJobCount();
            PendingPartitionWeight = TotalPartitionWeight;
            PendingPartitionChunkCount = TotalPartitionChunkCount;

            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(TotalPartitionJobCount + Config->SpareChunkListCount);

            InitJobSpecTemplates();

            LOG_INFO("Samples processed (PartitionJobCount: %d)",
                TotalPartitionJobCount);
        }
    }

    void ChooseJobCount()
    {
        TotalPartitionJobCount = GetJobCount(
            TotalPartitionWeight,
            Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize,
            Spec->PartitionJobCount,
            TotalPartitionChunkCount);
    }


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d}, "
            "PartitionJobs = {T: %d, R: %d, C: %d, P: %d}, "
            "PartitionChunks = {T: %d, C: %d, P: %d}, "
            "PartitionWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}, "
            "SortJobs = {R: %d, C: %d}, "
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
            RunningSortJobCount,
            CompletedSortJobCount,
            // SortWeight
            TotalSortWeight,
            CompletedSortWeight,
            PendingSortWeight);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
    //    BuildYsonMapFluently(consumer)
    //        .Item("chunks").Do(BIND(&TProgressCounter::ToYson, &ChunkCounter))
    //        .Item("weight").Do(BIND(&TProgressCounter::ToYson, &WeightCounter));
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

            TPartitionJobSpec partitionJobSpec;
            FOREACH (const auto* key, PartitionKeys) {
                *partitionJobSpec.add_partition_keys() = *key;
            }
            *partitionJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            ToProto(partitionJobSpec.mutable_key_columns(), Spec->KeyColumns);
            *PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpec::partition_job_spec) = partitionJobSpec;

            // Don't replicate partition chunks.
            PartitionJobSpecTemplate.set_io_config(SerializeToYson(GetJobIOCOnfig(false)));
        }
        {
            SortJobSpecTemplate.set_type(EJobType::Sort);

            TSortJobSpec sortJobSpec;
            *sortJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            ToProto(sortJobSpec.mutable_key_columns(), Spec->KeyColumns);
            *SortJobSpecTemplate.MutableExtension(TSortJobSpec::sort_job_spec) = sortJobSpec;

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

