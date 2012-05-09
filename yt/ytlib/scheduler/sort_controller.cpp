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
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
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
        , TotalPartitionJobCount(0)
        , CompletedPartitionJobCount(0)
        , TotalPartitionWeight(0)
        , PendingPartitionWeight(0)
        , CompletedPartitionWeight(0)
        , TotalPartitionChunkCount(0)
        , PendingPartitionChunkCount(0)
        , CompletedPartitionChunkCount(0)
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

    // Counters.
    int TotalPartitionJobCount;
    int CompletedPartitionJobCount;
    i64 TotalPartitionWeight;
    i64 PendingPartitionWeight;
    i64 CompletedPartitionWeight;
    int TotalPartitionChunkCount;
    int PendingPartitionChunkCount;
    int CompletedPartitionChunkCount;

    // Samples and partitions.
    TSamplesFetcherPtr SamplesFetcher;
    std::vector<const NTableClient::NProto::TKeySample*> SortedSamples;
    int PartitionCount;
    // |PartitionCount - 1| separating keys.
    std::vector<const NTableClient::NProto::TKey*> PartitionKeys;
    TAutoPtr<IChunkPool> PartitionChunkPool;

    // Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;

    // Init/finish.

    virtual int GetPendingJobCount()
    {
        return GetPendingPartitionJobCount();
    }

    int GetPendingPartitionJobCount()
    {
        return PendingPartitionWeight == 0
            ? 0
            : TotalPartitionJobCount - CompletedPartitionJobCount;
    }


    // Job scheduling and outcome handling.

    struct TPartitionJobInProgress
        : public TIntrinsicRefCounted
    {
        IChunkPool::TExtractResultPtr ExtractResult;
        TChunkListId ChunkListId;
    };

    typedef TIntrusivePtr<TPartitionJobInProgress> TPartitionJobInProgressPtr;

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(OutputTables.size())) {
            return NULL;
        }

        // We've got a job to do! :)
        if (GetPendingPartitionJobCount() > 0) {
            return SchedulePartitionJob(node);
        }

        YUNREACHABLE();
    }

    TJobPtr SchedulePartitionJob(TExecNodePtr node)
    {
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
        PendingPartitionChunkCount -= jip->ExtractResult->Chunks.size();
        PendingPartitionWeight -= jip->ExtractResult->Weight;

        return CreateJob(
            Operation,
            node,
            jobSpec,
            BIND(&TThis::OnPartitionJobCompleted, MakeWeak(this), jip),
            BIND(&TThis::OnPartitionJobFailed, MakeWeak(this), jip));
    }

    void OnPartitionJobCompleted(TPartitionJobInProgressPtr jip)
    {
        // TODO(babenko): handle partition success
    }

    void OnPartitionJobFailed(TPartitionJobInProgressPtr jip)
    {
        PendingPartitionChunkCount += jip->ExtractResult->Chunks.size();
        PendingPartitionWeight  += jip->ExtractResult->Weight;

        LOG_DEBUG("Returned %d chunks into the partition pool",
            static_cast<int>(jip->ExtractResult->Chunks.size()));
        PartitionChunkPool->PutBack(jip->ExtractResult);

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

            auto samplesFetcher = New<TSamplesFetcher>(
                Config,
                Spec,
                Host->GetBackgroundInvoker(),
                Operation->GetOperationId());

            // Compute statistics and prepare the fetcher.
            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                auto fetchRsp = table.FetchResponse;
                FOREACH (const auto& chunk, *fetchRsp->mutable_chunks()) {
                    auto misc = GetProtoExtension<NChunkHolder::NProto::TMisc>(chunk.extensions());
                    i64 rowCount = misc->row_count();
                    i64 dataSize = misc->uncompressed_size();

                    TotalPartitionWeight += dataSize;
                    ++TotalPartitionChunkCount;

                    samplesFetcher->AddChunk(chunk);
                }
            }

            // Check for empty inputs.
            if (TotalPartitionChunkCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return MakeFuture(TValueOrError<void>());
            }

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %" PRId64 ")",
                TotalPartitionWeight,
                TotalPartitionChunkCount);

            return samplesFetcher->Run();
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
        i64 totalSize = 0;
        FOREACH (const auto* sample, SortedSamples) {
            totalSize += sample->data_size();
        }

        // Use partition count provided by the user, if given.
        // Otherwise use size estimates.
        if (Spec->PartitionCount) {
            PartitionCount = Spec->PartitionCount.Get();
        } else {
            PartitionCount = static_cast<int>(totalSize / Config->MinSortPartitionSize);
        }

        // Don't create more partitions that we have nodes.
        PartitionCount = std::min(PartitionCount, ExecNodeCount);
        // Don't create more partitions than we have samples.
        PartitionCount = std::min(PartitionCount, static_cast<int>(SortedSamples.size()) + 1);

        YASSERT(PartitionCount > 0);

        if (PartitionCount == 1) {
            LOG_INFO("Sorting without partitioning");
            return;
        }

        // Take partition keys evenly.
        int samplesRemaining = PartitionCount - 1;
        i64 sizeRemaining = totalSize;
        i64 sizeTaken = 0;
        FOREACH (const auto* sample, SortedSamples) {
            if (sizeTaken >= sizeRemaining / samplesRemaining) {
                PartitionKeys.push_back(&sample->key());
                sizeRemaining -= sizeTaken;
                sizeTaken = 0;
                --samplesRemaining;
            }
            sizeTaken += sample->data_size();
        }

        // Do the final adjustments.
        PartitionCount = static_cast<int>(PartitionKeys.size()) + 1;
        LOG_INFO("Using %d partitions", PartitionCount);
    }

    void OnSamplesReceived()
    {
        PROFILE_TIMING ("/samples_processing_time") {
            SortSamples();
            BuildPartitions();
           
            ChooseJobCount();

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
        //LOG_DEBUG("Progress: "
        //    "Jobs = {T: %d, R: %d, C: %d, P: %d, F: %d}, "
        //    "PartitionJobs = {T: %d, R: %d, C: %d, P: %d, F: %d}, "
        //    "PartitionChunks = {T: %d, C: %d, P: %d}, "
        //    "PartitionWeight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}",
        //    TotalJobCount,
        //    RunningJobCount,
        //    CompletedJobCount,
        //    GetPendingJobCount(),
        //    FailedJobCount,
        //    TotalPartitionJobCount,


        //    TotalPartitionChunkCount,
        //    CompletedPartitionChunkCount,
        //    PendingPartitionChunkCount,
        //    TotalPartitionWeight,
        //    CompletedPartitionWeight,
        //    PendingPartitionWeight);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
    //    BuildYsonMapFluently(consumer)
    //        .Item("chunks").Do(BIND(&TProgressCounter::ToYson, &ChunkCounter))
    //        .Item("weight").Do(BIND(&TProgressCounter::ToYson, &WeightCounter));
    }

    // Unsorted helpers.

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(EJobType::Partition);

            TPartitionJobSpec partitionJobSpec;
            FOREACH (const auto* key, PartitionKeys) {
                *partitionJobSpec.add_partition_keys() = *key;
            }
            *partitionJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            *PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpec::partition_job_spec) = partitionJobSpec;

            PartitionJobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));
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

