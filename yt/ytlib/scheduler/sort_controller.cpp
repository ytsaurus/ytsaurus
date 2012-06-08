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
        , MaxSortJobCount(0)
        , RunningSortJobCount(0)
        , CompletedSortJobCount(0)
        , MaxMergeJobCount(0)
        , RunningMergeJobCount(0)
        , CompletedMergeJobCount(0)
        , SamplesFetcher(New<TSamplesFetcher>(
            Config,
            Spec,
            Host->GetBackgroundInvoker(),
            Operation->GetOperationId()))
        , PartitionTask(New<TPartitionTask>(this))
    { }

private:
    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;

    // Counters.
    int CompletedPartitionCount;
    TProgressCounter PartitionJobCounter;
    TProgressCounter PartitionWeightCounter;
    TProgressCounter PartitionChunkCounter;
    // Sort job counters.
    int MaxSortJobCount;
    int RunningSortJobCount;
    int CompletedSortJobCount;
    TProgressCounter SortWeightCounter;
    TProgressCounter SortChunkCounter;
    // Merge job counters.
    int MaxMergeJobCount;
    int RunningMergeJobCount;
    int CompletedMergeJobCount;

    // Forward declarations.
    class TPartitionTask;
    typedef TIntrusivePtr<TPartitionTask> TPartitionTaskPtr;

    class TSortTask;
    typedef TIntrusivePtr<TSortTask> TSortTaskPtr;

    class TMergeTask;
    typedef TIntrusivePtr<TMergeTask> TMergeTaskPtr;

    // Samples and partitions.
    struct TPartition
        : public TIntrinsicRefCounted
    {
        explicit TPartition(TSortController* controller, int index)
            : Index(index)
            , Small(false)
            , Completed(false)
            , SortTask(New<TSortTask>(controller, this))
            , MergeTask(New<TMergeTask>(controller, this))
        { }

        //! Sequential index (zero based).
        int Index;

        //! Small partitions contain data that fits into a single sort job.
        bool Small;

        //! Is partition completed?
        bool Completed;

        TSortTaskPtr SortTask;
        TMergeTaskPtr MergeTask;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    TSamplesFetcherPtr SamplesFetcher;
    std::vector<const NTableClient::NProto::TKey*> SortedSamples;

    //! |PartitionCount - 1| separating keys.
    std::vector<const NTableClient::NProto::TKey*> PartitionKeys;
    
    //! List of all partitions.
    std::vector<TPartitionPtr> Partitions;

    //! Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec SortJobSpecTemplate;
    TJobSpec MergeJobSpecTemplate;

    
    // Partition task.

    class TPartitionTask
        : public TTask
    {
    public:
        explicit TPartitionTask(TSortController* controller)
            : TTask(controller)
            , Controller(controller)
        {
            ChunkPool = CreateUnorderedChunkPool();
        }

        virtual Stroka GetId() const
        {
            return "Partition";
        }

        virtual int GetPendingJobCount() const
        {
            return
                IsCompleted() 
                ? 0
                : Controller->PartitionJobCounter.GetPending();
        }

        virtual TDuration GetMaxLocalityDelay() const
        {
            // TODO(babenko): make customizable
            return TDuration::Seconds(5);
        }

    private:
        TSortController* Controller;

        virtual int GetChunkListCountPerJob() const 
        {
            return 1;
        }

        virtual TNullable<i64> GetJobWeightThreshold() const
        {
            return GetJobWeightThresholdGeneric(
                GetPendingJobCount(),
                Controller->PartitionWeightCounter.GetPending());
        }

        virtual TJobSpec GetJobSpec(TJobInProgress* jip)
        {
            auto jobSpec = Controller->PartitionJobSpecTemplate;
            AddSequentialInputSpec(&jobSpec, jip);
            AddTabularOutputSpec(&jobSpec, jip, Controller->OutputTables[0]);
            return jobSpec;
        }

        virtual void OnJobStarted(TJobInProgress* jip)
        {
            Controller->PartitionJobCounter.Start(1);
            Controller->PartitionChunkCounter.Start(jip->PoolResult->TotalChunkCount);
            Controller->PartitionWeightCounter.Start(jip->PoolResult->TotalChunkWeight);
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            TTask::OnJobCompleted(jip);

            Controller->PartitionJobCounter.Completed(1);
            Controller->PartitionChunkCounter.Completed(jip->PoolResult->TotalChunkCount);
            Controller->PartitionWeightCounter.Completed(jip->PoolResult->TotalChunkWeight);

            auto* resultExt = jip->Job->Result().MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
            FOREACH (auto& partitionChunk, *resultExt->mutable_chunks()) {
                // We're keeping chunk information received from partition jobs to populate sort pools.
                // TPartitionsExt is, however, quite heavy.
                // Deserialize it and then drop its protobuf copy immediately.
                auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.extensions());
                RemoveProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.mutable_extensions());

                YCHECK(partitionsExt->sizes_size() == Controller->Partitions.size());
                LOG_DEBUG("Partition sizes are [%s]", ~JoinToString(partitionsExt->sizes()));
                for (int index = 0; index < partitionsExt->sizes_size(); ++index) {
                    i64 weight = partitionsExt->sizes(index);
                    if (weight > 0) {
                        auto stripe = New<TChunkStripe>(partitionChunk, weight);
                        auto partition = Controller->Partitions[index];
                        partition->SortTask->AddStripe(stripe);
                    }
                }
            }
        }

        virtual void OnJobFailed(TJobInProgress* jip)
        {
            TTask::OnJobFailed(jip);

            Controller->PartitionJobCounter.Failed(1);
            Controller->PartitionChunkCounter.Failed(jip->PoolResult->TotalChunkCount);
            Controller->PartitionWeightCounter.Failed(jip->PoolResult->TotalChunkWeight);
        }

        virtual void OnTaskCompleted()
        {
            // Kick-start all sort tasks.
            FOREACH (auto partition, Controller->Partitions) {
                Controller->RegisterTaskPendingHint(partition->SortTask);
            }
        }
    };

    TPartitionTaskPtr PartitionTask;


    // Sort task.

    class TSortTask
        : public TTask
    {
    public:
        TSortTask(TSortController* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        {
            ChunkPool = CreateUnorderedChunkPool();
        }

        virtual Stroka GetId() const
        {
            return Sprintf("Sort(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const
        {
            i64 weight = ChunkPool->WeightCounter().GetPending();
            i64 weightPerChunk = Controller->Spec->MaxSortJobDataSize;
            double fractionalJobCount = (double) weight / weightPerChunk;
            return
                Controller->PartitionTask->IsCompleted()
                ? static_cast<int>(ceil(fractionalJobCount))
                : static_cast<int>(floor(fractionalJobCount));
        }

        virtual TDuration GetMaxLocalityDelay() const
        {
            return TDuration::Zero();
        }

    private:
        TSortController* Controller;
        TPartition* Partition;

        virtual int GetChunkListCountPerJob() const 
        {
            return 1;
        }

        virtual TNullable<i64> GetJobWeightThreshold() const
        {
            return Controller->Spec->MaxSortJobDataSize;
        }

        virtual TJobSpec GetJobSpec(TJobInProgress* jip)
        {
            auto jobSpec = Controller->SortJobSpecTemplate;

            AddSequentialInputSpec(&jobSpec, jip);
            AddTabularOutputSpec(&jobSpec, jip, Controller->OutputTables[0]);

            {
                // Use output replication to sort jobs in small partitions since their chunks go directly to the output.
                // Don't use replication for sort jobs in large partitions since their chunks will be merged.
                auto ioConfig = Controller->PrepareJobIOConfig(Controller->Config->SortJobIO, Partition->Small);
                jobSpec.set_io_config(SerializeToYson(ioConfig));
            }

            {
                auto* jobSpecExt = jobSpec.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                if (Controller->Partitions.size() > 1) {
                    jobSpecExt->set_partition_tag(Partition->Index);
                }
            }

            return jobSpec;
        }

        virtual void OnJobStarted(TJobInProgress* jip)
        {
            TTask::OnJobStarted(jip);

            ++Controller->RunningSortJobCount;
            Controller->SortChunkCounter.Start(jip->PoolResult->TotalChunkCount);
            Controller->SortWeightCounter.Start(jip->PoolResult->TotalChunkWeight);
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            TTask::OnJobCompleted(jip);

            --Controller->RunningSortJobCount;
            ++Controller->CompletedSortJobCount;
            Controller->SortChunkCounter.Completed(jip->PoolResult->TotalChunkCount);
            Controller->SortWeightCounter.Completed(jip->PoolResult->TotalChunkWeight);

            if (Partition->Small) {
                // Sort outputs in small partitions go directly to the output table.
                Controller->CompletePartition(Partition, jip->ChunkListIds[0]);
                return;
            } 

            // Sort outputs in large partitions are queued for further merge.

            // Construct a stripe consisting of sorted chunks.
            const auto& resultExt = jip->Job->Result().GetExtension(TSortJobResultExt::sort_job_result_ext);
            auto stripe = New<TChunkStripe>();
            FOREACH (const auto& chunk, resultExt.chunks()) {
                auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                i64 weight = miscExt->data_weight();
                stripe->AddChunk(chunk, weight);
            }

            // Put the stripe into the pool.
            Partition->MergeTask->AddStripe(stripe);
        }

        virtual void OnJobFailed(TJobInProgress* jip)
        {
            TTask::OnJobFailed(jip);

            --Controller->RunningSortJobCount;
            Controller->SortChunkCounter.Failed(jip->PoolResult->TotalChunkCount);
            Controller->SortWeightCounter.Failed(jip->PoolResult->TotalChunkWeight);
        }

        virtual void OnTaskCompleted()
        {
            // Kick-start the corresponding merge task.
            Controller->RegisterTaskPendingHint(Partition->MergeTask);
        }
    };


    // Merge task.

    class TMergeTask
        : public TTask
    {
    public:
        TMergeTask(TSortController* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        { }

        virtual Stroka GetId() const
        {
            return Sprintf("Merge:%d", Partition->Index);
        }

        virtual int GetPendingJobCount() const
        {
            return
                !Partition->Small &&
                Controller->PartitionTask->IsCompleted() &&
                Partition->SortTask->IsCompleted() &&
                ChunkPool->IsPending()
                ? 1 : 0;
        }

        virtual TDuration GetMaxLocalityDelay() const
        {
            return TDuration::Zero();
        }

    private:
        TSortController* Controller;
        TPartition* Partition;

        virtual int GetChunkListCountPerJob() const 
        {
            return 1;
        }

        virtual TNullable<i64> GetJobWeightThreshold() const
        {
            return Null;
        }

        virtual TJobSpec GetJobSpec(TJobInProgress* jip)
        {
            auto jobSpec = Controller->MergeJobSpecTemplate;

            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                auto* inputSpec = jobSpec.add_input_specs();
                FOREACH (const auto& chunk, stripe->Chunks) {
                    *inputSpec->add_chunks() = chunk.InputChunk;
                }
            }

            {
                auto* outputSpec = jobSpec.add_output_specs();
                const auto& ouputTable = Controller->OutputTables[0];
                auto chunkListId = Controller->ChunkListPool->Extract();
                jip->ChunkListIds.push_back(chunkListId);
                *outputSpec->mutable_chunk_list_id() = chunkListId.ToProto();
                outputSpec->set_channels(ouputTable.Channels);
            }

            return jobSpec;
        }

        virtual void OnJobStarted(TJobInProgress* jip)
        {
            TTask::OnJobStarted(jip);

            ++Controller->RunningMergeJobCount;
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            TTask::OnJobCompleted(jip);

            --Controller->RunningMergeJobCount;
            ++Controller->CompletedMergeJobCount;

            YCHECK(ChunkPool->IsCompleted());
            Controller->CompletePartition(Partition, jip->ChunkListIds[0]);
        }

        virtual void OnJobFailed(TJobInProgress* jip)
        {
            TTask::OnJobFailed(jip);

            --Controller->RunningMergeJobCount;
        }
    };

    // Init/finish.

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


    // Job scheduling and outcome handling for sort phase.

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
            ->Add(BIND(&TSortController::RequestSamples, MakeStrong(this)))
            ->Add(BIND(&TSortController::OnSamplesReceived, MakeStrong(this)));
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

                    PartitionChunkCounter.Increment(1);
                    PartitionWeightCounter.Increment(weight);

                    SamplesFetcher->AddChunk(chunk);

                    auto stripe = New<TChunkStripe>(chunk, weight);
                    PartitionTask->AddStripe(stripe);
                }
            }

            // Check for empty inputs.
            if (PartitionChunkCounter.GetTotal() == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return MakeFuture(TValueOrError<void>());
            }

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %" PRId64 ")",
                PartitionWeightCounter.GetTotal(),
                PartitionChunkCounter.GetTotal());

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
                i64 weight = miscExt->data_weight();
                SortChunkCounter.Increment(1);
                SortWeightCounter.Increment(weight);
            }
        }

        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = Spec->PartitionCount
            ? Spec->PartitionCount.Get()
            : static_cast<int>(ceil((double) SortWeightCounter.GetTotal() / Spec->MinSortPartitionSize));

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
        auto partition = Partitions[0] = New<TPartition>(this, 0);
        partition->Small = true;

        // There will be no partition jobs, reset partition counters.
        PartitionChunkCounter.Set(0);
        PartitionWeightCounter.Set(0);

        // Put all input chunks into this unique partition.
        FOREACH (const auto& table, InputTables) {
            FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                i64 weight = miscExt->uncompressed_data_size();
                auto stripe = New<TChunkStripe>(chunk, weight);
                partition->SortTask->AddStripe(stripe);
                SortWeightCounter.Increment(weight);
                SortChunkCounter.Increment(1);
            }
        }

        // Init counters.
        MaxSortJobCount = GetJobCount(
            SortWeightCounter.GetTotal(),
            Spec->MaxSortJobDataSize,
            Spec->SortJobCount,
            SortChunkCounter.GetTotal());

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
            Partitions[partIndex] = New<TPartition>(this, partIndex);
        }

        // Init counters.
        PartitionJobCounter.Set(GetJobCount(
            PartitionWeightCounter.GetTotal(),
            Config->PartitionJobIO->ChunkSequenceWriter->DesiredChunkSize,
            Spec->PartitionJobCount,
            PartitionChunkCounter.GetTotal()));

        // Very rough estimates.
        MaxSortJobCount = GetJobCount(
            PartitionWeightCounter.GetTotal(),
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
                PartitionJobCounter.GetTotal() +
                MaxSortJobCount +
                MaxMergeJobCount +
                Config->SpareChunkListCount);

            InitJobSpecTemplates();

            LOG_INFO("Samples processed (PartitionJobCount: %" PRId64 ")",
                PartitionJobCounter.GetTotal());
        }
    }


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d}, "
            "Partitions = {T: %d, C: %d}, "
            "PartitionJobs = %s, "
            "PartitionChunks = %s, "
            "PartitionWeight = %s, "
            "SortJobs = {M: %d, R: %d, C: %d}, "
            "SortChunks = %s, "
            "SortWeight = %s, "
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
            ~ToString(PartitionJobCounter),
            ~ToString(PartitionChunkCounter),
            ~ToString(PartitionWeightCounter),
            // SortJobs
            MaxSortJobCount,
            RunningSortJobCount,
            CompletedSortJobCount,
            ~ToString(SortChunkCounter),
            ~ToString(SortWeightCounter),
            // MergeJobs
            MaxMergeJobCount,
            RunningMergeJobCount,
            CompletedMergeJobCount);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Scalar(Partitions.size())
                .Item("completed").Scalar(CompletedPartitionCount)
            .EndMap()
            .Item("partition_jobs").Do(BIND(&TProgressCounter::ToYson, &PartitionJobCounter))
            .Item("partition_chunks").Do(BIND(&TProgressCounter::ToYson, &PartitionChunkCounter))
            .Item("partition_weight").Do(BIND(&TProgressCounter::ToYson, &PartitionWeightCounter))
            .Item("sort_jobs").BeginMap()
                .Item("max").Scalar(MaxSortJobCount)
                .Item("running").Scalar(RunningSortJobCount)
                .Item("completed").Scalar(CompletedSortJobCount)
            .EndMap()
            .Item("sort_chunks").Do(BIND(&TProgressCounter::ToYson, &SortChunkCounter))
            .Item("sort_weight").Do(BIND(&TProgressCounter::ToYson, &SortWeightCounter))
            .Item("merge_jobs").BeginMap()
                .Item("max").Scalar(MaxMergeJobCount)
                .Item("running").Scalar(RunningMergeJobCount)
                .Item("completed").Scalar(CompletedMergeJobCount)
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

