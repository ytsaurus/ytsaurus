#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"
#include "job_resources.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_holder/chunk_meta_extensions.h>

#include <ytlib/job_proxy/config.h>

#include <ytlib/transaction_client/transaction.h>

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
        , TotalDataSize(0)
        , TotalRowCount(0)
        , TotalValueCount(0)
        , CompletedPartitionCount(0)
        , SortStartThresholdReached(false)
        , MergeStartThresholdReached(false)
        , PartitionTask(New<TPartitionTask>(this))
        , SamplesFetcher(New<TSamplesFetcher>(
            Config,
            Spec,
            Host->GetBackgroundInvoker(),
            Operation->GetOperationId()))
    { }

private:
    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;

    // Totals.
    i64 TotalDataSize;
    i64 TotalRowCount;
    i64 TotalValueCount;

    // Counters.
    int CompletedPartitionCount;
    TProgressCounter PartitionJobCounter;
    
    // Sort job counters.
    TProgressCounter SortJobCounter;
    TProgressCounter SortDataSizeCounter;

    // Start thresholds.
    bool SortStartThresholdReached;
    bool MergeStartThresholdReached;
    
    // Sorted merge job counters.
    TProgressCounter SortedMergeJobCounter;

    // Unordered merge job counters.
    TProgressCounter UnorderedMergeJobCounter;

    // Forward declarations.
    class TPartitionTask;
    typedef TIntrusivePtr<TPartitionTask> TPartitionTaskPtr;

    class TSortTask;
    typedef TIntrusivePtr<TSortTask> TSortTaskPtr;

    class TSortedMergeTask;
    typedef TIntrusivePtr<TSortedMergeTask> TSortedMergeTaskPtr;

    class TUnorderedMergeTask;
    typedef TIntrusivePtr<TUnorderedMergeTask> TUnorderedMergeTaskPtr;

    // Partitions.

    struct TPartition
        : public TIntrinsicRefCounted
    {
        TPartition(TSortController* controller, int index)
            : Index(index)
            , Completed(false)
            , SortedMergeNeeded(false)
            , Megalomaniac(false)
            , SortTask(New<TSortTask>(controller, this))
            , SortedMergeTask(New<TSortedMergeTask>(controller, this))
            , UnorderedMergeTask(New<TUnorderedMergeTask>(controller, this))
        { }

        //! Sequential index (zero based).
        int Index;

        //! Is partition completed?
        bool Completed;

        //! Do we need to run merge tasks for this partition?
        bool SortedMergeNeeded;

        //! Does the partition consist of rows with the same key?
        bool Megalomaniac;

        //! Accumulated attributes.
        NTableClient::NProto::TPartitionsExt::TPartitionAttributes TotalAttributes;

        //! The only node where sorting and merging must take place (in case of multiple partitions).
        Stroka AssignedAddress;

        // Tasks.
        TSortTaskPtr SortTask;
        TSortedMergeTaskPtr SortedMergeTask;
        TUnorderedMergeTaskPtr UnorderedMergeTask;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    std::vector<TPartitionPtr> Partitions;

    // Samples.
    TSamplesFetcherPtr SamplesFetcher;
    std::vector<const NTableClient::NProto::TKey*> SortedSamples;

    //! |PartitionCount - 1| separating keys.
    std::vector<NTableClient::NProto::TKey> PartitionKeys;
    
    //! Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec IntermediateSortJobSpecTemplate;
    TJobSpec FinalSortJobSpecTemplate;
    TJobSpec SortedMergeJobSpecTemplate;
    TJobSpec UnorderedMergeJobSpecTemplate;

    TPartitionTaskPtr PartitionTask;

    
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

        virtual Stroka GetId() const OVERRIDE
        {
            return "Partition";
        }

        virtual int GetPriority() const OVERRIDE
        {
            return 0;
        }

        virtual int GetPendingJobCount() const OVERRIDE
        {
            return
                IsCompleted() 
                ? 0
                : Controller->PartitionJobCounter.GetPending();
        }

        virtual TDuration GetLocalityTimeout() const OVERRIDE
        {
            return Controller->Spec->PartitionLocalityTimeout;
        }

        virtual NProto::TNodeResources GetMinRequestedResources() const
        {
            return GetPartitionJobResources(
                Controller->Config->PartitionJobIO,
                std::min(Controller->Spec->MaxDataSizePerPartitionJob, Controller->TotalDataSize),
                Controller->Partitions.size());
        }

        virtual NProto::TNodeResources GetRequestedResourcesForJip(TJobInProgressPtr jip) const
        {
            return GetPartitionJobResources(
                Controller->Config->PartitionJobIO,
                jip->PoolResult->TotalDataSize,
                Controller->Partitions.size());
        }

    private:
        TSortController* Controller;

        virtual int GetChunkListCountPerJob() const OVERRIDE
        {
            return 1;
        }

        virtual TNullable<i64> GetJobDataSizeThreshold() const OVERRIDE
        {
            return GetJobDataSizeThresholdGeneric(
                GetPendingJobCount(),
                DataSizeCounter().GetPending());
        }

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) OVERRIDE
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, jip);
            AddTabularOutputSpec(jobSpec, jip, 0);
        }

        virtual void OnJobStarted(TJobInProgressPtr jip) OVERRIDE
        {
            Controller->PartitionJobCounter.Start(1);

            TTask::OnJobStarted(jip);
        }

        virtual void OnJobCompleted(TJobInProgressPtr jip) OVERRIDE
        {
            TTask::OnJobCompleted(jip);

            Controller->PartitionJobCounter.Completed(1);

            auto* resultExt = jip->Job->Result().MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
            FOREACH (auto& partitionChunk, *resultExt->mutable_chunks()) {
                // We're keeping chunk information received from partition jobs to populate sort pools.
                // TPartitionsExt is, however, quite heavy.
                // Deserialize it and then drop its protobuf copy immediately.
                auto partitionsExt = GetProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.extensions());
                RemoveProtoExtension<NTableClient::NProto::TPartitionsExt>(partitionChunk.mutable_extensions());

                auto rcPartitionChunk = New<TRefCountedInputChunk>(partitionChunk);

                YCHECK(partitionsExt.partitions_size() == Controller->Partitions.size());
                LOG_TRACE("Job partition attributes are:");
                for (int index = 0; index < partitionsExt.partitions_size(); ++index) {
                    const auto& jobPartitionAttributes = partitionsExt.partitions(index);
                    LOG_TRACE("Partition[%d] = {%s}", index, ~jobPartitionAttributes.DebugString());

                    // Add up attributes.
                    auto partition = Controller->Partitions[index];
                    auto& totalAttributes = partition->TotalAttributes;
                    totalAttributes.set_uncompressed_data_size(
                        totalAttributes.uncompressed_data_size() + jobPartitionAttributes.uncompressed_data_size());
                    totalAttributes.set_row_count(totalAttributes.row_count() + jobPartitionAttributes.row_count());

                    if (jobPartitionAttributes.uncompressed_data_size() > 0) {
                        auto stripe = New<TChunkStripe>(
                            rcPartitionChunk,
                            jobPartitionAttributes.uncompressed_data_size(),
                            jobPartitionAttributes.row_count());
                        auto destinationTask = partition->Megalomaniac
                            ? TTaskPtr(partition->UnorderedMergeTask)
                            : TTaskPtr(partition->SortTask);
                        destinationTask->AddStripe(stripe);
                    }
                }
            }

            Controller->CheckSortStartThreshold();
        }

        virtual void OnJobFailed(TJobInProgressPtr jip) OVERRIDE
        {
            Controller->PartitionJobCounter.Failed(1);

            TTask::OnJobFailed(jip);
        }

        virtual void OnTaskCompleted() OVERRIDE
        {
            TTask::OnTaskCompleted();

            // Compute jobs totals.
            FOREACH (auto partition, Controller->Partitions) {
                if (partition->Megalomaniac) {
                    Controller->UnorderedMergeJobCounter.Increment(1);
                } else {
                    if (partition->SortTask->DataSizeCounter().GetTotal() > Controller->Spec->MaxDataSizePerSortJob) {
                        // This is still an estimate: sort job may occasionally get more input that
                        // dictated by MaxSizePerSortJob bound.
                        Controller->SortedMergeJobCounter.Increment(1);
                    }
                }
            }

            // Dump totals.
            LOG_DEBUG("Total partition attributes are:");
            for (int index = 0; index < static_cast<int>(Controller->Partitions.size()); ++index) {
                LOG_DEBUG("Partition[%d] = {%s}",
                    index,
                    ~Controller->Partitions[index]->TotalAttributes.DebugString());
            }

            // Mark empty partitions are completed.
            FOREACH (auto partition, Controller->Partitions) {
                if (partition->TotalAttributes.uncompressed_data_size() == 0) {
                    LOG_DEBUG("Partition is empty (Partition: %d)", partition->Index);
                    Controller->OnPartitionCompeted(partition);
                }
            }

            // Kick-start sort and unordered merge tasks.
            Controller->AddSortTasksPendingHints();
            Controller->AddMergeTasksPendingHints();
        }
    };

    //! Base class for tasks that are assigned to particular partitions.
    class TPartitionBoundTask
        : public TTask
    {
    public:
        TPartitionBoundTask(TSortController* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        { }

        virtual TDuration GetLocalityTimeout() const OVERRIDE
        {
            // Locality timeouts are only used for simple sort.
            return
                Controller->Partitions.size() == 1
                ? GetSimpleLocalityTimeout()
                : TDuration::Zero();
        }

        virtual i64 GetLocality(const Stroka& address) const OVERRIDE
        {
            if (Controller->Partitions.size() == 1) {
                // Any positive number will do.
                return 1;
            } 

            if (Partition->AssignedAddress != address) {
                // Never start sort jobs on a wrong node.
                return -1;
            }

            // Report locality proportional to the pending data size.
            // This facilitates uniform sort progress across partitions.
            return DataSizeCounter().GetPending();
        }

    protected:
        TSortController* Controller;
        TPartition* Partition;

        virtual TDuration GetSimpleLocalityTimeout() const = 0;

    };

    class TSortTask
        : public TPartitionBoundTask
    {
    public:
        TSortTask(TSortController* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        {
            ChunkPool = CreateUnorderedChunkPool(false);
        }

        virtual Stroka GetId() const OVERRIDE
        {
            return Sprintf("Sort(%d)", Partition->Index);
        }

        virtual int GetPriority() const OVERRIDE
        {
            return 1;
        }

        virtual int GetPendingJobCount() const OVERRIDE
        {
            // Check if enough partition jobs are completed.
            if (!Controller->SortStartThresholdReached) {
                return 0;
            }

            // Compute pending job count based on pooled data size and data size per job.
            // If partition phase is completed, take any remaining da.
            // If partition phase is still in progress, only take size exceeding size per job.
            i64 dataSize = ChunkPool->DataSizeCounter().GetPending();
            i64 dataSizePerJob = Controller->Spec->MaxDataSizePerSortJob;
            double fractionalJobCount = (double) dataSize / dataSizePerJob;
            return
                Controller->PartitionTask->IsCompleted()
                ? static_cast<int>(ceil(fractionalJobCount))
                : static_cast<int>(floor(fractionalJobCount));
        }

        virtual NProto::TNodeResources GetMinRequestedResources() const OVERRIDE
        {
            return GetRequestedResourcesForDataSize(std::min(
                Controller->Spec->MaxDataSizePerSortJob,
                Controller->TotalDataSize));
        }

        virtual NProto::TNodeResources GetRequestedResourcesForJip(TJobInProgressPtr jip) const OVERRIDE
        {
            return GetRequestedResourcesForDataSize(jip->PoolResult->TotalDataSize);
        }

        virtual void AddStripe(TChunkStripePtr stripe) OVERRIDE
        {
            i64 oldTotal = ChunkPool->DataSizeCounter().GetTotal();
            TPartitionBoundTask::AddStripe(stripe);
            i64 newTotal = ChunkPool->DataSizeCounter().GetTotal();
            Controller->SortDataSizeCounter.Increment(newTotal - oldTotal);
        }

        bool IsCompleted() const
        {
            return
                Controller->PartitionTask->IsCompleted() &&
                TPartitionBoundTask::IsCompleted();
        }

    private:
        NProto::TNodeResources GetRequestedResourcesForDataSize(i64 dataSize) const
        {
            i64 rowCount = Controller->GetRowCountEstimate(dataSize);
            i64 valueCount = Controller->GetValueCountEstimate(dataSize);
            if (Controller->Partitions.size() == 1) {
                return GetSimpleSortJobResources(
                    Controller->Config->SortJobIO,
                    Controller->Spec,
                    dataSize,
                    rowCount,
                    valueCount);
            } else {
                return GetPartitionSortJobResources(
                    Controller->Config->SortJobIO,
                    Controller->Spec,
                    dataSize,
                    rowCount);
            }
        }

        virtual TDuration GetSimpleLocalityTimeout() const OVERRIDE
        {
            return Controller->Spec->SortLocalityTimeout;
        }

        virtual int GetChunkListCountPerJob() const OVERRIDE
        {
            return 1;
        }

        virtual TNullable<i64> GetJobDataSizeThreshold() const OVERRIDE
        {
            return Controller->Spec->MaxDataSizePerSortJob;
        }

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) OVERRIDE
        {
            if (Controller->IsSortedMergeNeeded(Partition)) {
                jobSpec->CopyFrom(Controller->IntermediateSortJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller->FinalSortJobSpecTemplate);
            }

            AddSequentialInputSpec(jobSpec, jip);
            AddTabularOutputSpec(jobSpec, jip, 0);

            {
                auto* jobSpecExt = jobSpec->MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                if (Controller->Partitions.size() > 1) {
                    auto* inputSpec = jobSpec->mutable_input_specs(0);
                    FOREACH (auto& chunk, *inputSpec->mutable_chunks()) {
                        chunk.set_partition_tag(Partition->Index);
                    }
                }
            }
        }

        virtual void OnJobStarted(TJobInProgressPtr jip) OVERRIDE
        {
            TPartitionBoundTask::OnJobStarted(jip);

            YCHECK(!Partition->Megalomaniac);

            Controller->SortJobCounter.Start(1);
            Controller->SortDataSizeCounter.Start(jip->PoolResult->TotalDataSize);

            // Notify the controller that we're willing to use this node
            // for all subsequent jobs.
            auto address = jip->Job->GetNode()->GetAddress();
            Controller->AddTaskLocalityHint(this, address);
        }

        virtual void OnJobCompleted(TJobInProgressPtr jip) OVERRIDE
        {
            TPartitionBoundTask::OnJobCompleted(jip);

            Controller->SortJobCounter.Completed(1);
            Controller->SortDataSizeCounter.Completed(jip->PoolResult->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                const auto& resultExt = jip->Job->Result().GetExtension(TSortJobResultExt::sort_job_result_ext);
                auto stripe = New<TChunkStripe>();
                FOREACH (const auto& inputChunk, resultExt.chunks()) {
                    stripe->AddChunk(New<TRefCountedInputChunk>(inputChunk));
                }
                Partition->SortedMergeTask->AddStripe(stripe);
            } else {
                // Sort outputs in small partitions go directly to the output.
                Controller->RegisterOutputChunkTree(Partition, jip->ChunkListIds[0]);
                Controller->OnPartitionCompeted(Partition);
            }

            Controller->CheckMergeStartThreshold();
        }

        virtual void OnJobFailed(TJobInProgressPtr jip) OVERRIDE
        {
            Controller->SortJobCounter.Failed(1);
            Controller->SortDataSizeCounter.Failed(jip->PoolResult->TotalDataSize);

            TPartitionBoundTask::OnJobFailed(jip);
        }

        virtual void OnTaskCompleted() OVERRIDE
        {
            TPartitionBoundTask::OnTaskCompleted();

            // Kick-start the corresponding merge task.
            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->AddTaskPendingHint(Partition->SortedMergeTask);
            }
        }

        virtual void AddInputLocalityHint(TChunkStripePtr stripe) OVERRIDE
        {
            UNUSED(stripe);
            // See #GetLocality.
        }
    };

    //! Base class for both sorted and ordered merge.
    class TMergeTask
        : public TPartitionBoundTask
    {
    public:
        TMergeTask(TSortController* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual int GetPriority() const OVERRIDE
        {
            return 2;
        }

    protected:
        virtual TDuration GetSimpleLocalityTimeout() const OVERRIDE
        {
            return Controller->Spec->MergeLocalityTimeout;
        }

    };

    class TSortedMergeTask
        : public TMergeTask
    {
    public:
        TSortedMergeTask(TSortController* controller, TPartition* partition)
            : TMergeTask(controller, partition)
        {
            ChunkPool = CreateAtomicChunkPool();
        }

        virtual Stroka GetId() const OVERRIDE
        {
            return Sprintf("SortedMerge(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const OVERRIDE
        {
            if (!Controller->MergeStartThresholdReached) {
                return 0;
            }

            return
                Controller->IsSortedMergeNeeded(Partition) &&
                Partition->SortTask->IsCompleted() &&
                IsPending()
                ? 1 : 0;
        }

        virtual NProto::TNodeResources GetMinRequestedResources() const OVERRIDE
        {
            return GetSortedMergeDuringSortJobResources(
                Controller->Config->MergeJobIO,
                Controller->Spec,
                ChunkPool->StripeCounter().GetTotal());
        }

    private:
        virtual int GetChunkListCountPerJob() const OVERRIDE
        {
            return 1;
        }

        virtual TNullable<i64> GetJobDataSizeThreshold() const OVERRIDE
        {
            return Null;
        }

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) OVERRIDE
        {
            jobSpec->CopyFrom(Controller->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, jip);
            AddTabularOutputSpec(jobSpec, jip, 0);
        }

        virtual void OnJobStarted(TJobInProgressPtr jip) OVERRIDE
        {
            YCHECK(!Partition->Megalomaniac);

            Controller->SortedMergeJobCounter.Start(1);

            TMergeTask::OnJobStarted(jip);
        }

        virtual void OnJobCompleted(TJobInProgressPtr jip) OVERRIDE
        {
            TMergeTask::OnJobCompleted(jip);

            Controller->SortedMergeJobCounter.Completed(1);

            Controller->RegisterOutputChunkTree(Partition, jip->ChunkListIds[0]);

            YCHECK(ChunkPool->IsCompleted());
            Controller->OnPartitionCompeted(Partition);
        }

        virtual void OnJobFailed(TJobInProgressPtr jip) OVERRIDE
        {
            Controller->SortedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(jip);
        }
    };

    class TUnorderedMergeTask
        : public TMergeTask
    {
    public:
        TUnorderedMergeTask(TSortController* controller, TPartition* partition)
            : TMergeTask(controller, partition)
        {
            ChunkPool = CreateUnorderedChunkPool();
            MinRequestedResources = GetUnorderedMergeDuringSortJobResources(
                Controller->Config->MergeJobIO,
                Controller->Spec);
        }

        virtual Stroka GetId() const OVERRIDE
        {
            return Sprintf("UnorderedMerge(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const OVERRIDE
        {
            if (!Controller->MergeStartThresholdReached) {
                return 0;
            }

            if (!Controller->IsUnorderedMergeNeeded(Partition)) {
                return 0;
            }

            i64 dataSize = ChunkPool->DataSizeCounter().GetPending();
            i64 dataSizePerJob = Controller->Spec->MaxDataSizePerUnorderedMergeJob;
            return static_cast<int>(ceil((double) dataSize / dataSizePerJob));
        }

        virtual NProto::TNodeResources GetMinRequestedResources() const OVERRIDE
        {
            return MinRequestedResources;
        }

    private:
        NProto::TNodeResources MinRequestedResources;

        virtual int GetChunkListCountPerJob() const OVERRIDE
        {
            return 1;
        }

        virtual TNullable<i64> GetJobDataSizeThreshold() const OVERRIDE
        {
            return Controller->Spec->MaxDataSizePerUnorderedMergeJob;
        }

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) OVERRIDE
        {
            jobSpec->CopyFrom(Controller->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, jip);
            AddTabularOutputSpec(jobSpec, jip, 0);

            if (Controller->Partitions.size() > 1) {
                auto* inputSpec = jobSpec->mutable_input_specs(0);
                FOREACH (auto& chunk, *inputSpec->mutable_chunks()) {
                    chunk.set_partition_tag(Partition->Index);
                }
            }
        }

        virtual void OnJobStarted(TJobInProgressPtr jip) OVERRIDE
        {
            YCHECK(Partition->Megalomaniac);

            Controller->UnorderedMergeJobCounter.Start(1);

            TMergeTask::OnJobStarted(jip);
        }

        virtual void OnJobCompleted(TJobInProgressPtr jip) OVERRIDE
        {
            TMergeTask::OnJobCompleted(jip);

            Controller->UnorderedMergeJobCounter.Completed(1);

            Controller->RegisterOutputChunkTree(Partition, jip->ChunkListIds[0]);

            if (ChunkPool->IsCompleted()) {
                Controller->OnPartitionCompeted(Partition);
            }
        }

        virtual void OnJobFailed(TJobInProgressPtr jip) OVERRIDE
        {
            Controller->UnorderedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(jip);
        }
    };


    // Init/finish.

    virtual void DoInitialize() OVERRIDE
    {
        ScheduleClearOutputTables();
    }

    virtual void OnOperationCompleted() OVERRIDE
    {
        YCHECK(CompletedPartitionCount == Partitions.size());
        TOperationControllerBase::OnOperationCompleted();
    }


    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths() OVERRIDE
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths() OVERRIDE
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) OVERRIDE
    {
        return pipeline
            ->Add(BIND(&TSortController::RequestSamples, MakeStrong(this)))
            ->Add(BIND(&TSortController::OnSamplesReceived, MakeStrong(this)));
    }

    TFuture< TValueOrError<void> > RequestSamples()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            int chunkCount = 0;
            FOREACH (const auto& table, InputTables) {
                FOREACH (const auto& chunk, table.FetchResponse->chunks()) {
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    TotalDataSize += miscExt.uncompressed_data_size();
                    TotalRowCount += miscExt.row_count();
                    TotalValueCount += miscExt.value_count();

                    SamplesFetcher->AddChunk(chunk);
                    ++chunkCount;
                }
            }

            LOG_INFO("Totals collected (DataSize: %" PRId64 ", RowCount: % " PRId64 ", ValueCount: %" PRId64 ")",
                TotalDataSize,
                TotalRowCount,
                TotalValueCount);

            // Check for empty inputs.
            if (chunkCount == 0) {
                LOG_INFO("Empty input");
                OnOperationCompleted();
                return NewPromise< TValueOrError<void> >();
            }

            return SamplesFetcher->Run(GetPartitionCountEstimate() * Spec->SamplesPerPartition);
        }
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) OVERRIDE
    {
        UNUSED(batchRsp);

        // TODO(babenko): unless overwrite mode is ON
        CheckOutputTablesEmpty();
        ScheduleSetOutputTablesSorted(Spec->KeyColumns);
    }

    void RegisterOutputChunkTree(TPartitionPtr partition, const TChunkTreeId& chunkTreeId)
    {
        TOperationControllerBase::RegisterOutputChunkTree(chunkTreeId, partition->Index, 0);
    }

    void OnPartitionCompeted(TPartitionPtr partition)
    {
        YCHECK(!partition->Completed);
        partition->Completed = true;

        ++CompletedPartitionCount;

        LOG_INFO("Partition completed (Partition: %d)", partition->Index);
    }

    bool IsSortedMergeNeeded(TPartitionPtr partition)
    {
        // Check for cached value.
        if (partition->SortedMergeNeeded) {
            return true;
        }

        // Some easy cases.
        if (partition->Megalomaniac) {
            return false;
        }

        // Check if the sort task only handles a fraction of the partition.
        // Two cases are possible:
        // 1) Partition task is still running and thus may enqueue
        // additional data to be sorted.
        // 2) The sort pool hasn't been exhausted by the current job.
        bool mergeNeeded =
            !PartitionTask->IsCompleted() ||
            partition->SortTask->IsPending();

        if (mergeNeeded) {
            LOG_DEBUG("Partition needs sorted merge (Partition: %d)", partition->Index);
            partition->SortedMergeNeeded = true;
        }

        return mergeNeeded;
    }

    bool IsUnorderedMergeNeeded(TPartitionPtr partition)
    {
        return
            partition->Megalomaniac &&
            PartitionTask->IsCompleted();
    }

    void CheckSortStartThreshold()
    {
        if (SortStartThresholdReached)
            return;

        const auto& partitionDataSizeCounter = PartitionTask->DataSizeCounter();
        if (partitionDataSizeCounter.GetCompleted() < partitionDataSizeCounter.GetTotal() * Spec->SortStartThreshold)
            return;

        LOG_INFO("Sort start threshold reached");

        SortStartThresholdReached = true;
        AssignPartitions();
        AddSortTasksPendingHints();
    }

    void CheckMergeStartThreshold()
    {
        if (MergeStartThresholdReached)
            return;

        if (!PartitionTask->IsCompleted())
            return;

        if (SortDataSizeCounter.GetCompleted() < SortDataSizeCounter.GetTotal() * Spec->MergeStartThreshold)
            return;

        LOG_INFO("Merge start threshold reached");

        MergeStartThresholdReached = true;
        AddMergeTasksPendingHints();
    }

    void AddSortTasksPendingHints()
    {
        FOREACH (auto partition, Partitions) {
            if (!partition->Megalomaniac) {
                AddTaskPendingHint(partition->SortTask);
            }
        }   
    }

    void AddMergeTasksPendingHints()
    {
        FOREACH (auto partition, Partitions) {
            auto taskToKick = partition->Megalomaniac
                ? TTaskPtr(partition->UnorderedMergeTask)
                : TTaskPtr(partition->SortedMergeTask);
            AddTaskPendingHint(taskToKick);
        }
    }

    void AssignPartition(TPartitionPtr partition, TExecNodePtr node)
    {
        LOG_DEBUG("Partition assigned: %d -> %s",
            partition->Index,
            ~node->GetAddress());
        partition->AssignedAddress = node->GetAddress();
    }

    void AssignPartitions()
    {
        auto nodes = Host->GetExecNodes();
        if (nodes.empty()) {
            OnOperationFailed(TError("No online exec nodes to assign partitions"));
        }

        std::random_shuffle(nodes.begin(), nodes.end());

        int currentNode = 0;
        FOREACH (auto partition, Partitions) {
            AssignPartition(partition, nodes[currentNode]);
            currentNode = (currentNode + 1) % nodes.size();
        }
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
        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = GetPartitionCountEstimate();

        // Don't create more partitions than we have samples (plus one).
        partitionCount = std::min(partitionCount, static_cast<int>(SortedSamples.size()) + 1);

        // Don't create more partitions than allowed by the global config.
        partitionCount = std::min(partitionCount, Config->MaxPartitionCount);

        YCHECK(partitionCount > 0);

        if (partitionCount == 1) {
            BuildSinglePartition();
        } else {
            BuildMulitplePartitions(partitionCount);
        }
    }

    void BuildSinglePartition()
    {
        // Create a single partition.
        Partitions.resize(1);
        auto partition = Partitions[0] = New<TPartition>(this, 0);

        // Put all input chunks into this unique partition.
        auto inputChunks = CollectInputTablesChunks();
        auto stripes = PrepareChunkStripes(
            inputChunks,
            Spec->SortJobCount,
            Spec->SortJobSliceDataSize);
        partition->SortTask->AddStripes(stripes);

        // A pretty accurate estimate.
        SortJobCounter.Set(GetJobCount(
            partition->SortTask->DataSizeCounter().GetTotal(),
            Spec->MaxDataSizePerSortJob,
            Spec->SortJobCount,
            partition->SortTask->ChunkCounter().GetTotal()));

        // Can be zero but better be pessimists.
        SortedMergeJobCounter.Set(1);

        LOG_INFO("Sorting without partitioning");

        // Kick-start the sort task.
        SortStartThresholdReached = true;
        AddTaskPendingHint(partition->SortTask);
    }

    void AddPartition(const NTableClient::NProto::TKey& key)
    {
        using NTableClient::ToString;
        using ::ToString;

        int index = static_cast<int>(Partitions.size());
        LOG_DEBUG("Partition %d has starting key %s",
            index,
            ~ToString(key));

        YCHECK(PartitionKeys.empty() || CompareKeys(PartitionKeys.back(), key) < 0);

        PartitionKeys.push_back(key);
        Partitions.push_back(New<TPartition>(this, index));
    }

    void BuildMulitplePartitions(int partitionCount)
    {
        LOG_DEBUG("Building partition keys");

        auto GetSampleKey = [&](int sampleIndex) {
            return SortedSamples[(sampleIndex + 1) * (SortedSamples.size() - 1) / partitionCount];
        };

        // Construct the leftmost partition.
        Partitions.push_back(New<TPartition>(this, 0));

        // Invariant:
        //   lastPartition = Partitions.back()
        //   lastKey = PartitionKeys.back()
        //   lastPartition receives keys in [lastKey, ...)
        //   
        // Initially PartitionKeys is empty so lastKey is assumed to be -inf.

        // Take partition keys evenly.
        int sampleIndex = 0;
        while (sampleIndex < partitionCount - 1) {
            auto* sampleKey = GetSampleKey(sampleIndex);
            // Check for same keys.
            if (PartitionKeys.empty() || CompareKeys(*sampleKey, PartitionKeys.back()) != 0) {
                AddPartition(*sampleKey);
                ++sampleIndex;
            } else {
                // Skip same keys.
                int skippedCount = 0;
                while (sampleIndex < partitionCount - 1 &&
                       CompareKeys(*GetSampleKey(sampleIndex), PartitionKeys.back()) == 0)
                {
                    ++sampleIndex;
                    ++skippedCount;
                }

                auto lastPartition = Partitions.back();
                LOG_DEBUG("Partition %d is a megalomaniac, skipped %d samples",
                    lastPartition->Index,
                    skippedCount);

                lastPartition->Megalomaniac = true;
                YCHECK(skippedCount >= 1);
                
                auto successorKey = GetSuccessorKey(*sampleKey);
                AddPartition(successorKey);
            }
        }

        // Populate the partition pool.
        auto inputChunks = CollectInputTablesChunks();
        auto stripes = PrepareChunkStripes(
            inputChunks,
            Spec->PartitionJobCount,
            Spec->PartitionJobSliceDataSize);
        PartitionTask->AddStripes(stripes);

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %" PRId64 ")",
            PartitionTask->DataSizeCounter().GetTotal(),
            PartitionTask->ChunkCounter().GetTotal());

        // Init counters.
        PartitionJobCounter.Set(GetJobCount(
            PartitionTask->DataSizeCounter().GetTotal(),
            Config->PartitionJobIO->TableWriter->DesiredChunkSize,
            Spec->PartitionJobCount,
            PartitionTask->ChunkCounter().GetTotal()));

        // Some upper bound.
        SortJobCounter.Set(
            GetJobCount(
                PartitionTask->DataSizeCounter().GetTotal(),
                Spec->MaxDataSizePerSortJob,
                Null,
                std::numeric_limits<int>::max()) +
            Partitions.size());

        LOG_INFO("Sorting with partitioning (PartitionCount: %d, PartitionJobCount: %" PRId64 ")",
            static_cast<int>(Partitions.size()),
            PartitionJobCounter.GetTotal());

        // Kick-start the partition task.
        AddTaskPendingHint(PartitionTask);
    }

    void OnSamplesReceived()
    {
        PROFILE_TIMING ("/samples_processing_time") {
            SortSamples();
            BuildPartitions();

            SamplesFetcher.Reset();
            SortedSamples.clear();

            InitJobSpecTemplates();
        }
    }


    virtual NProto::TNodeResources GetMinRequestedResources() const OVERRIDE
    {
        if (PartitionTask) {
            return PartitionTask->GetMinRequestedResources();
        }
        if (!Partitions.empty()) {
            return Partitions[0]->SortTask->GetMinRequestedResources();
        }
        return InfiniteResources();
    }


    // Progress reporting.

    virtual void LogProgress() OVERRIDE
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d}, "
            "Partitions = {T: %d, C: %d}, "
            "PartitionJobs = {%s}, "
            "SortJobs = {%s}, "
            "SortedMergeJobs = {%s}, "
            "UnorderedMergeJobs = {%s}",
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
            // SortJobs
            ~ToString(SortJobCounter),
            // SortedMergeJobs
            ~ToString(SortedMergeJobCounter),
            // UnorderedMergeJobs
            ~ToString(UnorderedMergeJobCounter));
    }

    virtual void DoGetProgress(IYsonConsumer* consumer) OVERRIDE
    {
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Scalar(Partitions.size())
                .Item("completed").Scalar(CompletedPartitionCount)
            .EndMap()
            .Item("partition_jobs").Do(BIND(&TProgressCounter::ToYson, &PartitionJobCounter))
            .Item("sort_jobs").Do(BIND(&TProgressCounter::ToYson, &SortJobCounter))
            .Item("sorted_merge_jobs").Do(BIND(&TProgressCounter::ToYson, &SortedMergeJobCounter))
            .Item("unordered_merge_jobs").Do(BIND(&TProgressCounter::ToYson, &UnorderedMergeJobCounter));
    }

    // Unsorted helpers.

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(EJobType::Partition);
            *PartitionJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            FOREACH (const auto& key, PartitionKeys) {
                *specExt->add_partition_keys() = key;
            }
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);

            auto ioConfig = BuildJobIOConfig(Config->PartitionJobIO, Spec->PartitionJobIO);
            InitIntermediateOutputConfig(ioConfig);
            PartitionJobSpecTemplate.set_io_config(ConvertToYsonString(ioConfig).Data());
        }
        {
            TJobSpec sortJobSpecTemplate;
            sortJobSpecTemplate.set_type(Partitions.size() == 1 ? EJobType::SimpleSort : EJobType::PartitionSort);
            *sortJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);

            auto ioConfig = BuildJobIOConfig(Config->SortJobIO, Spec->SortJobIO);
            
            // Disable master requests for inputs coming from partition jobs.
            if (Partitions.size() > 1) {
                InitIntermediateInputConfig(ioConfig);
            }

            // Use output replication to sort jobs in small partitions since their chunks go directly to the output.
            // Don't use replication for sort jobs in large partitions since their chunks will be merged.

            auto finalIOConfig = ioConfig;
            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_io_config(ConvertToYsonString(finalIOConfig).Data());

            auto intermediateIOConfig = CloneConfigurable(ioConfig);
            InitIntermediateOutputConfig(ioConfig);
            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_io_config(ConvertToYsonString(intermediateIOConfig).Data());
        }
        {
            SortedMergeJobSpecTemplate.set_type(EJobType::SortedMerge);
            *SortedMergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);

            auto ioConfig = BuildJobIOConfig(Config->MergeJobIO, Spec->MergeJobIO);
            InitIntermediateInputConfig(ioConfig);
            InitIntermediateOutputConfig(ioConfig);
            SortedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(ioConfig).Data());
        }
        {
            UnorderedMergeJobSpecTemplate.set_type(EJobType::UnorderedMerge);
            *UnorderedMergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->KeyColumns);

            auto ioConfig = BuildJobIOConfig(Config->MergeJobIO, Spec->MergeJobIO);
            InitIntermediateInputConfig(ioConfig);
            InitIntermediateOutputConfig(ioConfig);
            UnorderedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(ioConfig).Data());
        }
    }

    i64 GetRowCountEstimate(i64 dataSize) const
    {
        return static_cast<i64>((double) TotalRowCount * dataSize / TotalDataSize);
    }

    i64 GetValueCountEstimate(i64 dataSize) const
    {
        return static_cast<i64>((double) TotalValueCount * dataSize / TotalDataSize);
    }

    int GetPartitionCountEstimate() const
    {
        YCHECK(TotalDataSize > 0);

        int result = Spec->PartitionCount ? 
            Spec->PartitionCount.Get() : 
            static_cast<int>(ceil(
                (double) TotalDataSize /
                Spec->MaxDataSizePerSortJob *
                Spec->PartitionCountBoostFactor));
        return std::max(1, result);
    }
};

IOperationControllerPtr CreateSortController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TSortOperationSpec>(operation);
    return New<TSortController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

