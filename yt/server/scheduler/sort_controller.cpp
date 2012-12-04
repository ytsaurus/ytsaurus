#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"
#include "chunk_info_collector.h"
#include "job_resources.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/key.h>
#include <ytlib/table_client/channel_writer.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/job_proxy/config.h>

#include <ytlib/transaction_client/transaction.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationLogger);
static NProfiling::TProfiler Profiler("/operations/sort");

////////////////////////////////////////////////////////////////////

class TSortControllerBase
    : public TOperationControllerBase
{
public:
    TSortControllerBase(
        TSchedulerConfigPtr config,
        TSortOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
        , Spec(spec)
        , Config(config)
        , TotalInputDataSize(0)
        , TotalInputRowCount(0)
        , TotalInputValueCount(0)
        , CompletedPartitionCount(0)
        , SortStartThresholdReached(false)
        , MergeStartThresholdReached(false)
        , SimpleSort(false)
    { }

    virtual TNodeResources GetMinNeededResources() override
    {
        if (PartitionTask && PartitionTask->IsPending()) {
            return PartitionTask->GetMinNeededResources();
        }
        if (!Partitions.empty()) {
            return Partitions[0]->SortTask->GetMinNeededResources();
        }

        return InfiniteNodeResources();
    }

private:
    TSortOperationSpecBasePtr Spec;

protected:
    TSchedulerConfigPtr Config;

    // Totals.
    i64 TotalInputDataSize;
    i64 TotalInputRowCount;
    i64 TotalInputValueCount;

    // Counters.
    int CompletedPartitionCount;
    TProgressCounter PartitionJobCounter;
    
    // Sort job counters.
    TProgressCounter IntermediateSortJobCounter;
    TProgressCounter FinalSortJobCounter;
    TProgressCounter SortDataSizeCounter;

    // Start thresholds.
    bool SortStartThresholdReached;
    bool MergeStartThresholdReached;
    
    // Sorted merge job counters.
    mutable TProgressCounter SortedMergeJobCounter;

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
        TPartition(TSortControllerBase* controller, int index)
            : Index(index)
            , Completed(false)
            , SortedMergeNeeded(false)
            , Maniac(false)
            , SortTask(New<TSortTask>(controller, this))
            , SortedMergeTask(New<TSortedMergeTask>(controller, this))
            , UnorderedMergeTask(New<TUnorderedMergeTask>(controller, this))
            , ChunkPoolOutput(NULL)
        { }

        //! Sequential index (zero based).
        int Index;

        //! Is partition completed?
        bool Completed;

        //! Do we need to run merge tasks for this partition?
        bool SortedMergeNeeded;

        //! Does the partition consist of rows with the same key?
        bool Maniac;

        //! The only node where sorting and merging must take place (in case of multiple partitions).
        Stroka AssignedAddress;

        // Tasks.
        TSortTaskPtr SortTask;
        TSortedMergeTaskPtr SortedMergeTask;
        TUnorderedMergeTaskPtr UnorderedMergeTask;

        // Chunk pool output obtained from the shuffle pool.
        IChunkPoolOutput* ChunkPoolOutput;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    //! Is equivalent to |Partitions.size() == 1| but enables
    //! checking for simple sort when #Partitions is still being constructed.
    bool SimpleSort;
    std::vector<TPartitionPtr> Partitions;

    //! Templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec IntermediateSortJobSpecTemplate;
    TJobSpec FinalSortJobSpecTemplate;
    TJobSpec SortedMergeJobSpecTemplate;
    TJobSpec UnorderedMergeJobSpecTemplate;

    TJobIOConfigPtr PartitionJobIOConfig;
    TJobIOConfigPtr IntermediateSortJobIOConfig;
    TJobIOConfigPtr FinalSortJobIOConfig;
    TJobIOConfigPtr SortedMergeJobIOConfig;
    TJobIOConfigPtr UnorderedMergeJobIOConfig;

    TAutoPtr<IShuffleChunkPool> ShufflePool;
    TAutoPtr<IChunkPool> SimpleSortPool;

    TPartitionTaskPtr PartitionTask;
    
    //! Implements partition phase for sort operations and map phase for map-reduce operations.
    class TPartitionTask
        : public TTask
    {
    public:
        explicit TPartitionTask(TSortControllerBase* controller)
            : TTask(controller)
            , Controller(controller)
        {
            ChunkPool = CreateUnorderedChunkPool(
                Controller->PartitionJobCounter.GetTotal(),
                Controller->Spec->MaxDataSizePerPartitionJob);
        }

        virtual Stroka GetId() const override
        {
            return "Partition";
        }

        virtual int GetPriority() const override
        {
            return 0;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->PartitionLocalityTimeout;
        }

        virtual TNodeResources GetMinNeededResources() const override
        {
            return Controller->GetMinNeededPartitionResources();
        }

        virtual TNodeResources GetAvgNeededResources() const override
        {
            int jobCount = GetPendingJobCount();
            if (jobCount == 0) {
                return ZeroNodeResources();
            }
            i64 dataSizePerJob = GetPendingDataSize() / jobCount;
            return Controller->GetPartitionResources(dataSizePerJob);
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            return Controller->GetPartitionResources(joblet->InputStripeList->TotalDataSize);
        }

    private:
        TSortControllerBase* Controller;

        TAutoPtr<IChunkPool> ChunkPool;

        
        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return 1;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddIntermediateOutputSpec(jobSpec, joblet);
            Controller->CustomizeJobSpec(joblet, jobSpec);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            Controller->PartitionJobCounter.Start(1);

            TTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            Controller->PartitionJobCounter.Completed(1);

            auto* resultExt = joblet->Job->Result().MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
            auto stripe = New<TChunkStripe>();
            FOREACH (auto& inputChunk, *resultExt->mutable_chunks()) {
                auto rcInputChunk = New<TRefCountedInputChunk>(inputChunk);
                stripe->Chunks.push_back(rcInputChunk);
            }

            Controller->ShufflePool->GetInput()->Add(stripe);

            // Drop TPartitionsExt, they are no longer needed.
            FOREACH (auto inputChunk, stripe->Chunks) {
                RemoveProtoExtension<NTableClient::NProto::TPartitionsExt>(inputChunk->mutable_extensions());
            }

            // Kick-start sort and unordered merge tasks.
            FOREACH (auto partition, Controller->Partitions) {
                auto task = partition->Maniac
                    ? TTaskPtr(partition->UnorderedMergeTask)
                    : TTaskPtr(partition->SortTask);
                Controller->AddTaskPendingHint(task);
            }

            Controller->CheckSortStartThreshold();
        }

        virtual void OnJobFailed(TJobletPtr joblet) override
        {
            Controller->PartitionJobCounter.Failed(1);

            TTask::OnJobFailed(joblet);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            Controller->PartitionJobCounter.Aborted(1);

            TTask::OnJobAborted(joblet);
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            Controller->PartitionJobCounter.Finalize();
            Controller->ShufflePool->GetInput()->Finish();

            // Dump totals.
            // Mark empty partitions are completed.
            LOG_DEBUG("Partition sizes collected");
            FOREACH (auto partition, Controller->Partitions) {
                i64 dataSize = partition->ChunkPoolOutput->GetTotalDataSize();
                if (dataSize == 0) {
                    LOG_DEBUG("Partition %d is empty", partition->Index);
                    Controller->OnPartitionCompleted(partition);
                } else {
                    LOG_DEBUG("Partition[%d] = %" PRId64,
                        partition->Index,
                        dataSize);
                }
            }

            // Kick-start sort and unordered merge tasks.
            Controller->AddSortTasksPendingHints();

            Controller->CheckMergeStartThreshold();
            Controller->AddMergeTasksPendingHints();
        }
    };

    //! Base class for tasks that are assigned to particular partitions.
    class TPartitionBoundTask
        : public TTask
    {
    public:
        TPartitionBoundTask(TSortControllerBase* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        { }

        virtual TDuration GetLocalityTimeout() const override
        {
            if (Controller->SimpleSort) {
                return GetSimpleLocalityTimeout();
            }

            return TDuration::Zero();
        }

        virtual i64 GetLocality(const Stroka& address) const override
        {
            if (Controller->SimpleSort) {
                return TTask::GetLocality(address);
            } 

            // Report locality proportional to the pending data size.
            // This facilitates uniform sort progress across partitions.
            // Never return 0 since this is a local task.
            i64 pendingDataSize = GetPendingDataSize();
            return pendingDataSize == 0 ? 1 : pendingDataSize;
        }

        virtual bool IsStrictlyLocal() const override
        {
            if (Controller->SimpleSort) {
                return TTask::IsStrictlyLocal();
            }

            return true;
        }

    protected:
        TSortControllerBase* Controller;
        TPartition* Partition;

        virtual TDuration GetSimpleLocalityTimeout() const = 0;

    };

    //! Implements sort phase (either simple or partitioned) for sort operations
    //! and partition reduce phase for map-reduce operations.
    class TSortTask
        : public TPartitionBoundTask
    {
    public:
        TSortTask(TSortControllerBase* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return Sprintf("Sort(%d)", Partition->Index);
        }

        virtual int GetPriority() const override
        {
            return 1;
        }

        virtual int GetPendingJobCount() const override
        {
            // Check if enough partition jobs are completed.
            if (!Controller->SortStartThresholdReached) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual TNodeResources GetMinNeededResources() const override
        {
            return Controller->GetMinNeededPartitionSortResources(Partition);
        }

        virtual TNodeResources GetAvgNeededResources() const override
        {
            int jobCount = GetPendingJobCount();
            if (jobCount == 0) {
                return ZeroNodeResources();
            }
            i64 dataSizePerJob = GetPendingDataSize() / jobCount;
            return GetNeededResourcesForDataSize(dataSizePerJob);
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            return GetNeededResourcesForDataSize(joblet->InputStripeList->TotalDataSize);
        }

        using TTask::AddInput;

        virtual void AddInput(TChunkStripePtr stripe) override
        {
            auto* chunkPoolOutput = GetChunkPoolOutput();
            i64 oldTotal = chunkPoolOutput->GetTotalDataSize();
            TPartitionBoundTask::AddInput(stripe);
            i64 newTotal = chunkPoolOutput->GetTotalDataSize();
            Controller->SortDataSizeCounter.Increment(newTotal - oldTotal);
        }

        bool IsCompleted() const
        {
            return
                Controller->PartitionTask->IsCompleted() &&
                TPartitionBoundTask::IsCompleted();
        }

    private:
        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return
                Controller->SimpleSort
                ? ~Controller->SimpleSortPool
                : Controller->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return
                Controller->SimpleSort
                ? ~Controller->SimpleSortPool
                : Partition->ChunkPoolOutput;
        }

        TNodeResources GetNeededResourcesForDataSize(i64 dataSize) const
        {
            i64 rowCount = Controller->GetRowCountEstimate(Partition, dataSize);
            i64 valueCount = Controller->GetValueCountEstimate(dataSize);
            return
                Controller->SimpleSort
                ? Controller->GetSimpleSortResources(dataSize, rowCount, valueCount)
                : Controller->GetPartitionSortResources(Partition, dataSize, rowCount);
        }

        virtual TDuration GetSimpleLocalityTimeout() const override
        {
            return Controller->Spec->SortLocalityTimeout;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->IsSortedMergeNeeded(Partition)
                ? 1
                : Controller->OutputTables.size();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            if (Controller->IsSortedMergeNeeded(Partition)) {
                jobSpec->CopyFrom(Controller->IntermediateSortJobSpecTemplate);
                AddIntermediateOutputSpec(jobSpec, joblet);
            } else {
                jobSpec->CopyFrom(Controller->FinalSortJobSpecTemplate);
                AddOutputSpecs(jobSpec, joblet);
            }

            AddSequentialInputSpec(jobSpec, joblet);
            Controller->CustomizeJobSpec(joblet, jobSpec);

            if (!Controller->SimpleSort) {
                auto* inputSpec = jobSpec->mutable_input_specs(0);
                FOREACH (auto& chunk, *inputSpec->mutable_chunks()) {
                    chunk.set_partition_tag(Partition->Index);
                }
            }
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            TPartitionBoundTask::OnJobStarted(joblet);

            YCHECK(!Partition->Maniac);

            Controller->SortDataSizeCounter.Start(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Start(1);
            } else {
                Controller->FinalSortJobCounter.Start(1);
            }
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TPartitionBoundTask::OnJobCompleted(joblet);

            Controller->SortDataSizeCounter.Completed(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Completed(1);

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                const auto& resultExt = joblet->Job->Result().GetExtension(TSortJobResultExt::sort_job_result_ext);
                auto stripe = New<TChunkStripe>();
                FOREACH (const auto& inputChunk, resultExt.chunks()) {
                    stripe->Chunks.push_back(New<TRefCountedInputChunk>(inputChunk));
                }
                Partition->SortedMergeTask->AddInput(stripe);
            } else {
                Controller->FinalSortJobCounter.Completed(1);

                // Sort outputs in small partitions go directly to the output.
                Controller->RegisterOutputChunkTrees(joblet, Partition);
                Controller->OnPartitionCompleted(Partition);
            }

            Controller->CheckMergeStartThreshold();
        }

        virtual void OnJobFailed(TJobletPtr joblet) override
        {
            Controller->SortDataSizeCounter.Failed(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Failed(1);
            } else {
                Controller->FinalSortJobCounter.Failed(1);
            }

            TPartitionBoundTask::OnJobFailed(joblet);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            Controller->SortDataSizeCounter.Aborted(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Aborted(1);
            } else {
                Controller->FinalSortJobCounter.Aborted(1);
            }

            TPartitionBoundTask::OnJobAborted(joblet);
        }

        virtual void OnTaskCompleted() override
        {
            TPartitionBoundTask::OnTaskCompleted();

            // Kick-start the corresponding merge task.
            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->AddTaskPendingHint(Partition->SortedMergeTask);
            }
        }

        virtual void AddInputLocalityHint(TChunkStripePtr stripe) override
        {
            if (Controller->SimpleSort) {
                TTask::AddInputLocalityHint(stripe);
            }
        }
    };

    //! Base class for both sorted and ordered merge.
    class TMergeTask
        : public TPartitionBoundTask
    {
    public:
        TMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual int GetPriority() const override
        {
            return 2;
        }

    protected:
        virtual TDuration GetSimpleLocalityTimeout() const override
        {
            return Controller->Spec->MergeLocalityTimeout;
        }

        virtual void OnTaskCompleted() override
        {
            Controller->OnPartitionCompleted(Partition);
        }
    };

    //! Implements sorted merge phase for sort operations and
    //! sorted reduce phase for map-reduce operations.
    class TSortedMergeTask
        : public TMergeTask
    {
    public:
        TSortedMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TMergeTask(controller, partition)
        {
            ChunkPool = CreateAtomicChunkPool();
        }

        virtual Stroka GetId() const override
        {
            return Sprintf("SortedMerge(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const override
        {
            if (!Controller->MergeStartThresholdReached) {
                return 0;
            }

            if (!Controller->IsSortedMergeNeeded(Partition)) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual TNodeResources GetMinNeededResources() const override
        {
            return Controller->GetSortedMergeResources(ChunkPool->GetTotalStripeCount());
        }

    private:
        TAutoPtr<IChunkPool> ChunkPool;

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->OutputTables.size();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputSpecs(jobSpec, joblet);
            Controller->CustomizeJobSpec(joblet, jobSpec);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            YCHECK(!Partition->Maniac);

            Controller->SortedMergeJobCounter.Start(1);

            TMergeTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TMergeTask::OnJobCompleted(joblet);

            Controller->SortedMergeJobCounter.Completed(1);
            Controller->RegisterOutputChunkTrees(joblet, Partition);
        }

        virtual void OnJobFailed(TJobletPtr joblet) override
        {
            Controller->SortedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(joblet);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            Controller->SortedMergeJobCounter.Aborted(1);

            TMergeTask::OnJobAborted(joblet);
        }

    };

    //! Implements unordered merge of maniac partitions for sort operation.
    //! Not used in map-reduce operations.
    class TUnorderedMergeTask
        : public TMergeTask
    {
    public:
        TUnorderedMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TMergeTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return Sprintf("UnorderedMerge(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const override
        {
            if (!Controller->MergeStartThresholdReached) {
                return 0;
            }

            if (!Controller->IsUnorderedMergeNeeded(Partition)) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual i64 GetLocality(const Stroka& address) const override
        {
            // Unordered merge job does not respect locality.
            return 0;
        }

        virtual bool IsStrictlyLocal() const override
        {
            return false;
        }

        virtual TNodeResources GetMinNeededResources() const override
        {
            return Controller->GetUnorderedMergeResources();
        }

    private:
        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return Controller->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return Partition->ChunkPoolOutput;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return 1;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputSpecs(jobSpec, joblet);
            Controller->CustomizeJobSpec(joblet, jobSpec);

            if (!Controller->SimpleSort) {
                auto* inputSpec = jobSpec->mutable_input_specs(0);
                FOREACH (auto& chunk, *inputSpec->mutable_chunks()) {
                    chunk.set_partition_tag(Partition->Index);
                }
            }
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            YCHECK(Partition->Maniac);

            Controller->UnorderedMergeJobCounter.Start(1);

            TMergeTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TMergeTask::OnJobCompleted(joblet);

            Controller->UnorderedMergeJobCounter.Completed(1);
            Controller->RegisterOutputChunkTrees(joblet, Partition);
        }

        virtual void OnJobFailed(TJobletPtr joblet) override
        {
            Controller->UnorderedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(joblet);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            Controller->UnorderedMergeJobCounter.Aborted(1);

            TMergeTask::OnJobAborted(joblet);
        }
    };


    // Init/finish.

    void InitShufflePool()
    {
        ShufflePool = CreateShuffleChunkPool(
            static_cast<int>(Partitions.size()),
            Spec->MaxDataSizePerSortJob);

        FOREACH (auto partition, Partitions) {
            partition->ChunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        }
    }

    void InitSimpleSortPool(int sortJobCount)
    {
        SimpleSortPool = CreateUnorderedChunkPool(
            sortJobCount,
            Spec->MaxDataSizePerSortJob);
    }
    
    virtual void OnOperationCompleted() override
    {
        YCHECK(CompletedPartitionCount == Partitions.size());
        TOperationControllerBase::OnOperationCompleted();
    }


    void OnNodeOffline(TExecNodePtr node) override
    {
        if (SortStartThresholdReached) {
            std::vector<TPartitionPtr> affectedPartitions;
            auto failedAddress = node->GetAddress();

            LOG_DEBUG("Node %s gone offline, reassigning partitions", ~failedAddress);
            FOREACH (auto partition, Partitions) {
                if (partition->AssignedAddress == failedAddress) {
                    affectedPartitions.push_back(partition);
                }
            }

            LOG_DEBUG("Reassigning %d partitions", static_cast<int>(affectedPartitions.size()));

            AssignPartitions(affectedPartitions);
        }
    }

    void RegisterOutputChunkTrees(TJobInProgressPtr jip, TPartition* partition)
    {
        TOperationControllerBase::RegisterOutputChunkTrees(joblet, partition->Index);
    }

    void OnPartitionCompleted(TPartitionPtr partition)
    {
        YCHECK(!partition->Completed);
        partition->Completed = true;

        ++CompletedPartitionCount;

        LOG_INFO("Partition completed (Partition: %d)", partition->Index);
    }

    bool IsSortedMergeNeeded(TPartitionPtr partition) const
    {
        // Check for cached value.
        if (partition->SortedMergeNeeded) {
            return true;
        }

        bool mergeNeeded = IsSortedMergeNeededImpl(partition);

        if (mergeNeeded) {
            LOG_DEBUG("Partition needs sorted merge (Partition: %d)", partition->Index);
            SortedMergeJobCounter.Increment(1);
            partition->SortedMergeNeeded = true;
        }

        return mergeNeeded;
    }

    bool IsSortedMergeNeededImpl(TPartitionPtr partition) const
    {
        return
            !partition->Maniac &&
            partition->ChunkPoolOutput->GetTotalJobCount() > 1;
    }

    bool IsUnorderedMergeNeeded(TPartitionPtr partition)
    {
        return partition->Maniac && PartitionTask->IsCompleted();
    }

    void CheckSortStartThreshold()
    {
        if (SortStartThresholdReached)
            return;

        if (PartitionTask->GetCompletedDataSize() < PartitionTask->GetTotalDataSize() * Spec->ShuffleStartThreshold)
            return;

        LOG_INFO("Sort start threshold reached");

        SortStartThresholdReached = true;
        AssignPartitions(Partitions);
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
            if (!partition->Maniac) {
                AddTaskPendingHint(partition->SortTask);
            }
        }   
    }

    void AddMergeTasksPendingHints()
    {
        FOREACH (auto partition, Partitions) {
            auto taskToKick = partition->Maniac
                ? TTaskPtr(partition->UnorderedMergeTask)
                : TTaskPtr(partition->SortedMergeTask);
            AddTaskPendingHint(taskToKick);
        }
    }

    void AssignPartitions(const std::vector<TPartitionPtr>& partitions)
    {
        auto nodes = Host->GetExecNodes();
        if (nodes.empty()) {
            OnOperationFailed(TError("No online exec nodes to assign partitions"));
            return;
        }

        yhash_map<TExecNodePtr, i64> nodeToDataSize;
        std::vector<TExecNodePtr> nodeHeap;

        auto compareNodes = [&] (const TExecNodePtr& lhs, const TExecNodePtr& rhs) {
            return nodeToDataSize[lhs] > nodeToDataSize[rhs];
        };

        auto comparePartitions = [&] (const TPartitionPtr& lhs, const TPartitionPtr& rhs) {
            return lhs->ChunkPoolOutput->GetTotalDataSize() >
                   rhs->ChunkPoolOutput->GetTotalDataSize();
        };

        FOREACH (auto node, nodes) {
            YCHECK(nodeToDataSize.insert(std::make_pair(node, 0)).second);
            nodeHeap.push_back(node);
        }

        auto sortedPartitions = partitions;
        std::sort(sortedPartitions.begin(), sortedPartitions.end(), comparePartitions);

        // This is actually redundant since all values are 0.
        std::make_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
        
        LOG_DEBUG("Assigning partitions");
        FOREACH (auto partition, sortedPartitions) {
            auto node = nodeHeap.front();
            auto address = node->GetAddress();

            partition->AssignedAddress = address;
            AddTaskLocalityHint(partition->SortTask, address);
            AddTaskLocalityHint(partition->SortedMergeTask, address);

            nodeToDataSize[node] += partition->ChunkPoolOutput->GetTotalDataSize();

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            LOG_DEBUG("Partition assigned: %d -> %s",
                partition->Index,
                ~address);
        }

        LOG_DEBUG("Partitions assigned");
        FOREACH (const auto& pair, nodeToDataSize) {
            LOG_DEBUG("Node %s -> %" PRId64,
                ~pair.first->GetAddress(),
                pair.second);
        }
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec)
    { }


    // Resource management.

    virtual bool IsPartitionJobNonexpanding() const = 0;

    virtual TNodeResources GetPartitionResources(
        i64 dataSize) const = 0;

    TNodeResources GetMinNeededPartitionResources() const
    {
        // Holds both for sort and map-reduce.
        return GetPartitionResources(std::min(
            Spec->MaxDataSizePerPartitionJob,
            TotalInputDataSize));
    }

    virtual TNodeResources GetSimpleSortResources(
        i64 dataSize,
        i64 rowCount,
        i64 valueCount) const = 0;
    
    virtual TNodeResources GetPartitionSortResources(
        TPartitionPtr partition,
        i64 dataSize,
        i64 rowCount) const = 0;

    TNodeResources GetMinNeededPartitionSortResources(
        TPartitionPtr partition) const
    {
        i64 dataSize = Spec->MaxDataSizePerSortJob;
        if (IsPartitionJobNonexpanding()) {
            dataSize = std::min(dataSize, TotalInputDataSize);
        }
        i64 rowCount = GetRowCountEstimate(partition, dataSize);
        return GetPartitionSortResources(partition, dataSize, rowCount);
    }

    virtual TNodeResources GetSortedMergeResources(
        int stripeCount) const = 0;

    virtual TNodeResources GetUnorderedMergeResources() const = 0;


    // Unsorted helpers.

    i64 GetRowCountEstimate(TPartitionPtr partition, i64 dataSize) const
    {
        auto* chunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        i64 totalDataSize = chunkPoolOutput->GetTotalDataSize();
        if (totalDataSize == 0) {
            return 0;
        }
        i64 totalRowCount = chunkPoolOutput->GetTotalRowCount();
        return static_cast<i64>((double) totalRowCount * dataSize / totalDataSize);
    }

    // TODO(babenko): this is the input estimate, not the partitioned one!
    // Should get rid of this "value count" stuff completely.
    i64 GetValueCountEstimate(i64 dataSize) const
    {
        return static_cast<i64>((double) TotalInputValueCount * dataSize / TotalInputDataSize);
    }

    int SuggestPartitionCount() const
    {
        YCHECK(TotalInputDataSize > 0);
        int minSuggestion = static_cast<int>(ceil((double) TotalInputDataSize / Spec->MaxPartitionDataSize));
        int maxSuggestion = static_cast<int>(ceil((double) TotalInputDataSize / Spec->MinPartitionDataSize));
        int result = Spec->PartitionCount.Get(minSuggestion);
        result = std::min(result, maxSuggestion);
        result = std::max(result, 1);
        return result;
    }
};

////////////////////////////////////////////////////////////////////

class TSortController
    : public TSortControllerBase
{
public:
    TSortController(
        TSchedulerConfigPtr config,
        TSortOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TSortControllerBase(
            config,
            spec,
            host,
            operation)
        , Spec(spec)
    { }

private:
    TSortOperationSpecPtr Spec;

    // Samples.
    TSamplesFetcherPtr SamplesFetcher;
    TSamplesCollectorPtr SamplesCollector;

    std::vector<const NTableClient::NProto::TKey*> SortedSamples;

    //! |PartitionCount - 1| separating keys.
    std::vector<NTableClient::NProto::TKey> PartitionKeys;


    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TSortControllerBase::DoInitialize();

        auto& table = OutputTables[0];
        table.Clear = true;
        table.LockMode = ELockMode::Exclusive;
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline
            ->Add(BIND(&TSortController::RequestSamples, MakeStrong(this)))
            ->Add(BIND(&TSortController::OnSamplesReceived, MakeStrong(this)));
    }

    TFuture< TValueOrError<void> > RequestSamples()
    {
        SamplesFetcher = New<TSamplesFetcher>(
            Config,
            Spec,
            Operation->GetOperationId());

        SamplesCollector = New<TSamplesCollector>(
            SamplesFetcher,
            Host->GetBackgroundInvoker());

        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            int chunkCount = 0;
            FOREACH (const auto& table, InputTables) {
                FOREACH (const auto& chunk, table.FetchResponse->chunks()) {
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    TotalInputDataSize += miscExt.uncompressed_data_size();
                    TotalInputRowCount += miscExt.row_count();
                    TotalInputValueCount += miscExt.value_count();

                    auto refCountedChunk = New<TRefCountedInputChunk>(chunk);
                    SamplesCollector->AddChunk(refCountedChunk);
                    ++chunkCount;
                }
            }

            LOG_INFO("Totals collected (DataSize: %" PRId64 ", RowCount: % " PRId64 ", ValueCount: %" PRId64 ")",
                TotalInputDataSize,
                TotalInputRowCount,
                TotalInputValueCount);

            // Check for empty inputs.
            if (chunkCount == 0) {
                LOG_INFO("Empty input");
                OnOperationCompleted();
                return NewPromise< TValueOrError<void> >();
            }

            SamplesFetcher->SetDesiredSampleCount(SuggestPartitionCount() * Spec->SamplesPerPartition);
            return SamplesCollector->Run();
        }
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        UNUSED(batchRsp);

        OutputTables[0].KeyColumns = Spec->SortBy;
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

        std::sort(
            SortedSamples.begin(),
            SortedSamples.end(), 
            [] (const NTableClient::NProto::TKey* lhs, const NTableClient::NProto::TKey* rhs) {
                return CompareKeys(*lhs, *rhs) < 0;
            }
        );
    }

    void BuildPartitions()
    {
        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = SuggestPartitionCount();

        // Don't create more partitions than we have samples (plus one).
        partitionCount = std::min(partitionCount, static_cast<int>(SortedSamples.size()) + 1);

        // Don't create more partitions than allowed by the global config.
        partitionCount = std::min(partitionCount, Config->MaxPartitionCount);

        YCHECK(partitionCount > 0);

        SimpleSort = partitionCount == 1;

        InitJobIOConfigs();
        
        if (SimpleSort) {
            BuildSinglePartition();
        } else {
            BuildMulitplePartitions(partitionCount);
        }
    }

    void BuildSinglePartition()
    {
        std::vector<TChunkStripePtr> stripes;
        i64 totalDataSize;
        int totalChunkCount;
        SliceChunks(
            CollectInputTablesChunks(),
            Spec->SortJobCount,
            Spec->SortJobSliceDataSize,
            &stripes,
            &totalDataSize,
            &totalChunkCount);

        // Choose sort job count and initialize the pool.
        int sortJobCount = SuggestJobCount(
            totalDataSize,
            Spec->MinDataSizePerSortJob,
            Spec->MaxDataSizePerSortJob,
            Spec->SortJobCount,
            totalChunkCount);
        InitSimpleSortPool(sortJobCount);

        // Create the fake partition.
        auto partition = New<TPartition>(this, 0);
        Partitions.push_back(partition);
        partition->SortTask->AddInput(stripes);
        partition->SortTask->FinishInput();

        LOG_INFO("Sorting without partitioning");

        // Kick-start the sort task.
        SortStartThresholdReached = true;
        AddTaskPendingHint(partition->SortTask);
    }

    void AddPartition(const NTableClient::NProto::TKey& key)
    {
        using NTableClient::ToString;

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
                LOG_DEBUG("Partition %d is a maniac, skipped %d samples",
                    lastPartition->Index,
                    skippedCount);

                lastPartition->Maniac= true;
                YCHECK(skippedCount >= 1);

                auto successorKey = GetSuccessorKey(*sampleKey);
                AddPartition(successorKey);
            }
        }

        // Construct shuffle pool.
        ShufflePool = CreateShuffleChunkPool(
            partitionCount,
            Spec->MaxDataSizePerSortJob);

        // Populate the partition pool.
        std::vector<TChunkStripePtr> stripes;
        i64 totalDataSize;
        int totalChunkCount;
        SliceChunks(
            CollectInputTablesChunks(),
            Spec->PartitionJobCount,
            Spec->PartitionJobSliceDataSize,
            &stripes,
            &totalDataSize,
            &totalChunkCount);
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %" PRId64 ")",
            totalDataSize,
            totalChunkCount);

        // Init counters.
        PartitionJobCounter.Set(SuggestJobCount(
            PartitionTask->GetTotalDataSize(),
            Spec->MinDataSizePerPartitionJob,
            Spec->MaxDataSizePerPartitionJob,
            Spec->PartitionJobCount,
            totalChunkCount));

        LOG_INFO("Sorting with partitioning (PartitionCount: %d, PartitionJobCount: %" PRId64 ")",
            partitionCount,
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
            SamplesCollector.Reset();
            SortedSamples.clear();

            InitJobSpecTemplates();
        }
    }

    void InitJobIOConfigs() 
    {
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        InitIntermediateOutputConfig(PartitionJobIOConfig);

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        if (!SimpleSort) {
            InitIntermediateInputConfig(IntermediateSortJobIOConfig);
        }
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        FinalSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        if (!SimpleSort) {
            InitIntermediateInputConfig(FinalSortJobIOConfig);
        }   
        InitFinalOutputConfig(FinalSortJobIOConfig);

        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        InitIntermediateInputConfig(SortedMergeJobIOConfig);
        InitFinalOutputConfig(SortedMergeJobIOConfig);

        UnorderedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        InitIntermediateInputConfig(UnorderedMergeJobIOConfig);
        InitFinalOutputConfig(UnorderedMergeJobIOConfig);
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(EJobType::Partition);
            *PartitionJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            specExt->set_partition_count(Partitions.size());
            FOREACH (const auto& key, PartitionKeys) {
                *specExt->add_partition_keys() = key;
            }
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            PartitionJobSpecTemplate.set_io_config(ConvertToYsonString(PartitionJobIOConfig).Data());
        }
        
        {
            TJobSpec sortJobSpecTemplate;
            sortJobSpecTemplate.set_type(SimpleSort ? EJobType::SimpleSort : EJobType::PartitionSort);
            *sortJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).Data());

            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_io_config(ConvertToYsonString(FinalSortJobIOConfig).Data());
        }
        
        {
            SortedMergeJobSpecTemplate.set_type(EJobType::SortedMerge);
            *SortedMergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            SortedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).Data());
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(EJobType::UnorderedMerge);
            *UnorderedMergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            UnorderedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(UnorderedMergeJobIOConfig).Data());
        }
    }


    // Resource management.

    virtual bool IsPartitionJobNonexpanding() const
    {
        return true;
    }

    virtual TNodeResources GetPartitionResources(
        i64 dataSize) const override
    {
        i64 bufferSize = std::min(
            PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            dataSize);

        bufferSize = std::min(
            bufferSize + NTableClient::TChannelWriter::MaxReserveSize * static_cast<i64>(Partitions.size()),
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            // NB: due to large MaxBufferSize for partition that was accounted in buffer size 
            // we eliminate number of output streams to zero.
            GetIOMemorySize(PartitionJobIOConfig, 1, 0) + 
            bufferSize +
            GetFootprintMemorySize());
        return result;
    }

    virtual TNodeResources GetSimpleSortResources(
        i64 dataSize,
        i64 rowCount,
        i64 valueCount) const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            // NB: Sort jobs typically have large prefetch window that
            // drastically increases the estimated consumption returned by GetIOMemorySize.
            // Setting input count to zero to eliminates this term.
            GetIOMemorySize(FinalSortJobIOConfig, 0, 1) +
            dataSize +
            // TODO(babenko): *2 are due to lack of reserve, remove this once simple sort
            // starts reserving arrays of appropriate sizes.
            (i64) 16 * Spec->SortBy.size() * rowCount +
            (i64) 16 * rowCount +
            (i64) 32 * valueCount * 2 +
            GetFootprintMemorySize());
        return result;
    }

    virtual TNodeResources GetPartitionSortResources(
        TPartitionPtr partition,
        i64 dataSize,
        i64 rowCount) const override
    {
        auto ioConfig = IsSortedMergeNeeded(partition) ? IntermediateSortJobIOConfig : FinalSortJobIOConfig;
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            // NB: See comment above for GetSimpleSortJobResources.
            GetIOMemorySize(ioConfig, 0, 1) +
            dataSize +
            (i64) 16 * Spec->SortBy.size() * rowCount +
            (i64) 12 * rowCount +
            GetFootprintMemorySize());
        result.set_network(Spec->ShuffleNetworkLimit);
        return result;
    }

    virtual TNodeResources GetSortedMergeResources(
        int stripeCount) const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            GetIOMemorySize(SortedMergeJobIOConfig, stripeCount, 1) +
            GetFootprintMemorySize());
        return result;
    }

    virtual TNodeResources GetUnorderedMergeResources() const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(1);
        result.set_memory(
            GetIOMemorySize(UnorderedMergeJobIOConfig, 1, 1) +
            GetFootprintMemorySize());
        return result;
    }


    // Progress reporting.

    virtual void LogProgress() override
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d, L: %d}, "
            "Partitions = {T: %d, C: %d}, "
            "PartitionJobs = {%s}, "
            "IntermediateSortJobs = {%s}, "
            "FinalSortJobs = {%s}, "
            "SortedMergeJobs = {%s}, "
            "UnorderedMergeJobs = {%s}",
            // Jobs
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            JobCounter.GetLost(),
            // Partitions
            static_cast<int>(Partitions.size()),
            CompletedPartitionCount,
            // PartitionJobs
            ~ToString(PartitionJobCounter),
            // IntermediateSortJobs
            ~ToString(IntermediateSortJobCounter),
            // FinaSortJobs
            ~ToString(FinalSortJobCounter),
            // SortedMergeJobs
            ~ToString(SortedMergeJobCounter),
            // UnorderedMergeJobs
            ~ToString(UnorderedMergeJobCounter));
    }

    virtual void BuildProgressYson(IYsonConsumer* consumer) override
    {
        TSortControllerBase::BuildProgressYson(consumer);
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Scalar(Partitions.size())
                .Item("completed").Scalar(CompletedPartitionCount)
            .EndMap()
            .Item("partition_jobs").Scalar(PartitionJobCounter)
            .Item("intermediate_sort_jobs").Scalar(IntermediateSortJobCounter)
            .Item("final_sort_jobs").Scalar(FinalSortJobCounter)
            .Item("sorted_merge_jobs").Scalar(SortedMergeJobCounter)
            .Item("unordered_merge_jobs").Scalar(UnorderedMergeJobCounter);
    }

};

IOperationControllerPtr CreateSortController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TSortOperationSpec>(
        operation,
        config->SortOperationSpec);
    return New<TSortController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

class TMapReduceController
    : public TSortControllerBase
{
public:
    TMapReduceController(
        TSchedulerConfigPtr config,
        TMapReduceOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TSortControllerBase(
            config,
            spec,
            host,
            operation)
        , Spec(spec)
        , MapStartRowCount(0)
        , ReduceStartRowCount(0)
    { }

private:
    TMapReduceOperationSpecPtr Spec;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserFile> ReducerFiles;

    i64 MapStartRowCount;
    i64 ReduceStartRowCount;


    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TSortControllerBase::DoInitialize();

        if (!CheckKeyColumnsCompatible(Spec->SortBy, Spec->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce columns %s are not compatible with sort columns %s",
                ~ConvertToYsonString(Spec->ReduceBy, EYsonFormat::Text).Data(),
                ~ConvertToYsonString(Spec->SortBy, EYsonFormat::Text).Data());
        }
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TRichYPath> GetFilePaths() const override
    {
        // Combine mapper and reducer files into a single collection.
        std::vector<TRichYPath> result;
        if (Spec->Mapper) {
            result.insert(
                result.end(),
                Spec->Mapper->FilePaths.begin(),
                Spec->Mapper->FilePaths.end());
        }
        result.insert(
            result.end(),
            Spec->Reducer->FilePaths.begin(),
            Spec->Reducer->FilePaths.end());
        return result;
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        // Separate mapper and reducer files.
        auto it = Files.begin();
        
        if (Spec->Mapper) {
            for (int i = 0; i < static_cast<int>(Spec->Mapper->FilePaths.size()); ++i) {
                MapperFiles.push_back(*it++);
            }
        }

        for (int i = 0; i < static_cast<int>(Spec->Reducer->FilePaths.size()); ++i) {
            ReducerFiles.push_back(*it++);
        }

        YCHECK(it == Files.end());
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline->Add(BIND(&TMapReduceController::ProcessInputs, MakeStrong(this)));
    }

    TFuture<void> ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            int chunkCount = 0;
            FOREACH (const auto& table, InputTables) {
                FOREACH (const auto& chunk, table.FetchResponse->chunks()) {
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk.extensions());
                    TotalInputDataSize += miscExt.uncompressed_data_size();
                    TotalInputRowCount += miscExt.row_count();
                    TotalInputValueCount += miscExt.value_count();
                    ++chunkCount;
                }
            }

            LOG_INFO("Totals collected (DataSize: %" PRId64 ", RowCount: % " PRId64 ", ValueCount: %" PRId64 ")",
                TotalInputDataSize,
                TotalInputRowCount,
                TotalInputValueCount);

            // Check for empty inputs.
            if (chunkCount == 0) {
                LOG_INFO("Empty input");
                OnOperationCompleted();
                return NewPromise<void>();
            }

            BuildPartitions();
            InitJobSpecTemplates();
        }

        return MakeFuture();
    }

    void BuildPartitions()
    {
        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = SuggestPartitionCount();

        // Don't create more partitions than allowed by the global config.
        partitionCount = std::min(partitionCount, Config->MaxPartitionCount);

        // Single partition is a special case for sort and is not supported by map-reduce.
        partitionCount = std::max(partitionCount, 2);

        YCHECK(partitionCount >= 2);

        InitJobIOConfigs(partitionCount);
        BuildMultiplePartitions(partitionCount);
    }

    void BuildMultiplePartitions(int partitionCount)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }

        InitShufflePool();

        std::vector<TChunkStripePtr> stripes;
        i64 totalDataSize;
        int totalChunkCount;
        SliceChunks(
            CollectInputTablesChunks(),
            Spec->PartitionJobCount,
            Spec->PartitionJobSliceDataSize,
            &stripes,
            &totalDataSize,
            &totalChunkCount);

        // Init counters.
        PartitionJobCounter.Set(SuggestJobCount(
            totalDataSize,
            Spec->MinDataSizePerPartitionJob,
            Spec->MaxDataSizePerPartitionJob,
            Spec->PartitionJobCount,
            totalChunkCount));

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();

        LOG_INFO("Inputs processed (DataSize: %" PRId64 ", ChunkCount: %" PRId64 ", PartitionCount: %d, PartitionJobCount: %" PRId64 ")",
            totalDataSize,
            totalChunkCount,
            partitionCount,
            PartitionJobCounter.GetTotal());

        // Kick-start the partition task.
        AddTaskPendingHint(PartitionTask);
    }

    void InitJobIOConfigs(int partitionCount)
    {
        {
            // This is not a typo!
            PartitionJobIOConfig = Spec->MapJobIO;
            InitIntermediateOutputConfig(PartitionJobIOConfig);
        }

        {
            IntermediateSortJobIOConfig = Spec->SortJobIO;
            InitIntermediateInputConfig(IntermediateSortJobIOConfig);
            InitIntermediateOutputConfig(IntermediateSortJobIOConfig);
        }

        {
            FinalSortJobIOConfig = Spec->ReduceJobIO;
            InitIntermediateInputConfig(FinalSortJobIOConfig);
        }

        {
            SortedMergeJobIOConfig = Spec->ReduceJobIO;
            InitIntermediateInputConfig(SortedMergeJobIOConfig);
        }
    }

    void InitJobSpecTemplates()
    {
        {
            *PartitionJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
            
            auto* specExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            specExt->set_partition_count(Partitions.size());
            ToProto(specExt->mutable_key_columns(), Spec->ReduceBy);
            
            if (Spec->Mapper) {
                PartitionJobSpecTemplate.set_type(EJobType::PartitionMap);
                InitUserJobSpec(
                    specExt->mutable_mapper_spec(),
                    Spec->Mapper,
                    MapperFiles);
            } else {
                PartitionJobSpecTemplate.set_type(EJobType::Partition);
            }

            PartitionJobSpecTemplate.set_io_config(ConvertToYsonString(PartitionJobIOConfig).Data());
        }

        {
            IntermediateSortJobSpecTemplate.set_type(EJobType::PartitionSort);
            *IntermediateSortJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = IntermediateSortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            IntermediateSortJobSpecTemplate.set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).Data());
        }

        {
            FinalSortJobSpecTemplate.set_type(EJobType::PartitionReduce);
            *FinalSortJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = FinalSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            InitUserJobSpec(
                specExt->mutable_reducer_spec(),
                Spec->Reducer,
                ReducerFiles);

            FinalSortJobSpecTemplate.set_io_config(ConvertToYsonString(FinalSortJobIOConfig).Data());
        }

        {
            SortedMergeJobSpecTemplate.set_type(EJobType::SortedReduce);
            *SortedMergeJobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

            auto* specExt = SortedMergeJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            ToProto(specExt->mutable_key_columns(), Spec->SortBy);

            InitUserJobSpec(
                specExt->mutable_reducer_spec(),
                Spec->Reducer,
                ReducerFiles);

            SortedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).Data());
        }
    }

    void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec) override
    {
        switch (jobSpec->type()) {
        case EJobType::PartitionMap: {
            auto* jobSpecExt = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            AddUserJobEnvironment(
            	jobSpecExt->mutable_mapper_spec(),
            	jip,
            	MapStartRowCount);
            MapStartRowCount += jip->PoolResult->TotalRowCount;
            break;
        }

        case EJobType::PartitionReduce:
        case EJobType::SortedReduce: {
            auto* jobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            AddUserJobEnvironment(
            	jobSpecExt->mutable_reducer_spec(),
            	jip,
            	ReduceStartRowCount);
            ReduceStartRowCount += jip->PoolResult->TotalRowCount;
            break;
        }

        default:
            // For other jobs do nothing.
            break;
        }
    }

    // Resource management.

    virtual bool IsPartitionJobNonexpanding() const
    {
        return false;
    }

    virtual TNodeResources GetPartitionResources(
        i64 dataSize) const override
    {
        i64 reserveSize = NTableClient::TChannelWriter::MaxReserveSize * static_cast<i64>(Partitions.size());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()), 
            PartitionJobIOConfig->TableWriter->MaxBufferSize);
        i64 windowSize = PartitionJobIOConfig->TableWriter->WindowSize + 
            PartitionJobIOConfig->TableWriter->EncodeWindowSize;

        TNodeResources result;
        result.set_slots(1);
        if (Spec->Mapper) {
            bufferSize += windowSize;
            result.set_cpu(Spec->Mapper->CpuLimit);
            result.set_memory(GetIOMemorySize(PartitionJobIOConfig, 1, 0) + 
                bufferSize +
                Spec->Mapper->MemoryLimit +
                GetFootprintMemorySize());
        } else {
            bufferSize = std::min(bufferSize, dataSize + reserveSize);
            bufferSize += windowSize;
            result.set_cpu(1);
            result.set_memory(
                GetIOMemorySize(PartitionJobIOConfig, 1, 0) + 
                bufferSize +
                GetFootprintMemorySize());
        }
        return result;
    }

    virtual TNodeResources GetSimpleSortResources(
        i64 dataSize,
        i64 rowCount,
        i64 valueCount) const override
    {
        YUNREACHABLE();
    }

    virtual TNodeResources GetPartitionSortResources(
        TPartitionPtr partition,
        i64 dataSize,
        i64 rowCount) const override
    {
        TNodeResources result;
        result.set_slots(1);
        if (IsSortedMergeNeeded(partition)) {
            result.set_cpu(1);
            result.set_memory(
                GetIOMemorySize(IntermediateSortJobIOConfig, 0, 1) +
                dataSize +
                (i64) 16 * Spec->SortBy.size() * rowCount +
                (i64) 12 * rowCount +
                GetFootprintMemorySize());
        } else {
            result.set_cpu(Spec->Reducer->CpuLimit);
            result.set_memory(
                GetIOMemorySize(FinalSortJobIOConfig, 0, Spec->OutputTablePaths.size()) +
                dataSize +
                (i64) 16 * Spec->SortBy.size() * rowCount +
                (i64) 16 * rowCount +
                Spec->Reducer->MemoryLimit +
                GetFootprintMemorySize());
        }
        result.set_network(Spec->ShuffleNetworkLimit);
        return result;
    }

    virtual TNodeResources GetSortedMergeResources(
        int stripeCount) const override
    {
        TNodeResources result;
        result.set_slots(1);
        result.set_cpu(Spec->Reducer->CpuLimit);
        result.set_memory(
            GetIOMemorySize(SortedMergeJobIOConfig, stripeCount, Spec->OutputTablePaths.size()) +
            Spec->Reducer->MemoryLimit +
            GetFootprintMemorySize());
        return result;
    }

    virtual TNodeResources GetUnorderedMergeResources() const override
    {
        YUNREACHABLE();
    }


    // Progress reporting.

    virtual void LogProgress() override
    {
        LOG_DEBUG("Progress: "
            "Jobs = {R: %d, C: %d, P: %d, F: %d, A: %d}, "
            "Partitions = {T: %d, C: %d}, "
            "MapJobs = {%s}, "
            "SortJobs = {%s}, "
            "PartitionReduceJobs = {%s}, "
            "SortedReduceJobs = {%s}",
            // Jobs
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            // Partitions
            static_cast<int>(Partitions.size()),
            CompletedPartitionCount,
            // MapJobs
            ~ToString(PartitionJobCounter),
            // SortJobs
            ~ToString(IntermediateSortJobCounter),
            // PartitionReduceJobs
            ~ToString(FinalSortJobCounter),
            // SortedReduceJobs
            ~ToString(SortedMergeJobCounter));
    }

    virtual void BuildProgressYson(IYsonConsumer* consumer) override
    {
        TSortControllerBase::BuildProgressYson(consumer);
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Scalar(Partitions.size())
                .Item("completed").Scalar(CompletedPartitionCount)
            .EndMap()
            .Item("map_jobs").Scalar(PartitionJobCounter)
            .Item("sort_jobs").Scalar(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Scalar(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Scalar(SortedMergeJobCounter);
    }


};

IOperationControllerPtr CreateMapReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(
        operation,
        config->MapReduceOperationSpec);
    return New<TMapReduceController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

