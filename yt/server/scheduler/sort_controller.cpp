#include "sort_controller.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "helpers.h"
#include "job_memory.h"
#include "map_controller.h"
#include "operation_controller_detail.h"

#include <yt/ytlib/chunk_client/chunk_scraper.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/schemaless_block_writer.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkServer;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NChunkClient;

using NTableClient::TOwningKey;
using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/sort");

//! Maximum number of buckets for partition progress aggregation.
static const int MaxProgressBuckets = 100;

//! Maximum number of buckets for partition size histogram aggregation.
static const int MaxSizeHistogramBuckets = 100;

////////////////////////////////////////////////////////////////////

class TSortControllerBase
    : public TOperationControllerBase
{
public:
    TSortControllerBase(
        TSchedulerConfigPtr config,
        TSortOperationSpecBasePtr spec,
        TSortOperationOptionsBasePtr options,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, host, operation)
        , Spec(spec)
        , Options(options)
        , Config(config)
        , CompletedPartitionCount(0)
        , SortedMergeJobCounter(0)
        // Cannot do similar for UnorderedMergeJobCounter since the number of unsorted merge jobs
        // is hard to predict.
        , SortDataSizeCounter(0)
        , SortStartThresholdReached(false)
        , MergeStartThresholdReached(false)
        , TotalOutputRowCount(0)
        , SimpleSort(false)
    { }

    // Persistence.
    virtual void Persist(TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;

        Persist(context, CompletedPartitionCount);
        Persist(context, PartitionJobCounter);
        Persist(context, IntermediateSortJobCounter);
        Persist(context, FinalSortJobCounter);
        Persist(context, SortDataSizeCounter);
        Persist(context, SortedMergeJobCounter);
        Persist(context, UnorderedMergeJobCounter);

        Persist(context, SortStartThresholdReached);
        Persist(context, MergeStartThresholdReached);

        Persist(context, TotalOutputRowCount);

        Persist(context, SimpleSort);
        Persist(context, Partitions);

        Persist(context, PartitionJobSpecTemplate);
        Persist(context, IntermediateSortJobSpecTemplate);
        Persist(context, FinalSortJobSpecTemplate);
        Persist(context, SortedMergeJobSpecTemplate);
        Persist(context, UnorderedMergeJobSpecTemplate);

        Persist(context, PartitionJobIOConfig);
        Persist(context, IntermediateSortJobIOConfig);
        Persist(context, FinalSortJobIOConfig);
        Persist(context, SortedMergeJobIOConfig);
        Persist(context, UnorderedMergeJobIOConfig);

        Persist(context, PartitionTableReaderOptions);

        Persist(context, ShufflePool);
        Persist(context, SimpleSortPool);

        Persist(context, PartitionTaskGroup);
        Persist(context, SortTaskGroup);
        Persist(context, MergeTaskGroup);

        Persist(context, PartitionTask);
    }

private:
    TSortOperationSpecBasePtr Spec;

protected:
    TSortOperationOptionsBasePtr Options;

    TSchedulerConfigPtr Config;

    // Counters.
    int CompletedPartitionCount;
    TProgressCounter PartitionJobCounter;
    mutable TProgressCounter SortedMergeJobCounter;
    TProgressCounter UnorderedMergeJobCounter;

    // Sort job counters.
    TProgressCounter IntermediateSortJobCounter;
    TProgressCounter FinalSortJobCounter;
    TProgressCounter SortDataSizeCounter;

    // Start thresholds.
    bool SortStartThresholdReached;
    bool MergeStartThresholdReached;

    i64 TotalOutputRowCount;

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
        //! For persistence only.
        TPartition()
            : Index(-1)
            , Completed(false)
            , CachedSortedMergeNeeded(false)
            , Maniac(false)
            , ChunkPoolOutput(nullptr)
        { }

        TPartition(TSortControllerBase* controller, int index)
            : Index(index)
            , Completed(false)
            , CachedSortedMergeNeeded(false)
            , Maniac(false)
            , ChunkPoolOutput(nullptr)
        {
            SortTask = controller->SimpleSort
                ? TSortTaskPtr(New<TSimpleSortTask>(controller, this))
                : TSortTaskPtr(New<TPartitionSortTask>(controller, this));
            SortTask->Initialize();
            controller->RegisterTask(SortTask);

            SortedMergeTask = New<TSortedMergeTask>(controller, this);
            SortedMergeTask->Initialize();
            controller->RegisterTask(SortedMergeTask);

            if (!controller->SimpleSort) {
                UnorderedMergeTask = New<TUnorderedMergeTask>(controller, this);
                UnorderedMergeTask->Initialize();
                controller->RegisterTask(UnorderedMergeTask);
            }
        }

        //! Sequential index (zero based).
        int Index;

        //! Is partition completed?
        bool Completed;

        //! Do we need to run merge tasks for this partition?
        //! Cached value, updated by #IsSortedMergeNeeded.
        bool CachedSortedMergeNeeded;

        //! Does the partition consist of rows with the same key?
        bool Maniac;

        //! Number of sorted bytes residing at a given host.
        yhash_map<TNodeId, i64> NodeIdToLocality;

        //! The node assigned to this partition, #InvalidNodeId if none.
        NNodeTrackerClient::TNodeId AssignedNodeId = NNodeTrackerClient::InvalidNodeId;

        // Tasks.
        TSortTaskPtr SortTask;
        TSortedMergeTaskPtr SortedMergeTask;
        TUnorderedMergeTaskPtr UnorderedMergeTask;

        // Chunk pool output obtained from the shuffle pool.
        IChunkPoolOutput* ChunkPoolOutput;


        void Persist(TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Index);

            Persist(context, Completed);

            Persist(context, CachedSortedMergeNeeded);

            Persist(context, Maniac);

            Persist(context, NodeIdToLocality);
            Persist(context, AssignedNodeId);

            Persist(context, SortTask);
            Persist(context, SortedMergeTask);
            Persist(context, UnorderedMergeTask);

            Persist(context, ChunkPoolOutput);
        }

    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    //! Equivalent to |Partitions.size() == 1| but enables checking
    //! for simple sort when #Partitions is still being constructed.
    bool SimpleSort;
    std::vector<TPartitionPtr> Partitions;

    //! Spec templates for starting new jobs.
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec IntermediateSortJobSpecTemplate;
    TJobSpec FinalSortJobSpecTemplate;
    TJobSpec SortedMergeJobSpecTemplate;
    TJobSpec UnorderedMergeJobSpecTemplate;

    //! IO configs for various job types.
    TJobIOConfigPtr PartitionJobIOConfig;
    TJobIOConfigPtr IntermediateSortJobIOConfig;
    TJobIOConfigPtr FinalSortJobIOConfig;
    TJobIOConfigPtr SortedMergeJobIOConfig;
    TJobIOConfigPtr UnorderedMergeJobIOConfig;

    //! Table reader options for various job types.
    TTableReaderOptionsPtr PartitionTableReaderOptions;

    std::unique_ptr<IShuffleChunkPool> ShufflePool;
    std::unique_ptr<IChunkPool> SimpleSortPool;

    TTaskGroupPtr PartitionTaskGroup;
    TTaskGroupPtr SortTaskGroup;
    TTaskGroupPtr MergeTaskGroup;

    TPartitionTaskPtr PartitionTask;

    //! Implements partition phase for sort operations and map phase for map-reduce operations.
    class TPartitionTask
        : public TTask
    {
    public:
        //! For persistence only.
        TPartitionTask()
            : Controller(nullptr)
        { }

        explicit TPartitionTask(TSortControllerBase* controller)
            : TTask(controller)
            , Controller(controller)
            , ChunkPool(CreateUnorderedChunkPool(
                Controller->PartitionJobCounter.GetTotal(),
                Controller->Config->MaxChunkStripesPerJob))
        { }

        virtual Stroka GetId() const override
        {
            return "Partition";
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->PartitionTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->PartitionLocalityTimeout;
        }

        virtual TJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto resources = Controller->GetPartitionResources(
                joblet->InputStripeList->GetStatistics(),
                joblet->MemoryReserveEnabled);
            return resources;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool.get();
        }

        virtual void Persist(TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, ChunkPool);
            Persist(context, NodeIdToDataSize);
            Persist(context, ScheduledDataSize);
            Persist(context, DataSizePerJob);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTask, 0x63a4c761);

        TSortControllerBase* Controller;
        std::unique_ptr<IChunkPool> ChunkPool;

        //! The total data size of jobs assigned to a particular node
        //! All data sizes are IO weight-adjusted.
        //! No zero values are allowed.
        yhash_map<TNodeId, i64> NodeIdToDataSize;
        //! The sum of all sizes appearing in #NodeIdToDataSize;
        i64 ScheduledDataSize = 0;
        //! Max-aggregated each time a new job is scheduled.
        i64 DataSizePerJob = 0;


        void UpdateNodeDataSize(const TExecNodeDescriptor& descriptor, i64 delta)
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing) {
                return;
            }

            auto ioWeight = descriptor.IOWeight;
            YASSERT(ioWeight > 0);
            auto adjustedDelta = static_cast<i64>(delta / ioWeight);

            auto nodeId = descriptor.Id;
            auto updatedNodeDataSize = (NodeIdToDataSize[nodeId] += adjustedDelta);
            YCHECK(updatedNodeDataSize >= 0);

            if (updatedNodeDataSize == 0) {
                YCHECK(NodeIdToDataSize.erase(nodeId) == 1);
            }

            auto updatedScheduledDataSize = (ScheduledDataSize += adjustedDelta);
            YCHECK(updatedScheduledDataSize >= 0);
        }


        virtual bool CanScheduleJob(
            ISchedulingContext* context,
            const TJobResources& /*jobLimits*/) override
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing) {
                return true;
            }

            if (context->GetNodeDescriptor().IOWeight == 0) {
                return false;
            }

            if (NodeIdToDataSize.empty()) {
                return true;
            }

            auto nodeId = context->GetNodeDescriptor().Id;
            auto updatedScheduledDataSize = ScheduledDataSize + DataSizePerJob;
            auto updatedAvgDataSize = updatedScheduledDataSize / NodeIdToDataSize.size();
            auto updatedNodeDataSize = NodeIdToDataSize[nodeId] + DataSizePerJob;
            return
                updatedNodeDataSize <=
                updatedAvgDataSize + Controller->Spec->PartitionedDataBalancingTolerance * DataSizePerJob;
        }

        virtual bool IsMemoryReserveEnabled() const override
        {
            return Controller->IsMemoryReserveEnabled(Controller->PartitionJobCounter);
        }

        virtual TTableReaderOptionsPtr GetTableReaderOptions() const override
        {
            // TODO(psushin): Distinguish between map and partition.
            return Controller->PartitionTableReaderOptions;
        }

        virtual TJobResources GetMinNeededResourcesHeavy() const override
        {
            auto statistics = ChunkPool->GetApproximateStripeStatistics();
            return Controller->GetPartitionResources(
                statistics,
                IsMemoryReserveEnabled());
        }

        virtual bool IsIntermediateOutput() const override
        {
            return true;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->PartitionJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddIntermediateOutputSpec(jobSpec, joblet, TKeyColumns());
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            Controller->PartitionJobCounter.Start(1);

            auto dataSize = joblet->InputStripeList->TotalDataSize;
            DataSizePerJob = std::max(DataSizePerJob, dataSize);
            UpdateNodeDataSize(joblet->NodeDescriptor, +dataSize);

            TTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            Controller->PartitionJobCounter.Completed(1);

            auto* resultExt = jobSummary.Result->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunks());

            RegisterIntermediate(
                joblet,
                stripe,
                Controller->ShufflePool->GetInput(),
                true);

            // Kick-start sort and unordered merge tasks.
            // Compute sort data size delta.
            i64 oldSortDataSize = Controller->SortDataSizeCounter.GetTotal();
            i64 newSortDataSize = 0;
            for (auto partition : Controller->Partitions) {
                if (partition->Maniac) {
                    Controller->AddTaskPendingHint(partition->UnorderedMergeTask);
                } else {
                    newSortDataSize += partition->ChunkPoolOutput->GetTotalDataSize();
                    Controller->AddTaskPendingHint(partition->SortTask);
                }
            }
            LOG_DEBUG("Sort data size updated: %v -> %v",
                oldSortDataSize,
                newSortDataSize);
            Controller->SortDataSizeCounter.Increment(newSortDataSize - oldSortDataSize);

            Controller->CheckSortStartThreshold();

            // NB: don't move it to OnTaskCompleted since jobs may run after the task has been completed.
            // Kick-start sort and unordered merge tasks.
            Controller->AddSortTasksPendingHints();
            Controller->AddMergeTasksPendingHints();
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            UpdateNodeDataSize(completedJob->NodeDescriptor, -completedJob->DataSize);
            Controller->PartitionJobCounter.Lost(1);
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            TTask::OnJobFailed(joblet, jobSummary);

            UpdateNodeDataSize(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataSize);
            Controller->PartitionJobCounter.Failed(1);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);

            UpdateNodeDataSize(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataSize);
            Controller->PartitionJobCounter.Aborted(1, jobSummary.AbortReason);
            Controller->UpdateAllTasksIfNeeded(Controller->PartitionJobCounter);
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            Controller->PartitionJobCounter.Finalize();
            Controller->ShufflePool->GetInput()->Finish();

            // Dump totals.
            // Mark empty partitions are completed.
            LOG_DEBUG("Partition sizes collected");
            for (auto partition : Controller->Partitions) {
                i64 dataSize = partition->ChunkPoolOutput->GetTotalDataSize();
                if (dataSize == 0) {
                    LOG_DEBUG("Partition %v is empty", partition->Index);
                    // Job restarts may cause the partition task to complete several times.
                    // Thus we might have already marked the partition as completed, let's be careful.
                    if (!partition->Completed) {
                        Controller->OnPartitionCompleted(partition);
                    }
                } else {
                    LOG_DEBUG("Partition[%v] = %v",
                        partition->Index,
                        dataSize);
                }
            }

            if (Controller->Spec->EnablePartitionedDataBalancing) {
                auto nodeDescriptors = Controller->Host->GetExecNodeDescriptors(Controller->Operation->GetSchedulingTag());
                yhash_map<TNodeId, TExecNodeDescriptor> idToNodeDescriptor;
                for (const auto& descriptor : nodeDescriptors) {
                    YCHECK(idToNodeDescriptor.insert(std::make_pair(descriptor.Id, descriptor)).second);
                }

                LOG_DEBUG("Per-node partitioned sizes collected");
                for (const auto& pair : NodeIdToDataSize) {
                    auto nodeId = pair.first;
                    auto dataSize = pair.second;
                    auto nodeIt = idToNodeDescriptor.find(nodeId);
                    LOG_DEBUG("Node[%v] = %v",
                        nodeIt == idToNodeDescriptor.end() ? ToString(nodeId) : nodeIt->second.Address,
                        dataSize);
                }
            }

            Controller->AssignPartitions();

            // NB: this is required at least to mark tasks completed, when there are no pending jobs.
            // This couldn't have been done earlier since we've just finished populating shuffle pool.
            Controller->AddSortTasksPendingHints();

            Controller->CheckMergeStartThreshold();
        }

    };

    //! Base class for tasks that are assigned to particular partitions.
    class TPartitionBoundTask
        : public TTask
    {
    public:
        //! For persistence only.
        TPartitionBoundTask()
            : Controller(nullptr)
            , Partition(nullptr)
        { }

        TPartitionBoundTask(TSortControllerBase* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        { }

        virtual void Persist(TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, Partition);
        }

        virtual int GetPendingJobCount() const override
        {
            return IsActive() ? TTask::GetPendingJobCount() : 0;
        }

        virtual int GetTotalJobCount() const override
        {
            return IsActive() ? TTask::GetTotalJobCount() : 0;
        }


    protected:
        TSortControllerBase* Controller;
        TPartition* Partition;

        virtual TTableReaderOptionsPtr GetTableReaderOptions() const override
        {
            static const auto options = New<TTableReaderOptions>();
            return options;
        }

    };

    //! Base class implementing sort phase for sort operations
    //! and partition reduce phase for map-reduce operations.
    class TSortTask
        : public TPartitionBoundTask
    {
    public:
        //! For persistence only.
        TSortTask()
        { }

        TSortTask(TSortControllerBase* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->SortTaskGroup;
        }

        virtual TJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            return GetNeededResourcesForChunkStripe(
                joblet->InputStripeList->GetAggregateStatistics(),
                joblet->MemoryReserveEnabled);
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return Controller->SimpleSort
                ? Controller->SimpleSortPool.get()
                : Controller->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return Controller->SimpleSort
                ? Controller->SimpleSortPool.get()
                : Partition->ChunkPoolOutput;
        }

    protected:
        virtual bool IsMemoryReserveEnabled() const override
        {
            if (Controller->IsSortedMergeNeeded(Partition)) {
                return Controller->IsMemoryReserveEnabled(Controller->IntermediateSortJobCounter);
            } else {
                return Controller->IsMemoryReserveEnabled(Controller->FinalSortJobCounter);
            }
        }

        TJobResources GetNeededResourcesForChunkStripe(
            const TChunkStripeStatistics& stat,
            bool memoryReserveEnabled) const
        {
            if (Controller->SimpleSort) {
                i64 valueCount = Controller->GetValueCountEstimate(stat.DataSize);
                return Controller->GetSimpleSortResources(
                    stat,
                    valueCount);
            } else {
                return Controller->GetPartitionSortResources(
                    Partition,
                    stat,
                    memoryReserveEnabled);
            }
        }

        virtual TJobResources GetMinNeededResourcesHeavy() const override
        {
            auto stat = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            if (Controller->SimpleSort && stat.size() > 1) {
                stat = AggregateStatistics(stat);
            } else {
                YCHECK(stat.size() == 1);
            }
            return GetNeededResourcesForChunkStripe(stat.front(), IsMemoryReserveEnabled());
        }

        virtual bool IsIntermediateOutput() const override
        {
            return Controller->IsSortedMergeNeeded(Partition);
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(
                Controller->IsSortedMergeNeeded(Partition)
                ? Controller->IntermediateSortJobSpecTemplate.type()
                : Controller->FinalSortJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            if (Controller->IsSortedMergeNeeded(Partition)) {
                jobSpec->CopyFrom(Controller->IntermediateSortJobSpecTemplate);
                AddIntermediateOutputSpec(jobSpec, joblet, Controller->Spec->SortBy);
            } else {
                jobSpec->CopyFrom(Controller->FinalSortJobSpecTemplate);
                AddFinalOutputSpecs(jobSpec, joblet);
            }

            auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_is_approximate(joblet->InputStripeList->IsApproximate);

            AddSequentialInputSpec(jobSpec, joblet);

            const auto& list = joblet->InputStripeList;
            if (list->PartitionTag) {
                auto jobType = GetJobType();
                if (jobType == EJobType::PartitionReduce || jobType == EJobType::ReduceCombiner) {
                    auto* reduceJobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                    reduceJobSpecExt->set_partition_tag(*list->PartitionTag);
                } else {
                    auto* sortJobSpecExt = jobSpec->MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                    sortJobSpecExt->set_partition_tag(*list->PartitionTag);
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

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TPartitionBoundTask::OnJobCompleted(joblet, jobSummary);

            Controller->SortDataSizeCounter.Completed(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Completed(1);

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto* resultExt = jobSummary.Result->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunks());

                RegisterIntermediate(
                    joblet,
                    stripe,
                    Partition->SortedMergeTask,
                    false);
            } else {
                Controller->FinalSortJobCounter.Completed(1);

                // Sort outputs in small partitions go directly to the output.
                RegisterOutput(joblet, Partition->Index, jobSummary);
                Controller->OnPartitionCompleted(Partition);
            }

            Controller->CheckMergeStartThreshold();

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->AddTaskPendingHint(Partition->SortedMergeTask);
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            Controller->SortDataSizeCounter.Failed(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Failed(1);
            } else {
                Controller->FinalSortJobCounter.Failed(1);
            }

            TTask::OnJobFailed(joblet, jobSummary);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            Controller->SortDataSizeCounter.Aborted(joblet->InputStripeList->TotalDataSize);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter.Aborted(1, jobSummary.AbortReason);
            } else {
                Controller->FinalSortJobCounter.Aborted(1, jobSummary.AbortReason);
                Controller->UpdateAllTasksIfNeeded(Controller->FinalSortJobCounter);
            }

            TTask::OnJobAborted(joblet, jobSummary);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            Controller->IntermediateSortJobCounter.Lost(1);
            auto stripeList = completedJob->SourceTask->GetChunkPoolOutput()->GetStripeList(completedJob->OutputCookie);
            Controller->SortDataSizeCounter.Lost(stripeList->TotalDataSize);

            TTask::OnJobLost(completedJob);
        }

        virtual void OnTaskCompleted() override
        {
            TPartitionBoundTask::OnTaskCompleted();

            // Kick-start the corresponding merge task.
            if (Controller->IsSortedMergeNeeded(Partition)) {
                Partition->SortedMergeTask->FinishInput();
            }
        }

    };

    //! Implements partition sort for sort operations and
    //! partition reduce phase for map-reduce operations.
    class TPartitionSortTask
        : public TSortTask
    {
    public:
        //! For persistence only.
        TPartitionSortTask()
        { }

        TPartitionSortTask(TSortControllerBase* controller, TPartition* partition)
            : TSortTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return Format("Sort(%v)", Partition->Index);
        }


        virtual TDuration GetLocalityTimeout() const override
        {
            return Partition->AssignedNodeId != InvalidNodeId
                ? Controller->Spec->SortAssignmentTimeout
                : Controller->Spec->SortLocalityTimeout;
        }

        virtual i64 GetLocality(TNodeId nodeId) const override
        {
            if (Partition->AssignedNodeId == nodeId) {
                // Handle initially assigned address.
                return 1;
            } else {
                // Handle data-driven locality.
                auto it = Partition->NodeIdToLocality.find(nodeId);
                return it == Partition->NodeIdToLocality.end() ? 0 : it->second;
            }
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionSortTask, 0x4f9a6cd9);

        virtual bool IsActive() const override
        {
            return Controller->SortStartThresholdReached && !Partition->Maniac;
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            auto nodeId = joblet->NodeDescriptor.Id;

            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            Partition->NodeIdToLocality[nodeId] += joblet->InputStripeList->TotalDataSize;

            // Don't rely on static assignment anymore.
            Partition->AssignedNodeId = InvalidNodeId;

            // Also add a hint to ensure that subsequent jobs are also scheduled here.
            AddLocalityHint(nodeId);

            TSortTask::OnJobStarted(joblet);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            auto nodeId = completedJob->NodeDescriptor.Id;
            YCHECK((Partition->NodeIdToLocality[nodeId] -= completedJob->DataSize) >= 0);

            Controller->ResetTaskLocalityDelays();

            TSortTask::OnJobLost(completedJob);
        }
    };

    //! Implements simple sort phase for sort operations.
    class TSimpleSortTask
        : public TSortTask
    {
    public:
        //! For persistence only.
        TSimpleSortTask()
        { }

        TSimpleSortTask(TSortControllerBase* controller, TPartition* partition)
            : TSortTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return "SimpleSort";
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->SimpleSortLocalityTimeout;
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleSortTask, 0xb32d4f02);

    };

    //! Base class for both sorted and ordered merge.
    class TMergeTask
        : public TPartitionBoundTask
    {
    public:
        //! For persistence only.
        TMergeTask()
        { }

        TMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->MergeTaskGroup;
        }

    private:
        virtual void OnTaskCompleted() override
        {
            if (!Partition->Completed) {
                // In extremely rare situations we may want to complete partition twice,
                // e.g. maniac partition with no data. Don't do that.
                Controller->OnPartitionCompleted(Partition);
            }

            TPartitionBoundTask::OnTaskCompleted();
        }

    };

    //! Implements sorted merge phase for sort operations and
    //! sorted reduce phase for map-reduce operations.
    class TSortedMergeTask
        : public TMergeTask
    {
    public:
        //! For persistence only.
        TSortedMergeTask()
        { }

        TSortedMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TMergeTask(controller, partition)
            , ChunkPool(CreateAtomicChunkPool())
        { }

        virtual Stroka GetId() const override
        {
            return Format("SortedMerge(%v)", Partition->Index);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return
                Controller->SimpleSort
                ? Controller->Spec->SimpleMergeLocalityTimeout
                : Controller->Spec->MergeLocalityTimeout;
        }

        virtual TJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            return Controller->GetSortedMergeResources(
                joblet->InputStripeList->GetStatistics(),
                joblet->MemoryReserveEnabled);
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool.get();
        }

        virtual void Persist(TPersistenceContext& context) override
        {
            TMergeTask::Persist(context);

            using NYT::Persist;
            Persist(context, ChunkPool);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeTask, 0x4ab19c75);

        std::unique_ptr<IChunkPool> ChunkPool;

        virtual bool IsActive() const override
        {
            return Controller->MergeStartThresholdReached && !Partition->Maniac;
        }

        virtual bool IsMemoryReserveEnabled() const override
        {
            return Controller->IsMemoryReserveEnabled(Controller->SortedMergeJobCounter);
        }

        virtual TJobResources GetMinNeededResourcesHeavy() const override
        {
            return Controller->GetSortedMergeResources(
                ChunkPool->GetApproximateStripeStatistics(),
                IsMemoryReserveEnabled());
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool.get();
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->SortedMergeJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, joblet);
            AddFinalOutputSpecs(jobSpec, joblet);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            YCHECK(!Partition->Maniac);

            Controller->SortedMergeJobCounter.Start(1);

            TMergeTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobCompleted(joblet, jobSummary);

            Controller->SortedMergeJobCounter.Completed(1);
            RegisterOutput(joblet, Partition->Index, jobSummary);
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(joblet, jobSummary);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter.Aborted(1, jobSummary.AbortReason);

            Controller->UpdateAllTasksIfNeeded(Controller->SortedMergeJobCounter);

            TMergeTask::OnJobAborted(joblet, jobSummary);
        }

    };

    //! Implements unordered merge of maniac partitions for sort operation.
    //! Not used in map-reduce operations.
    class TUnorderedMergeTask
        : public TMergeTask
    {
    public:
        //! For persistence only.
        TUnorderedMergeTask()
        { }

        TUnorderedMergeTask(TSortControllerBase* controller, TPartition* partition)
            : TMergeTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return Format("UnorderedMerge(%v)", Partition->Index);
        }

        virtual i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            // Locality is unimportant.
            return 0;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            // Makes no sense to wait.
            return TDuration::Zero();
        }

        virtual TJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            return Controller->GetUnorderedMergeResources(
                joblet->InputStripeList->GetStatistics());
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return Controller->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return Partition->ChunkPoolOutput;
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeTask, 0xbba17c0f);

        virtual bool IsActive() const override
        {
             return Controller->MergeStartThresholdReached && Partition->Maniac;
        }

        virtual bool IsMemoryReserveEnabled() const override
        {
            return true;
        }

        virtual TJobResources GetMinNeededResourcesHeavy() const override
        {
            return Controller->GetUnorderedMergeResources(
                Partition->ChunkPoolOutput->GetApproximateStripeStatistics());
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->UnorderedMergeJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddFinalOutputSpecs(jobSpec, joblet);

            const auto& list = joblet->InputStripeList;
            if (list->PartitionTag) {
                auto* mergeJobSpecExt = jobSpec->MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
                mergeJobSpecExt->set_partition_tag(*list->PartitionTag);
            }
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            YCHECK(Partition->Maniac);
            TMergeTask::OnJobStarted(joblet);

            Controller->UnorderedMergeJobCounter.Start(1);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobCompleted(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter.Completed(1);
            RegisterOutput(joblet, Partition->Index, jobSummary);
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobFailed(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter.Failed(1);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobAborted(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter.Aborted(1, jobSummary.AbortReason);
        }

    };


    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        // NB: Register groups in the order of _descending_ priority.
        MergeTaskGroup = New<TTaskGroup>();
        MergeTaskGroup->MinNeededResources.SetCpu(1);
        RegisterTaskGroup(MergeTaskGroup);

        SortTaskGroup = New<TTaskGroup>();
        SortTaskGroup->MinNeededResources.SetCpu(1);
        SortTaskGroup->MinNeededResources.SetNetwork(Spec->ShuffleNetworkLimit);
        RegisterTaskGroup(SortTaskGroup);

        PartitionTaskGroup = New<TTaskGroup>();
        PartitionTaskGroup->MinNeededResources.SetCpu(1);
        RegisterTaskGroup(PartitionTaskGroup);
    }


    // Init/finish.

    void AssignPartitions()
    {
        struct TAssignedNode
            : public TIntrinsicRefCounted
        {
            TAssignedNode(const TExecNodeDescriptor& descriptor, double weight)
                : Descriptor(descriptor)
                , Weight(weight)
            { }

            TExecNodeDescriptor Descriptor;
            double Weight;
            i64 AssignedDataSize = 0;
        };

        typedef TIntrusivePtr<TAssignedNode> TAssignedNodePtr;

        auto compareNodes = [&] (const TAssignedNodePtr& lhs, const TAssignedNodePtr& rhs) {
            return lhs->AssignedDataSize / lhs->Weight > rhs->AssignedDataSize / rhs->Weight;
        };

        auto comparePartitions = [&] (const TPartitionPtr& lhs, const TPartitionPtr& rhs) {
            return lhs->ChunkPoolOutput->GetTotalDataSize() > rhs->ChunkPoolOutput->GetTotalDataSize();
        };

        LOG_DEBUG("Examining online nodes");

        const auto& nodeDescriptors = GetExecNodeDescriptors();
        auto maxResourceLimits = ZeroJobResources();
        double maxIOWeight = 0;
        for (const auto& descriptor : nodeDescriptors) {
            maxResourceLimits = Max(maxResourceLimits, descriptor.ResourceLimits);
            maxIOWeight = std::max(maxIOWeight, descriptor.IOWeight);
        }

        std::vector<TAssignedNodePtr> nodeHeap;
        for (const auto& node : nodeDescriptors) {
            double weight = 1.0;
            weight = std::min(weight, GetMinResourceRatio(node.ResourceLimits, maxResourceLimits));
            weight = std::min(weight, node.IOWeight > 0 ? node.IOWeight / maxIOWeight : 0);
            if (weight > 0) {
                auto assignedNode = New<TAssignedNode>(node, weight);
                nodeHeap.push_back(assignedNode);
            }
        }

        std::vector<TPartitionPtr> partitionsToAssign;
        for (const auto& partition : Partitions) {
            // Only take partitions for which no jobs are launched yet.
            if (partition->NodeIdToLocality.empty()) {
                partitionsToAssign.push_back(partition);
            }
        }
        std::sort(partitionsToAssign.begin(), partitionsToAssign.end(), comparePartitions);

        // This is actually redundant since all values are 0.
        std::make_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

        LOG_DEBUG("Assigning partitions");

        for (const auto& partition : partitionsToAssign) {
            auto node = nodeHeap.front();
            auto nodeId = node->Descriptor.Id;

            partition->AssignedNodeId = nodeId;
            auto task = partition->Maniac
                ? static_cast<TTaskPtr>(partition->UnorderedMergeTask)
                : static_cast<TTaskPtr>(partition->SortTask);

            AddTaskLocalityHint(task, nodeId);

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataSize += partition->ChunkPoolOutput->GetTotalDataSize();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            LOG_DEBUG("Partition assigned (Index: %v, DataSize: %v, Address: %v)",
                partition->Index,
                partition->ChunkPoolOutput->GetTotalDataSize(),
                node->Descriptor.Address);
        }

        for (const auto& node : nodeHeap) {
            if (node->AssignedDataSize > 0) {
                LOG_DEBUG("Node used (Address: %v, Weight: %.4lf, AssignedDataSize: %v, AdjustedDataSize: %v)",
                    node->Descriptor.Address,
                    node->Weight,
                    node->AssignedDataSize,
                    static_cast<i64>(node->AssignedDataSize / node->Weight));
            }
        }

        LOG_DEBUG("Partitions assigned");
    }

    void InitShufflePool()
    {
        ShufflePool = CreateShuffleChunkPool(
            static_cast<int>(Partitions.size()),
            Spec->DataSizePerSortJob);

        for (auto partition : Partitions) {
            partition->ChunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        }
    }

    void InitSimpleSortPool(int sortJobCount)
    {
        SimpleSortPool = CreateUnorderedChunkPool(
            sortJobCount,
            Config->MaxChunkStripesPerJob);
    }

    virtual bool IsCompleted() const override
    {
        return CompletedPartitionCount == Partitions.size();
    }

    virtual void OnOperationCompleted() override
    {
        if (IsRowCountPreserved()) {
            i64 totalInputRowCount = 0;
            for (auto partition : Partitions) {
                totalInputRowCount += partition->ChunkPoolOutput->GetTotalRowCount();
            }
            if (totalInputRowCount != TotalOutputRowCount) {
                OnOperationFailed(TError(
                    "Input/output row count mismatch in sort operation: %v != %v",
                    totalInputRowCount,
                    TotalOutputRowCount));
            }
        }

        YCHECK(CompletedPartitionCount == Partitions.size());
        TOperationControllerBase::OnOperationCompleted();
    }

    void OnPartitionCompleted(TPartitionPtr partition)
    {
        YCHECK(!partition->Completed);
        partition->Completed = true;

        ++CompletedPartitionCount;

        LOG_INFO("Partition completed (Partition: %v)", partition->Index);
    }

    bool IsSortedMergeNeeded(TPartitionPtr partition) const
    {
        if (partition->CachedSortedMergeNeeded) {
            return true;
        }

        if (SimpleSort) {
            if (partition->ChunkPoolOutput->GetTotalJobCount() <= 1) {
                return false;
            }
        } else {
            if (partition->Maniac) {
                return false;
            }

            if (partition->SortTask->GetPendingJobCount() == 0) {
                return false;
            }

            if (partition->ChunkPoolOutput->GetTotalJobCount() <= 1 && PartitionTask->IsCompleted()) {
                return false;
            }
        }

        LOG_DEBUG("Partition needs sorted merge (Partition: %v)", partition->Index);
        SortedMergeJobCounter.Increment(1);
        partition->CachedSortedMergeNeeded = true;
        return true;
    }

    void CheckSortStartThreshold()
    {
        if (SortStartThresholdReached)
            return;

        if (!SimpleSort && PartitionTask->GetCompletedDataSize() < PartitionTask->GetTotalDataSize() * Spec->ShuffleStartThreshold)
            return;

        LOG_INFO("Sort start threshold reached");

        SortStartThresholdReached = true;
        AddSortTasksPendingHints();
    }

    static int AdjustPartitionCountToWriterBufferSize(
        int partitionCount, 
        TChunkWriterConfigPtr config)
    {
        auto averageBufferSize = config->MaxBufferSize / partitionCount / 2;
        if (averageBufferSize < THorizontalSchemalessBlockWriter::MinReserveSize) {
            i64 minAppropriateSize = 2 * THorizontalSchemalessBlockWriter::MinReserveSize;
            return std::max(config->MaxBufferSize / minAppropriateSize, (i64)1);
        }
        return partitionCount;
    }

    void CheckMergeStartThreshold()
    {
        if (MergeStartThresholdReached)
            return;

        if (!SimpleSort) {
            if (!PartitionTask->IsCompleted())
                return;
            if (SortDataSizeCounter.GetCompleted() < SortDataSizeCounter.GetTotal() * Spec->MergeStartThreshold)
                return;
        }

        LOG_INFO("Merge start threshold reached");

        MergeStartThresholdReached = true;
        AddMergeTasksPendingHints();
    }

    void AddSortTasksPendingHints()
    {
        for (auto partition : Partitions) {
            if (!partition->Maniac) {
                AddTaskPendingHint(partition->SortTask);
            }
        }
    }

    void AddMergeTasksPendingHints()
    {
        for (auto partition : Partitions) {
            auto taskToKick = partition->Maniac
                ? TTaskPtr(partition->UnorderedMergeTask)
                : TTaskPtr(partition->SortedMergeTask);
            AddTaskPendingHint(taskToKick);
        }
    }

    // Resource management.

    virtual TJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool memoryReserveEnabled) const = 0;

    virtual TJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const = 0;

    virtual TJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat,
        bool memoryReserveEnabled) const = 0;

    virtual TJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics,
        bool memoryReserveEnabled) const = 0;

    virtual TJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const = 0;

    // Unsorted helpers.

    i64 GetSortBuffersMemorySize(const TChunkStripeStatistics& stat) const
    {
        // Calculate total size of buffers, presented in TSchemalessPartitionSortReader.
        return
            (i64) 16 * Spec->SortBy.size() * stat.RowCount + // KeyBuffer
            (i64) 12 * stat.RowCount +                       // RowDescriptorBuffer
            (i64) 4 * stat.RowCount +                        // Buckets
            (i64) 4 * stat.RowCount;                         // SortedIndexes
    }

    i64 GetRowCountEstimate(TPartitionPtr partition, i64 dataSize) const
    {
        i64 totalDataSize = partition->ChunkPoolOutput->GetTotalDataSize();
        if (totalDataSize == 0) {
            return 0;
        }
        i64 totalRowCount = partition->ChunkPoolOutput->GetTotalRowCount();
        return static_cast<i64>((double) totalRowCount * dataSize / totalDataSize);
    }

    // TODO(babenko): this is the input estimate, not the partitioned one!
    // Should get rid of this "value count" stuff completely.
    i64 GetValueCountEstimate(i64 dataSize) const
    {
        return static_cast<i64>((double) TotalEstimatedInputValueCount * dataSize / TotalEstimatedInputDataSize);
    }

    // Returns compression ratio of input data.
    double GetCompressionRatio() const
    {
        return static_cast<double>(TotalEstimatedCompressedDataSize) / TotalEstimatedInputDataSize;
    }

    i64 GetMaxPartitionJobBufferSize() const
    {
        return Spec->PartitionJobIO->TableWriter->MaxBufferSize;
    }

    int SuggestPartitionCount() const
    {
        YCHECK(TotalEstimatedInputDataSize > 0);
        i64 dataSizeAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataSize * Spec->MapSelectivityFactor);

        int result;
        if (Spec->PartitionCount) {
            result = Spec->PartitionCount.Get();
        } else if (Spec->PartitionDataSize) {
            result = 1 + dataSizeAfterPartition / Spec->PartitionDataSize.Get();
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options->CompressedBlockSize / GetCompressionRatio());
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec->PartitionJobIO->TableWriter->BlockSize);

            // Product may not fit into i64.
            double partitionDataSize = sqrt(dataSizeAfterPartition) * sqrt(uncompressedBlockSize);

            int maxPartitionCount = GetMaxPartitionJobBufferSize() / uncompressedBlockSize;
            result = std::min(static_cast<int>(dataSizeAfterPartition / partitionDataSize), maxPartitionCount);
        }

        return std::max(result, 1);
    }

    int SuggestPartitionJobCount() const
    {
        if (Spec->DataSizePerPartitionJob || Spec->PartitionJobCount) {
            return SuggestJobCount(
                TotalEstimatedInputDataSize,
                Spec->DataSizePerPartitionJob.Get(TotalEstimatedInputDataSize),
                Spec->PartitionJobCount,
                Options->MaxPartitionJobCount);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options->CompressedBlockSize / GetCompressionRatio());
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec->PartitionJobIO->TableWriter->BlockSize);

            // Product may not fit into i64.
            double partitionJobDataSize = sqrt(TotalEstimatedInputDataSize) * sqrt(uncompressedBlockSize);
            partitionJobDataSize = std::min(partitionJobDataSize, static_cast<double>(GetMaxPartitionJobBufferSize()));

            return static_cast<int>(Clamp(
                static_cast<i64>(TotalEstimatedInputDataSize / partitionJobDataSize),
                1,
                Options->MaxPartitionJobCount));
        }
    }

    // Partition progress.

    struct TPartitionProgress
    {
        std::vector<i64> Total;
        std::vector<i64> Runnning;
        std::vector<i64> Completed;
    };

    static std::vector<i64> AggregateValues(const std::vector<i64>& values, int maxBuckets)
    {
        if (values.size() < maxBuckets) {
            return values;
        }

        std::vector<i64> result(maxBuckets);
        for (int i = 0; i < maxBuckets; ++i) {
            int lo = static_cast<int>(i * values.size() / maxBuckets);
            int hi = static_cast<int>((i + 1) * values.size() / maxBuckets);
            i64 sum = 0;
            for (int j = lo; j < hi; ++j) {
                sum += values[j];
            }
            result[i] = sum * values.size() / (hi - lo) / maxBuckets;
        }

        return result;
    }

    TPartitionProgress ComputePartitionProgress() const
    {
        TPartitionProgress result;
        std::vector<i64> sizes(Partitions.size());
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetTotalDataSize();
            }
            result.Total = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetRunningDataSize();
            }
            result.Runnning = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetCompletedDataSize();
            }
            result.Completed = AggregateValues(sizes, MaxProgressBuckets);
        }
        return result;
    }

    // Partition sizes histogram.

    struct TPartitionSizeHistogram
    {
        i64 Min;
        i64 Max;
        std::vector<i64> Count;
    };

    TPartitionSizeHistogram ComputePartitionSizeHistogram() const
    {
        TPartitionSizeHistogram result;

        result.Min = std::numeric_limits<i64>::max();
        result.Max = std::numeric_limits<i64>::min();
        for (auto partition : Partitions) {
            i64 size = partition->ChunkPoolOutput->GetTotalDataSize();
            if (size == 0)
                continue;
            result.Min = std::min(result.Min, size);
            result.Max = std::max(result.Max, size);
        }

        if (result.Min > result.Max)
            return result;

        int bucketCount = result.Min == result.Max ? 1 : MaxSizeHistogramBuckets;
        result.Count.resize(bucketCount);

        auto computeBucket = [&] (i64 size) -> int {
            if (result.Min == result.Max) {
                return 0;
            }

            int bucket = (size - result.Min) * MaxSizeHistogramBuckets / (result.Max - result.Min);
            if (bucket == bucketCount) {
                bucket = bucketCount - 1;
            }

            return bucket;
        };

        for (auto partition : Partitions) {
            i64 size = partition->ChunkPoolOutput->GetTotalDataSize();
            if (size == 0)
                continue;
            int bucket = computeBucket(size);
            ++result.Count[bucket];
        }

        return result;
    }

    void BuildPartitionsProgressYson(IYsonConsumer* consumer) const
    {
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Value(Partitions.size())
                .Item("completed").Value(CompletedPartitionCount)
            .EndMap();

        auto progress = ComputePartitionProgress();
        BuildYsonMapFluently(consumer)
            .Item("partition_sizes").BeginMap()
                .Item("total").Value(progress.Total)
                .Item("running").Value(progress.Runnning)
                .Item("completed").Value(progress.Completed)
            .EndMap();

        auto sizeHistogram = ComputePartitionSizeHistogram();
        BuildYsonMapFluently(consumer)
            .Item("partition_size_histogram").BeginMap()
                .Item("min").Value(sizeHistogram.Min)
                .Item("max").Value(sizeHistogram.Max)
                .Item("count").Value(sizeHistogram.Count)
            .EndMap();
    }

    virtual void RegisterOutput(TJobletPtr joblet, int key, const TCompletedJobSummary& jobSummary) override
    {
        TotalOutputRowCount += GetTotalOutputDataStatistics(jobSummary.Statistics).row_count();
        TOperationControllerBase::RegisterOutput(std::move(joblet), key, jobSummary);
    }

    void InitJobIOConfigs()
    {
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        InitIntermediateOutputConfig(PartitionJobIOConfig);

        PartitionTableReaderOptions = CreateTableReaderOptions(Spec->PartitionJobIO);
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TPartitionTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TPartitionSortTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TSimpleSortTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TSortedMergeTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TUnorderedMergeTask);

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
            config->SortOperationOptions,
            host,
            operation)
        , Spec(spec)
    { }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortController, 0xbca37afe);

    TSortOperationSpecPtr Spec;

    //! |PartitionCount - 1| separating keys.
    std::vector<TOwningKey> PartitionKeys;

    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TSortControllerBase::DoInitialize();

        auto& table = OutputTables[0];
        table.UpdateMode = EUpdateMode::Overwrite;
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

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        OutputTables[0].KeyColumns = Spec->SortBy;

        if (TotalEstimatedInputDataSize == 0)
            return;

        TSamplesFetcherPtr samplesFetcher;

        TFuture<void> asyncSamplesResult;
        PROFILE_TIMING ("/input_processing_time") {
            auto chunks = CollectInputChunks();
            int sampleCount = SuggestPartitionCount() * Spec->SamplesPerPartition;

            TScrapeChunksCallback scraperCallback;
            if (Spec->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
                scraperCallback = CreateScrapeChunksSessionCallback(
                    Config,
                    GetCancelableInvoker(),
                    Host->GetChunkLocationThrottlerManager(),
                    AuthenticatedInputMasterClient,
                    InputNodeDirectory,
                    Logger);
            }

            samplesFetcher = New<TSamplesFetcher>(
                Config->Fetcher,
                sampleCount,
                Spec->SortBy,
                Options->MaxSampleSize,
                InputNodeDirectory,
                GetCancelableInvoker(),
                scraperCallback,
                Host->GetMasterClient(),
                Logger);

            for (const auto& chunk : chunks) {
                samplesFetcher->AddChunk(chunk);
            }

            asyncSamplesResult = samplesFetcher->Fetch();
        }

        WaitFor(asyncSamplesResult)
            .ThrowOnError();

        InitJobIOConfigs();
        
        PROFILE_TIMING ("/samples_processing_time") {
            auto sortedSamples = SortSamples(samplesFetcher->GetSamples());
            BuildPartitions(sortedSamples);
        }

        InitJobSpecTemplates();
    }

    std::vector<const TSample*> SortSamples(const std::vector<TSample>& samples)
    {
        int sampleCount = static_cast<int>(samples.size());
        LOG_INFO("Sorting %v samples", sampleCount);

        std::vector<const TSample*> sortedSamples;
        sortedSamples.reserve(sampleCount);
        try {
            for (const auto& sample : samples) {
                ValidateClientKey(sample.Key);
                sortedSamples.push_back(&sample);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating table samples") << ex;
        }

        std::sort(
            sortedSamples.begin(),
            sortedSamples.end(),
            [] (const TSample* lhs, const TSample* rhs) {
                return *lhs < *rhs;
            });

        return sortedSamples;
    }

    void BuildPartitions(const std::vector<const TSample*>& sortedSamples)
    {
        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = SuggestPartitionCount();

        // Don't create more partitions than we have samples (plus one).
        partitionCount = std::min(partitionCount, static_cast<int>(sortedSamples.size()) + 1);

        YCHECK(partitionCount > 0);
        
        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount, 
            PartitionJobIOConfig->TableWriter);
        SimpleSort = (partitionCount == 1);


        if (SimpleSort) {
            BuildSinglePartition();
        } else {
            BuildMulitplePartitions(sortedSamples, partitionCount);
        }
    }

    void BuildSinglePartition()
    {
        // Choose sort job count and initialize the pool.
        int sortJobCount = static_cast<int>(
            Clamp(
                1 + TotalEstimatedInputDataSize / Spec->DataSizePerSortJob,
                1,
                Options->MaxPartitionJobCount));
        auto stripes = SliceInputChunks(Options->SortJobMaxSliceDataSize, &sortJobCount);

        // Create the fake partition.
        InitSimpleSortPool(sortJobCount);
        auto partition = New<TPartition>(this, 0);
        Partitions.push_back(partition);
        partition->ChunkPoolOutput = SimpleSortPool.get();
        partition->SortTask->AddInput(stripes);
        partition->SortTask->FinishInput();

        // Initialize counters.
        PartitionJobCounter.Set(0);
        // NB: Cannot use TotalEstimatedInputDataSize due to slicing and rounding issues.
        SortDataSizeCounter.Set(SimpleSortPool->GetTotalDataSize());

        LOG_INFO("Sorting without partitioning (SortJobCount: %v)",
            sortJobCount);

        // Kick-start the sort task.
        SortStartThresholdReached = true;
    }

    void AddPartition(const TOwningKey& key)
    {
        using NChunkClient::ToString;

        int index = static_cast<int>(Partitions.size());
        LOG_DEBUG("Partition %v has starting key %v",
            index,
            key);

        YCHECK(PartitionKeys.empty() || CompareRows(PartitionKeys.back(), key) < 0);

        PartitionKeys.push_back(key);
        Partitions.push_back(New<TPartition>(this, index));
    }

    void BuildMulitplePartitions(const std::vector<const TSample*>& sortedSamples, int partitionCount)
    {
        LOG_INFO("Building partition keys");

        i64 totalSamplesWeight = 0;
        for (const auto* sample : sortedSamples) {
            totalSamplesWeight += sample->Weight;
        }

        // Select samples evenly wrt weights.
        std::vector<const TSample*> selectedSamples;
        selectedSamples.reserve(partitionCount - 1);

        double weightPerPartition = (double)totalSamplesWeight / partitionCount;
        i64 processedWeight = 0;
        for (const auto* sample : sortedSamples) {
            processedWeight += sample->Weight;
            if (processedWeight / weightPerPartition > selectedSamples.size() + 1) {
                selectedSamples.push_back(sample);
            }
            if (selectedSamples.size() == partitionCount - 1) {
                // We need exactly partitionCount - 1 partition keys.
                break;
            }
        }

        // Construct the leftmost partition.
        Partitions.push_back(New<TPartition>(this, 0));

        // Invariant:
        //   lastPartition = Partitions.back()
        //   lastKey = PartitionKeys.back()
        //   lastPartition receives keys in [lastKey, ...)
        //
        // Initially PartitionKeys is empty so lastKey is assumed to be -inf.

        int sampleIndex = 0;
        while (sampleIndex < selectedSamples.size()) {
            auto* sample = selectedSamples[sampleIndex];
            // Check for same keys.
            if (PartitionKeys.empty() || CompareRows(sample->Key, PartitionKeys.back()) != 0) {
                AddPartition(sample->Key);
                ++sampleIndex;
            } else {
                // Skip same keys.
                int skippedCount = 0;
                while (sampleIndex < selectedSamples.size() &&
                    CompareRows(selectedSamples[sampleIndex]->Key, PartitionKeys.back()) == 0)
                {
                    ++sampleIndex;
                    ++skippedCount;
                }

                auto* lastManiacSample = selectedSamples[sampleIndex - 1];
                auto lastPartition = Partitions.back();

                if (!lastManiacSample->Incomplete) {
                    LOG_DEBUG("Partition %v is a maniac, skipped %v samples",
                        lastPartition->Index,
                        skippedCount);

                    lastPartition->Maniac = true;
                    YCHECK(skippedCount >= 1);

                    // NB: in partitioner we compare keys with the whole rows,
                    // so key prefix successor in required here.
                    auto successorKey = GetKeyPrefixSuccessor(sample->Key, Spec->SortBy.size());
                    AddPartition(successorKey);
                } else {
                    // If sample keys are incomplete, we cannot use UnorderedMerge,
                    // because full keys may be different.
                    LOG_DEBUG("Partition %v is oversized, skipped %v samples",
                        lastPartition->Index,
                        skippedCount);
                    AddPartition(selectedSamples[sampleIndex]->Key);
                    ++sampleIndex;
                }
            }
        }

        InitShufflePool();

        int partitionJobCount = SuggestPartitionJobCount();
        auto stripes = SliceInputChunks(Options->PartitionJobMaxSliceDataSize, &partitionJobCount);

        PartitionJobCounter.Set(partitionJobCount);

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();
        RegisterTask(PartitionTask);

        LOG_INFO("Sorting with partitioning (PartitionCount: %v, PartitionJobCount: %v)",
            partitionCount,
            PartitionJobCounter.GetTotal());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        if (!SimpleSort) {
            InitIntermediateInputConfig(IntermediateSortJobIOConfig);
        }
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        // Final sort: reader like sort and output like merge.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        FinalSortJobIOConfig->TableWriter = CloneYsonSerializable(Spec->MergeJobIO->TableWriter);
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
            PartitionJobSpecTemplate.set_type(static_cast<int>(EJobType::Partition));
            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).Data());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_partition_count(Partitions.size());
            for (const auto& key : PartitionKeys) {
                ToProto(partitionJobSpecExt->add_partition_keys(), key);
            }
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), Spec->SortBy);
        }

        TJobSpec sortJobSpecTemplate;
        {
            auto* schedulerJobSpecExt = sortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);

            auto* sortJobSpecExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(sortJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }

        {
            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_type(static_cast<int>(SimpleSort ? EJobType::SimpleSort : EJobType::IntermediateSort));
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).Data());
        }

        {
            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_type(static_cast<int>(SimpleSort ? EJobType::SimpleSort : EJobType::FinalSort));
            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).Data());
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedMerge));
            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).Data());

            ToProto(mergeJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
            auto* schedulerJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(UnorderedMergeJobIOConfig).Data());

            ToProto(mergeJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }
    }


    // Resource management.

    virtual TJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool memoryReserveEnabled) const override
    {
        UNUSED(memoryReserveEnabled);
        auto stat = AggregateStatistics(statistics).front();

        i64 outputBufferSize = std::min(
            PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            stat.DataSize);

        outputBufferSize += THorizontalSchemalessBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());

        outputBufferSize = std::min(
            outputBufferSize,
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(1);
        result.SetMemory(
            // NB: due to large MaxBufferSize for partition that was accounted in buffer size
            // we eliminate number of output streams to zero.
            GetInputIOMemorySize(PartitionJobIOConfig, stat) +
            outputBufferSize +
            GetOutputWindowMemorySize(PartitionJobIOConfig) +
            GetFootprintMemorySize());
        return result;
    }

    virtual TJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const override
    {
        // ToDo(psushin): rewrite simple sort estimates.
        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(1);
        result.SetMemory(
            GetSortInputIOMemorySize(stat) +
            GetFinalOutputIOMemorySize(FinalSortJobIOConfig) +
            GetSortBuffersMemorySize(stat) +
            // TODO(babenko): *2 are due to lack of reserve, remove this once simple sort
            // starts reserving arrays of appropriate sizes.
            (i64) 32 * valueCount * 2 +
            GetFootprintMemorySize());
        return result;
    }

    virtual TJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat,
        bool memoryReserveEnabled) const override
    {
        UNUSED(memoryReserveEnabled);
        i64 memory =
            GetSortBuffersMemorySize(stat) +
            GetSortInputIOMemorySize(stat) +
            GetFootprintMemorySize();

        if (IsSortedMergeNeeded(partition)) {
            memory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
        } else {
            memory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig);
        }


        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(1);
        result.SetMemory(memory);
        result.SetNetwork(Spec->ShuffleNetworkLimit);

        return result;
    }

    virtual TJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics,
        bool memoryReserveEnabled) const override
    {
        UNUSED(memoryReserveEnabled);

        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(1);
        result.SetMemory(
            GetFinalIOMemorySize(SortedMergeJobIOConfig, statistics) +
            GetFootprintMemorySize());
        return result;
    }

    virtual bool IsRowCountPreserved() const override
    {
        return true;
    }

    virtual TJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(1);
        result.SetMemory(
            GetFinalIOMemorySize(UnorderedMergeJobIOConfig, AggregateStatistics(statistics)) +
            GetFootprintMemorySize());
        return result;
    }


    // Progress reporting.

    virtual Stroka GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, L: %v}, "
            "Partitions = {T: %v, C: %v}, "
            "PartitionJobs = %v, "
            "IntermediateSortJobs = %v, "
            "FinalSortJobs = %v, "
            "SortedMergeJobs = %v, "
            "UnorderedMergeJobs = %v, "
            "UnavailableInputChunks: %v",
            // Jobs
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            JobCounter.GetLost(),
            // Partitions
            Partitions.size(),
            CompletedPartitionCount,
            // PartitionJobs
            PartitionJobCounter,
            // IntermediateSortJobs
            IntermediateSortJobCounter,
            // FinaSortJobs
            FinalSortJobCounter,
            // SortedMergeJobs
            SortedMergeJobCounter,
            // UnorderedMergeJobs
            UnorderedMergeJobCounter,
            UnavailableInputChunkCount);
    }

    virtual void BuildProgress(IYsonConsumer* consumer) const override
    {
        TSortControllerBase::BuildProgress(consumer);
        BuildYsonMapFluently(consumer)
            .Do(BIND(&TSortController::BuildPartitionsProgressYson, Unretained(this)))
            .Item("partition_jobs").Value(PartitionJobCounter)
            .Item("intermediate_sort_jobs").Value(IntermediateSortJobCounter)
            .Item("final_sort_jobs").Value(FinalSortJobCounter)
            .Item("sorted_merge_jobs").Value(SortedMergeJobCounter)
            .Item("unordered_merge_jobs").Value(UnorderedMergeJobCounter);
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortController);

IOperationControllerPtr CreateSortController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TSortOperationSpec>(operation->GetSpec());
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
            config->MapReduceOperationOptions,
            host,
            operation)
        , Spec(spec)
        , MapStartRowIndex(0)
        , ReduceStartRowIndex(0)
    { }

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TSortControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .DoIf(Spec->Mapper.operator bool(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                      .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
                    .EndMap();
            })
            .DoIf(Spec->Reducer.operator bool(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reducer").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->Reducer->Command))
                    .EndMap();
            })
            .DoIf(Spec->ReduceCombiner.operator bool(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_combiner").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->ReduceCombiner->Command))
                    .EndMap();
            });
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapReduceController, 0xca7286bd);

    TMapReduceOperationSpecPtr Spec;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserFile> ReduceCombinerFiles;
    std::vector<TUserFile> ReducerFiles;

    i64 MapStartRowIndex;
    i64 ReduceStartRowIndex;

    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TSortControllerBase::DoInitialize();

        ValidateUserFileCount(Spec->Mapper, "mapper");
        ValidateUserFileCount(Spec->Reducer, "reducer");
        ValidateUserFileCount(Spec->ReduceCombiner, "reduce combiner");

        if (!CheckKeyColumnsCompatible(Spec->SortBy, Spec->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce columns %v are not compatible with sort columns %v",
                Spec->ReduceBy,
                Spec->SortBy);
        }

        LOG_DEBUG("ReduceCcolumns: %v, SortColumns: %v",
            Spec->ReduceBy,
            Spec->SortBy);
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        // Combine mapper and reducer files into a single collection.
        std::vector<TPathWithStage> result;
        if (Spec->Mapper) {
            for (const auto& path : Spec->Mapper->FilePaths) {
                result.push_back(std::make_pair(path, EOperationStage::Map));
            }
        }

        if (Spec->ReduceCombiner) {
            for (const auto& path : Spec->ReduceCombiner->FilePaths) {
                result.push_back(std::make_pair(path, EOperationStage::ReduceCombiner));
            }
        }

        for (const auto& path : Spec->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        if (TotalEstimatedInputDataSize == 0)
            return;

        for (const auto& file : Files) {
            switch (file.Stage) {
                case EOperationStage::Map:
                    MapperFiles.push_back(file);
                    break;

                case EOperationStage::ReduceCombiner:
                    ReduceCombinerFiles.push_back(file);
                    break;

                case EOperationStage::Reduce:
                    ReducerFiles.push_back(file);
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        InitJobIOConfigs();

        PROFILE_TIMING ("/input_processing_time") {
            BuildPartitions();
        }

        InitJobSpecTemplates();
    }

    void BuildPartitions()
    {
        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = SuggestPartitionCount();

        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount, 
            PartitionJobIOConfig->TableWriter);

        BuildMultiplePartitions(partitionCount);
    }

    void BuildMultiplePartitions(int partitionCount)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }

        InitShufflePool();

        int partitionJobCount = SuggestPartitionJobCount();
        auto stripes = SliceInputChunks(
            Options->PartitionJobMaxSliceDataSize,
            &partitionJobCount);

        PartitionJobCounter.Set(partitionJobCount);

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();
        RegisterTask(PartitionTask);

        LOG_INFO("Map-reducing with partitioning (PartitionCount: %v, PartitionJobCount: %v)",
            partitionCount,
            PartitionJobCounter.GetTotal());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        // This is not a typo!
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        InitIntermediateOutputConfig(PartitionJobIOConfig);

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        InitIntermediateInputConfig(IntermediateSortJobIOConfig);
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        FinalSortJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);
        InitIntermediateInputConfig(FinalSortJobIOConfig);
        InitFinalOutputConfig(FinalSortJobIOConfig);

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        InitIntermediateInputConfig(SortedMergeJobIOConfig);
        InitFinalOutputConfig(SortedMergeJobIOConfig);
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(static_cast<int>(Spec->Mapper ? EJobType::PartitionMap : EJobType::Partition));

            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            if (Spec->InputQuery) {
                InitQuerySpec(schedulerJobSpecExt, *Spec->InputQuery, *Spec->InputSchema);
            }

            AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);

            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).Data());

            partitionJobSpecExt->set_partition_count(Partitions.size());
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), Spec->SortBy);

            if (Spec->Mapper) {
                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->Mapper,
                    MapperFiles);
            }
        }

        {
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).Data());

            if (Spec->ReduceCombiner) {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::ReduceCombiner));
                AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());

                auto* reduceJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
                reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->ReduceCombiner,
                    ReduceCombinerFiles);
            } else {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::IntermediateSort));
                auto* sortJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                ToProto(sortJobSpecExt->mutable_key_columns(), Spec->SortBy);
            }
        }

        {
            FinalSortJobSpecTemplate.set_type(static_cast<int>(EJobType::PartitionReduce));

            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());

            auto* reduceJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).Data());

            ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles);
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedReduce));

            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            AuxNodeDirectory->DumpTo(schedulerJobSpecExt->mutable_aux_node_directory());

            auto* reduceJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransactionId);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).Data());

            ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles);
        }
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        switch (joblet->JobType) {
            case EJobType::PartitionMap:
                joblet->StartRowIndex = MapStartRowIndex;
                MapStartRowIndex += joblet->InputStripeList->TotalRowCount;
                break;

            case EJobType::PartitionReduce:
            case EJobType::SortedReduce:
                joblet->StartRowIndex = ReduceStartRowIndex;
                ReduceStartRowIndex += joblet->InputStripeList->TotalRowCount;
                break;

            default:
                break;
        }
    }

    virtual void CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
    {
        auto getUserJobSpec = [=] () -> TUserJobSpecPtr {
            switch (EJobType(jobSpec->type())) {
                case EJobType::PartitionMap:
                    return Spec->Mapper;

                case EJobType::SortedReduce:
                case EJobType::PartitionReduce:
                    return Spec->Reducer;

                case EJobType::ReduceCombiner:
                    return Spec->ReduceCombiner;

                default:
                    return nullptr;
            }
        };

        auto userJobSpec = getUserJobSpec();
        if (!userJobSpec) {
            return;
        }

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpec(
            schedulerJobSpecExt->mutable_user_job_spec(),
            joblet,
            GetMemoryReserve(joblet->MemoryReserveEnabled, userJobSpec));
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    virtual bool IsIntermediateLivePreviewSupported() const override
    {
        return true;
    }

    // Resource management.

    virtual TJobResources GetPartitionResources(
            const TChunkStripeStatisticsVector& statistics,
            bool memoryReserveEnabled) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 reserveSize = THorizontalSchemalessBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TJobResources result;
        result.SetUserSlots(1);
        if (Spec->Mapper) {
            bufferSize += GetOutputWindowMemorySize(PartitionJobIOConfig);
            result.SetCpu(Spec->Mapper->CpuLimit);
            result.SetMemory(
                GetInputIOMemorySize(PartitionJobIOConfig, stat) +
                bufferSize +
                GetMemoryReserve(memoryReserveEnabled, Spec->Mapper) +
                GetFootprintMemorySize());
        } else {
            bufferSize = std::min(bufferSize, stat.DataSize + reserveSize);
            bufferSize += GetOutputWindowMemorySize(PartitionJobIOConfig);
            result.SetCpu(1);
            result.SetMemory(
                GetInputIOMemorySize(PartitionJobIOConfig, stat) +
                bufferSize +
                GetFootprintMemorySize());
        }
        return result;
    }

    virtual TJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const override
    {
        YUNREACHABLE();
    }

    virtual TJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat,
        bool memoryReserveEnabled) const override
    {
        TJobResources result;
        result.SetUserSlots(1);

        i64 memory =
            GetSortInputIOMemorySize(stat) +
            GetSortBuffersMemorySize(stat) +
            GetFootprintMemorySize();

        if (!IsSortedMergeNeeded(partition)) {
            result.SetCpu(Spec->Reducer->CpuLimit);
            result.SetMemory(
                memory +
                GetFinalOutputIOMemorySize(FinalSortJobIOConfig) +
                GetMemoryReserve(memoryReserveEnabled, Spec->Reducer));
        } else if (Spec->ReduceCombiner) {
            result.SetCpu(Spec->ReduceCombiner->CpuLimit);
            result.SetMemory(
                memory +
                GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig) +
                GetMemoryReserve(memoryReserveEnabled, Spec->ReduceCombiner));
        } else {
            result.SetCpu(1);
            result.SetMemory(
                memory +
                GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig));
        }

        result.SetNetwork(Spec->ShuffleNetworkLimit);
        return result;
    }

    virtual TJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics,
        bool memoryReserveEnabled) const override
    {
        TJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(Spec->Reducer->CpuLimit);
        result.SetMemory(
            GetFinalIOMemorySize(
                SortedMergeJobIOConfig,
                statistics) +
            GetMemoryReserve(memoryReserveEnabled, Spec->Reducer) +
            GetFootprintMemorySize());
        return result;
    }

    virtual TJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        YUNREACHABLE();
    }

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    // Progress reporting.

    virtual Stroka GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, L: %v}, "
            "Partitions = {T: %v, C: %v}, "
            "MapJobs = %v, "
            "SortJobs = %v, "
            "PartitionReduceJobs = %v, "
            "SortedReduceJobs = %v, "
            "UnavailableInputChunks: %v",
            // Jobs
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            JobCounter.GetLost(),
            // Partitions
            Partitions.size(),
            CompletedPartitionCount,
            // MapJobs
            PartitionJobCounter,
            // SortJobs
            IntermediateSortJobCounter,
            // PartitionReduceJobs
            FinalSortJobCounter,
            // SortedReduceJobs
            SortedMergeJobCounter,
            UnavailableInputChunkCount);
    }

    virtual void BuildProgress(IYsonConsumer* consumer) const override
    {
        TSortControllerBase::BuildProgress(consumer);
        BuildYsonMapFluently(consumer)
            .Do(BIND(&TMapReduceController::BuildPartitionsProgressYson, Unretained(this)))
            .Item(Spec->Mapper ? "partition_jobs" : "map_jobs").Value(PartitionJobCounter)
            .Item(Spec->ReduceCombiner ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
    }


};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMapReduceController);

IOperationControllerPtr CreateMapReduceController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(operation->GetSpec());
    return New<TMapReduceController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

