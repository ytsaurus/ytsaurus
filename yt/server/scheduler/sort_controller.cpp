#include "sort_controller.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "helpers.h"
#include "job_memory.h"
#include "map_controller.h"
#include "operation_controller_detail.h"
#include "sorted_chunk_pool.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/core/ytree/permission.h>

#include <yt/core/misc/numeric_helpers.h>

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
using namespace NSecurityClient;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NChunkClient;

using NTableClient::TKey;
using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/sort");

//! Maximum number of buckets for partition progress aggregation.
static const int MaxProgressBuckets = 100;

////////////////////////////////////////////////////////////////////////////////

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
        : TOperationControllerBase(config, spec, options, host, operation)
        , Spec(spec)
        , Options(options)
        , Config(config)
        , CompletedPartitionCount(0)
        // Cannot do similar for SortedMergeJobCounter and UnorderedMergeJobCounter since the number
        // of unsorted these jobs is hard to predict.
        , SortDataSizeCounter(0)
        , SortStartThresholdReached(false)
        , MergeStartThresholdReached(false)
        , TotalOutputRowCount(0)
        , SimpleSort(false)
    { }

    // Persistence.
    virtual void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;

        Persist(context, CompletedPartitionCount);
        Persist(context, SortedMergeJobCounter);
        Persist(context, UnorderedMergeJobCounter);
        Persist(context, IntermediateSortJobCounter);
        Persist(context, FinalSortJobCounter);
        Persist(context, SortDataSizeCounter);

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

        Persist(context, PartitionPool);
        Persist(context, ShufflePool);
        Persist(context, SimpleSortPool);

        Persist(context, PartitionTaskGroup);
        Persist(context, SortTaskGroup);
        Persist(context, MergeTaskGroup);

        Persist(context, PartitionTask);

        // COMPAT(psushin).
        auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
        if (context.IsLoad() &&
            GetSortedMergeJobType() == EJobType::SortedMerge &&
            !partitionJobSpecExt->has_wire_partition_keys())
        {
            // To properly load from old snapshots.
            InitTemplatePartitionKeys(partitionJobSpecExt);
        }
    }

private:
    TSortOperationSpecBasePtr Spec;

protected:
    TSortOperationOptionsBasePtr Options;

    TSchedulerConfigPtr Config;

    // Counters.
    int CompletedPartitionCount;
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

        TPartition(
            TSortControllerBase* controller,
            int index,
            TKey key = TKey())
            : Index(index)
            , Key(key)
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

        //! Starting key of this partition.
        //! Always null for map-reduce operation.
        TKey Key;

        //! Is partition completed?
        bool Completed;

        //! Do we need to run merge tasks for this partition?
        //! Cached value, updated by #IsSortedMergeNeeded.
        bool CachedSortedMergeNeeded;

        //! Does the partition consist of rows with the same key?
        bool Maniac;

        //! Number of sorted bytes residing at a given host.
        yhash<TNodeId, i64> NodeIdToLocality;

        //! The node assigned to this partition, #InvalidNodeId if none.
        NNodeTrackerClient::TNodeId AssignedNodeId = NNodeTrackerClient::InvalidNodeId;

        // Tasks.
        TSortTaskPtr SortTask;
        TSortedMergeTaskPtr SortedMergeTask;
        TUnorderedMergeTaskPtr UnorderedMergeTask;

        // Chunk pool output obtained from the shuffle pool.
        IChunkPoolOutput* ChunkPoolOutput;


        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Index);
            Persist(context, Key);

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

    std::unique_ptr<IChunkPool> PartitionPool;
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

        TPartitionTask(TSortControllerBase* controller)
            : TTask(controller)
            , Controller(controller)
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

        virtual TExtendedJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto result = Controller->GetPartitionResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return Controller->PartitionPool.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return Controller->PartitionPool.get();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, NodeIdToAdjustedDataSize);
            Persist(context, AdjustedScheduledDataSize);
            Persist(context, MaxDataSizePerJob);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTask, 0x63a4c761);

        TSortControllerBase* Controller;

        //! The total data size of jobs assigned to a particular node
        //! All data sizes are IO weight-adjusted.
        //! No zero values are allowed.
        yhash<TNodeId, i64> NodeIdToAdjustedDataSize;
        //! The sum of all sizes appearing in #NodeIdToDataSize.
        //! This value is IO weight-adjusted.
        i64 AdjustedScheduledDataSize = 0;
        //! Max-aggregated each time a new job is scheduled.
        //! This value is not IO weight-adjusted.
        i64 MaxDataSizePerJob = 0;


        void UpdateNodeDataSize(const TJobNodeDescriptor& descriptor, i64 delta)
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing) {
                return;
            }

            auto ioWeight = descriptor.IOWeight;
            Y_ASSERT(ioWeight > 0);
            auto adjustedDelta = static_cast<i64>(delta / ioWeight);

            auto nodeId = descriptor.Id;
            auto newAdjustedDataSize = (NodeIdToAdjustedDataSize[nodeId] += adjustedDelta);
            YCHECK(newAdjustedDataSize >= 0);

            if (newAdjustedDataSize == 0) {
                YCHECK(NodeIdToAdjustedDataSize.erase(nodeId) == 1);
            }

            YCHECK((AdjustedScheduledDataSize += adjustedDelta) >= 0);
        }


        virtual bool CanScheduleJob(
            ISchedulingContext* context,
            const TJobResources& /*jobLimits*/) override
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing) {
                return true;
            }

            auto ioWeight = context->GetNodeDescriptor().IOWeight;
            if (ioWeight == 0) {
                return false;
            }

            if (NodeIdToAdjustedDataSize.empty()) {
                return true;
            }

            // We don't have a job at hand here, let's make a (worst-case) guess.
            auto adjustedJobDataSize = MaxDataSizePerJob / ioWeight;
            auto nodeId = context->GetNodeDescriptor().Id;
            auto newAdjustedScheduledDataSize = AdjustedScheduledDataSize + adjustedJobDataSize;
            auto newAvgAdjustedScheduledDataSize = newAdjustedScheduledDataSize / NodeIdToAdjustedDataSize.size();
            auto newAdjustedNodeDataSize = NodeIdToAdjustedDataSize[nodeId] + adjustedJobDataSize;
            return
                newAdjustedNodeDataSize <=
                newAvgAdjustedScheduledDataSize + Controller->Spec->PartitionedDataBalancingTolerance * adjustedJobDataSize;
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto statistics = Controller->PartitionPool->GetApproximateStripeStatistics();
            auto result = Controller->GetPartitionResources(
                statistics);
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual bool IsIntermediateOutput() const override
        {
            return true;
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetPartitionJobType();
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetPartitionUserJobSpec();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddIntermediateOutputSpec(jobSpec, joblet, TKeyColumns());
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            auto dataSize = joblet->InputStripeList->TotalDataSize;
            MaxDataSizePerJob = std::max(MaxDataSizePerJob, dataSize);
            UpdateNodeDataSize(joblet->NodeDescriptor, +dataSize);

            TTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            auto& result = const_cast<TJobResult&>(jobSummary.Result);
            auto* resultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunk_specs());

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

            // NB: don't move it to OnTaskCompleted since jobs may run after the task has been completed.
            // Kick-start sort and unordered merge tasks.
            Controller->CheckSortStartThreshold();
            Controller->CheckMergeStartThreshold();
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            UpdateNodeDataSize(completedJob->NodeDescriptor, -completedJob->DataSize);

            if (!Controller->IsShuffleCompleted()) {
                // Add pending hint if shuffle is in progress and some partition jobs were lost.
                Controller->AddTaskPendingHint(this);
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            TTask::OnJobFailed(joblet, jobSummary);

            UpdateNodeDataSize(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataSize);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);

            UpdateNodeDataSize(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataSize);
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

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
                auto nodeDescriptors = Controller->GetExecNodeDescriptors();
                yhash<TNodeId, TExecNodeDescriptor> idToNodeDescriptor;
                for (const auto& descriptor : nodeDescriptors) {
                    YCHECK(idToNodeDescriptor.insert(std::make_pair(descriptor.Id, descriptor)).second);
                }

                LOG_DEBUG("Per-node partitioned sizes collected");
                for (const auto& pair : NodeIdToAdjustedDataSize) {
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
            Controller->CheckSortStartThreshold();
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

        virtual void Persist(const TPersistenceContext& context) override
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

        virtual TExtendedJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto result = GetNeededResourcesForChunkStripe(
                joblet->InputStripeList->GetAggregateStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
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

        virtual void Persist(const TPersistenceContext& context) override
        {
            TPartitionBoundTask::Persist(context);

            using NYT::Persist;
            Persist(context, CurrentInputStreamIndex_);
        }

    protected:
        TExtendedJobResources GetNeededResourcesForChunkStripe(
            const TChunkStripeStatistics& stat) const
        {
            if (Controller->SimpleSort) {
                // Value count estimate has been removed, using 0 instead.
                i64 valueCount = 0;
                return Controller->GetSimpleSortResources(
                    stat,
                    valueCount);
            } else {
                return Controller->GetPartitionSortResources(Partition, stat);
            }
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto stat = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            if (Controller->SimpleSort && stat.size() > 1) {
                stat = AggregateStatistics(stat);
            } else {
                YCHECK(stat.size() == 1);
            }
            auto result = GetNeededResourcesForChunkStripe(stat.front());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual bool IsIntermediateOutput() const override
        {
            return Controller->IsSortedMergeNeeded(Partition);
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->IsSortedMergeNeeded(Partition)
                ? Controller->GetIntermediateSortJobType()
                : Controller->GetFinalSortJobType();
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
                int inputStreamIndex = CurrentInputStreamIndex_++;
                Controller->IntermediateSortJobCounter.Completed(1);

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto& result = const_cast<TJobResult&>(jobSummary.Result);
                auto* resultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunk_specs());

                for (const auto& dataSlice : stripe->DataSlices) {
                    InferLimitsFromBoundaryKeys(dataSlice, Controller->RowBuffer);
                    dataSlice->InputStreamIndex = inputStreamIndex;
                }

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
            }

            TTask::OnJobAborted(joblet, jobSummary);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            Controller->IntermediateSortJobCounter.Lost(1);
            auto stripeList = completedJob->SourceTask->GetChunkPoolOutput()->GetStripeList(completedJob->OutputCookie);
            Controller->SortDataSizeCounter.Lost(stripeList->TotalDataSize);

            TTask::OnJobLost(completedJob);

            if (!Partition->Completed && Controller->PartitionTask) {
                Controller->AddTaskPendingHint(this);
                Controller->AddTaskPendingHint(Controller->PartitionTask);
            }
        }

        virtual void OnTaskCompleted() override
        {
            TPartitionBoundTask::OnTaskCompleted();

            // Kick-start the corresponding merge task.
            if (Controller->IsSortedMergeNeeded(Partition)) {
                Partition->SortedMergeTask->FinishInput();
            }
        }

    private:
        int CurrentInputStreamIndex_ = 0;
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
            return Partition->AssignedNodeId == InvalidNodeId
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

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetPartitionSortUserJobSpec(Partition);
        }

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

    protected:
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
            , ChunkPool_(controller->CreateSortedMergeChunkPool())
        {
            SetupCallbacks();
        }

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

        virtual i64 GetLocality(TNodeId nodeId) const override
        {
            return Partition->AssignedNodeId == nodeId || Partition->AssignedNodeId == InvalidNodeId;
        }

        virtual TExtendedJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto result = Controller->GetSortedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool_.get();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TMergeTask::Persist(context);

            using NYT::Persist;
            Persist(context, ChunkPool_);
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, ActiveJoblets_);
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, InvalidatedJoblets_);
            Persist(context, JobOutputs_);
            Persist(context, Finished_);
            Persist(context, FrozenTotalJobCount_);

            if (context.IsLoad()) {
                SetupCallbacks();
            }
        }

        i64 GetOutputRowCount()
        {
            i64 outputRowCount = 0;
            for (auto& jobOutput : JobOutputs_) {
                outputRowCount += GetTotalOutputDataStatistics(jobOutput.JobSummary.Statistics).row_count();
            }
            return outputRowCount;
        }

        virtual int GetPendingJobCount() const override
        {
            return Finished_ ? 0 : TPartitionBoundTask::GetPendingJobCount();
        }

        virtual int GetTotalJobCount() const override
        {
            return Finished_ ? FrozenTotalJobCount_ : TPartitionBoundTask::GetTotalJobCount();
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeTask, 0x4ab19c75);

        std::unique_ptr<IChunkPool> ChunkPool_;

        yhash_set<TJobletPtr> ActiveJoblets_;
        yhash_set<TJobletPtr> InvalidatedJoblets_;
        bool Finished_ = false;
        //! This is a dirty hack to make GetTotalJobCount() work correctly
        //! in case when chunk pool was invalidated after the task has been completed.
        //! We want to "freeze" the total job count and the pending job count at the values
        //! by that moment. For pending job count it should be equal to 0, while for total
        //! job count we have to remember the exact value.
        int FrozenTotalJobCount_ = 0;

        struct TJobOutput
        {
            std::vector<NChunkClient::TChunkListId> ChunkListIds;
            int PartitionIndex;
            TCompletedJobSummary JobSummary;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;

                Persist(context, ChunkListIds);
                Persist(context, PartitionIndex);
                Persist(context, JobSummary);
            }
        };
        std::vector<TJobOutput> JobOutputs_;

        void SetupCallbacks()
        {
            ChunkPool_->SubscribePoolOutputInvalidated(BIND(&TSortedMergeTask::AbortAllActiveJoblets, MakeWeak(this)));
        }

        void AbortAllActiveJoblets(const TError& error)
        {
            if (Finished_) {
                LOG_WARNING(error, "Pool output has been invalidated, but the task has already finished (Task: %v)", GetId());
                return;
            }
            LOG_WARNING(error, "Aborting all jobs in task because of pool output invalidation (Task: %v)", GetId());
            for (const auto& joblet : ActiveJoblets_) {
                Controller->Host->GetJobHost(joblet->JobId)->AbortJob(
                    TError("Job is aborted due to chunk pool output invalidation")
                        << error);
                InvalidatedJoblets_.insert(joblet);
            }
            JobOutputs_.clear();
        }

        void RegisterAllOutputs()
        {
            for (auto& jobOutput : JobOutputs_) {
                Controller->RegisterOutput(jobOutput.ChunkListIds, jobOutput.PartitionIndex, jobOutput.JobSummary);
            }
        }

        virtual bool IsActive() const override
        {
            return Controller->MergeStartThresholdReached && !Partition->Maniac;
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller->GetSortedMergeResources(
                ChunkPool_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool_.get();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetSortedMergeJobType();
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetSortedMergeUserJobSpec();
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
            YCHECK(ActiveJoblets_.insert(joblet).second);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobCompleted(joblet, jobSummary);

            Controller->SortedMergeJobCounter.Completed(1);
            YCHECK(ActiveJoblets_.erase(joblet) == 1);
            if (!InvalidatedJoblets_.has(joblet)) {
                JobOutputs_.emplace_back(TJobOutput{joblet->ChunkListIds, Partition->Index, jobSummary});
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter.Failed(1);

            TMergeTask::OnJobFailed(joblet, jobSummary);
            YCHECK(ActiveJoblets_.erase(joblet) == 1);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter.Aborted(1, jobSummary.AbortReason);

            TMergeTask::OnJobAborted(joblet, jobSummary);
            YCHECK(ActiveJoblets_.erase(joblet) == 1);
        }

        virtual void OnTaskCompleted() override
        {
            YCHECK(!Finished_);
            TMergeTask::OnTaskCompleted();

            RegisterAllOutputs();
            FrozenTotalJobCount_ = TPartitionBoundTask::GetTotalJobCount();
            Finished_ = true;
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

        virtual TExtendedJobResources GetNeededResources(TJobletPtr joblet) const override
        {
            auto result = Controller->GetUnorderedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
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

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller->GetUnorderedMergeResources(
                Partition->ChunkPoolOutput->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType::UnorderedMerge;
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

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const
    {
        return nullptr;
    }

    virtual TUserJobSpecPtr GetPartitionSortUserJobSpec(TPartitionPtr partition) const
    {
        return nullptr;
    }

    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        // NB: Register groups in the order of _descending_ priority.
        MergeTaskGroup = New<TTaskGroup>();
        MergeTaskGroup->MinNeededResources.SetCpu(GetMergeCpuLimit());
        RegisterTaskGroup(MergeTaskGroup);

        SortTaskGroup = New<TTaskGroup>();
        SortTaskGroup->MinNeededResources.SetCpu(GetSortCpuLimit());
        SortTaskGroup->MinNeededResources.SetNetwork(Spec->ShuffleNetworkLimit);
        RegisterTaskGroup(SortTaskGroup);

        PartitionTaskGroup = New<TTaskGroup>();
        PartitionTaskGroup->MinNeededResources.SetCpu(GetPartitionCpuLimit());
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

        if (nodeHeap.empty()) {
            LOG_DEBUG("No alive exec nodes to assign partitions");
            return;
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

    void InitPartitionPool(IJobSizeConstraintsPtr jobSizeConstraints, TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig)
    {
        PartitionPool = CreateUnorderedChunkPool(
            std::move(jobSizeConstraints),
            std::move(jobSizeAdjusterConfig));
    }

    void InitShufflePool()
    {
        ShufflePool = CreateShuffleChunkPool(
            static_cast<int>(Partitions.size()),
            Spec->DataSizePerShuffleJob);

        for (auto partition : Partitions) {
            partition->ChunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        }
    }

    void InitSimpleSortPool(IJobSizeConstraintsPtr jobSizeConstraints)
    {
        SimpleSortPool = CreateUnorderedChunkPool(
            std::move(jobSizeConstraints),
            nullptr);
    }

    virtual bool IsCompleted() const override
    {
        return CompletedPartitionCount == Partitions.size();
    }

    virtual void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables();

            if (IsRowCountPreserved() && !(SimpleSort && isNontrivialInput)) {
                // We don't check row count for simple sort if nontrivial read limits are specified,
                // since input row count can be estimated inaccurate.
                i64 totalInputRowCount = 0;
                for (auto partition : Partitions) {
                    i64 inputRowCount = partition->ChunkPoolOutput->GetTotalRowCount();
                    totalInputRowCount += inputRowCount;
                    if (IsSortedMergeNeeded(partition)) {
                        i64 outputRowCount = partition->SortedMergeTask->GetOutputRowCount();
                        if (inputRowCount != outputRowCount) {
                            LOG_DEBUG("Input/output row count mismatch in sorted merge task "
                                "(Task: %v, InputRowCount: %v, OutputRowCount: %v)",
                                partition->SortedMergeTask->GetId(),
                                inputRowCount,
                                outputRowCount);
                        }
                    }
                }
                LOG_ERROR_IF(totalInputRowCount != TotalOutputRowCount,
                    "Input/output row count mismatch in sort operation (TotalInputRowCount: %v, TotalOutputRowCount: %v)",
                    totalInputRowCount,
                    TotalOutputRowCount);
                YCHECK(totalInputRowCount == TotalOutputRowCount);
            }

            YCHECK(CompletedPartitionCount == Partitions.size());
        }

        TOperationControllerBase::OnOperationCompleted(interrupted);
    }

    void OnPartitionCompleted(TPartitionPtr partition)
    {
        YCHECK(!partition->Completed);
        partition->Completed = true;

        ++CompletedPartitionCount;

        LOG_DEBUG("Partition completed (Partition: %v)", partition->Index);
    }

    virtual bool IsSortedMergeNeeded(TPartitionPtr partition) const
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
        partition->CachedSortedMergeNeeded = true;
        return true;
    }

    void CheckSortStartThreshold()
    {
        if (!SortStartThresholdReached) {
            if (!SimpleSort && PartitionTask->GetCompletedDataSize() < PartitionTask->GetTotalDataSize() * Spec->ShuffleStartThreshold)
                return;

            LOG_INFO("Sort start threshold reached");

            SortStartThresholdReached = true;
        }

        AddSortTasksPendingHints();
    }

    bool IsShuffleCompleted() const
    {
        for (const auto& partition : Partitions) {
            if (partition->Completed) {
                continue;
            }

            const auto& task = partition->Maniac
                ? static_cast<TTaskPtr>(partition->UnorderedMergeTask)
                : static_cast<TTaskPtr>(partition->SortTask);

            if (!task->IsCompleted()) {
              return false;
            }
        }

        return true;
    }

    int AdjustPartitionCountToWriterBufferSize(
        int partitionCount,
        TChunkWriterConfigPtr config) const
    {
        i64 dataSizeAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataSize * Spec->MapSelectivityFactor);
        i64 bufferSize = std::min(config->MaxBufferSize, dataSizeAfterPartition);
        i64 partitionBufferSize = bufferSize / partitionCount;
        if (partitionBufferSize < Options->MinUncompressedBlockSize) {
            return std::max(bufferSize / Options->MinUncompressedBlockSize, (i64)1);
        } else {
            return partitionCount;
        }
    }

    void CheckMergeStartThreshold()
    {
        if (!MergeStartThresholdReached) {
            if (!SimpleSort) {
                if (!PartitionTask->IsCompleted())
                    return;
                if (SortDataSizeCounter.GetCompletedTotal() < SortDataSizeCounter.GetTotal() * Spec->MergeStartThreshold)
                    return;
            }

            LOG_INFO("Merge start threshold reached");

            MergeStartThresholdReached = true;
        }

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

    virtual TCpuResource GetPartitionCpuLimit() const = 0;
    virtual TCpuResource GetSortCpuLimit() const = 0;
    virtual TCpuResource GetMergeCpuLimit() const = 0;

    virtual TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics) const = 0;

    virtual TExtendedJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const = 0;

    virtual TExtendedJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat) const = 0;

    virtual TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const = 0;

    virtual TExtendedJobResources GetUnorderedMergeResources(
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

    // Returns compression ratio of input data.
    double GetCompressionRatio() const
    {
        return static_cast<double>(TotalEstimatedCompressedDataSize) / TotalEstimatedInputDataSize;
    }

    void InitTemplatePartitionKeys(TPartitionJobSpecExt* partitionJobSpecExt)
    {
        auto keySetWriter = New<TKeySetWriter>();
        for (const auto& partition : Partitions) {
            auto key = partition->Key;
            if (key && key != MinKey()) {
                keySetWriter->WriteKey(key);
            }
        }
        auto data = keySetWriter->Finish();
        partitionJobSpecExt->set_wire_partition_keys(ToString(data));

        // COMPAT(psushin).
        // NB: This dummy key guards us from old nodes and silent sort corruptions.
        partitionJobSpecExt->add_partition_keys("not_a_key");
    }

    i64 GetMaxPartitionJobBufferSize() const
    {
        return Spec->PartitionJobIO->TableWriter->MaxBufferSize;
    }

    int SuggestPartitionCount() const
    {
        YCHECK(TotalEstimatedInputDataSize > 0);
        i64 dataSizeAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataSize * Spec->MapSelectivityFactor);
        // Use int64 during the initial stage to avoid overflow issues.
        i64 result;
        if (Spec->PartitionCount) {
            result = *Spec->PartitionCount;
        } else if (Spec->PartitionDataSize) {
            result = 1 + dataSizeAfterPartition / *Spec->PartitionDataSize;
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options->CompressedBlockSize / GetCompressionRatio());
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec->PartitionJobIO->TableWriter->BlockSize);

            // Product may not fit into i64.
            double partitionDataSize = sqrt(dataSizeAfterPartition) * sqrt(uncompressedBlockSize);
            partitionDataSize = std::max(partitionDataSize, static_cast<double>(Options->MinPartitionSize));

            i64 maxPartitionCount = GetMaxPartitionJobBufferSize() / uncompressedBlockSize;
            result = std::min(static_cast<i64>(dataSizeAfterPartition / partitionDataSize), maxPartitionCount);
        }
        // Cast to int32 is safe since MaxPartitionCount is int32.
        return static_cast<int>(Clamp<i64>(result, 1, Options->MaxPartitionCount));
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

    const TProgressCounter& GetPartitionJobCounter() const
    {
        if (PartitionPool) {
            return PartitionPool->GetJobCounter();
        }
        return NullProgressCounter;
    }

    // Partition sizes histogram.

    std::unique_ptr<IHistogram> ComputePartitionSizeHistogram() const
    {
        auto histogram = CreateHistogram();
        for (auto partition : Partitions) {
            i64 size = partition->ChunkPoolOutput->GetTotalDataSize();
            if (size != 0) {
                histogram->AddValue(size);
            }
        }
        histogram->BuildHistogramView();
        return histogram;
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
            .Item("partition_size_histogram").Value(*sizeHistogram);
    }

    virtual void RegisterOutput(TJobletPtr joblet, int key, const TCompletedJobSummary& jobSummary) override
    {
        TOperationControllerBase::RegisterOutput(std::move(joblet), key, jobSummary);
    }

    virtual void RegisterOutput(
        const std::vector<TChunkListId>& chunkListIds,
        int key,
        const TCompletedJobSummary& jobSummary) override
    {
        TotalOutputRowCount += GetTotalOutputDataStatistics(jobSummary.Statistics).row_count();
        TOperationControllerBase::RegisterOutput(chunkListIds, key, jobSummary);
    }

    void InitJobIOConfigs()
    {
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        InitIntermediateOutputConfig(PartitionJobIOConfig);
    }

    virtual void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        auto user = AuthenticatedUser;
        auto account = Spec->IntermediateDataAccount;

        const auto& client = Host->GetMasterClient();
        auto asyncResult = client->CheckPermission(
            user,
            "//sys/accounts/" + account,
            EPermission::Use);
        auto result = WaitFor(asyncResult)
            .ValueOrThrow();

        if (result.Action == ESecurityAction::Deny) {
            THROW_ERROR_EXCEPTION("User %Qv has been denied access to intermediate account %Qv",
                user,
                account);
        }

        for (const auto& table : InputTables) {
            for (const auto& name : Spec->SortBy) {
                if (auto column = table.Schema.FindColumn(name)) {
                    if (column->Aggregate) {
                        THROW_ERROR_EXCEPTION("Sort by aggreate column is not alowed")
                            << TErrorAttribute("table_path", table.Path.GetPath())
                            << TErrorAttribute("column_name", name);
                    }
                }
            }
        }
    }

    std::unique_ptr<IChunkPool> CreateSortedMergeChunkPool()
    {
        if (Spec->UseLegacyController) {
            return CreateAtomicChunkPool();
        } else {
            TSortedChunkPoolOptions chunkPoolOptions;
            TSortedJobOptions jobOptions;
            jobOptions.EnableKeyGuarantee = GetSortedMergeJobType() == EJobType::SortedReduce;
            jobOptions.PrimaryPrefixLength = GetSortedMergeKeyColumnCount();
            jobOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;
            // NB: otherwise we could easily be persisted during preparing the jobs. Sorted chunk pool
            // can't handle this.
            jobOptions.EnablePeriodicYielder = false;
            chunkPoolOptions.OperationId = OperationId;
            chunkPoolOptions.SortedJobOptions = jobOptions;
            chunkPoolOptions.JobSizeConstraints = CreatePartitionBoundSortedJobSizeConstraints(
                Spec,
                Options,
                GetOutputTablePaths().size());
            return CreateSortedChunkPool(chunkPoolOptions, nullptr /* chunkSliceFetcher */, IntermediateInputStreamDirectory);
        }
    }

    virtual EJobType GetPartitionJobType() const = 0;
    virtual EJobType GetIntermediateSortJobType() const = 0;
    virtual EJobType GetFinalSortJobType() const = 0;
    virtual EJobType GetSortedMergeJobType() const = 0;

    virtual TUserJobSpecPtr GetSortedMergeUserJobSpec() const = 0;

    virtual int GetSortedMergeKeyColumnCount() const = 0;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TPartitionTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TPartitionSortTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TSimpleSortTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TSortedMergeTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TUnorderedMergeTask);

////////////////////////////////////////////////////////////////////////////////

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
    {
        RegisterJobProxyMemoryDigest(EJobType::Partition, spec->PartitionJobProxyMemoryDigest);
        RegisterJobProxyMemoryDigest(EJobType::SimpleSort, spec->SortJobProxyMemoryDigest);
        RegisterJobProxyMemoryDigest(EJobType::IntermediateSort, spec->SortJobProxyMemoryDigest);
        RegisterJobProxyMemoryDigest(EJobType::FinalSort, spec->SortJobProxyMemoryDigest);
        RegisterJobProxyMemoryDigest(EJobType::SortedMerge, spec->MergeJobProxyMemoryDigest);
        RegisterJobProxyMemoryDigest(EJobType::UnorderedMerge, spec->MergeJobProxyMemoryDigest);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortController, 0xbca37afe);

    TSortOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.

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

    virtual void PrepareOutputTables() override
    {
        auto& table = OutputTables[0];
        table.TableUploadOptions.LockMode = ELockMode::Exclusive;
        table.Options->EvaluateComputedColumns = false;

        // Sort output MUST be sorted.
        table.Options->ExplodeOnValidationError = true;

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(Spec->SortBy);
                } else {
                    table.TableUploadOptions.TableSchema =
                        table.TableUploadOptions.TableSchema.ToSorted(Spec->SortBy);

                    for (const auto& inputTable : InputTables) {
                        if (inputTable.SchemaMode == ETableSchemaMode::Strong) {
                            ValidateTableSchemaCompatibility(
                                inputTable.Schema,
                                table.TableUploadOptions.TableSchema,
                                /* ignoreSortOrder */ true)
                            .ThrowOnError();
                        }
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(Spec->SortBy);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table.TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(Spec->SortBy);
                } else {
                    table.TableUploadOptions.TableSchema =
                        table.TableUploadOptions.TableSchema.ToSorted(Spec->SortBy);
                }
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        if (TotalEstimatedInputDataSize == 0)
            return;

        TSamplesFetcherPtr samplesFetcher;

        TFuture<void> asyncSamplesResult;
        PROFILE_TIMING ("/input_processing_time") {
            int sampleCount = SuggestPartitionCount() * Spec->SamplesPerPartition;

            TScrapeChunksCallback scraperCallback;
            if (Spec->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
                scraperCallback = CreateScrapeChunksSessionCallback(
                    Config->ChunkScraper,
                    GetCancelableInvoker(),
                    Host->GetChunkLocationThrottlerManager(),
                    AuthenticatedInputMasterClient,
                    InputNodeDirectory,
                    Logger);
            }

            auto samplesRowBuffer = New<TRowBuffer>(
                TRowBufferTag(),
                Config->ControllerRowBufferChunkSize);

            samplesFetcher = New<TSamplesFetcher>(
                Config->Fetcher,
                ESamplingPolicy::Sorting,
                sampleCount,
                Spec->SortBy,
                Options->MaxSampleSize,
                InputNodeDirectory,
                GetCancelableInvoker(),
                samplesRowBuffer,
                scraperCallback,
                Host->GetMasterClient(),
                Logger);

            for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
                samplesFetcher->AddChunk(chunk);
            }
            for (const auto& chunk : CollectPrimaryVersionedChunks()) {
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
        LOG_INFO("Suggested partition count %v, samples count %v", partitionCount, sortedSamples.size());

        // Don't create more partitions than we have samples (plus one).
        partitionCount = std::min(partitionCount, static_cast<int>(sortedSamples.size()) + 1);

        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount,
            PartitionJobIOConfig->TableWriter);
        LOG_INFO("Adjusted partition count %v", partitionCount);

        YCHECK(partitionCount > 0);
        SimpleSort = (partitionCount == 1);

        if (SimpleSort) {
            BuildSinglePartition();
        } else {
            // Finally adjust partition count wrt block size constraints.
            partitionCount = AdjustPartitionCountToWriterBufferSize(
                partitionCount,
                PartitionJobIOConfig->TableWriter);

            LOG_INFO("Adjusted partition count %v", partitionCount);

            BuildMulitplePartitions(sortedSamples, partitionCount);
        }
    }

    void BuildSinglePartition()
    {
        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec,
            Options,
            TotalEstimatedInputDataSize);

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(jobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(jobSizeConstraints, &stripes);

        // Create the fake partition.
        InitSimpleSortPool(jobSizeConstraints);
        auto partition = New<TPartition>(this, 0);
        Partitions.push_back(partition);
        partition->ChunkPoolOutput = SimpleSortPool.get();
        partition->SortTask->AddInput(stripes);
        partition->SortTask->FinishInput();

        // NB: Cannot use TotalEstimatedInputDataSize due to slicing and rounding issues.
        SortDataSizeCounter.Set(SimpleSortPool->GetTotalDataSize());

        LOG_INFO("Sorting without partitioning (SortJobCount: %v, DataSizePerJob: %v)",
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataSizePerJob());

        // Kick-start the sort task.
        SortStartThresholdReached = true;
    }

    void AddPartition(TKey key)
    {
        int index = static_cast<int>(Partitions.size());
        LOG_DEBUG("Partition %v has starting key %v",
            index,
            key);

        YCHECK(CompareRows(Partitions.back()->Key, key) < 0);
        Partitions.push_back(New<TPartition>(this, index, key));
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
        Partitions.push_back(New<TPartition>(this, 0, MinKey()));

        // Invariant:
        //   lastPartition = Partitions.back()
        //   lastKey = Partition.back()->Key
        //   lastPartition receives keys in [lastKey, ...)
        //
        // Initially Partitions consists of the leftmost partition are empty so lastKey is assumed to be -inf.

        int sampleIndex = 0;
        while (sampleIndex < selectedSamples.size()) {
            auto* sample = selectedSamples[sampleIndex];
            // Check for same keys.
            if (CompareRows(sample->Key, Partitions.back()->Key) != 0) {
                AddPartition(RowBuffer->Capture(sample->Key));
                ++sampleIndex;
            } else {
                // Skip same keys.
                int skippedCount = 0;
                while (sampleIndex < selectedSamples.size() &&
                    CompareRows(selectedSamples[sampleIndex]->Key, Partitions.back()->Key) == 0)
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
                    auto successorKey = GetKeyPrefixSuccessor(sample->Key, Spec->SortBy.size(), RowBuffer);
                    AddPartition(successorKey);
                } else {
                    // If sample keys are incomplete, we cannot use UnorderedMerge,
                    // because full keys may be different.
                    LOG_DEBUG("Partition %v is oversized, skipped %v samples",
                        lastPartition->Index,
                        skippedCount);
                    AddPartition(RowBuffer->Capture(selectedSamples[sampleIndex]->Key));
                    ++sampleIndex;
                }
            }
        }

        InitShufflePool();

        auto jobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            TotalEstimatedInputDataSize,
            TotalEstimatedInputRowCount,
            GetCompressionRatio());

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(jobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(jobSizeConstraints, &stripes);

        InitPartitionPool(jobSizeConstraints, nullptr);

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();
        RegisterTask(PartitionTask);

        LOG_INFO("Sorting with partitioning (PartitionCount: %v, PartitionJobCount: %v, DataSizePerPartitionJob: %v)",
            partitionCount,
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataSizePerJob());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        // Final sort: reader like sort and output like merge.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        FinalSortJobIOConfig->TableWriter = CloneYsonSerializable(Spec->MergeJobIO->TableWriter);
        InitFinalOutputConfig(FinalSortJobIOConfig);

        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        InitFinalOutputConfig(SortedMergeJobIOConfig);

        UnorderedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        // Since we're reading from huge number of paritition chunks, we must use larger buffers,
        // as we do for sort jobs.
        UnorderedMergeJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);
        InitFinalOutputConfig(UnorderedMergeJobIOConfig);
    }

    virtual EJobType GetIntermediateSortJobType() const override
    {
        return SimpleSort ? EJobType::SimpleSort : EJobType::IntermediateSort;
    }

    virtual EJobType GetFinalSortJobType() const override
    {
        return SimpleSort ? EJobType::SimpleSort : EJobType::FinalSort;
    }

    virtual EJobType GetSortedMergeJobType() const override
    {
        return EJobType::SortedMerge;
    }

    virtual TUserJobSpecPtr GetSortedMergeUserJobSpec() const override
    {
        return nullptr;
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(static_cast<int>(EJobType::Partition));
            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).GetData());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_partition_count(Partitions.size());
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), Spec->SortBy);

            InitTemplatePartitionKeys(partitionJobSpecExt);
        }

        auto intermediateReaderOptions = New<TTableReaderOptions>();

        TJobSpec sortJobSpecTemplate;
        {
            auto* schedulerJobSpecExt = sortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());

            if (SimpleSort) {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).GetData());
                ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());
            } else {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
                ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());
            }

            auto* sortJobSpecExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(sortJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }

        {
            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_type(static_cast<int>(GetIntermediateSortJobType()));
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).GetData());
        }

        {
            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_type(static_cast<int>(GetFinalSortJobType()));
            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).GetData());
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedMerge));
            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).GetData());

            ToProto(mergeJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
            auto* schedulerJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(UnorderedMergeJobIOConfig).GetData());

            ToProto(mergeJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }
    }


    // Resource management.

    virtual TCpuResource GetPartitionCpuLimit() const override
    {
        return 1;
    }

    virtual TCpuResource GetSortCpuLimit() const override
    {
        return 1;
    }

    virtual TCpuResource GetMergeCpuLimit() const override
    {
        return 1;
    }

    virtual TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 outputBufferSize = std::min(
            PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            stat.DataSize);

        outputBufferSize += THorizontalSchemalessBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());

        outputBufferSize = std::min(
            outputBufferSize,
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetPartitionCpuLimit());
        result.SetJobProxyMemory(GetInputIOMemorySize(PartitionJobIOConfig, stat)
            + outputBufferSize
            + GetOutputWindowMemorySize(PartitionJobIOConfig));
        return result;
    }

    virtual TExtendedJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const override
    {
        // ToDo(psushin): rewrite simple sort estimates.
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());
        result.SetJobProxyMemory(GetSortInputIOMemorySize(stat) +
            GetFinalOutputIOMemorySize(FinalSortJobIOConfig) +
            GetSortBuffersMemorySize(stat) +
            // TODO(babenko): *2 are due to lack of reserve, remove this once simple sort
            // starts reserving arrays of appropriate sizes.
            (i64) 32 * valueCount * 2);
        return result;
    }

    virtual TExtendedJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat) const override
    {
        i64 jobProxyMemory =
            GetSortBuffersMemorySize(stat) +
            GetSortInputIOMemorySize(stat);

        if (IsSortedMergeNeeded(partition)) {
            jobProxyMemory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
        } else {
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig);
        }

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());
        result.SetJobProxyMemory(jobProxyMemory);
        result.SetNetwork(Spec->ShuffleNetworkLimit);
        return result;
    }

    virtual TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(SortedMergeJobIOConfig, statistics));
        return result;
    }

    virtual bool IsRowCountPreserved() const override
    {
        return true;
    }

    virtual TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(UnorderedMergeJobIOConfig, AggregateStatistics(statistics)));
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
            JobCounter.GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAbortedTotal(),
            JobCounter.GetLost(),
            // Partitions
            Partitions.size(),
            CompletedPartitionCount,
            // PartitionJobs
            GetPartitionJobCounter(),
            // IntermediateSortJobs
            IntermediateSortJobCounter,
            // FinalSortJobs
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
            .Item("partition_jobs").Value(GetPartitionJobCounter())
            .Item("intermediate_sort_jobs").Value(IntermediateSortJobCounter)
            .Item("final_sort_jobs").Value(FinalSortJobCounter)
            .Item("sorted_merge_jobs").Value(SortedMergeJobCounter)
            .Item("unordered_merge_jobs").Value(UnorderedMergeJobCounter);
    }

    virtual EJobType GetPartitionJobType() const override
    {
        return EJobType::Partition;
    }

    virtual int GetSortedMergeKeyColumnCount() const override
    {
        return Spec->SortBy.size();
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

////////////////////////////////////////////////////////////////////////////////

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
    {
        if (spec->Mapper) {
            RegisterJobProxyMemoryDigest(EJobType::PartitionMap, spec->PartitionJobProxyMemoryDigest);
            RegisterUserJobMemoryDigest(EJobType::PartitionMap, spec->Mapper->MemoryReserveFactor);
        } else {
            RegisterJobProxyMemoryDigest(EJobType::Partition, spec->PartitionJobProxyMemoryDigest);
        }

        if (spec->ReduceCombiner) {
            RegisterJobProxyMemoryDigest(EJobType::ReduceCombiner, spec->ReduceCombinerJobProxyMemoryDigest);
            RegisterUserJobMemoryDigest(EJobType::ReduceCombiner, spec->ReduceCombiner->MemoryReserveFactor);
        } else {
            RegisterJobProxyMemoryDigest(EJobType::IntermediateSort, spec->SortJobProxyMemoryDigest);
        }

        RegisterJobProxyMemoryDigest(EJobType::SortedReduce, spec->SortedReduceJobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::SortedReduce, spec->Reducer->MemoryReserveFactor);

        RegisterJobProxyMemoryDigest(EJobType::PartitionReduce, spec->PartitionReduceJobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::PartitionReduce, spec->Reducer->MemoryReserveFactor);
    }

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

        LOG_DEBUG("ReduceColumns: %v, SortColumns: %v",
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

    virtual TNullable<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriterConfig;
    }

    virtual TNullable<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriterConfig;
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
                    Y_UNREACHABLE();
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
        LOG_INFO("Suggested partition count %v", partitionCount);

        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount,
            PartitionJobIOConfig->TableWriter);
        LOG_INFO("Adjusted partition count %v", partitionCount);

        BuildMultiplePartitions(partitionCount);
    }

    void BuildMultiplePartitions(int partitionCount)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }

        InitShufflePool();

        auto jobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            TotalEstimatedInputDataSize,
            TotalEstimatedInputRowCount,
            GetCompressionRatio());

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(jobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(jobSizeConstraints, &stripes);

        InitPartitionPool(jobSizeConstraints, Config->EnablePartitionMapJobSizeAdjustment
            ? Options->PartitionJobSizeAdjuster
            : nullptr);

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();
        RegisterTask(PartitionTask);

        LOG_INFO("Map-reducing with partitioning (PartitionCount: %v, PartitionJobCount: %v, PartitionDataSizePerJob: %v)",
            partitionCount,
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataSizePerJob());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        // This is not a typo!
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        InitIntermediateOutputConfig(PartitionJobIOConfig);

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        FinalSortJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);
        InitFinalOutputConfig(FinalSortJobIOConfig);

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        InitFinalOutputConfig(SortedMergeJobIOConfig);
    }

    virtual EJobType GetPartitionJobType() const override
    {
        return Spec->Mapper ? EJobType::PartitionMap : EJobType::Partition;
    }

    virtual EJobType GetIntermediateSortJobType() const override
    {
        return Spec->ReduceCombiner ? EJobType::ReduceCombiner : EJobType::IntermediateSort;
    }

    virtual EJobType GetFinalSortJobType() const override
    {
        return EJobType::PartitionReduce;
    }

    virtual EJobType GetSortedMergeJobType() const override
    {
        return EJobType::SortedReduce;
    }

    virtual TUserJobSpecPtr GetSortedMergeUserJobSpec() const override
    {
        return Spec->Reducer;
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            ParseInputQuery(*Spec->InputQuery, Spec->InputSchema);
        }
    }

    void InitJobSpecTemplates()
    {
        {
            PartitionJobSpecTemplate.set_type(static_cast<int>(GetPartitionJobType()));

            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), MakeInputDataSources());

            if (Spec->InputQuery) {
                WriteInputQueryToJobSpec(schedulerJobSpecExt);
            }

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);

            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).GetData());

            partitionJobSpecExt->set_partition_count(Partitions.size());
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), Spec->SortBy);

            if (Spec->Mapper) {
                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->Mapper,
                    MapperFiles,
                    Spec->JobNodeAccount);
            }
        }

        auto intermediateReaderOptions = New<TTableReaderOptions>();
        intermediateReaderOptions->AllowFetchingSeedsFromMaster = false;

        {
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).GetData());

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());

            if (Spec->ReduceCombiner) {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::ReduceCombiner));

                auto* reduceJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
                reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->ReduceCombiner,
                    ReduceCombinerFiles,
                    Spec->JobNodeAccount);
            } else {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::IntermediateSort));
                auto* sortJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                ToProto(sortJobSpecExt->mutable_key_columns(), Spec->SortBy);
            }
        }

        {
            FinalSortJobSpecTemplate.set_type(static_cast<int>(EJobType::PartitionReduce));

            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* reduceJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).GetData());

            ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->JobNodeAccount);
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedReduce));

            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* reduceJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            ToProto(schedulerJobSpecExt->mutable_data_source_directory(), CreateIntermediateDataSource());

            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).GetData());

            ToProto(reduceJobSpecExt->mutable_key_columns(), Spec->SortBy);
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->JobNodeAccount);
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
            joblet);
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    virtual bool IsIntermediateLivePreviewSupported() const override
    {
        return true;
    }

    virtual bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    // Resource management.

    virtual TCpuResource GetPartitionCpuLimit() const override
    {
        return Spec->Mapper ? Spec->Mapper->CpuLimit : 1;
    }

    virtual TCpuResource GetSortCpuLimit() const override
    {
        // At least one cpu, may be more in PartitionReduce job.
        return 1;
    }

    virtual TCpuResource GetMergeCpuLimit() const override
    {
        return Spec->Reducer->CpuLimit;
    }

    virtual TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 reserveSize = THorizontalSchemalessBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        if (Spec->Mapper) {
            result.SetCpu(Spec->Mapper->CpuLimit);
            result.SetJobProxyMemory(
                GetInputIOMemorySize(PartitionJobIOConfig, stat) +
                GetOutputWindowMemorySize(PartitionJobIOConfig) +
                bufferSize);
        } else {
            result.SetCpu(1);
            bufferSize = std::min(bufferSize, stat.DataSize + reserveSize);
            result.SetJobProxyMemory(
                GetInputIOMemorySize(PartitionJobIOConfig, stat) +
                GetOutputWindowMemorySize(PartitionJobIOConfig) +
                bufferSize);
        }
        return result;
    }

    virtual TExtendedJobResources GetSimpleSortResources(
        const TChunkStripeStatistics& stat,
        i64 valueCount) const override
    {
        Y_UNREACHABLE();
    }

    virtual bool IsSortedMergeNeeded(TPartitionPtr partition) const override
    {
        return Spec->ForceReduceCombiners || TSortControllerBase::IsSortedMergeNeeded(partition);
    }

    virtual TUserJobSpecPtr GetPartitionSortUserJobSpec(
        TPartitionPtr partition) const override
    {
        if (!IsSortedMergeNeeded(partition)) {
            return Spec->Reducer;
        } else if (Spec->ReduceCombiner) {
            return Spec->ReduceCombiner;
        } else {
            return nullptr;
        }
    }

    virtual TExtendedJobResources GetPartitionSortResources(
        TPartitionPtr partition,
        const TChunkStripeStatistics& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);

        i64 memory =
            GetSortInputIOMemorySize(stat) +
            GetSortBuffersMemorySize(stat);

        if (!IsSortedMergeNeeded(partition)) {
            result.SetCpu(Spec->Reducer->CpuLimit);
            memory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig);
            result.SetJobProxyMemory(memory);
        } else if (Spec->ReduceCombiner) {
            result.SetCpu(Spec->ReduceCombiner->CpuLimit);
            memory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
            result.SetJobProxyMemory(memory);
        } else {
            result.SetCpu(1);
            memory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
            result.SetJobProxyMemory(memory);
        }

        result.SetNetwork(Spec->ShuffleNetworkLimit);
        return result;
    }

    virtual TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(Spec->Reducer->CpuLimit);
        result.SetJobProxyMemory(GetFinalIOMemorySize(SortedMergeJobIOConfig, statistics));
        return result;
    }

    virtual TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        Y_UNREACHABLE();
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
            JobCounter.GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAbortedTotal(),
            JobCounter.GetLost(),
            // Partitions
            Partitions.size(),
            CompletedPartitionCount,
            // MapJobs
            GetPartitionJobCounter(),
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
            .Item(Spec->Mapper ? "map_jobs" : "partition_jobs").Value(GetPartitionJobCounter())
            .Item(Spec->ReduceCombiner ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
    }

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const override
    {
        return Spec->Mapper;
    }

    virtual int GetSortedMergeKeyColumnCount() const override
    {
        return Spec->ReduceBy.size();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

