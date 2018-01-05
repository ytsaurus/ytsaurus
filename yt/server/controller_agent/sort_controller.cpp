#include "sort_controller.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool_adapters.h"
#include "helpers.h"
#include "job_info.h"
#include "job_memory.h"
#include "unordered_controller.h"
#include "operation_controller_detail.h"
#include "task.h"

#include <yt/server/chunk_pools/chunk_pool.h>
#include <yt/server/chunk_pools/shuffle_chunk_pool.h>
#include <yt/server/chunk_pools/sorted_chunk_pool.h>
#include <yt/server/chunk_pools/unordered_chunk_pool.h>

#include <yt/server/scheduler/helpers.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/scheduling_context.h>

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/ytlib/job_tracker_client/helpers.h>

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
namespace NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkServer;
using namespace NChunkPools;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NSecurityClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NScheduler;

using NTableClient::TKey;
using NNodeTrackerClient::TNodeId;

using NScheduler::NProto::TPartitionJobSpecExt;
using NScheduler::NProto::TReduceJobSpecExt;
using NScheduler::NProto::TSortJobSpecExt;
using NScheduler::NProto::TMergeJobSpecExt;
using NScheduler::NProto::TSchedulerJobSpecExt;
using NScheduler::NProto::TSchedulerJobResultExt;

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
        TSortOperationSpecBasePtr spec,
        TControllerAgentConfigPtr config,
        TSortOperationOptionsBasePtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TOperationControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec(spec)
        , Options(options)
        , CompletedPartitionCount(0)
        // Cannot do similar for SortedMergeJobCounter and UnorderedMergeJobCounter since the number
        // of these jobs is hard to predict.
        , SortDataWeightCounter(New<TProgressCounter>(0))
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
        Persist(context, SortDataWeightCounter);

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
        Persist(context, ShufflePoolInput);
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

    // Counters.
    int CompletedPartitionCount;
    TProgressCounterPtr SortedMergeJobCounter = New<TProgressCounter>();
    TProgressCounterPtr UnorderedMergeJobCounter = New<TProgressCounter>();

    // Sort job counters.
    TProgressCounterPtr IntermediateSortJobCounter = New<TProgressCounter>();
    TProgressCounterPtr FinalSortJobCounter = New<TProgressCounter>();
    TProgressCounterPtr SortDataWeightCounter = New<TProgressCounter>();

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
                ? TSortTaskPtr(New<TSimpleSortTask>(controller, this, controller->GetFinalEdgeDescriptors()))
                : TSortTaskPtr(New<TPartitionSortTask>(controller, this, controller->GetFinalEdgeDescriptors()));
            controller->RegisterTask(SortTask);

            SortedMergeTask = New<TSortedMergeTask>(controller, this, controller->GetFinalEdgeDescriptors());
            controller->RegisterTask(SortedMergeTask);

            if (!controller->SimpleSort) {
                UnorderedMergeTask = New<TUnorderedMergeTask>(controller, this, controller->GetFinalEdgeDescriptors());
                controller->RegisterTask(UnorderedMergeTask);
                UnorderedMergeTask->SetInputVertex(controller->GetPartitionJobType());
            }

            SortTask->SetInputVertex(controller->GetPartitionJobType());
            SortedMergeTask->SetInputVertex(controller->GetIntermediateSortJobType());
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
    std::unique_ptr<IChunkPoolInput> ShufflePoolInput;
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

        TPartitionTask(TSortControllerBase* controller, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller(controller)
        { }

        virtual TString GetId() const override
        {
            return "Partition";
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->PartitionTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->IsLocalityEnabled()
                ? Controller->Spec->PartitionLocalityTimeout
                : TDuration::Zero();
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller->GetPartitionResources(joblet->InputStripeList->GetStatistics());
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

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetPartitionUserJobSpec();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetPartitionJobType();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, NodeIdToAdjustedDataWeight);
            Persist(context, AdjustedScheduledDataWeight);

            // COMPAT(psushin).
            if (context.IsLoad() && context.GetVersion() < 200926) {
                i64 _;
                Persist(context, _);
            }
        }

        virtual bool SupportsInputPathYson() const override
        {
            return true;
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTask, 0x63a4c761);

        TSortControllerBase* Controller;

        //! The total data size of jobs assigned to a particular node
        //! All data sizes are IO weight-adjusted.
        //! No zero values are allowed.
        yhash<TNodeId, i64> NodeIdToAdjustedDataWeight;
        //! The sum of all sizes appearing in #NodeIdToDataWeight.
        //! This value is IO weight-adjusted.
        i64 AdjustedScheduledDataWeight = 0;


        void UpdateNodeDataWeight(const TJobNodeDescriptor& descriptor, i64 delta)
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing ||
                Controller->TotalEstimatedInputDataWeight < Controller->Spec->MinLocalityInputDataWeight)
            {
                return;
            }

            auto ioWeight = descriptor.IOWeight;
            YCHECK(ioWeight > 0);
            auto adjustedDelta = static_cast<i64>(delta / ioWeight);

            auto nodeId = descriptor.Id;
            auto newAdjustedDataWeight = (NodeIdToAdjustedDataWeight[nodeId] += adjustedDelta);
            YCHECK(newAdjustedDataWeight >= 0);

            if (newAdjustedDataWeight == 0) {
                YCHECK(NodeIdToAdjustedDataWeight.erase(nodeId) == 1);
            }

            YCHECK((AdjustedScheduledDataWeight += adjustedDelta) >= 0);
        }

        virtual bool CanLoseJobs() const override
        {
            return Controller->Spec->EnableIntermediateOutputRecalculation;
        }

        virtual TNullable<EScheduleJobFailReason> GetScheduleFailReason(
            ISchedulingContext* context,
            const TJobResources& /*jobLimits*/) override
        {
            if (!Controller->Spec->EnablePartitionedDataBalancing ||
                Controller->TotalEstimatedInputDataWeight < Controller->Spec->MinLocalityInputDataWeight)
            {
                return Null;
            }

            auto ioWeight = context->GetNodeDescriptor().IOWeight;
            if (ioWeight == 0) {
                return EScheduleJobFailReason::DataBalancingViolation;
            }

            if (NodeIdToAdjustedDataWeight.empty()) {
                return Null;
            }

            // We don't have a job at hand here, let's make a guess.
            auto approximateStatistics = GetChunkPoolOutput()->GetApproximateStripeStatistics()[0];
            auto adjustedJobDataWeight = approximateStatistics.DataWeight / ioWeight;
            auto nodeId = context->GetNodeDescriptor().Id;
            auto newAdjustedScheduledDataWeight = AdjustedScheduledDataWeight + adjustedJobDataWeight;
            auto newAvgAdjustedScheduledDataWeight = newAdjustedScheduledDataWeight / NodeIdToAdjustedDataWeight.size();
            auto newAdjustedNodeDataWeight = NodeIdToAdjustedDataWeight[nodeId] + adjustedJobDataWeight;
            auto result =
                newAdjustedNodeDataWeight <=
                newAvgAdjustedScheduledDataWeight + Controller->Spec->PartitionedDataBalancingTolerance * adjustedJobDataWeight;

            return MakeNullable(!result, EScheduleJobFailReason::DataBalancingViolation);
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto statistics = Controller->PartitionPool->GetApproximateStripeStatistics();
            auto result = Controller->GetPartitionResources(
                statistics);
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            auto dataWeight = joblet->InputStripeList->TotalDataWeight;
            UpdateNodeDataWeight(joblet->NodeDescriptor, +dataWeight);

            TTask::OnJobStarted(joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            // Kick-start sort and unordered merge tasks.
            // Compute sort data size delta.
            i64 oldSortDataWeight = Controller->SortDataWeightCounter->GetTotal();
            i64 newSortDataWeight = 0;
            for (auto partition : Controller->Partitions) {
                if (partition->Maniac) {
                    Controller->AddTaskPendingHint(partition->UnorderedMergeTask);
                } else {
                    newSortDataWeight += partition->ChunkPoolOutput->GetTotalDataWeight();
                    Controller->AddTaskPendingHint(partition->SortTask);
                }
            }
            LOG_DEBUG("Sort data weight updated: %v -> %v",
                oldSortDataWeight,
                newSortDataWeight);
            Controller->SortDataWeightCounter->Increment(newSortDataWeight - oldSortDataWeight);

            // NB: don't move it to OnTaskCompleted since jobs may run after the task has been completed.
            // Kick-start sort and unordered merge tasks.
            Controller->CheckSortStartThreshold();
            Controller->CheckMergeStartThreshold();
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            UpdateNodeDataWeight(completedJob->NodeDescriptor, -completedJob->DataWeight);

            if (!Controller->IsShuffleCompleted()) {
                // Add pending hint if shuffle is in progress and some partition jobs were lost.
                Controller->AddTaskPendingHint(this);
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            TTask::OnJobFailed(joblet, jobSummary);

            UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TTask::OnJobAborted(joblet, jobSummary);

            UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            Controller->ShufflePool->GetInput()->Finish();

            // Dump totals.
            // Mark empty partitions are completed.
            LOG_DEBUG("Partition sizes collected");
            for (auto partition : Controller->Partitions) {
                i64 dataWeight = partition->ChunkPoolOutput->GetTotalDataWeight();
                if (dataWeight == 0) {
                    LOG_DEBUG("Partition %v is empty", partition->Index);
                    // Job restarts may cause the partition task to complete several times.
                    // Thus we might have already marked the partition as completed, let's be careful.
                    if (!partition->Completed) {
                        Controller->OnPartitionCompleted(partition);
                    }
                } else {
                    LOG_DEBUG("Partition[%v] = %v",
                        partition->Index,
                        dataWeight);

                    if (partition->SortTask) {
                        partition->SortTask->FinishInput();
                    }
                    if (partition->UnorderedMergeTask) {
                        partition->UnorderedMergeTask->FinishInput();
                    }
                }
            }

            if (Controller->Spec->EnablePartitionedDataBalancing) {
                auto nodeDescriptors = Controller->GetExecNodeDescriptors();
                yhash<TNodeId, TExecNodeDescriptor> idToNodeDescriptor;
                for (const auto& descriptor : nodeDescriptors) {
                    YCHECK(idToNodeDescriptor.insert(std::make_pair(descriptor.Id, descriptor)).second);
                }

                LOG_DEBUG("Per-node partitioned data weights collected");
                for (const auto& pair : NodeIdToAdjustedDataWeight) {
                    auto nodeId = pair.first;
                    auto dataWeight = pair.second;
                    auto nodeIt = idToNodeDescriptor.find(nodeId);
                    LOG_DEBUG("Node[%v] = %v",
                        nodeIt == idToNodeDescriptor.end() ? ToString(nodeId) : nodeIt->second.Address,
                        dataWeight);
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

        TPartitionBoundTask(
            TSortControllerBase* controller,
            TPartition* partition,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
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

        virtual bool SupportsInputPathYson() const override
        {
            return false;
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

        TSortTask(TSortControllerBase* controller, TPartition* partition, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TPartitionBoundTask(controller, partition, std::move(edgeDescriptors))
        { }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->SortTaskGroup;
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
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

        virtual EJobType GetJobType() const override
        {
            return Controller->IsSortedMergeNeeded(Partition)
                ? Controller->GetIntermediateSortJobType()
                : Controller->GetFinalSortJobType();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TPartitionBoundTask::Persist(context);

            using NYT::Persist;
            Persist(context, CurrentInputStreamIndex_);
        }

        // TODO(max42): this is a dirty way to change the edge descriptor when we
        // finally understand that sorted merge is needed. Re-write this.
        void OnSortedMergeNeeded()
        {
            EdgeDescriptors_.resize(1);
            EdgeDescriptors_[0].DestinationPool = Partition->SortedMergeTask->GetChunkPoolInput();
            EdgeDescriptors_[0].TableWriterOptions = Controller->GetIntermediateTableWriterOptions();
            EdgeDescriptors_[0].TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(Controller->Spec->SortBy);
            EdgeDescriptors_[0].RequiresRecoveryInfo = true;
            EdgeDescriptors_[0].IsFinalOutput = false;
        }

    protected:
        TExtendedJobResources GetNeededResourcesForChunkStripe(const TChunkStripeStatistics& stat) const
        {
            if (Controller->SimpleSort) {
                return Controller->GetSimpleSortResources(stat);
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

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            if (Controller->IsSortedMergeNeeded(Partition)) {
                jobSpec->CopyFrom(Controller->IntermediateSortJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller->FinalSortJobSpecTemplate);
            }
            AddOutputTableSpecs(jobSpec, joblet);

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

            Controller->SortDataWeightCounter->Start(joblet->InputStripeList->TotalDataWeight);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter->Start(1);
            } else {
                Controller->FinalSortJobCounter->Start(1);
            }
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TPartitionBoundTask::OnJobCompleted(joblet, jobSummary);

            Controller->SortDataWeightCounter->Completed(joblet->InputStripeList->TotalDataWeight);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                int inputStreamIndex = CurrentInputStreamIndex_++;
                Controller->IntermediateSortJobCounter->Completed(1);

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto* resultExt = jobSummary.Result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunk_specs());

                for (const auto& dataSlice : stripe->DataSlices) {
                    InferLimitsFromBoundaryKeys(dataSlice, Controller->RowBuffer);
                    dataSlice->InputStreamIndex = inputStreamIndex;
                }

                RegisterStripe(
                    stripe,
                    EdgeDescriptors_[0],
                    joblet);
            } else {
                Controller->FinalSortJobCounter->Completed(1);

                Controller->AccountRows(jobSummary.Statistics);

                RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

                Controller->OnPartitionCompleted(Partition);
            }

            Controller->CheckMergeStartThreshold();

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->AddTaskPendingHint(Partition->SortedMergeTask);
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            Controller->SortDataWeightCounter->Failed(joblet->InputStripeList->TotalDataWeight);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter->Failed(1);
            } else {
                Controller->FinalSortJobCounter->Failed(1);
            }

            TTask::OnJobFailed(joblet, jobSummary);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            Controller->SortDataWeightCounter->Aborted(joblet->InputStripeList->TotalDataWeight);

            if (Controller->IsSortedMergeNeeded(Partition)) {
                Controller->IntermediateSortJobCounter->Aborted(1, jobSummary.AbortReason);
            } else {
                Controller->FinalSortJobCounter->Aborted(1, jobSummary.AbortReason);
            }

            TTask::OnJobAborted(joblet, jobSummary);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            Controller->IntermediateSortJobCounter->Lost(1);
            auto stripeList = completedJob->SourceTask->GetChunkPoolOutput()->GetStripeList(completedJob->OutputCookie);
            Controller->SortDataWeightCounter->Lost(stripeList->TotalDataWeight);

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

        TPartitionSortTask(
            TSortControllerBase* controller,
            TPartition* partition,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TSortTask(controller, partition, std::move(edgeDescriptors))
        { }

        virtual TString GetId() const override
        {
            return Format("Sort(%v)", Partition->Index);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            if (!Controller->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

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

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetPartitionSortUserJobSpec(Partition);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionSortTask, 0x4f9a6cd9);

        virtual bool CanLoseJobs() const override
        {
            return Controller->Spec->EnableIntermediateOutputRecalculation;
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
            Partition->NodeIdToLocality[nodeId] += joblet->InputStripeList->TotalDataWeight;

            // Don't rely on static assignment anymore.
            Partition->AssignedNodeId = InvalidNodeId;

            // Also add a hint to ensure that subsequent jobs are also scheduled here.
            AddLocalityHint(nodeId);

            TSortTask::OnJobStarted(joblet);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            auto nodeId = completedJob->NodeDescriptor.Id;
            YCHECK((Partition->NodeIdToLocality[nodeId] -= completedJob->DataWeight) >= 0);

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

        TSimpleSortTask(TSortControllerBase* controller, TPartition* partition, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TSortTask(controller, partition, std::move(edgeDescriptors))
        { }

        virtual TString GetId() const override
        {
            return "SimpleSort";
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->IsLocalityEnabled()
                ? Controller->Spec->SimpleSortLocalityTimeout
                : TDuration::Zero();
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

        TMergeTask(TSortControllerBase* controller, TPartition* partition, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TPartitionBoundTask(controller, partition, std::move(edgeDescriptors))
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

        TSortedMergeTask(
            TSortControllerBase* controller,
            TPartition* partition,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TMergeTask(controller, partition, std::move(edgeDescriptors))
        {
            ChunkPool_ = controller->CreateSortedMergeChunkPool(GetId());
            ChunkPoolInput_ = CreateHintAddingAdapter(ChunkPool_.get(), this);
        }

        virtual TString GetId() const override
        {
            return Format("SortedMerge(%v)", Partition->Index);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            if (!Controller->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

            return
                Controller->SimpleSort
                ? Controller->Spec->SimpleMergeLocalityTimeout
                : Controller->Spec->MergeLocalityTimeout;
        }

        virtual i64 GetLocality(TNodeId nodeId) const override
        {
            return Partition->AssignedNodeId == nodeId || Partition->AssignedNodeId == InvalidNodeId;
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller->GetSortedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPoolInput_.get();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TMergeTask::Persist(context);

            using NYT::Persist;
            Persist(context, ChunkPool_);
            Persist(context, ChunkPoolInput_);
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, ActiveJoblets_);
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, InvalidatedJoblets_);
            Persist(context, JobOutputs_);
            Persist(context, Finished_);
            Persist(context, FrozenTotalJobCount_);
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetSortedMergeUserJobSpec();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetSortedMergeJobType();
        }

        i64 GetOutputRowCount()
        {
            i64 outputRowCount = 0;
            for (auto& jobOutput : JobOutputs_) {
                YCHECK(jobOutput.JobSummary.Statistics);
                outputRowCount += GetTotalOutputDataStatistics(*jobOutput.JobSummary.Statistics).row_count();
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
        std::unique_ptr<IChunkPoolInput> ChunkPoolInput_;

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
            TCompletedJobSummary JobSummary;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;

                Persist(context, ChunkListIds);
                Persist(context, JobSummary);
            }
        };
        std::vector<TJobOutput> JobOutputs_;

        virtual void SetupCallbacks() override
        {
            TTask::SetupCallbacks();

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
                Controller->Host->AbortJob(
                    joblet->JobId,
                    TError("Job is aborted due to chunk pool output invalidation")
                        << error);
                InvalidatedJoblets_.insert(joblet);
            }
            JobOutputs_.clear();
        }

        void RegisterAllOutputs()
        {
            for (auto& jobOutput : JobOutputs_) {
                Controller->AccountRows(jobOutput.JobSummary.Statistics);
                // We definitely know that output of current job is going directly to the sink, so
                // it is ok not to specify joblet at all.
                RegisterOutput(&jobOutput.JobSummary.Result, jobOutput.ChunkListIds, nullptr /* joblet */);
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

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            YCHECK(!Partition->Maniac);

            Controller->SortedMergeJobCounter->Start(1);

            TMergeTask::OnJobStarted(joblet);
            YCHECK(ActiveJoblets_.insert(joblet).second);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobCompleted(joblet, jobSummary);

            Controller->SortedMergeJobCounter->Completed(1);
            YCHECK(ActiveJoblets_.erase(joblet) == 1);
            if (!InvalidatedJoblets_.has(joblet)) {
                JobOutputs_.emplace_back(TJobOutput{joblet->ChunkListIds, jobSummary});
            }
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter->Failed(1);

            TMergeTask::OnJobFailed(joblet, jobSummary);
            YCHECK(ActiveJoblets_.erase(joblet) == 1);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            Controller->SortedMergeJobCounter->Aborted(1, jobSummary.AbortReason);

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

        TUnorderedMergeTask(
            TSortControllerBase* controller,
            TPartition* partition,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TMergeTask(controller, partition, std::move(edgeDescriptors))
        { }

        virtual TString GetId() const override
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

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
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

        virtual EJobType GetJobType() const override
        {
            return EJobType::UnorderedMerge;
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

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);

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

            Controller->UnorderedMergeJobCounter->Start(1);
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobCompleted(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter->Completed(1);

            Controller->AccountRows(jobSummary.Statistics);
            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);
        }

        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobFailed(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter->Failed(1);
        }

        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            TMergeTask::OnJobAborted(joblet, jobSummary);

            Controller->UnorderedMergeJobCounter->Aborted(1, jobSummary.AbortReason);
        }

    };

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const
    {
        return nullptr;
    }

    virtual TUserJobSpecPtr GetPartitionSortUserJobSpec(const TPartitionPtr& partition) const
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
            i64 AssignedDataWeight = 0;
        };

        typedef TIntrusivePtr<TAssignedNode> TAssignedNodePtr;

        auto compareNodes = [&] (const TAssignedNodePtr& lhs, const TAssignedNodePtr& rhs) {
            return lhs->AssignedDataWeight / lhs->Weight > rhs->AssignedDataWeight / rhs->Weight;
        };

        auto comparePartitions = [&] (const TPartitionPtr& lhs, const TPartitionPtr& rhs) {
            return lhs->ChunkPoolOutput->GetTotalDataWeight() > rhs->ChunkPoolOutput->GetTotalDataWeight();
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

            AddTaskLocalityHint(nodeId, task);

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataWeight += partition->ChunkPoolOutput->GetTotalDataWeight();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            LOG_DEBUG("Partition assigned (Index: %v, DataWeight: %v, Address: %v)",
                partition->Index,
                partition->ChunkPoolOutput->GetTotalDataWeight(),
                node->Descriptor.Address);
        }

        for (const auto& node : nodeHeap) {
            if (node->AssignedDataWeight > 0) {
                LOG_DEBUG("Node used (Address: %v, Weight: %.4lf, AssignedDataWeight: %v, AdjustedDataWeight: %v)",
                    node->Descriptor.Address,
                    node->Weight,
                    node->AssignedDataWeight,
                    static_cast<i64>(node->AssignedDataWeight / node->Weight));
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
            Spec->DataWeightPerShuffleJob);

        ShufflePoolInput = CreateIntermediateLivePreviewAdapter(ShufflePool->GetInput(), this);

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

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }

    void OnPartitionCompleted(TPartitionPtr partition)
    {
        YCHECK(!partition->Completed);
        partition->Completed = true;

        ++CompletedPartitionCount;

        LOG_DEBUG("Partition completed (Partition: %v)", partition->Index);
    }

    virtual bool IsSortedMergeNeeded(const TPartitionPtr& partition) const
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
        partition->SortTask->OnSortedMergeNeeded();
        return true;
    }

    void CheckSortStartThreshold()
    {
        if (!SortStartThresholdReached) {
            if (!SimpleSort && PartitionTask->GetCompletedDataWeight() <
                PartitionTask->GetTotalDataWeight() * Spec->ShuffleStartThreshold)
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
        int partitionJobCount,
        TChunkWriterConfigPtr config) const
    {
        i64 dataWeightAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataWeight * Spec->MapSelectivityFactor);
        i64 bufferSize = std::min(config->MaxBufferSize, DivCeil<i64>(dataWeightAfterPartition, partitionJobCount));
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
                if (SortDataWeightCounter->GetCompletedTotal() < SortDataWeightCounter->GetTotal() * Spec->MergeStartThreshold)
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
        const TChunkStripeStatistics& stat) const = 0;

    virtual TExtendedJobResources GetPartitionSortResources(
        const TPartitionPtr& partition,
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

    i64 GetRowCountEstimate(TPartitionPtr partition, i64 dataWeight) const
    {
        i64 totalDataWeight = partition->ChunkPoolOutput->GetTotalDataWeight();
        if (totalDataWeight == 0) {
            return 0;
        }
        i64 totalRowCount = partition->ChunkPoolOutput->GetTotalRowCount();
        return static_cast<i64>((double) totalRowCount * dataWeight / totalDataWeight);
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
    }

    i64 GetMaxPartitionJobBufferSize() const
    {
        return Spec->PartitionJobIO->TableWriter->MaxBufferSize;
    }

    int SuggestPartitionCount() const
    {
        YCHECK(TotalEstimatedInputDataWeight > 0);
        i64 dataWeightAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataWeight * Spec->MapSelectivityFactor);
        // Use int64 during the initial stage to avoid overflow issues.
        i64 result;
        if (Spec->PartitionCount) {
            result = *Spec->PartitionCount;
        } else if (Spec->PartitionDataWeight) {
            auto partitionDataWeight = std::max(*Spec->PartitionDataWeight, Options->MinPartitionWeight);
            result = DivCeil<i64>(dataWeightAfterPartition, partitionDataWeight);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options->CompressedBlockSize / InputCompressionRatio);
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec->PartitionJobIO->TableWriter->BlockSize);

            // Just in case compression ratio is very large.
            uncompressedBlockSize = std::max(i64(1), uncompressedBlockSize);

            // Product may not fit into i64.
            double partitionDataWeight = sqrt(dataWeightAfterPartition) * sqrt(uncompressedBlockSize);
            partitionDataWeight = std::max(partitionDataWeight, static_cast<double>(Options->MinPartitionWeight));

            i64 maxPartitionCount = GetMaxPartitionJobBufferSize() / uncompressedBlockSize;
            result = std::min(static_cast<i64>(dataWeightAfterPartition / partitionDataWeight), maxPartitionCount);

            if (result == 1 && TotalEstimatedInputUncompressedDataSize > Spec->DataWeightPerShuffleJob) {
                // Sometimes data size can be much larger than data weight.
                // Let's protect from such outliers and prevent simple sort in such case.
                result = DivCeil(TotalEstimatedInputUncompressedDataSize, Spec->DataWeightPerShuffleJob);
            } else if (result > 1) {
                // Calculate upper limit for partition data weight.
                auto uncompressedSortedChunkSize = static_cast<i64>(Spec->SortJobIO->TableWriter->DesiredChunkSize /
                    InputCompressionRatio);
                uncompressedSortedChunkSize = std::max<i64>(1, uncompressedSortedChunkSize);
                auto maxInputStreamsPerPartition = std::max<i64>(1,
                    Spec->MaxDataWeightPerJob / uncompressedSortedChunkSize);
                auto maxPartitionDataWeight = std::max<i64>(Options->MinPartitionWeight,
                    static_cast<i64>(0.9 * maxInputStreamsPerPartition * Spec->DataWeightPerShuffleJob));

                if (dataWeightAfterPartition / result > maxPartitionDataWeight) {
                    result = dataWeightAfterPartition / maxPartitionDataWeight;
                }

                LOG_DEBUG("Suggesting partition count (UncompressedBlockSize: %v, PartitionDataWeight: %v, "
                    "MaxPartitionDataWeight: %v, PartitionCount: %v, MaxPartitionCount: %v)",
                    uncompressedBlockSize,
                    partitionDataWeight,
                    maxPartitionDataWeight,
                    result,
                    maxPartitionCount);
            }
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
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetTotalDataWeight();
            }
            result.Total = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetRunningDataWeight();
            }
            result.Runnning = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetCompletedDataWeight();
            }
            result.Completed = AggregateValues(sizes, MaxProgressBuckets);
        }
        return result;
    }

    const TProgressCounterPtr& GetPartitionJobCounter() const
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
            i64 size = partition->ChunkPoolOutput->GetTotalDataWeight();
            if (size != 0) {
                histogram->AddValue(size);
            }
        }
        histogram->BuildHistogramView();
        return histogram;
    }

    void BuildPartitionsProgressYson(TFluentMap fluent) const
    {
        auto progress = ComputePartitionProgress();
        auto sizeHistogram = ComputePartitionSizeHistogram();

        fluent
            .Item("partitions").BeginMap()
                .Item("total").Value(Partitions.size())
                .Item("completed").Value(CompletedPartitionCount)
            .EndMap()
            .Item("partition_sizes").BeginMap()
                .Item("total").Value(progress.Total)
                .Item("running").Value(progress.Runnning)
                .Item("completed").Value(progress.Completed)
            .EndMap()
            .Item("partition_size_histogram").Value(*sizeHistogram);
    }

    void AnalyzePartitionHistogram() override
    {
        TError error;

        auto sizeHistogram = ComputePartitionSizeHistogram();
        auto view = sizeHistogram->GetHistogramView();

        i64 minIqr = Config->OperationAlerts->IntermediateDataSkewAlertMinInterquartileRange;

        if (view.Max > Config->OperationAlerts->IntermediateDataSkewAlertMinPartitionSize) {
            auto quartiles = ComputeHistogramQuartiles(view);
            i64 iqr = quartiles.Q75 - quartiles.Q25;
            if (iqr > minIqr && quartiles.Q50 + 2 * iqr < view.Max) {
                error = TError(
                    "Intermediate data skew is too high (see partitions histogram); "
                    "operation is likely to have stragglers");
            }
        }

        SetOperationAlert(EOperationAlertType::IntermediateDataSkew, error);
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

        const auto& client = Host->GetClient();
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
                    if (column->Aggregate()) {
                        THROW_ERROR_EXCEPTION("Sort by aggregate column is not allowed")
                            << TErrorAttribute("table_path", table.Path.GetPath())
                            << TErrorAttribute("column_name", name);
                    }
                }
            }
        }
    }

    virtual const std::vector<TEdgeDescriptor>& GetFinalEdgeDescriptors() const
    {
        return GetStandardEdgeDescriptors();
    }

    std::unique_ptr<IChunkPool> CreateSortedMergeChunkPool(TString taskId)
    {
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
        chunkPoolOptions.Task = taskId;
        return CreateSortedChunkPool(chunkPoolOptions, nullptr /* chunkSliceFetcher */, IntermediateInputStreamDirectory);
    }

    void AccountRows(const TNullable<NJobTrackerClient::TStatistics>& statistics)
    {
        YCHECK(statistics);
        TotalOutputRowCount += GetTotalOutputDataStatistics(*statistics).row_count();
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
        TSortOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TSortOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortControllerBase(
            spec,
            config,
            options,
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

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        switch (jobType) {
            case EJobType::Partition:
                return STRINGBUF("data_weight_per_partition_job");
            case EJobType::FinalSort:
                return STRINGBUF("partition_data_weight");
            default:
                Y_UNREACHABLE();
        }
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::Partition, EJobType::FinalSort};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortController, 0xbca37afe);

    TSortOperationSpecPtr Spec;

    IFetcherChunkScraperPtr FetcherChunkScraper;

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
        auto& table = OutputTables_[0];
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

                    ValidateOutputSchemaCompatibility(true, true);
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

        if (TotalEstimatedInputDataWeight == 0)
            return;

        TSamplesFetcherPtr samplesFetcher;

        TFuture<void> asyncSamplesResult;
        PROFILE_TIMING ("/input_processing_time") {
            int sampleCount = SuggestPartitionCount() * Spec->SamplesPerPartition;

            if (Spec->UnavailableChunkStrategy == EUnavailableChunkAction::Wait) {
                FetcherChunkScraper = CreateFetcherChunkScraper(
                    Config->ChunkScraper,
                    GetCancelableInvoker(),
                    Host->GetChunkLocationThrottlerManager(),
                    InputClient,
                    InputNodeDirectory_,
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
                InputNodeDirectory_,
                GetCancelableInvoker(),
                samplesRowBuffer,
                FetcherChunkScraper,
                Host->GetClient(),
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

        FetcherChunkScraper.Reset();

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

        YCHECK(partitionCount > 0);
        SimpleSort = (partitionCount == 1);

        if (SimpleSort) {
            BuildSinglePartition();
        } else {
            auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
                Spec,
                Options,
                TotalEstimatedInputUncompressedDataSize,
                TotalEstimatedInputDataWeight,
                TotalEstimatedInputRowCount,
                InputCompressionRatio);

            // Finally adjust partition count wrt block size constraints.
            partitionCount = AdjustPartitionCountToWriterBufferSize(
                partitionCount,
                partitionJobSizeConstraints->GetJobCount(),
                PartitionJobIOConfig->TableWriter);

            LOG_INFO("Adjusted partition count %v", partitionCount);

            BuildMulitplePartitions(sortedSamples, partitionCount, partitionJobSizeConstraints);
        }
    }

    void BuildSinglePartition()
    {
        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec,
            Options,
            TotalEstimatedInputDataWeight);

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(jobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(jobSizeConstraints, &stripes);

        // Create the fake partition.
        InitSimpleSortPool(jobSizeConstraints);
        auto partition = New<TPartition>(this, 0);
        Partitions.push_back(partition);
        partition->ChunkPoolOutput = SimpleSortPool.get();
        partition->SortTask->AddInput(stripes);
        partition->SortTask->SetInputVertex(GetPartitionJobType());
        partition->SortedMergeTask->SetInputVertex(GetIntermediateSortJobType());

        partition->SortTask->FinishInput();

        // NB: Cannot use TotalEstimatedInputDataWeight due to slicing and rounding issues.
        SortDataWeightCounter->Increment(SimpleSortPool->GetTotalDataWeight());

        LOG_INFO("Sorting without partitioning (SortJobCount: %v, DataWeightPerJob: %v)",
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataWeightPerJob());

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

    void BuildMulitplePartitions(
        const std::vector<const TSample*>& sortedSamples,
        int partitionCount,
        const IJobSizeConstraintsPtr& partitionJobSizeConstraints)
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

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(partitionJobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(partitionJobSizeConstraints, &stripes);

        InitPartitionPool(partitionJobSizeConstraints, nullptr);

        TEdgeDescriptor shuffleEdgeDescriptor = GetIntermediateEdgeDescriptorTemplate();
        shuffleEdgeDescriptor.DestinationPool = ShufflePoolInput.get();
        shuffleEdgeDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;
        PartitionTask = New<TPartitionTask>(this, std::vector<TEdgeDescriptor> {shuffleEdgeDescriptor});
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        FinishTaskInput(PartitionTask);
        RegisterTask(PartitionTask);

        LOG_INFO("Sorting with partitioning (PartitionCount: %v, PartitionJobCount: %v, DataWeightPerPartitionJob: %v)",
            partitionCount,
            partitionJobSizeConstraints->GetJobCount(),
            partitionJobSizeConstraints->GetDataWeightPerJob());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        InitIntermediateOutputConfig(IntermediateSortJobIOConfig);

        // Final sort: reader like sort and output like merge.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);
        FinalSortJobIOConfig->TableWriter = CloneYsonSerializable(Spec->MergeJobIO->TableWriter);

        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);

        UnorderedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        // Since we're reading from huge number of paritition chunks, we must use larger buffers,
        // as we do for sort jobs.
        UnorderedMergeJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);
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
            SetInputDataSources(schedulerJobSpecExt);

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
                SetInputDataSources(schedulerJobSpecExt);
            } else {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
                SetIntermediateDataSource(schedulerJobSpecExt);
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
            SetIntermediateDataSource(schedulerJobSpecExt);

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
            SetIntermediateDataSource(schedulerJobSpecExt);

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
            stat.DataWeight);

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

    virtual TExtendedJobResources GetSimpleSortResources(const TChunkStripeStatistics& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());
        result.SetJobProxyMemory(GetSortInputIOMemorySize(stat) +
            GetFinalOutputIOMemorySize(FinalSortJobIOConfig) +
            // Data weight is an approximate estimate for string data + row data
            // memory footprint inside SchemalessSortingReader.
            stat.DataWeight);
        return result;
    }

    virtual TExtendedJobResources GetPartitionSortResources(
        const TPartitionPtr& partition,
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
        const TChunkStripeStatisticsVector& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(SortedMergeJobIOConfig, stat));
        return result;
    }

    virtual bool IsRowCountPreserved() const override
    {
        return true;
    }

    virtual i64 GetUnavailableInputChunkCount() const override
    {
        if (FetcherChunkScraper && State == EControllerState::Preparing) {
            return FetcherChunkScraper->GetUnavailableChunkCount();
        }

        return TOperationControllerBase::GetUnavailableInputChunkCount();
    }

    virtual TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(UnorderedMergeJobIOConfig, AggregateStatistics(stat)));
        return result;
    }

    // Progress reporting.

    virtual TString GetLoggingProgress() const override
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
            JobCounter->GetTotal(),
            JobCounter->GetRunning(),
            JobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter->GetFailed(),
            JobCounter->GetAbortedTotal(),
            JobCounter->GetLost(),
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
            GetUnavailableInputChunkCount());
    }

    virtual void BuildProgress(TFluentMap fluent) const override
    {
        TSortControllerBase::BuildProgress(fluent);
        fluent
            .Do(BIND(&TSortController::BuildPartitionsProgressYson, Unretained(this)))
            .Item(JobTypeAsKey(EJobType::Partition)).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(EJobType::IntermediateSort)).Value(IntermediateSortJobCounter)
            .Item(JobTypeAsKey(EJobType::FinalSort)).Value(FinalSortJobCounter)
            .Item(JobTypeAsKey(EJobType::SortedMerge)).Value(SortedMergeJobCounter)
            .Item(JobTypeAsKey(EJobType::UnorderedMerge)).Value(UnorderedMergeJobCounter)
            // TODO(ignat): remove when UI migrate to new keys.
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

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortController);

IOperationControllerPtr CreateSortController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TSortOperationSpec>(operation->GetSpec());
    return New<TSortController>(spec, config, config->SortOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TMapReduceController
    : public TSortControllerBase
{
public:
    TMapReduceController(
        TMapReduceOperationSpecPtr spec,
        TControllerAgentConfigPtr config,
        TMapReduceOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation)
        : TSortControllerBase(
            spec,
            config,
            options,
            host,
            operation)
        , Spec(spec)
    {
        if (spec->Mapper) {
            RegisterJobProxyMemoryDigest(EJobType::PartitionMap, spec->PartitionJobProxyMemoryDigest);
            RegisterUserJobMemoryDigest(EJobType::PartitionMap, spec->Mapper->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);
        } else {
            RegisterJobProxyMemoryDigest(EJobType::Partition, spec->PartitionJobProxyMemoryDigest);
        }

        if (spec->ReduceCombiner) {
            RegisterJobProxyMemoryDigest(EJobType::ReduceCombiner, spec->ReduceCombinerJobProxyMemoryDigest);
            RegisterUserJobMemoryDigest(EJobType::ReduceCombiner, spec->ReduceCombiner->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);
        } else {
            RegisterJobProxyMemoryDigest(EJobType::IntermediateSort, spec->SortJobProxyMemoryDigest);
        }

        RegisterJobProxyMemoryDigest(EJobType::SortedReduce, spec->SortedReduceJobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::SortedReduce, spec->Reducer->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);

        RegisterJobProxyMemoryDigest(EJobType::PartitionReduce, spec->PartitionReduceJobProxyMemoryDigest);
        RegisterUserJobMemoryDigest(EJobType::PartitionReduce, spec->Reducer->UserJobMemoryDigestDefaultValue, spec->Reducer->UserJobMemoryDigestLowerBound);
    }

    virtual void BuildBriefSpec(TFluentMap fluent) const override
    {
        TSortControllerBase::BuildBriefSpec(fluent);
        fluent
            .DoIf(Spec->Mapper.Get(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
                    .EndMap();
            })
            .DoIf(Spec->Reducer.Get(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reducer").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->Reducer->Command))
                    .EndMap();
            })
            .DoIf(Spec->ReduceCombiner.Get(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_combiner").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->ReduceCombiner->Command))
                    .EndMap();
            });
    }

    void Persist(const TPersistenceContext& context)
    {
        TSortControllerBase::Persist(context);

        using NYT::Persist;

        Persist(context, MapperSinkEdges_);
        Persist(context, ReducerSinkEdges_);
    }

    void InitEdgeDescriptors()
    {
        const auto& edgeDescriptors = GetStandardEdgeDescriptors();

        MapperSinkEdges_ = std::vector<TEdgeDescriptor>(
            edgeDescriptors.begin(),
            edgeDescriptors.begin() + Spec->MapperOutputTableCount);
        for (int index = 0; index < MapperSinkEdges_.size(); ++index) {
            MapperSinkEdges_[index].TableWriterOptions->TableIndex = index + 1;
        }

        ReducerSinkEdges_ = std::vector<TEdgeDescriptor>(
            edgeDescriptors.begin() + Spec->MapperOutputTableCount,
            edgeDescriptors.end());
        for (int index = 0; index < ReducerSinkEdges_.size(); ++index) {
            ReducerSinkEdges_[index].TableWriterOptions->TableIndex = index;
        }
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        switch (jobType) {
            case EJobType::PartitionMap:
            case EJobType::Partition:
                return STRINGBUF("data_weight_per_map_job");
            case EJobType::PartitionReduce:
            case EJobType::SortedReduce:
                return STRINGBUF("partition_data_weight");
           default:
                Y_UNREACHABLE();
        }
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {
            EJobType::PartitionMap,
            EJobType::Partition,
            EJobType::PartitionReduce,
            EJobType::SortedReduce
        };
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapReduceController, 0xca7286bd);

    TMapReduceOperationSpecPtr Spec;

    // Mapper edge descriptors are for the data that is written from mappers directly to the first
    // `Spec->MapperOutputTableCount` output tables skipping the shuffle and reduce phases.
    std::vector<TEdgeDescriptor> MapperSinkEdges_;
    std::vector<TEdgeDescriptor> ReducerSinkEdges_;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserFile> ReduceCombinerFiles;
    std::vector<TUserFile> ReducerFiles;

    i64 MapStartRowIndex = 0;
    i64 ReduceStartRowIndex = 0;

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

    virtual std::vector<TPathWithStage> GetLayerPaths() const override
    {
        // Combine mapper and reducer files into a single collection.
        std::vector<TPathWithStage> result;
        if (Spec->Mapper) {
            for (const auto& path : Spec->Mapper->LayerPaths) {
                result.push_back(std::make_pair(path, EOperationStage::Map));
            }
        }

        if (Spec->ReduceCombiner) {
            for (const auto& path : Spec->ReduceCombiner->LayerPaths) {
                result.push_back(std::make_pair(path, EOperationStage::ReduceCombiner));
            }
        }

        for (const auto& path : Spec->Reducer->LayerPaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        if (TotalEstimatedInputDataWeight == 0)
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
        InitEdgeDescriptors();

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

        auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);

        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount,
            partitionJobSizeConstraints->GetJobCount(),
            PartitionJobIOConfig->TableWriter);
        LOG_INFO("Adjusted partition count %v", partitionCount);

        BuildMultiplePartitions(partitionCount, partitionJobSizeConstraints);
    }

    void BuildMultiplePartitions(
        int partitionCount,
        const IJobSizeConstraintsPtr& partitionJobSizeConstraints)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }

        InitShufflePool();

        std::vector<TChunkStripePtr> stripes;
        SlicePrimaryUnversionedChunks(partitionJobSizeConstraints, &stripes);
        SlicePrimaryVersionedChunks(partitionJobSizeConstraints, &stripes);

        InitPartitionPool(partitionJobSizeConstraints, Config->EnablePartitionMapJobSizeAdjustment
            ? Options->PartitionJobSizeAdjuster
            : nullptr);

        std::vector<TEdgeDescriptor> partitionEdgeDescriptors;

        // Primary edge descriptor for shuffled output of the mapper.
        TEdgeDescriptor shuffleEdgeDescriptor = GetIntermediateEdgeDescriptorTemplate();
        shuffleEdgeDescriptor.DestinationPool = ShufflePoolInput.get();
        shuffleEdgeDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;

        partitionEdgeDescriptors.emplace_back(std::move(shuffleEdgeDescriptor));
        partitionEdgeDescriptors.insert(
            partitionEdgeDescriptors.end(),
            MapperSinkEdges_.begin(),
            MapperSinkEdges_.end());

        PartitionTask = New<TPartitionTask>(this, std::move(partitionEdgeDescriptors));
        PartitionTask->Initialize();
        PartitionTask->AddInput(stripes);
        FinishTaskInput(PartitionTask);
        RegisterTask(PartitionTask);

        LOG_INFO("Map-reducing with partitioning (PartitionCount: %v, PartitionJobCount: %v, PartitionDataWeightPerJob: %v)",
            partitionCount,
            partitionJobSizeConstraints->GetJobCount(),
            partitionJobSizeConstraints->GetDataWeightPerJob());
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

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
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

    virtual const std::vector<TEdgeDescriptor>& GetFinalEdgeDescriptors() const override
    {
        return ReducerSinkEdges_;
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
            SetInputDataSources(schedulerJobSpecExt);

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
        {
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());
            ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), OutputTransaction->GetId());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).GetData());

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            SetIntermediateDataSource(schedulerJobSpecExt);

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
            SetIntermediateDataSource(schedulerJobSpecExt);

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
            SetIntermediateDataSource(schedulerJobSpecExt);

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

    virtual void CustomizeJoblet(const TJobletPtr& joblet) override
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

    virtual void CustomizeJobSpec(const TJobletPtr& joblet, TJobSpec* jobSpec) override
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
            bufferSize = std::min(bufferSize, stat.DataWeight + reserveSize);
            result.SetJobProxyMemory(
                GetInputIOMemorySize(PartitionJobIOConfig, stat) +
                GetOutputWindowMemorySize(PartitionJobIOConfig) +
                bufferSize);
        }
        return result;
    }

    virtual TExtendedJobResources GetSimpleSortResources(const TChunkStripeStatistics& stat) const override
    {
        Y_UNREACHABLE();
    }

    virtual bool IsSortedMergeNeeded(const TPartitionPtr& partition) const override
    {
        if (Spec->ForceReduceCombiners) {
            partition->CachedSortedMergeNeeded = true;
            partition->SortTask->OnSortedMergeNeeded();
        }
        return TSortControllerBase::IsSortedMergeNeeded(partition);
    }

    virtual TUserJobSpecPtr GetPartitionSortUserJobSpec(const TPartitionPtr& partition) const override
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
        const TPartitionPtr& partition,
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

    virtual TString GetLoggingProgress() const override
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
            JobCounter->GetTotal(),
            JobCounter->GetRunning(),
            JobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            JobCounter->GetFailed(),
            JobCounter->GetAbortedTotal(),
            JobCounter->GetLost(),
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
            GetUnavailableInputChunkCount());
    }

    virtual void BuildProgress(TFluentMap fluent) const override
    {
        TSortControllerBase::BuildProgress(fluent);
        fluent
            .Do(BIND(&TMapReduceController::BuildPartitionsProgressYson, Unretained(this)))
            .Item(JobTypeAsKey(GetPartitionJobType())).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(GetIntermediateSortJobType())).Value(IntermediateSortJobCounter)
            .Item(JobTypeAsKey(GetFinalSortJobType())).Value(FinalSortJobCounter)
            .Item(JobTypeAsKey(GetSortedMergeJobType())).Value(SortedMergeJobCounter)
            // TODO(ignat): remove when UI migrate to new keys.
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

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMapReduceController);

IOperationControllerPtr CreateMapReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(operation->GetSpec());
    return New<TMapReduceController>(spec, config, config->MapReduceOperationOptions, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

