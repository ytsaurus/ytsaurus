#include "sort_controller.h"

#include "chunk_pool_adapters.h"
#include "data_balancer.h"
#include "job_info.h"
#include "job_memory.h"
#include "helpers.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "unordered_controller.h"

#include <yt/server/controller_agent/chunk_list_pool.h>
#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/job_size_constraints.h>
#include <yt/server/controller_agent/operation.h>
#include <yt/server/controller_agent/scheduling_context.h>
#include <yt/server/controller_agent/config.h>

#include <yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/server/lib/chunk_pools/shuffle_chunk_pool.h>
#include <yt/server/lib/chunk_pools/sorted_chunk_pool.h>
#include <yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/ytlib/chunk_client/key_set.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/samples_fetcher.h>
#include <yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/ytree/permission.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>

#include <cmath>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkPools;
using namespace NTableClient;
using namespace NJobProxy;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NSecurityClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NScheduler;

using NYT::FromProto;
using NYT::ToProto;

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

        Persist(context, Spec);

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

        Persist(context, AssignedPartitionsByNodeId);
        Persist(context, PartitionsLocalityByNodeId);

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
        Persist(context, ShuffleChunkMapping_);

        Persist(context, PartitionTaskGroup);
        Persist(context, SortTaskGroup);
        Persist(context, MergeTaskGroup);

        Persist(context, PartitionTask);
        Persist(context, SimpleSortTask);
        Persist(context, IntermediateSortTask);
        Persist(context, FinalSortTask);
        Persist(context, UnorderedMergeTask);
        Persist(context, SortedMergeTask);
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

    class TSimpleSortTask;
    typedef TIntrusivePtr<TSimpleSortTask> TSimpleSortTaskPtr;

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
            : Controller(controller)
            , Index(index)
            , Key(key)
            , Completed(false)
            , CachedSortedMergeNeeded(false)
            , Maniac(false)
            , ChunkPoolOutput(nullptr)
        { }

        TSortControllerBase* Controller;

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

        // Chunk pool output obtained from the shuffle pool.
        IChunkPoolOutputPtr ChunkPoolOutput;

        void SetAssignedNodeId(TNodeId nodeId)
        {
            if (AssignedNodeId != InvalidNodeId) {
                YT_VERIFY(Controller->AssignedPartitionsByNodeId[AssignedNodeId].erase(Index) == 1);
            }

            AssignedNodeId = nodeId;

            if (AssignedNodeId != InvalidNodeId) {
                YT_VERIFY(Controller->AssignedPartitionsByNodeId[AssignedNodeId].emplace(Index).second);
            }
        }

        void AddLocality(TNodeId nodeId, i64 delta)
        {
            auto& localityMap = Controller->PartitionsLocalityByNodeId[nodeId];
            localityMap[Index] += delta;
            YT_VERIFY(localityMap[Index] >= 0);
            if (localityMap[Index] == 0) {
                YT_VERIFY(localityMap.erase(Index) == 1);
            }
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Controller);

            Persist(context, Index);
            Persist(context, Key);

            Persist(context, Completed);

            Persist(context, CachedSortedMergeNeeded);

            Persist(context, Maniac);

            Persist(context, AssignedNodeId);

            Persist(context, ChunkPoolOutput);
        }

    private:
        //! The node assigned to this partition, #InvalidNodeId if none.
        TNodeId AssignedNodeId = InvalidNodeId;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    //! Equivalent to |Partitions.size() == 1| but enables checking
    //! for simple sort when #Partitions is still being constructed.
    bool SimpleSort;
    std::vector<TPartitionPtr> Partitions;

    // Locality stuff.

    struct TLocalityEntry
    {
        int PartitionIndex;

        i64 Locality;        
    };

    //! NodeId -> set of partitions assigned to it.
    THashMap<TNodeId, THashSet<int>> AssignedPartitionsByNodeId;

    //! NodeId -> map<partition_index, locality>.
    THashMap<TNodeId, THashMap<int, int>> PartitionsLocalityByNodeId;

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

    IChunkPoolPtr PartitionPool;
    IShuffleChunkPoolPtr ShufflePool;
    IChunkPoolInputPtr ShufflePoolInput;
    IChunkPoolPtr SimpleSortPool;
    TInputChunkMappingPtr ShuffleChunkMapping_;

    TTaskGroupPtr PartitionTaskGroup;
    TTaskGroupPtr SortTaskGroup;
    TTaskGroupPtr MergeTaskGroup;

    TPartitionTaskPtr PartitionTask;

    TSimpleSortTaskPtr SimpleSortTask;

    TSortTaskPtr IntermediateSortTask;
    TSortTaskPtr FinalSortTask;

    TUnorderedMergeTaskPtr UnorderedMergeTask;

    TSortedMergeTaskPtr SortedMergeTask;

    //! Implements partition phase for sort operations and map phase for map-reduce operations.
    class TPartitionTask
        : public TTask
    {
    public:
        //! For persistence only.
        TPartitionTask() = default;

        TPartitionTask(TSortControllerBase* controller, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller(controller)
        { }

        virtual void FinishInput() override
        {
            // NB: we try to use the value as close to the total data weight of all extracted stripe lists as possible.
            // In particular, we do not use Controller->TotalEstimatedInputDataWeight here.
            auto totalDataWeight = GetChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
            if (Controller->Spec->EnablePartitionedDataBalancing &&
                totalDataWeight >= Controller->Spec->MinLocalityInputDataWeight)
            {
                YT_LOG_INFO("Data balancing enabled (TotalDataWeight: %v)", totalDataWeight);
                DataBalancer_ = New<TDataBalancer>(
                    Controller->Options->DataBalancer,
                    totalDataWeight,
                    Controller->GetOnlineExecNodeDescriptors());
                DataBalancer_->SetLogger(Logger);
            }

            TTask::FinishInput();
        }

        virtual void Initialize() override
        {
            TTask::Initialize();

            if (DataBalancer_) {
                DataBalancer_->SetLogger(Logger);
            }
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

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller->PartitionPool;
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return Controller->PartitionPool;
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
            Persist(context, DataBalancer_);

            if (context.IsLoad() && DataBalancer_) {
                DataBalancer_->OnExecNodesUpdated(Controller->GetOnlineExecNodeDescriptors());
            }
        }

        void OnExecNodesUpdated()
        {
            if (DataBalancer_) {
                DataBalancer_->OnExecNodesUpdated(Controller->GetOnlineExecNodeDescriptors());
            }
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTask, 0x63a4c761);

        TSortControllerBase* Controller = nullptr;

        TDataBalancerPtr DataBalancer_;

        virtual bool CanLoseJobs() const override
        {
            return Controller->Spec->EnableIntermediateOutputRecalculation;
        }

        virtual std::optional<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context) override
        {
            // We don't have a job at hand here, let's make a guess.
            auto approximateStatistics = GetChunkPoolOutput()->GetApproximateStripeStatistics()[0];
            const auto& node = context->GetNodeDescriptor();

            if (DataBalancer_ && !DataBalancer_->CanScheduleJob(node, approximateStatistics.DataWeight)) {
                return EScheduleJobFailReason::DataBalancingViolation;
            }

            return std::nullopt;
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
            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, +dataWeight);
            }

            TTask::OnJobStarted(joblet);
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            // Kick-start sort and unordered merge tasks.
            // Compute sort data size delta.
            for (auto partition : Controller->Partitions) {
                if (partition->Maniac) {
                    Controller->AddTaskPendingHint(Controller->UnorderedMergeTask);
                } else {
                    if (Controller->SimpleSort) {
                        Controller->AddTaskPendingHint(Controller->SimpleSortTask);
                    } else if (Controller->IsSortedMergeNeeded(partition)) {
                        Controller->AddTaskPendingHint(Controller->IntermediateSortTask);
                    } else {
                        Controller->AddTaskPendingHint(Controller->FinalSortTask);
                    }
                }
            }

            // NB: don't move it to OnTaskCompleted since jobs may run after the task has been completed.
            // Kick-start sort and unordered merge tasks.
            Controller->CheckSortStartThreshold();
            Controller->CheckMergeStartThreshold();

            if (Controller->ShufflePool->GetTotalDataSliceCount() > Controller->Spec->MaxShuffleDataSliceCount) {
                Controller->OnOperationFailed(TError("Too many data slices in shuffle pool, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_data_slice_count", Controller->ShufflePool->GetTotalDataSliceCount())
                    << TErrorAttribute("max_shuffle_data_slice_count", Controller->Spec->MaxShuffleDataSliceCount));
            }

            if (Controller->ShufflePool->GetTotalJobCount() > Controller->Spec->MaxShuffleJobCount) {
                Controller->OnOperationFailed(TError("Too many shuffle jobs, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_job_count", Controller->ShufflePool->GetTotalJobCount())
                    << TErrorAttribute("max_shuffle_job_count", Controller->Spec->MaxShuffleJobCount));
            }

            return result;
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(completedJob->NodeDescriptor, -completedJob->DataWeight);
            }

            if (!Controller->IsShuffleCompleted()) {
                // Add pending hint if shuffle is in progress and some partition jobs were lost.
                Controller->AddTaskPendingHint(this);
            }
        }

        virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobFailed(joblet, jobSummary);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
            }

            return result;
        }

        virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
            }

            return result;
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            Controller->ShufflePool->GetInput()->Finish();

            // Dump totals.
            // Mark empty partitions are completed.
            YT_LOG_DEBUG("Partition sizes collected");
            for (auto partition : Controller->Partitions) {
                i64 dataWeight = partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
                if (dataWeight == 0) {
                    YT_LOG_DEBUG("Partition %v is empty", partition->Index);
                    Controller->OnPartitionCompleted(partition);
                } else {
                    YT_LOG_DEBUG("Partition[%v] = %v",
                        partition->Index,
                        dataWeight);

                    if (!partition->Maniac && !Controller->IsSortedMergeNeeded(partition)) {
                        Controller->FinalSortTask->RegisterPartition(partition);
                    }

                    if (partition->Maniac) {
                        Controller->UnorderedMergeTask->RegisterPartition(partition);
                    }
                }
            }

            if (Controller->FinalSortTask) {
                Controller->FinalSortTask->Finalize();
                Controller->FinalSortTask->FinishInput();
            }

            if (Controller->IntermediateSortTask) {
                Controller->IntermediateSortTask->Finalize();
                Controller->IntermediateSortTask->FinishInput();
            }

            if (Controller->UnorderedMergeTask) {
                Controller->UnorderedMergeTask->FinishInput();
                Controller->UnorderedMergeTask->Finalize();
            }

            if (Controller->SortedMergeTask) {
                Controller->SortedMergeTask->Finalize();
            }

            Controller->ValidateMergeDataSliceLimit();

            if (DataBalancer_) {
                DataBalancer_->LogStatistics();
            }

            Controller->AssignPartitions();

            // NB: this is required at least to mark tasks completed, when there are no pending jobs.
            // This couldn't have been done earlier since we've just finished populating shuffle pool.
            Controller->CheckSortStartThreshold();
            Controller->CheckMergeStartThreshold();
        }
    };

    //! Base class implementing sort phase for sort operations
    //! and partition reduce phase for map-reduce operations.
    class TSortTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TSortTaskBase() = default;

        TSortTaskBase(TSortControllerBase* controller, std::vector<TEdgeDescriptor> edgeDescriptors, bool isFinalSort)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller_(controller)
            , IsFinalSort_(isFinalSort)
        {
            JobProxyMemoryDigest_ = CreateLogDigest(New<TLogDigestConfig>(
                1.0, // LowerLimit - we do not want to adjust memory reserve lower limit for sort jobs - we are pretty sure in our initial estimates.
                Controller_->Spec->JobProxyMemoryDigest->UpperBound,
                Controller_->Spec->JobProxyMemoryDigest->DefaultValue.value_or(1.0)));
        }

        void SetupJobCounters()
        {
            // Cannot move it to ctor since GetJobCounter is a virtual function.
            if (IsFinalSort_) {
                GetJobCounter()->AddParent(Controller_->FinalSortJobCounter);
            } else {
                GetJobCounter()->AddParent(Controller_->IntermediateSortJobCounter);
            }
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->SortTaskGroup;
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = GetNeededResourcesForChunkStripe(
                joblet->InputStripeList->GetAggregateStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual EJobType GetJobType() const override
        {
            if (IsFinalSort_) {
                return Controller_->GetFinalSortJobType();
            } else {
                return Controller_->GetIntermediateSortJobType();
            }
        }

        virtual bool IsSimpleTask() const override
        {
            return false;
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, IsFinalSort_);
            Persist(context, CurrentInputStreamIndex_);
        }

        virtual void OnStripeRegistrationFailed(
            TError error,
            IChunkPoolInput::TCookie cookie,
            const TChunkStripePtr& stripe,
            const TEdgeDescriptor& descriptor) override
        {
            if (IsFinalSort_) {
                // Somehow we failed resuming a lost stripe in a sink. No comments.
                TTask::OnStripeRegistrationFailed(error, cookie, stripe, descriptor);
            }
            Controller_->SortedMergeTask->AbortAllActiveJoblets(error, *stripe->PartitionTag);
            // TODO(max42): maybe moving chunk mapping outside of the pool was not that great idea.
            // Let's live like this a bit, and then maybe move it inside pool.
            descriptor.DestinationPool->Reset(cookie, stripe, descriptor.ChunkMapping);
            descriptor.ChunkMapping->Reset(cookie, stripe);
        }

    protected:
        TExtendedJobResources GetNeededResourcesForChunkStripe(const TChunkStripeStatistics& stat) const
        {
            if (Controller_->SimpleSort) {
                return Controller_->GetSimpleSortResources(stat);
            } else {
                return Controller_->GetPartitionSortResources(IsFinalSort_, stat);
            }
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto stat = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            if (Controller_->SimpleSort && stat.size() > 1) {
                stat = AggregateStatistics(stat);
            } else {
                YT_VERIFY(stat.size() == 1);
            }
            auto result = GetNeededResourcesForChunkStripe(stat.front());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            if (IsFinalSort_) {
                jobSpec->CopyFrom(Controller_->FinalSortJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller_->IntermediateSortJobSpecTemplate);
            }

            AddOutputTableSpecs(jobSpec, joblet);

            auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_is_approximate(joblet->InputStripeList->IsApproximate);

            AddSequentialInputSpec(jobSpec, joblet);

            auto partitionIndex = joblet->InputStripeList->PartitionTag;
            if (partitionIndex) {
                auto jobType = GetJobType();
                if (jobType == EJobType::PartitionReduce || jobType == EJobType::ReduceCombiner) {
                    auto* reduceJobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                    reduceJobSpecExt->set_partition_tag(*partitionIndex);
                } else {
                    auto* sortJobSpecExt = jobSpec->MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                    sortJobSpecExt->set_partition_tag(*partitionIndex);
                }
            }
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            if (IsFinalSort_) {
                Controller_->AccountRows(jobSummary.Statistics);

                RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);
            } else {
                int inputStreamIndex = CurrentInputStreamIndex_++;

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto* resultExt = jobSummary.Result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_output_chunk_specs());

                for (const auto& dataSlice : stripe->DataSlices) {
                    InferLimitsFromBoundaryKeys(dataSlice, Controller_->RowBuffer);
                    dataSlice->InputStreamIndex = inputStreamIndex;
                }

                if (Controller_->SimpleSort) {
                    stripe->PartitionTag = 0;
                } else {
                    stripe->PartitionTag = joblet->InputStripeList->PartitionTag;
                }

                RegisterStripe(
                    stripe,
                    EdgeDescriptors_[0],
                    joblet);
            }

            Controller_->CheckMergeStartThreshold();

            if (!IsFinalSort_) {
                Controller_->AddTaskPendingHint(Controller_->SortedMergeTask);
            }

            return result;
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            if (Controller_->PartitionTask) {
                Controller_->AddTaskPendingHint(this);
                Controller_->AddTaskPendingHint(Controller_->PartitionTask);
            }
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (!IsFinalSort_) {
                Controller_->SortedMergeTask->FinishInput();
                Controller_->AddMergeTasksPendingHints();
                Controller_->ValidateMergeDataSliceLimit();
            }
        }

        virtual bool CanLoseJobs() const override
        {
            return Controller_->Spec->EnableIntermediateOutputRecalculation;
        }

        void SetIsFinalSort(bool isFinalSort)
        {
            IsFinalSort_ = isFinalSort;
        }

    protected:
        TSortControllerBase* Controller_;

        bool IsFinalSort_ = false;

    private:
        int CurrentInputStreamIndex_ = 0;
    };

    //! Implements partition sort for sort operations and
    //! partition reduce phase for map-reduce operations.
    class TSortTask
        : public TSortTaskBase
    {
    public:
        //! For persistence only.
        TSortTask() = default;

        TSortTask(
            TSortControllerBase* controller,
            std::vector<TEdgeDescriptor> edgeDescriptors,
            bool isFinalSort)
            : TSortTaskBase(controller, std::move(edgeDescriptors), isFinalSort)
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput({}))
        { }

        virtual TDuration GetLocalityTimeout() const override
        {
            if (!Controller_->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

            return Controller_->Spec->SortLocalityTimeout;
        }

        virtual i64 GetLocality(TNodeId nodeId) const override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                return localityEntry->Locality;
            } else {
                return 0;
            }
        }

        virtual IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId) override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                auto partitionIndex = localityEntry->PartitionIndex;
                return MultiChunkPoolOutput_->ExtractFromPool(partitionIndex, nodeId);
            } else {
                return MultiChunkPoolOutput_->Extract(nodeId);
            }
        }

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPoolOutput_;
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetSortUserJobSpec(IsFinalSort_);
        }

        virtual TInputChunkMappingPtr GetChunkMapping() const override
        {
            return Controller_->ShuffleChunkMapping_;
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            MultiChunkPoolOutput_->AddPoolOutput(partition->ChunkPoolOutput, partition->Index);

            Partitions_.push_back(std::move(partition));
        }

        void Finalize()
        {
            MultiChunkPoolOutput_->Finalize();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TSortTaskBase::Persist(context);

            using NYT::Persist;
            Persist(context, MultiChunkPoolOutput_);
            Persist(context, Partitions_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortTask, 0x4f9a6cd9);

        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TPartitionPtr> Partitions_;

        virtual bool IsActive() const override
        {
            return Controller_->SortStartThresholdReached;
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            auto nodeId = joblet->NodeDescriptor.Id;

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            auto& partition = Controller_->Partitions[partitionIndex];

            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            partition->AddLocality(nodeId, joblet->InputStripeList->TotalDataWeight);

            // Don't rely on static assignment anymore.
            partition->SetAssignedNodeId(InvalidNodeId);

            // Also add a hint to ensure that subsequent jobs are also scheduled here.
            AddLocalityHint(nodeId);

            TSortTaskBase::OnJobStarted(joblet);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            auto partitionIndex = *completedJob->InputStripe->PartitionTag;
            auto& partition = Partitions_[partitionIndex];
            auto nodeId = completedJob->NodeDescriptor.Id;
            partition->AddLocality(nodeId, -completedJob->DataWeight);

            Controller_->ResetTaskLocalityDelays();

            TSortTaskBase::OnJobLost(completedJob);
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TSortTaskBase::OnJobCompleted(joblet, jobSummary);

            // TODO(gritukan): It seems to be the easiest way to distgunish data sent from partition task
            // to intermediate sort and to final sort. Do it more generic way.
            if (!jobSummary.Abandoned) {
                auto totalInputStatistics = GetTotalInputDataStatistics(*jobSummary.Statistics);
                TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
                    Controller_->PartitionTask->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    totalInputStatistics);
            }

            return result;
        }

        virtual void OnTaskCompleted() override
        {
            TSortTaskBase::OnTaskCompleted();

            if (IsFinalSort_)  {
                for (const auto& partition : Partitions_) {
                    Controller_->OnPartitionCompleted(partition);
                }
            }
        }
    };

    //! Implements simple sort phase for sort operations.
    class TSimpleSortTask
        : public TSortTaskBase
    {
    public:
        //! For persistence only.
        TSimpleSortTask() = default;

        TSimpleSortTask(TSortControllerBase* controller, TPartition* partition, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TSortTaskBase(controller, std::move(edgeDescriptors), /*isFinalSort=*/true)
            , Partition_(partition)
        {
            GetChunkPoolOutput()->GetDataWeightCounter()->AddParent(Controller_->SortDataWeightCounter);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec->SimpleSortLocalityTimeout
                : TDuration::Zero();
        }

        virtual TString GetTitle() const override
        {
            return Format("SimpleSort");
        }

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->SimpleSortPool;
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return Controller_->SimpleSortPool;
        }

        virtual void OnTaskCompleted() override
        {
            TSortTaskBase::OnTaskCompleted();

            if (Controller_->SortedMergeTask) {
                Controller_->SortedMergeTask->Finalize();
            }

            if (IsFinalSort_) {
                Controller_->OnPartitionCompleted(Partition_);
            }
        }

        // TODO(max42, gritukan): this is a dirty way add sorted merge when we
        // finally understand that it is needed. Re-write this.
        void OnSortedMergeNeeded()
        {
            GetJobCounter()->RemoveParent(Controller_->FinalSortJobCounter);
            GetJobCounter()->AddParent(Controller_->IntermediateSortJobCounter);

            SetIsFinalSort(false);
            EdgeDescriptors_ = Controller_->GetSortedMergeEdgeDescriptors();
        }

        void Persist(const TPersistenceContext& context)
        {
            TSortTaskBase::Persist(context);

            using NYT::Persist;
            Persist(context, Partition_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleSortTask, 0xb32d4f02);

        TPartition* Partition_;
    };

    //! Implements sorted merge phase for sort operations and
    //! sorted reduce phase for map-reduce operations.
    class TSortedMergeTask
        : public TTask
    {
    public:
        //! For persistence only.
        TSortedMergeTask() = default;

        TSortedMergeTask(
            TSortControllerBase* controller,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller_(controller)
            , MultiChunkPool_(CreateMultiChunkPool({}))
        {
            ChunkPoolInput_ = CreateHintAddingAdapter(MultiChunkPool_, this);

            MultiChunkPool_->GetJobCounter()->AddParent(Controller_->SortedMergeJobCounter);
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            if (!Controller_->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

            return
                Controller_->SimpleSort
                ? Controller_->Spec->SimpleMergeLocalityTimeout
                : Controller_->Spec->MergeLocalityTimeout;
        }

        virtual i64 GetLocality(TNodeId nodeId) const override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                return localityEntry->Locality;
            } else {
                return 0;
            }
        }

        virtual IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId) override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                auto partitionIndex = localityEntry->PartitionIndex;
                return MultiChunkPool_->ExtractFromPool(partitionIndex, nodeId);
            } else {
                return MultiChunkPool_->Extract(nodeId);
            }
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto resources = Controller_->GetSortedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(resources);
            return resources;
        }

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPoolInput_;
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, Partitions_);
            Persist(context, MultiChunkPool_);
            Persist(context, ChunkPoolInput_);
            Persist<TVectorSerializer<TSetSerializer<TDefaultSerializer, TUnsortedTag>>>(context, ActiveJoblets_);
            Persist<TVectorSerializer<TSetSerializer<TDefaultSerializer, TUnsortedTag>>>(context, InvalidatedJoblets_);
            Persist(context, JobOutputs_);
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetSortedMergeUserJobSpec();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller_->GetSortedMergeJobType();
        }

        void AbortAllActiveJoblets(const TError& error, int partitionIndex)
        {
            const auto& partition = Controller_->Partitions[partitionIndex];
            if (partition->Completed) {
                YT_LOG_INFO(error, "Chunk mapping has been invalidated, but the partition has already finished (PartitionIndex: %v)",
                    partitionIndex);
                return;
            }
            YT_LOG_INFO(error, "Aborting all jobs in partition because of chunk mapping invalidation (PartitionIndex: %v)",
                partitionIndex);
            for (const auto& joblet : ActiveJoblets_[partitionIndex]) {
                Controller_->Host->AbortJob(
                    joblet->JobId,
                    TError("Job is aborted due to chunk mapping invalidation")
                        << error
                        << TErrorAttribute("abort_reason", EAbortReason::ChunkMappingInvalidated));
                InvalidatedJoblets_[partitionIndex].insert(joblet);
            }
            for (const auto& jobOutput : JobOutputs_[partitionIndex]) {
                YT_VERIFY(jobOutput.JobSummary.Statistics);
                auto tableIndex = Controller_->GetRowCountLimitTableIndex();
                if (tableIndex) {
                    auto optionalCount = FindNumericValue(
                        *jobOutput.JobSummary.Statistics,
                        Format("/data/output/%v/row_count", *tableIndex));
                    if (optionalCount) {
                        // We have to unregister registered output rows.
                        Controller_->RegisterOutputRows(-(*optionalCount), *tableIndex);
                    }
                }
            }
            JobOutputs_.clear();
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            auto partitionIndex = partition->Index;

            auto partitionChunkPool = Controller_->CreateSortedMergeChunkPool(Format("%v(%v)", GetTitle(), partitionIndex));
            MultiChunkPool_->AddPool(std::move(partitionChunkPool), partitionIndex);

            Partitions_.push_back(std::move(partition));

            if (partitionIndex >= ActiveJoblets_.size()) {
                ActiveJoblets_.resize(partitionIndex + 1);
                InvalidatedJoblets_.resize(partitionIndex + 1);
                JobOutputs_.resize(partitionIndex + 1);
            }
        }

        void Finalize()
        {
            MultiChunkPool_->Finalize();
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeTask, 0x4ab19c75);

        TSortControllerBase* Controller_;

        std::vector<TPartitionPtr> Partitions_;

        IMultiChunkPoolPtr MultiChunkPool_;
        IChunkPoolInputPtr ChunkPoolInput_;

        //! Partition index -> list of active joblets.
        std::vector<THashSet<TJobletPtr>> ActiveJoblets_;

        //! Partition index -> list of invalidated joblets.
        std::vector<THashSet<TJobletPtr>> InvalidatedJoblets_;

        struct TJobOutput
        {
            TJobletPtr Joblet;
            TCompletedJobSummary JobSummary;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;

                Persist(context, Joblet);
                Persist(context, JobSummary);
            }
        };

        //! Partition index -> list of job outputs.
        std::vector<std::vector<TJobOutput>> JobOutputs_;

        void RegisterAllOutputs()
        {
            for (auto& jobOutputs : JobOutputs_) {
                for (auto& jobOutput : jobOutputs) {
                    Controller_->AccountRows(jobOutput.JobSummary.Statistics);
                    RegisterOutput(&jobOutput.JobSummary.Result, jobOutput.Joblet->ChunkListIds, jobOutput.Joblet);
                }
            }
        }

        virtual bool IsActive() const override
        {
            return Controller_->MergeStartThresholdReached;
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller_->GetSortedMergeResources(
                MultiChunkPool_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPool_;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            TTask::OnJobStarted(joblet);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].insert(joblet).second);
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);
            if (!InvalidatedJoblets_[partitionIndex].contains(joblet)) {
                JobOutputs_[partitionIndex].emplace_back(TJobOutput{joblet, jobSummary});
            }

            return result;
        }

        virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobFailed(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);

            return result;
        }

        virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);

            return result;
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->MergeTaskGroup;
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            RegisterAllOutputs();

            for (const auto& partition : Partitions_) {
                Controller_->OnPartitionCompleted(partition);
            }
        }
    };

    //! Implements unordered merge of maniac partitions for sort operation.
    //! Not used in map-reduce operations.
    class TUnorderedMergeTask
        : public TTask
    {
    public:
        //! For persistence only.
        TUnorderedMergeTask() = default;

        TUnorderedMergeTask(
            TSortControllerBase* controller,
            std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller_(controller)
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput({}))
        {
            MultiChunkPoolOutput_->GetJobCounter()->AddParent(Controller_->UnorderedMergeJobCounter);
        }

        virtual i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            // Locality is unimportant.
            return 0;
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetUnorderedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShufflePool->GetInput();
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPoolOutput_;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType::UnorderedMerge;
        }

        virtual TInputChunkMappingPtr GetChunkMapping() const override
        {
            return Controller_->ShuffleChunkMapping_;
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            MultiChunkPoolOutput_->AddPoolOutput(partition->ChunkPoolOutput, partition->Index);
            Partitions_.push_back(std::move(partition));
        }

        void Finalize()
        {
            MultiChunkPoolOutput_->Finalize();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, MultiChunkPoolOutput_);
            Persist(context, Partitions_);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeTask, 0xbba17c0f);

        TSortControllerBase* Controller_;

        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TPartitionPtr> Partitions_;

        virtual bool IsActive() const override
        {
            return Controller_->MergeStartThresholdReached;
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto resources = Controller_->GetUnorderedMergeResources(
                MultiChunkPoolOutput_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(resources);
            return resources;
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller_->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);

            const auto& list = joblet->InputStripeList;
            if (list->PartitionTag) {
                auto* mergeJobSpecExt = jobSpec->MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
                mergeJobSpecExt->set_partition_tag(*list->PartitionTag);
            }
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            Controller_->AccountRows(jobSummary.Statistics);
            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            // TODO(gritukan): It seems to be the easiest way to distgunish data sent from partition task
            // to intermediate sort and to final sort. Do it more generic way.
            if (!jobSummary.Abandoned) {
                auto totalInputStatistics = GetTotalInputDataStatistics(*jobSummary.Statistics);
                TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
                    Controller_->PartitionTask->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    totalInputStatistics);
            }

            return result;
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller_->MergeTaskGroup;
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            for (const auto& partition : Partitions_) {
                Controller_->OnPartitionCompleted(partition);
            }
        }
    };

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

    void PrepareSortTasks()
    {
        if (SimpleSort) {
            return;
        }

        IntermediateSortTask = TSortTaskPtr(New<TSortTask>(this, GetSortedMergeEdgeDescriptors(), /*isFinalSort=*/false));
        IntermediateSortTask->SetupJobCounters();
        IntermediateSortTask->RegisterInGraph(FormatEnum(GetPartitionJobType()));
        RegisterTask(IntermediateSortTask);

        FinalSortTask = TSortTaskPtr(New<TSortTask>(this, GetFinalEdgeDescriptors(), /*isFinalSort=*/true));
        FinalSortTask->SetupJobCounters();
        FinalSortTask->RegisterInGraph(FormatEnum(GetPartitionJobType()));
        RegisterTask(FinalSortTask);
    }

    void PrepareSortedMergeTask()
    {
        SortedMergeTask = New<TSortedMergeTask>(this, GetFinalEdgeDescriptors());
        RegisterTask(SortedMergeTask);
        SortedMergeTask->SetInputVertex(FormatEnum(GetIntermediateSortJobType()));
        SortedMergeTask->RegisterInGraph();
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
            return lhs->ChunkPoolOutput->GetDataWeightCounter()->GetTotal() >
                   rhs->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
        };

        YT_LOG_DEBUG("Examining online nodes");

        const auto& nodeDescriptors = GetOnlineExecNodeDescriptors();
        TJobResources maxResourceLimits;
        double maxIOWeight = 0;
        for (const auto& [nodeId, descriptor] : nodeDescriptors) {
            maxResourceLimits = Max(maxResourceLimits, descriptor.ResourceLimits);
            maxIOWeight = std::max(maxIOWeight, descriptor.IOWeight);
        }

        std::vector<TAssignedNodePtr> nodeHeap;
        for (const auto& [nodeId, descriptor] : nodeDescriptors) {
            double weight = 1.0;
            weight = std::min(weight, GetMinResourceRatio(descriptor.ResourceLimits, maxResourceLimits));
            weight = std::min(weight, descriptor.IOWeight > 0 ? descriptor.IOWeight / maxIOWeight : 0);
            if (weight > 0) {
                auto assignedNode = New<TAssignedNode>(descriptor, weight);
                nodeHeap.push_back(assignedNode);
            }
        }

        if (nodeHeap.empty()) {
            YT_LOG_DEBUG("No alive exec nodes to assign partitions");
            return;
        }

        std::vector<TPartitionPtr> partitionsToAssign;
        for (const auto& partition : Partitions) {
            // Only take partitions for which no jobs are launched yet.
            if (PartitionsLocalityByNodeId[partition->Index].empty()) {
                partitionsToAssign.push_back(partition);
            }
        }
        std::sort(partitionsToAssign.begin(), partitionsToAssign.end(), comparePartitions);

        // This is actually redundant since all values are 0.
        std::make_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

        YT_LOG_DEBUG("Assigning partitions");

        for (const auto& partition : partitionsToAssign) {
            auto node = nodeHeap.front();
            auto nodeId = node->Descriptor.Id;

            partition->SetAssignedNodeId(nodeId);

            TTaskPtr task;
            if (partition->Maniac) {
                task = UnorderedMergeTask;
            } else if (SimpleSort) {
                task = SimpleSortTask;
            } else if (IsSortedMergeNeeded(partition)) {
                task = IntermediateSortTask;
            } else {
                task = FinalSortTask;
            }

            AddTaskLocalityHint(nodeId, task);

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataWeight += partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            YT_LOG_DEBUG("Partition assigned (Index: %v, DataWeight: %v, Address: %v)",
                partition->Index,
                partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal(),
                node->Descriptor.Address);
        }

        for (const auto& node : nodeHeap) {
            if (node->AssignedDataWeight > 0) {
                YT_LOG_DEBUG("Node used (Address: %v, Weight: %.4lf, AssignedDataWeight: %v, AdjustedDataWeight: %v)",
                    node->Descriptor.Address,
                    node->Weight,
                    node->AssignedDataWeight,
                    static_cast<i64>(node->AssignedDataWeight / node->Weight));
            }
        }

        YT_LOG_DEBUG("Partitions assigned");
    }

    void InitPartitionPool(
        IJobSizeConstraintsPtr jobSizeConstraints,
        TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig,
        bool ordered)
    {
        if (ordered) {
            TOrderedChunkPoolOptions options;
            options.JobSizeConstraints = std::move(jobSizeConstraints);
            options.OperationId = OperationId;
            options.Task = PartitionTask->GetTitle();
            options.MaxTotalSliceCount = Config->MaxTotalSliceCount;
            options.EnablePeriodicYielder = true;
            options.ShouldSliceByRowIndices = true;

            PartitionPool = CreateOrderedChunkPool(
                std::move(options),
                IntermediateInputStreamDirectory);
        } else {
            TUnorderedChunkPoolOptions options;
            options.JobSizeConstraints = std::move(jobSizeConstraints);
            options.JobSizeAdjusterConfig = std::move(jobSizeAdjusterConfig);
            options.OperationId = OperationId;
            options.Name = PartitionTask->GetTitle();

            PartitionPool = CreateUnorderedChunkPool(
                std::move(options),
                GetInputStreamDirectory());
        }

        PartitionPool->GetDataWeightCounter()->AddParent(SortDataWeightCounter);
    }

    void InitShufflePool()
    {
        ShufflePool = CreateShuffleChunkPool(
            static_cast<int>(Partitions.size()),
            Spec->DataWeightPerShuffleJob,
            Spec->MaxChunkSlicePerShuffleJob);

        ShuffleChunkMapping_ = New<TInputChunkMapping>(EChunkMappingMode::Unordered);

        ShufflePoolInput = CreateIntermediateLivePreviewAdapter(ShufflePool->GetInput(), this);

        for (auto partition : Partitions) {
            partition->ChunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        }
    }

    void InitSimpleSortPool(IJobSizeConstraintsPtr jobSizeConstraints)
    {
        TUnorderedChunkPoolOptions options;
        options.JobSizeConstraints = std::move(jobSizeConstraints);
        options.OperationId = OperationId;
        options.Name = "SimpleSort";

        SimpleSortPool = CreateUnorderedChunkPool(
            std::move(options),
            GetInputStreamDirectory());
    }

    virtual bool IsCompleted() const override
    {
        return CompletedPartitionCount == Partitions.size();
    }

    bool IsSamplingEnabled() const
    {
        for (const auto& jobIOConfig : {
            PartitionJobIOConfig,
            IntermediateSortJobIOConfig,
            FinalSortJobIOConfig,
            SortedMergeJobIOConfig,
            UnorderedMergeJobIOConfig,
        })
        {
            if (jobIOConfig && jobIOConfig->TableReader->SamplingRate) {
                return true;
            }
        }
        return false;
    }

    virtual void OnOperationCompleted(bool interrupted) override
    {
        if (!interrupted) {
            auto isNontrivialInput = InputHasReadLimits() || InputHasVersionedTables();

            if (IsRowCountPreserved() && !(SimpleSort && isNontrivialInput) && !IsSamplingEnabled()) {
                // We don't check row count for simple sort if nontrivial read limits are specified,
                // since input row count can be estimated inaccurate.
                i64 totalInputRowCount = 0;
                for (const auto& partition : Partitions) {
                    i64 inputRowCount = partition->ChunkPoolOutput->GetRowCounter()->GetTotal();
                    totalInputRowCount += inputRowCount;
                }
                YT_LOG_ERROR_IF(totalInputRowCount != TotalOutputRowCount,
                    "Input/output row count mismatch in sort operation (TotalInputRowCount: %v, TotalOutputRowCount: %v)",
                    totalInputRowCount,
                    TotalOutputRowCount);
                YT_VERIFY(totalInputRowCount == TotalOutputRowCount);
            }

            YT_VERIFY(CompletedPartitionCount == Partitions.size());
        } else {
            if (RowCountLimitTableIndex && CompletedRowCount_ >= RowCountLimit) {
                // We have to save all output in SortedMergeTask.
                for (const auto& task : Tasks) {
                    task->CheckCompleted();
                    if (!task->IsCompleted() && task->GetJobType() == EJobType::SortedMerge) {
                        // Dirty hack to save chunks.
                        task->ForceComplete();
                    }
                }
            }
        }

        TOperationControllerBase::OnOperationCompleted(interrupted);
    }

    void OnPartitionCompleted(TPartitionPtr partition)
    {
        if (partition->Completed) {
            return;
        }

        partition->Completed = true;

        ++CompletedPartitionCount;

        YT_LOG_DEBUG("Partition completed (Partition: %v)", partition->Index);
    }

    virtual bool IsSortedMergeNeeded(const TPartitionPtr& partition) const
    {
        if (partition->CachedSortedMergeNeeded) {
            return true;
        }

        if (partition->Maniac) {
            return false;
        }

        if (partition->ChunkPoolOutput->GetJobCounter()->GetTotal() <= 1) {
            return false;
        }

        YT_LOG_DEBUG("Partition needs sorted merge (Partition: %v)", partition->Index);
        partition->CachedSortedMergeNeeded = true;
        SortedMergeTask->RegisterPartition(partition);

        if (SimpleSort) {
            SimpleSortTask->OnSortedMergeNeeded();
        } else {
            IntermediateSortTask->RegisterPartition(partition);
        }
        return true;
    }

    void CheckSortStartThreshold()
    {
        if (!SortStartThresholdReached) {
            if (!SimpleSort && PartitionTask->GetCompletedDataWeight() <
                PartitionTask->GetTotalDataWeight() * Spec->ShuffleStartThreshold)
                return;

            YT_LOG_INFO("Sort start threshold reached");

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

            TTaskPtr task;
            if (partition->Maniac) {
                task = UnorderedMergeTask;
            } else if (SimpleSort) {
                task = SimpleSortTask;
            } else if (IsSortedMergeNeeded(partition)) {
                task = IntermediateSortTask;
            } else {
                task = FinalSortTask;
            }

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
                if (!PartitionTask->IsCompleted()) {
                    return;
                }
                if (SortDataWeightCounter->GetCompletedTotal() < SortDataWeightCounter->GetTotal() * Spec->MergeStartThreshold) {
                    return;
                }
            }

            YT_LOG_INFO("Merge start threshold reached");

            MergeStartThresholdReached = true;
        }

        AddMergeTasksPendingHints();
    }

    void AddSortTasksPendingHints()
    {
        for (auto partition : Partitions) {
            if (!partition->Maniac) {
                if (SimpleSort) {
                    AddTaskPendingHint(SimpleSortTask);
                } else if (IsSortedMergeNeeded(partition)) {
                    AddTaskPendingHint(IntermediateSortTask);
                } else {
                    AddTaskPendingHint(FinalSortTask);
                }
            }
        }
    }

    void AddMergeTasksPendingHints()
    {
        for (auto partition : Partitions) {
            auto taskToKick = partition->Maniac
                ? TTaskPtr(UnorderedMergeTask)
                : TTaskPtr(SortedMergeTask);
            AddTaskPendingHint(taskToKick);
        }
    }

    std::optional<TLocalityEntry> GetLocalityEntry(TNodeId nodeId) const
    {
        {
            auto it = AssignedPartitionsByNodeId.find(nodeId);
            if (it != AssignedPartitionsByNodeId.end()) {
                const auto& partitions = it->second;
                if (!partitions.empty()) {
                    return TLocalityEntry{
                        .PartitionIndex = *partitions.begin(),
                        .Locality = 1
                    };
                }
            }
        }
        {
            auto it = PartitionsLocalityByNodeId.find(nodeId);
            if (it != PartitionsLocalityByNodeId.end()) {
                const auto& partitionLocalities = it->second;
                if (!partitionLocalities.empty()) {
                    const auto& partitionLocality = *partitionLocalities.begin();
                    return TLocalityEntry{
                        .PartitionIndex = partitionLocality.first,
                        .Locality = partitionLocality.second
                    };
                }
            }
        }

        return std::nullopt;
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
        bool isFinalSort,
        const TChunkStripeStatistics& stat) const = 0;

    virtual TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const = 0;

    virtual TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const = 0;

    virtual void OnExecNodesUpdated()
    {
        if (PartitionTask) {
            PartitionTask->OnExecNodesUpdated();
        }
    }

    void ProcessInputs(const TTaskPtr& inputTask, const IJobSizeConstraintsPtr& jobSizeConstraints)
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        int unversionedSlices = 0;
        int versionedSlices = 0;
        for (auto& chunk : CollectPrimaryUnversionedChunks()) {
            const auto& slice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            inputTask->AddInput(New<TChunkStripe>(std::move(slice)));
            ++unversionedSlices;
            yielder.TryYield();
        }
        for (auto& slice : CollectPrimaryVersionedDataSlices(jobSizeConstraints->GetInputSliceDataWeight())) {
            inputTask->AddInput(New<TChunkStripe>(std::move(slice)));
            ++versionedSlices;
            yielder.TryYield();
        }

        YT_LOG_INFO("Processed inputs (UnversionedSlices: %v, VersionedSlices: %v)",
            unversionedSlices,
            versionedSlices);
    }

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
        YT_VERIFY(TotalEstimatedInputDataWeight > 0);
        i64 dataWeightAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataWeight * Spec->MapSelectivityFactor);
        // Use int64 during the initial stage to avoid overflow issues.
        i64 result;
        if (Spec->PartitionCount) {
            result = *Spec->PartitionCount;
        } else if (Spec->PartitionDataWeight) {
            result = DivCeil<i64>(dataWeightAfterPartition, *Spec->PartitionDataWeight);
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

                YT_LOG_DEBUG("Suggesting partition count (UncompressedBlockSize: %v, PartitionDataWeight: %v, "
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
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
            }
            result.Total = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetDataWeightCounter()->GetRunning();
            }
            result.Runnning = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
                sizes[i] = Partitions[i]->ChunkPoolOutput->GetDataWeightCounter()->GetCompletedTotal();
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
            i64 size = partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
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
        PartitionJobIOConfig = Spec->PartitionJobIO;
    }

    virtual void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        ValidateIntermediateDataAccountPermission(EPermission::Use);

        for (const auto& table : InputTables_) {
            for (const auto& name : Spec->SortBy) {
                if (auto column = table->Schema->FindColumn(name)) {
                    if (column->Aggregate()) {
                        THROW_ERROR_EXCEPTION("Sort by aggregate column is not allowed")
                            << TErrorAttribute("table_path", table->Path)
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

    virtual std::vector<TEdgeDescriptor> GetSortedMergeEdgeDescriptors() const
    {
        // TODO(gritukan): Here should be intermediate edge descriptor, but it breaks complex types.
        auto edgeDescriptor = GetFinalEdgeDescriptors()[0];

        edgeDescriptor.DestinationPool = SortedMergeTask->GetChunkPoolInput();
        edgeDescriptor.ChunkMapping = SortedMergeTask->GetChunkMapping();
        edgeDescriptor.TableWriterOptions = GetIntermediateTableWriterOptions();
        if (edgeDescriptor.TableUploadOptions.TableSchema->GetKeyColumns() != Spec->SortBy) {
            edgeDescriptor.TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(Spec->SortBy);
        }
        edgeDescriptor.RequiresRecoveryInfo = true;
        edgeDescriptor.IsFinalOutput = false;
        edgeDescriptor.TargetDescriptor = SortedMergeTask->GetVertexDescriptor();

        return {edgeDescriptor};
    }

    IChunkPoolPtr CreateSortedMergeChunkPool(TString taskId)
    {
        TSortedChunkPoolOptions chunkPoolOptions;
        TSortedJobOptions jobOptions;
        jobOptions.EnableKeyGuarantee = GetSortedMergeJobType() == EJobType::SortedReduce;
        jobOptions.PrimaryPrefixLength = GetSortedMergeKeyColumnCount();
        jobOptions.ShouldSlicePrimaryTableByKeys = GetSortedMergeJobType() == EJobType::SortedReduce;
        jobOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;

        // NB: otherwise we could easily be persisted during preparing the jobs. Sorted chunk pool
        // can't handle this.
        jobOptions.EnablePeriodicYielder = false;
        chunkPoolOptions.OperationId = OperationId;
        chunkPoolOptions.SortedJobOptions = jobOptions;
        chunkPoolOptions.JobSizeConstraints = CreatePartitionBoundSortedJobSizeConstraints(
            Spec,
            Options,
            Logger,
            GetOutputTablePaths().size());
        chunkPoolOptions.Task = taskId;
        return CreateSortedChunkPool(chunkPoolOptions, nullptr /* chunkSliceFetcher */, IntermediateInputStreamDirectory);
    }

    void AccountRows(const std::optional<TStatistics>& statistics)
    {
        YT_VERIFY(statistics);
        TotalOutputRowCount += GetTotalOutputDataStatistics(*statistics).row_count();
    }

    void ValidateMergeDataSliceLimit()
    {
        i64 dataSliceCount = 0;

        if (SortedMergeTask) {
            dataSliceCount += SortedMergeTask->GetInputDataSliceCount();
        }

        if (UnorderedMergeTask) {
            dataSliceCount += UnorderedMergeTask->GetInputDataSliceCount();
        }

        if (dataSliceCount > Spec->MaxMergeDataSliceCount) {
            OnOperationFailed(TError("Too many data slices in merge pools, try to decrease size of "
                "intermediate data or split operation into several smaller ones")
                << TErrorAttribute("merge_data_slice_count", dataSliceCount)
                << TErrorAttribute("max_merge_data_slice_count", Spec->MaxMergeDataSliceCount));
        }
    }

    std::vector<TPartitionKey> BuildPartitionKeysByPivotKeys()
    {
        std::vector<TPartitionKey> partitionKeys;
        partitionKeys.reserve(Spec->PivotKeys.size());

        for (const auto& key : Spec->PivotKeys) {
            partitionKeys.emplace_back(RowBuffer->Capture(key));
        }

        return partitionKeys;
    }

    void CreatePartitionsByPartitionKeys(const std::vector<TPartitionKey>& partitionKeys)
    {
        Partitions.reserve(partitionKeys.size() + 1);

        // Create the leftmost partition.
        Partitions.push_back(New<TPartition>(this, 0, MinKey()));

        for (int index = 0; index < partitionKeys.size(); ++index) {
            YT_LOG_DEBUG("Partition %v has starting key %v",
                index + 1,
                partitionKeys[index].Key);
            Partitions.push_back(New<TPartition>(this, index + 1, partitionKeys[index].Key));
            if (partitionKeys[index].Maniac) {
                YT_LOG_DEBUG("Partition %v is a maniac", index + 1);
                Partitions.back()->Maniac = true;
            }
        }
    }

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }

    virtual EJobType GetPartitionJobType() const = 0;
    virtual EJobType GetIntermediateSortJobType() const = 0;
    virtual EJobType GetFinalSortJobType() const = 0;
    virtual EJobType GetSortedMergeJobType() const = 0;

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const = 0;
    virtual TUserJobSpecPtr GetSortUserJobSpec(bool isFinalSort) const = 0;
    virtual TUserJobSpecPtr GetSortedMergeUserJobSpec() const = 0;

    virtual int GetSortedMergeKeyColumnCount() const = 0;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TPartitionTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TSortControllerBase::TSortTask);
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
    { }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        switch (jobType) {
            case EJobType::Partition:
                return AsStringBuf("data_weight_per_partition_job");
            case EJobType::FinalSort:
                return AsStringBuf("partition_data_weight");
            default:
                YT_ABORT();
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
    TSamplesFetcherPtr SamplesFetcher;

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
        if (!table->Dynamic) {
            table->TableUploadOptions.LockMode = ELockMode::Exclusive;
        }
        table->TableWriterOptions->EvaluateComputedColumns = false;

        // Sort output MUST be sorted.
        table->TableWriterOptions->ExplodeOnValidationError = true;

        ValidateSchemaInferenceMode(Spec->SchemaInferenceMode);

        if ((table->Dynamic || table->TableUploadOptions.UpdateMode == EUpdateMode::Append) &&
            table->TableUploadOptions.TableSchema->GetKeyColumns() != Spec->SortBy)
        {
            THROW_ERROR_EXCEPTION("sort_by is different from output table key columns")
                << TErrorAttribute("output_table_path", Spec->OutputTablePath)
                << TErrorAttribute("output_table_key_columns", table->TableUploadOptions.TableSchema->GetKeyColumns())
                << TErrorAttribute("sort_by", Spec->SortBy);
        }

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(Spec->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
                    ValidateOutputSchemaCompatibility(true, true);
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(Spec->SortBy);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table->TableUploadOptions.TableSchema = TTableSchema::FromKeyColumns(Spec->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
                }
                break;

            default:
                YT_ABORT();
        }
    }

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        if (TotalEstimatedInputDataWeight == 0)
            return;

        if (TotalEstimatedInputDataWeight > Spec->MaxInputDataWeight) {
            THROW_ERROR_EXCEPTION("Failed to initialize sort operation, input data weight is too large")
                << TErrorAttribute("estimated_input_data_weight", TotalEstimatedInputDataWeight)
                << TErrorAttribute("max_input_data_weight", Spec->MaxInputDataWeight);
        }

        InitJobIOConfigs();

        std::vector<TPartitionKey> partitionKeys;

        if (Spec->PivotKeys.empty()) {
            auto samples = FetchSamples();

            // Use partition count provided by user, if given.
            // Otherwise use size estimates.
            int partitionCount = SuggestPartitionCount();
            YT_LOG_INFO("Suggested partition count %v, samples count %v", partitionCount, samples.size());

            // Don't create more partitions than we have samples (plus one).
            partitionCount = std::min(partitionCount, static_cast<int>(samples.size()) + 1);
            SimpleSort = (partitionCount == 1);

            auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
                Spec,
                Options,
                Logger,
                TotalEstimatedInputUncompressedDataSize,
                TotalEstimatedInputDataWeight,
                TotalEstimatedInputRowCount,
                InputCompressionRatio);

            // Finally adjust partition count wrt block size constraints.
            partitionCount = AdjustPartitionCountToWriterBufferSize(
                partitionCount,
                partitionJobSizeConstraints->GetJobCount(),
                PartitionJobIOConfig->TableWriter);

            YT_LOG_INFO("Adjusted partition count %v", partitionCount);

            YT_LOG_INFO("Building partition keys");

            PROFILE_TIMING ("/samples_processing_time") {
                if (!SimpleSort) {
                    partitionKeys = BuildPartitionKeysBySamples(
                        samples,
                        partitionCount,
                        partitionJobSizeConstraints,
                        static_cast<int>(Spec->SortBy.size()),
                        RowBuffer);
                }
            }
        } else {
            partitionKeys = BuildPartitionKeysByPivotKeys();
        }

        CreatePartitionsByPartitionKeys(partitionKeys);

        PreparePartitionTask();

        PrepareSortedMergeTask();

        PrepareSortTasks();

        PrepareUnorderedMergeTask();

        PrepareSimpleSortTask();

        InitJobSpecTemplates();
    }

    void PreparePartitionTask()
    {
        if (SimpleSort) {
            return;
        }

        InitShufflePool();

        auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);
        std::vector<TChunkStripePtr> stripes;

        TEdgeDescriptor shuffleEdgeDescriptor = GetIntermediateEdgeDescriptorTemplate();
        shuffleEdgeDescriptor.DestinationPool = ShufflePoolInput;
        shuffleEdgeDescriptor.ChunkMapping = ShuffleChunkMapping_;
        shuffleEdgeDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;
        shuffleEdgeDescriptor.TableUploadOptions.TableSchema = OutputTables_[0]->TableUploadOptions.TableSchema;
        PartitionTask = New<TPartitionTask>(this, std::vector<TEdgeDescriptor> {shuffleEdgeDescriptor});
        InitPartitionPool(partitionJobSizeConstraints, nullptr, false /* ordered */);
        RegisterTask(PartitionTask);
        ProcessInputs(PartitionTask, partitionJobSizeConstraints);
        FinishTaskInput(PartitionTask);

        YT_LOG_INFO("Sorting with partitioning (PartitionCount: %v, PartitionJobCount: %v, DataWeightPerPartitionJob: %v)",
            Partitions.size(),
            partitionJobSizeConstraints->GetJobCount(),
            partitionJobSizeConstraints->GetDataWeightPerJob());
    }

    void PrepareSimpleSortTask()
    {
        if (!SimpleSort) {
            return;
        }

        auto& partition = Partitions[0];

        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputDataWeight);

        std::vector<TChunkStripePtr> stripes;

        InitSimpleSortPool(jobSizeConstraints);

        SimpleSortTask = New<TSimpleSortTask>(this, partition.Get(), GetFinalEdgeDescriptors());
        SimpleSortTask->SetupJobCounters();
        RegisterTask(SimpleSortTask);

        partition->ChunkPoolOutput = SimpleSortPool;
        SortedMergeTask->SetInputVertex(FormatEnum(GetIntermediateSortJobType()));
        ProcessInputs(SimpleSortTask, jobSizeConstraints);

        FinishTaskInput(SimpleSortTask);

        YT_LOG_INFO("Sorting without partitioning (SortJobCount: %v, DataWeightPerJob: %v)",
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataWeightPerJob());

        if (IsSortedMergeNeeded(partition)) {
            YT_LOG_DEBUG("Partition needs sorted merge (Partition: %v)", partition->Index);
        }

        // Kick-start the sort task.
        SortStartThresholdReached = true;
        AddSortTasksPendingHints();
    }

    void PrepareUnorderedMergeTask()
    {
        if (SimpleSort) {
            return;
        }

        UnorderedMergeTask = New<TUnorderedMergeTask>(this, GetFinalEdgeDescriptors());
        RegisterTask(UnorderedMergeTask);
        UnorderedMergeTask->SetInputVertex(FormatEnum(GetPartitionJobType()));
        UnorderedMergeTask->RegisterInGraph();
    }

    std::vector<TSample> FetchSamples()
    {
        TFuture<void> asyncSamplesResult;
        PROFILE_TIMING ("/input_processing_time") {
            int sampleCount = SuggestPartitionCount() * Spec->SamplesPerPartition;

            FetcherChunkScraper = CreateFetcherChunkScraper();

            auto samplesRowBuffer = New<TRowBuffer>(
                TRowBufferTag(),
                Config->ControllerRowBufferChunkSize);

            SamplesFetcher = New<TSamplesFetcher>(
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
                if (!chunk->IsDynamicStore()) {
                    SamplesFetcher->AddChunk(chunk);
                }
            }
            for (const auto& chunk : CollectPrimaryVersionedChunks()) {
                if (!chunk->IsDynamicStore()) {
                    SamplesFetcher->AddChunk(chunk);
                }
            }

            SamplesFetcher->SetCancelableContext(GetCancelableContext());
            asyncSamplesResult = SamplesFetcher->Fetch();
        }

        WaitFor(asyncSamplesResult)
            .ThrowOnError();

        FetcherChunkScraper.Reset();

        PROFILE_TIMING ("/samples_processing_time") {
            return SamplesFetcher->GetSamples();
        }
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        IntermediateSortJobIOConfig = CloneYsonSerializable(Spec->SortJobIO);

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

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const override
    {
        return nullptr;
    }

    virtual TUserJobSpecPtr GetSortUserJobSpec(bool /*isFinalSort*/) const override
    {
        return nullptr;
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
            SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));

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

            if (SimpleSort) {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).GetData());
                SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));
            } else {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
                SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());
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
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).GetData());

            ToProto(mergeJobSpecExt->mutable_key_columns(), Spec->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
            auto* schedulerJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

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

        outputBufferSize += THorizontalBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());

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
        bool isFinalSort,
        const TChunkStripeStatistics& stat) const override
    {
        i64 jobProxyMemory =
            GetSortBuffersMemorySize(stat) +
            GetSortInputIOMemorySize(stat);

        if (isFinalSort) {
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig);
        } else {
            jobProxyMemory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
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
        const auto& jobCounter = GetDataFlowGraph()->GetTotalJobCounter();
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
            jobCounter->GetTotal(),
            jobCounter->GetRunning(),
            jobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            jobCounter->GetFailed(),
            jobCounter->GetAbortedTotal(),
            jobCounter->GetLost(),
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
    auto options = config->SortOperationOptions;
    auto spec = ParseOperationSpec<TSortOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TSortController>(spec, config, options, host, operation);
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
    { }

    virtual void BuildBriefSpec(TFluentMap fluent) const override
    {
        TSortControllerBase::BuildBriefSpec(fluent);
        fluent
            .DoIf(Spec->HasNontrivialMapper(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
                    .EndMap();
            })
            .Item("reducer").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Reducer->Command))
            .EndMap()
            .DoIf(Spec->HasNontrivialReduceCombiner(), [&] (TFluentMap fluent) {
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
                return AsStringBuf("data_weight_per_map_job");
            case EJobType::PartitionReduce:
            case EJobType::SortedReduce:
                return AsStringBuf("partition_data_weight");
           default:
                YT_ABORT();
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

        if (Spec->HasNontrivialMapper()) {
            ValidateUserFileCount(Spec->Mapper, "mapper");
        }
        ValidateUserFileCount(Spec->Reducer, "reducer");
        if (Spec->HasNontrivialReduceCombiner()) {
            ValidateUserFileCount(Spec->ReduceCombiner, "reduce combiner");
        }

        if (!CheckKeyColumnsCompatible(Spec->SortBy, Spec->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce columns %v are not compatible with sort columns %v",
                Spec->ReduceBy,
                Spec->SortBy);
        }

        YT_LOG_DEBUG("ReduceColumns: %v, SortColumns: %v",
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

    virtual std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriter;
    }

    virtual std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriter;
    }

    virtual bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec->EnableCudaGpuCoreDump;
    }

    virtual std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        std::vector<TUserJobSpecPtr> result = {Spec->Reducer};
        if (Spec->HasNontrivialMapper()) {
            result.emplace_back(Spec->Mapper);
        }
        if (Spec->HasNontrivialReduceCombiner()) {
            result.emplace_back(Spec->ReduceCombiner);
        }

        return result;
    }

    virtual void CustomPrepare() override
    {
        TSortControllerBase::CustomPrepare();

        if (TotalEstimatedInputDataWeight == 0)
            return;

        MapperFiles = UserJobFiles_[Spec->Mapper];
        ReduceCombinerFiles = UserJobFiles_[Spec->ReduceCombiner];
        ReducerFiles = UserJobFiles_[Spec->Reducer];

        InitJobIOConfigs();
        InitEdgeDescriptors();

        // Use partition count provided by user, if given.
        // Otherwise use size estimates.
        int partitionCount = SuggestPartitionCount();
        YT_LOG_INFO("Suggested partition count %v", partitionCount);

        Spec->Sampling->MaxTotalSliceCount = Spec->Sampling->MaxTotalSliceCount.value_or(Config->MaxTotalSliceCount);

        auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);

        partitionCount = AdjustPartitionCountToWriterBufferSize(
            partitionCount,
            partitionJobSizeConstraints->GetJobCount(),
            PartitionJobIOConfig->TableWriter);
        YT_LOG_INFO("Adjusted partition count %v", partitionCount);

        PROFILE_TIMING ("/input_processing_time") {
            if (Spec->PivotKeys.empty()) {
                BuildHashReducePartition(partitionCount);
            } else {
                CreatePartitionsByPartitionKeys(BuildPartitionKeysByPivotKeys());
            }
        }

        PreparePartitionTask(partitionJobSizeConstraints);

        PrepareSortedMergeTask();

        PrepareSortTasks();

        InitJobSpecTemplates();
    }

    void PreparePartitionTask(const IJobSizeConstraintsPtr& partitionJobSizeConstraints)
    {
        InitShufflePool();

        std::vector<TChunkStripePtr> stripes;

        std::vector<TEdgeDescriptor> partitionEdgeDescriptors;

        // Primary edge descriptor for shuffled output of the mapper.
        TEdgeDescriptor shuffleEdgeDescriptor = GetIntermediateEdgeDescriptorTemplate();
        shuffleEdgeDescriptor.DestinationPool = ShufflePoolInput;
        shuffleEdgeDescriptor.ChunkMapping = ShuffleChunkMapping_;
        shuffleEdgeDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;

        partitionEdgeDescriptors.emplace_back(std::move(shuffleEdgeDescriptor));
        partitionEdgeDescriptors.insert(
            partitionEdgeDescriptors.end(),
            MapperSinkEdges_.begin(),
            MapperSinkEdges_.end());

        PartitionTask = New<TPartitionTask>(this, std::move(partitionEdgeDescriptors));

        InitPartitionPool(
            partitionJobSizeConstraints,
            Config->EnablePartitionMapJobSizeAdjustment && !Spec->Ordered
            ? Options->PartitionJobSizeAdjuster
            : nullptr,
            Spec->Ordered);

        ProcessInputs(PartitionTask, partitionJobSizeConstraints);
        RegisterTask(PartitionTask);
        FinishTaskInput(PartitionTask);

        YT_LOG_INFO("Map-reducing with partitioning (PartitionCount: %v, PartitionJobCount: %v, PartitionDataWeightPerJob: %v)",
            Partitions.size(),
            partitionJobSizeConstraints->GetJobCount(),
            partitionJobSizeConstraints->GetDataWeightPerJob());
    }

    void BuildHashReducePartition(int partitionCount)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        // This is not a typo!

        PartitionJobIOConfig = Spec->PartitionJobIO;
        IntermediateSortJobIOConfig = Spec->SortJobIO;

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        FinalSortJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
    }

    virtual EJobType GetPartitionJobType() const override
    {
        return Spec->HasNontrivialMapper() ? EJobType::PartitionMap : EJobType::Partition;
    }

    virtual EJobType GetIntermediateSortJobType() const override
    {
        return Spec->HasNontrivialReduceCombiner() ? EJobType::ReduceCombiner : EJobType::IntermediateSort;
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
            SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));

            if (Spec->InputQuery) {
                WriteInputQueryToJobSpec(schedulerJobSpecExt);
            }

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).GetData());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_partition_count(Partitions.size());
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            if (!Spec->PivotKeys.empty()) {
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

            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), Spec->SortBy);

            if (Spec->HasNontrivialMapper()) {
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
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).GetData());

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).GetData());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

            if (Spec->HasNontrivialReduceCombiner()) {
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
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

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
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

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

    virtual ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec->EnableLegacyLivePreview);
    }

    virtual ELegacyLivePreviewMode GetLegacyIntermediateLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec->EnableLegacyLivePreview);
    }

    virtual bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    // Resource management.

    virtual TCpuResource GetPartitionCpuLimit() const override
    {
        return Spec->HasNontrivialMapper() ? Spec->Mapper->CpuLimit : 1;
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

        i64 reserveSize = THorizontalBlockWriter::MaxReserveSize * static_cast<i64>(Partitions.size());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(Partitions.size()),
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        if (Spec->HasNontrivialMapper()) {
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
        YT_ABORT();
    }

    virtual bool IsSortedMergeNeeded(const TPartitionPtr& partition) const override
    {
        if (Spec->ForceReduceCombiners && !partition->CachedSortedMergeNeeded) {
            partition->CachedSortedMergeNeeded = true;
            SortedMergeTask->RegisterPartition(partition);
            IntermediateSortTask->RegisterPartition(partition);
        }
        return TSortControllerBase::IsSortedMergeNeeded(partition);
    }

    virtual TUserJobSpecPtr GetSortUserJobSpec(bool isFinalSort) const override
    {
        if (isFinalSort) {
            return Spec->Reducer;
        } else if (Spec->HasNontrivialReduceCombiner()) {
            return Spec->ReduceCombiner;
        } else {
            return nullptr;
        }
    }

    virtual TExtendedJobResources GetPartitionSortResources(
        bool isFinalSort,
        const TChunkStripeStatistics& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);

        i64 memory =
            GetSortInputIOMemorySize(stat) +
            GetSortBuffersMemorySize(stat);

        if (isFinalSort) {
            result.SetCpu(Spec->Reducer->CpuLimit);
            memory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig);
            result.SetJobProxyMemory(memory);
        } else if (Spec->HasNontrivialReduceCombiner()) {
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
        YT_ABORT();
    }

    // Progress reporting.

    virtual TString GetLoggingProgress() const override
    {
        const auto& jobCounter = GetDataFlowGraph()->GetTotalJobCounter();
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v, L: %v}, "
            "Partitions = {T: %v, C: %v}, "
            "MapJobs = %v, "
            "SortJobs = %v, "
            "PartitionReduceJobs = %v, "
            "SortedReduceJobs = %v, "
            "UnavailableInputChunks: %v",
            // Jobs
            jobCounter->GetTotal(),
            jobCounter->GetRunning(),
            jobCounter->GetCompletedTotal(),
            GetPendingJobCount(),
            jobCounter->GetFailed(),
            jobCounter->GetAbortedTotal(),
            jobCounter->GetLost(),
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
            .Item(Spec->HasNontrivialMapper() ? "map_jobs" : "partition_jobs").Value(GetPartitionJobCounter())
            .Item(Spec->HasNontrivialReduceCombiner() ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
    }

    virtual TUserJobSpecPtr GetPartitionUserJobSpec() const override
    {
        return Spec->HasNontrivialMapper() ? Spec->Mapper : nullptr;
    }

    virtual int GetSortedMergeKeyColumnCount() const override
    {
        return static_cast<int>(Spec->ReduceBy.size());
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
    auto options = config->MapReduceOperationOptions;
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    return New<TMapReduceController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
