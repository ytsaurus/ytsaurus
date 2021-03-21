#include "sort_controller.h"

#include "chunk_pool_adapters.h"
#include "data_balancer.h"
#include "job_info.h"
#include "job_memory.h"
#include "helpers.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "unordered_controller.h"

#include <yt/yt/server/controller_agent/chunk_list_pool.h>
#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/scheduling_context.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/shuffle_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/key_set.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/job_tracker_client/statistics.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <cmath>
#include <algorithm>

namespace NYT::NControllerAgent::NControllers {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkPools;
using namespace NTableClient;
using namespace NLogging;
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

using NTableClient::TLegacyKey;
using NNodeTrackerClient::TNodeId;

using NScheduler::NProto::TPartitionJobSpecExt;
using NScheduler::NProto::TReduceJobSpecExt;
using NScheduler::NProto::TSortJobSpecExt;
using NScheduler::NProto::TMergeJobSpecExt;
using NScheduler::NProto::TSchedulerJobSpecExt;
using NScheduler::NProto::TSchedulerJobResultExt;

////////////////////////////////////////////////////////////////////////////////

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
        Persist(context, PartitionJobCounter);
        Persist(context, SortedMergeJobCounter);
        Persist(context, UnorderedMergeJobCounter);
        Persist(context, IntermediateSortJobCounter);
        Persist(context, FinalSortJobCounter);
        Persist(context, SortDataWeightCounter);

        Persist(context, SortStartThresholdReached);
        Persist(context, MergeStartThresholdReached);

        Persist(context, TotalOutputRowCount);

        Persist(context, SimpleSort);
        Persist(context, PartitionsByLevels);
        Persist(context, PartitionTreeDepth);
        Persist(context, PartitionCount);
        Persist(context, MaxPartitionFactor);

        Persist(context, AssignedPartitionsByNodeId);
        Persist(context, PartitionsLocalityByNodeId);

        Persist(context, RootPartitionJobSpecTemplate);
        Persist(context, PartitionJobSpecTemplate);
        Persist(context, IntermediateSortJobSpecTemplate);
        Persist(context, FinalSortJobSpecTemplate);
        Persist(context, SortedMergeJobSpecTemplate);
        Persist(context, UnorderedMergeJobSpecTemplate);

        Persist(context, RootPartitionJobIOConfig);
        Persist(context, PartitionJobIOConfig);
        Persist(context, IntermediateSortJobIOConfig);
        Persist(context, FinalSortJobIOConfig);
        Persist(context, SortedMergeJobIOConfig);
        Persist(context, UnorderedMergeJobIOConfig);

        Persist(context, RootPartitionPoolJobSizeConstraints);
        Persist(context, RootPartitionPool);
        Persist(context, SimpleSortPool);

        Persist(context, ShuffleMultiChunkPoolInputs);
        Persist(context, ShuffleMultiInputChunkMappings);

        Persist(context, PartitionTasks);
        Persist(context, SimpleSortTask);
        Persist(context, IntermediateSortTask);
        Persist(context, FinalSortTask);
        Persist(context, UnorderedMergeTask);
        Persist(context, SortedMergeTask);

        if (context.IsLoad()) {
            SetupPartitioningCompletedCallbacks();
        }
    }

private:
    TSortOperationSpecBasePtr Spec;

protected:
    TSortOperationOptionsBasePtr Options;

    // Counters.
    int CompletedPartitionCount;
    TProgressCounterPtr PartitionJobCounter = New<TProgressCounter>();
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

    // Hierarchical partitions data model.
    //
    // Both Sort and MapReduce operations consist of two phases:
    // during the first phase some data (input data for Sort and Map result for MapReduce)
    // is split into groups called partitions which can be processed independently
    // and during the second phase some of the partitions are processed.
    // For Sort operation partition is a group of rows forming some contiguous interval of keys.
    // For MapReduce operation partition is a group of rows with same key hash.
    //
    // Partitions form tree in which root is a fake partition consisting of whole input data
    // and children of a partition are smaller partitions such that every row from parent partition
    // belongs to exactly one child partition.
    // Partitions that have no children are called final. Other partitions are called intermediate.
    // Sort operation sorts each final partition independently and then joins sorted final
    // partitions in proper order.
    // MapReduce operation reduces each final partition independently and then joins reduce
    // results in proper order.
    //
    // Level of a partition is a distance from the root of partition tree to partition.
    // For example, root partition is 0 level partition. In classical MapReduce implementation
    // all the final partitions are 1 level partitions.
    // For a sake of convenience all final partitions have the same level. This level is called partition tree depth.
    //
    // Partition task of i-th level is a task which takes all the i-th level partitions as a input
    // and splits them into (i+1)-th level partitions.
    // Partition task is called final if its outputs are final partitions. Other partition tasks are called intermediate.
    // Partitions task of level 0 is called root partition task.
    // For MapReduce operations root partition task is a partition map task, and all other tasks are partition tasks.
    // For Sort operations all the tasks are partition tasks.
    //
    // Each partition has some indices associated with it. Partition has parent partition tag equals k iff it is k-th
    // children of its parent. Parent partition tag is not defined for root partition.
    // Partition index is an index of a partition among the partitions at the same level.
    // That is, partition is uniquely defined by its level and index.
    //
    // For Sort operation partition with smaller partition index has keys that are not greater than keys
    // of a partition (at the same level) with greater partition index.
    // Results of per-partition sorts of final partitions are joined in order of partition indices.
    struct TPartition
        : public TRefCounted
    {
        //! For persistence only.
        TPartition() = default;

        TPartition(
            TSortControllerBase* controller,
            int level,
            int index)
            : Controller(controller)
            , Level(level)
            , Index(index)
            , Logger(controller->Logger)
        { }

        TSortControllerBase* Controller = nullptr;

        int Level = -1;

        int Index = -1;

        std::optional<int> ParentPartitionTag;

        std::vector<TIntrusivePtr<TPartition>> Children;

        //! Lower key bound of this partition.
        //! Always null for map-reduce operation.
        TKeyBound LowerBound;

        //! Is partition completed?
        bool Completed = false;

        //! Is all data corresponding to this partition in chunk pool output?
        bool PartitioningCompleted = false;

        //! Do we need to run merge tasks for this partition?
        //! Cached value, updated by #IsSortedMergeNeeded.
        bool CachedSortedMergeNeeded = false;

        //! Does the partition consist of rows with the same key?
        //! This field is set only in Sort operations and only in final partitions.
        bool Maniac = false;

        //! Chunk pool that moves data from this partition to children partitions.
        IShuffleChunkPoolPtr ShuffleChunkPool;

        //! Input of shuffle chunk pool wrapped into intermediate live preview adapter.
        IChunkPoolInputPtr ShuffleChunkPoolInput;

        //! Chunk pool containing data of this partition.
        //! For root partition it is either partition pool or simple sort pool.
        //! For non-root partitions it's an output of parent partition's shuffle pool.
        IChunkPoolOutputPtr ChunkPoolOutput;

        TLogger Logger;

        void SetAssignedNodeId(TNodeId nodeId)
        {
            // Locality is defined for final partitions only.
            YT_VERIFY(IsFinal());

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
            // Locality is defined for final partitions only.
            YT_VERIFY(IsFinal());

            auto& localityMap = Controller->PartitionsLocalityByNodeId[nodeId];
            localityMap[Index] += delta;
            YT_VERIFY(localityMap[Index] >= 0);
            if (localityMap[Index] == 0) {
                YT_VERIFY(localityMap.erase(Index) == 1);
            }
        }

        bool IsRoot() const
        {
            return Level == 0;
        }

        bool IsFinal() const
        {
            return Level == Controller->PartitionTreeDepth;
        }

        bool IsIntermediate() const
        {
            return !IsFinal();
        }

        //! Called when all of the partition data is in the
        //! chunk pool output.
        void OnPartitioningCompleted()
        {
            // This function can be called multiple times due the lost jobs
            // regeneration. We are interested in the first call only.
            if (PartitioningCompleted) {
                return;
            }

            PartitioningCompleted = true;

            if (IsIntermediate()) {
                i64 dataWeight = ChunkPoolOutput->GetDataWeightCounter()->GetTotal();

                YT_LOG_DEBUG("Intermediate partition data weight collected (PartitionLevel: %v, PartitionIndex: %v, PartitionDataWeight: %v)",
                    Level,
                    Index,
                    dataWeight);
            } else {
                i64 dataWeight = ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
                if (dataWeight == 0) {
                    YT_LOG_DEBUG("Final partition is empty (FinalPartitionIndex: %v)", Index);
                    Controller->OnFinalPartitionCompleted(this);
                } else {
                    YT_LOG_DEBUG("Final partition is ready for processing (FinalPartitionIndex: %v, DataWeight: %v)",
                        Index,
                        dataWeight);
                    if (!Maniac && !Controller->IsSortedMergeNeeded(this)) {
                        Controller->FinalSortTask->RegisterPartition(this);
                    }

                    if (Maniac) {
                        Controller->UnorderedMergeTask->RegisterPartition(this);
                    }
                }
            }
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Controller);

            Persist(context, Level);
            Persist(context, Index);
            Persist(context, ParentPartitionTag);

            Persist(context, Children);

            Persist(context, LowerBound);

            Persist(context, Completed);

            Persist(context, PartitioningCompleted);

            Persist(context, CachedSortedMergeNeeded);

            Persist(context, Maniac);

            Persist(context, AssignedNodeId);

            Persist(context, ShuffleChunkPool);
            Persist(context, ShuffleChunkPoolInput);
            Persist(context, ChunkPoolOutput);

            Persist(context, Logger);
        }

    private:
        //! The node assigned to this partition, #InvalidNodeId if none.
        TNodeId AssignedNodeId = InvalidNodeId;
    };

    typedef TIntrusivePtr<TPartition> TPartitionPtr;

    //! Equivalent to |Partitions.size() == 1| but enables checking
    //! for simple sort when #Partitions is still being constructed.
    bool SimpleSort;

    //! PartitionsByLevels[level][index] is a partition with corresponding
    //! level and index.
    std::vector<std::vector<TPartitionPtr>> PartitionsByLevels;

    int PartitionTreeDepth = 0;

    int PartitionCount = 0;

    int MaxPartitionFactor = 0;

    // Locality stuff.

    struct TLocalityEntry
    {
        int PartitionIndex;

        i64 Locality;
    };

    //! NodeId -> set of final partition indices assigned to it.
    THashMap<TNodeId, THashSet<int>> AssignedPartitionsByNodeId;

    //! NodeId -> map<final partition index, locality>.
    THashMap<TNodeId, THashMap<int, i64>> PartitionsLocalityByNodeId;

    //! Spec templates for starting new jobs.
    TJobSpec RootPartitionJobSpecTemplate;
    TJobSpec PartitionJobSpecTemplate;
    TJobSpec IntermediateSortJobSpecTemplate;
    TJobSpec FinalSortJobSpecTemplate;
    TJobSpec SortedMergeJobSpecTemplate;
    TJobSpec UnorderedMergeJobSpecTemplate;

    //! IO configs for various job types.
    TJobIOConfigPtr RootPartitionJobIOConfig;
    TJobIOConfigPtr PartitionJobIOConfig;
    TJobIOConfigPtr IntermediateSortJobIOConfig;
    TJobIOConfigPtr FinalSortJobIOConfig;
    TJobIOConfigPtr SortedMergeJobIOConfig;
    TJobIOConfigPtr UnorderedMergeJobIOConfig;

    IJobSizeConstraintsPtr RootPartitionPoolJobSizeConstraints;
    IChunkPoolPtr RootPartitionPool;
    IChunkPoolPtr SimpleSortPool;

    //! i-th element is a multi chunk pool built over the inputs of i-th level partitions' shuffle chunk pools.
    std::vector<IMultiChunkPoolInputPtr> ShuffleMultiChunkPoolInputs;

    //! i-th elemet is a input chunk mapping for i-th shuffle multi chunk pool input.
    std::vector<TInputChunkMappingPtr> ShuffleMultiInputChunkMappings;

    //! i-th element is a partition task of i-th level.
    std::vector<TPartitionTaskPtr> PartitionTasks;

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

        TPartitionTask(
            TSortControllerBase* controller,
            std::vector<TStreamDescriptor> streamDescriptors,
            int level)
            : TTask(controller, std::move(streamDescriptors))
            , Controller_(controller)
            , Level_(level)
        {
            if (!IsRoot()) {
                ShuffleMultiChunkOutput_ = Controller_->CreateLevelMultiChunkPoolOutput(Level_);
            }

            GetChunkPoolOutput()->GetJobCounter()->AddParent(Controller_->PartitionJobCounter);

            WirePartitionKeys_.reserve(GetInputPartitions().size());
            WirePartitionLowerBoundPrefixes_.reserve(GetInputPartitions().size());
            PartitionLowerBoundInclusivenesses_.reserve(GetInputPartitions().size());
            for (const auto& inputPartition : GetInputPartitions()) {
                auto keySetWriter = New<TKeySetWriter>();
                auto partitionLowerBoundPrefixesWriter = New<TKeySetWriter>();
                std::vector<bool> partitionLowerBoundInclusivenesses;

                int keysWritten = 0;
                for (int childPartitionIndex = 1; childPartitionIndex < inputPartition->Children.size(); ++childPartitionIndex) {
                    const auto& childPartition = inputPartition->Children[childPartitionIndex];
                    auto lowerBound = childPartition->LowerBound;
                    if (lowerBound && !lowerBound.IsUniversal()) {
                        keySetWriter->WriteKey(KeyBoundToLegacyRow(lowerBound));
                        partitionLowerBoundPrefixesWriter->WriteKey(lowerBound.Prefix);
                        partitionLowerBoundInclusivenesses.push_back(lowerBound.IsInclusive);
                        ++keysWritten;
                    }
                }
                YT_VERIFY(keysWritten == 0 || keysWritten + 1 == inputPartition->Children.size());
                if (keysWritten == 0) {
                    WirePartitionKeys_.push_back(std::nullopt);
                    WirePartitionLowerBoundPrefixes_.push_back(std::nullopt);
                    PartitionLowerBoundInclusivenesses_.push_back({});
                } else {
                    WirePartitionKeys_.push_back(ToString(keySetWriter->Finish()));
                    WirePartitionLowerBoundPrefixes_.push_back(ToString(partitionLowerBoundPrefixesWriter->Finish()));
                    PartitionLowerBoundInclusivenesses_.push_back(partitionLowerBoundInclusivenesses);
                }
            }
        }

        virtual TString GetTitle() const override
        {
            return Format("Partition(%v)", Level_);
        }

        virtual TString GetVertexDescriptor() const override
        {
            return Format("%v(%v)", TTask::GetVertexDescriptor(), Level_);
        }

        virtual void FinishInput() override
        {
            // NB: we try to use the value as close to the total data weight of all extracted stripe lists as possible.
            // In particular, we do not use Controller->TotalEstimatedInputDataWeight here.
            auto totalDataWeight = GetChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
            if (Controller_->Spec->EnablePartitionedDataBalancing &&
                totalDataWeight >= Controller_->Spec->MinLocalityInputDataWeight)
            {
                YT_LOG_INFO("Data balancing enabled (TotalDataWeight: %v)", totalDataWeight);
                DataBalancer_ = New<TDataBalancer>(
                    Controller_->Options->DataBalancer,
                    totalDataWeight,
                    Controller_->GetOnlineExecNodeDescriptors(),
                    Logger);
            }

            TTask::FinishInput();
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            // Locality is not defined for non-root partition tasks since their inputs are shuffled.
            if (!IsRoot()) {
                return TDuration::Zero();
            }

            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec->PartitionLocalityTimeout
                : TDuration::Zero();
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetPartitionResources(joblet->InputStripeList->GetStatistics(), IsRoot());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInputPtr GetChunkPoolInput() const override
        {
            if (IsRoot()) {
                return Controller_->RootPartitionPool;
            } else {
                return Controller_->ShuffleMultiChunkPoolInputs[Level_ - 1];
            }
        }

        virtual IChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            if (IsRoot()) {
                return Controller_->RootPartitionPool;
            } else {
                return ShuffleMultiChunkOutput_;
            }
        }

        virtual TInputChunkMappingPtr GetChunkMapping() const override
        {
            if (IsRoot()) {
                return TTask::GetChunkMapping();
            } else {
                return Controller_->ShuffleMultiInputChunkMappings[Level_ - 1];
            }
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetPartitionUserJobSpec(IsRoot());
        }

        virtual EJobType GetJobType() const override
        {
            return Controller_->GetPartitionJobType(IsRoot());
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller_);
            Persist(context, DataBalancer_);
            Persist(context, Level_);
            Persist(context, ShuffleMultiChunkOutput_);
            Persist(context, WirePartitionKeys_);
            Persist(context, WirePartitionLowerBoundPrefixes_);
            Persist(context, PartitionLowerBoundInclusivenesses_);

            if (context.IsLoad() && DataBalancer_) {
                DataBalancer_->OnExecNodesUpdated(Controller_->GetOnlineExecNodeDescriptors());
            }
        }

        void OnExecNodesUpdated()
        {
            if (DataBalancer_) {
                DataBalancer_->OnExecNodesUpdated(Controller_->GetOnlineExecNodeDescriptors());
            }
        }

        void PropagatePartitions(
            const std::vector<TStreamDescriptor>& streamDescriptors,
            const TChunkStripeListPtr& inputStripeList,
            std::vector<TChunkStripePtr>* outputStripes)
        {
            TTask::PropagatePartitions(streamDescriptors, inputStripeList, outputStripes);

            auto& shuffleStripe = outputStripes->front();
            shuffleStripe->PartitionTag = GetPartitionIndex(inputStripeList);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionTask, 0x63a4c761);

        TSortControllerBase* Controller_ = nullptr;

        TDataBalancerPtr DataBalancer_;

        int Level_ = -1;

        IMultiChunkPoolOutputPtr ShuffleMultiChunkOutput_;

        //! Partition index -> wire partition keys.
        std::vector<std::optional<TString>> WirePartitionKeys_;

        //! Partition index -> wire partition lower key bound prefixes.
        std::vector<std::optional<TString>> WirePartitionLowerBoundPrefixes_;

        //! Partition index -> partition lower bound inclusivenesses.
        std::vector<std::vector<bool>> PartitionLowerBoundInclusivenesses_;

        bool IsFinal() const
        {
            return Level_ == Controller_->PartitionTreeDepth - 1;
        }

        bool IsIntermediate() const
        {
            return !IsFinal();
        }

        bool IsRoot() const
        {
            return Level_ == 0;
        }

        const TPartitionTaskPtr& GetNextPartitionTask() const
        {
            YT_VERIFY(IsIntermediate());

            return Controller_->PartitionTasks[Level_ + 1];
        }

        const std::vector<TPartitionPtr>& GetInputPartitions() const
        {
            return Controller_->PartitionsByLevels[Level_];
        }

        const std::vector<TPartitionPtr>& GetOutputPartitions() const
        {
            return Controller_->PartitionsByLevels[Level_ + 1];
        }

        int GetPartitionIndex(const TChunkStripeListPtr& chunkStripeList)
        {
            if (IsRoot()) {
                return 0;
            } else {
                return *chunkStripeList->PartitionTag;
            }
        }

        virtual bool CanLoseJobs() const override
        {
            return Controller_->Spec->EnableIntermediateOutputRecalculation;
        }

        virtual std::optional<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context) override
        {
            // We don't have a job at hand here, let's make a guess.
            auto approximateStatistics = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            if (approximateStatistics.empty()) {
                return std::nullopt;
            }

            const auto& node = context->GetNodeDescriptor();

            if (DataBalancer_ && !DataBalancer_->CanScheduleJob(node, approximateStatistics.front().DataWeight)) {
                return EScheduleJobFailReason::DataBalancingViolation;
            }

            return std::nullopt;
        }

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto statistics = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            auto resources = Controller_->GetPartitionResources(statistics, IsRoot());
            AddFootprintAndUserJobResources(resources);

            return resources;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            if (IsRoot()) {
                jobSpec->CopyFrom(Controller_->RootPartitionJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller_->PartitionJobSpecTemplate);
            }

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            const auto& partition = GetInputPartitions()[partitionIndex];

            auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            if (auto parentPartitionTag = partition->ParentPartitionTag) {
                schedulerJobSpecExt->set_partition_tag(*parentPartitionTag);
            }

            auto* partitionJobSpecExt = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_partition_count(partition->Children.size());
            if (auto wirePartitionKeys = WirePartitionKeys_[partitionIndex]) {
                partitionJobSpecExt->set_wire_partition_keys(*wirePartitionKeys);
                partitionJobSpecExt->set_wire_partition_lower_bound_prefixes(*WirePartitionLowerBoundPrefixes_[partitionIndex]);
                ToProto(partitionJobSpecExt->mutable_partition_lower_bound_inclusivenesses(), PartitionLowerBoundInclusivenesses_[partitionIndex]);
            }
            partitionJobSpecExt->set_partition_task_level(Level_);

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
            // COMPAT(gritukan, levysotsky): Remove it when nodes will be fresh enough.
            auto* jobResult = &jobSummary.Result;
            auto* schedulerJobResultExt = jobResult->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            auto* outputChunkSpecs = schedulerJobResultExt->mutable_output_chunk_specs();
            for (int chunkSpecIndex = 0; chunkSpecIndex < outputChunkSpecs->size(); ++chunkSpecIndex) {
                auto* chunkSpec = outputChunkSpecs->Mutable(chunkSpecIndex);
                if (chunkSpec->table_index() == -1) {
                    chunkSpec->set_table_index(0);
                }
            }

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            if (IsIntermediate()) {
                Controller_->UpdateTask(GetNextPartitionTask());
            } else if (Controller_->SimpleSort) {
                Controller_->UpdateTask(Controller_->SimpleSortTask);
            } else {
                if (Controller_->UnorderedMergeTask) {
                    Controller_->UpdateTask(Controller_->UnorderedMergeTask);
                }
                if (Controller_->IntermediateSortTask) {
                    Controller_->UpdateTask(Controller_->IntermediateSortTask);
                }
                if (Controller_->FinalSortTask) {
                    Controller_->UpdateTask(Controller_->FinalSortTask);
                }
            }

            // NB: don't move it to OnTaskCompleted since jobs may run after the task has been completed.
            // Kick-start sort and unordered merge tasks.
            Controller_->CheckSortStartThreshold();
            Controller_->CheckMergeStartThreshold();

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            const auto& partition = Controller_->PartitionsByLevels[Level_][partitionIndex];
            const auto& shuffleChunkPool = partition->ShuffleChunkPool;
            if (shuffleChunkPool->GetTotalDataSliceCount() > Controller_->Spec->MaxShuffleDataSliceCount) {
                Controller_->OnOperationFailed(TError("Too many data slices in shuffle pool, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_data_slice_count", shuffleChunkPool->GetTotalDataSliceCount())
                    << TErrorAttribute("max_shuffle_data_slice_count", Controller_->Spec->MaxShuffleDataSliceCount));
            }

            if (shuffleChunkPool->GetTotalJobCount() > Controller_->Spec->MaxShuffleJobCount) {
                Controller_->OnOperationFailed(TError("Too many shuffle jobs, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_job_count", shuffleChunkPool->GetTotalJobCount())
                    << TErrorAttribute("max_shuffle_job_count", Controller_->Spec->MaxShuffleJobCount));
            }

            return result;
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(completedJob->NodeDescriptor, -completedJob->DataWeight);
            }

            if (!Controller_->IsShuffleCompleted()) {
                // Update task if shuffle is in progress and some partition jobs were lost.
                Controller_->UpdateTask(this);
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

            for (const auto& partition : GetInputPartitions()) {
                partition->ShuffleChunkPoolInput->Finish();
            }

            if (IsIntermediate()) {
                const auto& nextPartitionTask = GetNextPartitionTask();
                nextPartitionTask->FinishInput();
                Controller_->UpdateTask(nextPartitionTask);
            } else {
                if (Controller_->FinalSortTask) {
                    Controller_->FinalSortTask->Finalize();
                    Controller_->FinalSortTask->FinishInput();
                }

                if (Controller_->IntermediateSortTask) {
                    Controller_->IntermediateSortTask->Finalize();
                    Controller_->IntermediateSortTask->FinishInput();
                }

                if (Controller_->UnorderedMergeTask) {
                    Controller_->UnorderedMergeTask->FinishInput();
                    Controller_->UnorderedMergeTask->Finalize();
                }

                if (Controller_->SortedMergeTask) {
                    Controller_->SortedMergeTask->Finalize();
                }

                Controller_->ValidateMergeDataSliceLimit();

                if (DataBalancer_) {
                    DataBalancer_->LogStatistics();
                }

                Controller_->AssignPartitions();

                // NB: this is required at least to mark tasks completed, when there are no pending jobs.
                // This couldn't have been done earlier since we've just finished populating shuffle pool.
                Controller_->CheckSortStartThreshold();
                Controller_->CheckMergeStartThreshold();

                {
                    int twoPhasePartitionCount = 0;
                    int threePhasePartitionCount = 0;
                    for (const auto& partition : GetOutputPartitions()) {
                        if (Controller_->IsSortedMergeNeeded(partition)) {
                            ++threePhasePartitionCount;
                        } else {
                            ++twoPhasePartitionCount;
                        }
                    }

                    YT_LOG_DEBUG("Partitioning completed "
                        "(PartitionCount: %v, TwoPhasePartitionCount: %v, ThreePhasePartitionCount: %v)",
                        GetOutputPartitions().size(),
                        twoPhasePartitionCount,
                        threePhasePartitionCount);
                }
            }
        }

        virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            config->EnableJobSplitting &=
                (IsJobInterruptible() &&
                Controller_->InputTables_.size() <= Controller_->Options->JobSplitter->MaxInputTableCount);

            return config;
        }

        virtual bool IsJobInterruptible() const override
        {
            return false;
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

        TSortTaskBase(TSortControllerBase* controller, std::vector<TStreamDescriptor> streamDescriptors, bool isFinalSort)
            : TTask(controller, std::move(streamDescriptors))
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
            const TStreamDescriptor& descriptor) override
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

        virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // Sort jobs are unsplittable.
            config->EnableJobSplitting = false;

            return config;
        }

        virtual bool IsJobInterruptible() const override
        {
            return false;
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
            auto stat = AggregateStatistics(GetChunkPoolOutput()->GetApproximateStripeStatistics());
            YT_VERIFY(stat.size() == 1);
            auto result = GetNeededResourcesForChunkStripe(stat.front());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

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
                auto partitionTag = *Controller_->GetFinalPartition(*partitionIndex)->ParentPartitionTag;
                auto jobType = GetJobType();
                if (jobType == EJobType::PartitionReduce || jobType == EJobType::ReduceCombiner) {
                    auto* reduceJobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                    schedulerJobSpecExt->set_partition_tag(partitionTag);
                    reduceJobSpecExt->set_partition_tag(partitionTag);
                } else {
                    auto* sortJobSpecExt = jobSpec->MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                    schedulerJobSpecExt->set_partition_tag(partitionTag);
                    sortJobSpecExt->set_partition_tag(partitionTag);
                }
            }
        }

        virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            // COMPAT(gritukan, levysotsky): Remove it when nodes will be fresh enough.
            auto* jobResult = &jobSummary.Result;
            auto* schedulerJobResultExt = jobResult->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            auto* outputChunkSpecs = schedulerJobResultExt->mutable_output_chunk_specs();
            for (int chunkSpecIndex = 0; chunkSpecIndex < outputChunkSpecs->size(); ++chunkSpecIndex) {
                auto* chunkSpec = outputChunkSpecs->Mutable(chunkSpecIndex);
                if (chunkSpec->table_index() == -1) {
                    chunkSpec->set_table_index(0);
                }
            }

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
                    // NB: intermediate sort uses sort_by as a prefix, while pool expects reduce_by as a prefix.
                    auto keyColumnCount = Controller_->GetSortedMergeSortColumns().size();
                    SetLimitsFromShortenedBoundaryKeys(dataSlice, keyColumnCount, Controller_->RowBuffer);
                    // Transform data slice to legacy if legacy sorted pool is used in sorted merge.
                    if (!Controller_->Spec->UseNewSortedPool) {
                        dataSlice->TransformToLegacy(Controller_->RowBuffer);
                    }
                    dataSlice->InputStreamIndex = inputStreamIndex;
                }

                if (Controller_->SimpleSort) {
                    stripe->PartitionTag = 0;
                } else {
                    stripe->PartitionTag = joblet->InputStripeList->PartitionTag;
                }

                RegisterStripe(
                    stripe,
                    StreamDescriptors_[0],
                    joblet);
            }

            Controller_->CheckMergeStartThreshold();

            if (!IsFinalSort_) {
                Controller_->UpdateTask(Controller_->SortedMergeTask);
            }

            return result;
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            TTask::OnJobLost(completedJob);

            Controller_->UpdateTask(this);
            if (!Controller_->SimpleSort) {
                if (const auto& partitionTask = Controller_->GetFinalPartitionTask()) {
                    Controller_->UpdateTask(partitionTask);
                }
            }
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (!IsFinalSort_) {
                Controller_->SortedMergeTask->FinishInput();
                Controller_->UpdateMergeTasks();
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
            std::vector<TStreamDescriptor> streamDescriptors,
            bool isFinalSort)
            : TSortTaskBase(controller, std::move(streamDescriptors), isFinalSort)
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput({}))
        { }

        virtual TDuration GetLocalityTimeout() const override
        {
            // Locality for one-job partition is not important.
            if (IsFinalSort_) {
                return TDuration::Zero();
            }

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
            return Controller_->ShuffleMultiChunkPoolInputs.back();
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
            return Controller_->ShuffleMultiInputChunkMappings.back();
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            YT_VERIFY(partition->IsFinal());

            MultiChunkPoolOutput_->AddPoolOutput(partition->ChunkPoolOutput, partition->Index);
            Partitions_.push_back(std::move(partition));
            Controller_->UpdateTask(this);
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
            return Controller_->SortStartThresholdReached || IsFinalSort_;
        }

        virtual bool HasInputLocality() const override
        {
            return false;
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            auto nodeId = joblet->NodeDescriptor.Id;

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            auto& partition = Controller_->GetFinalPartition(partitionIndex);

            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            partition->AddLocality(nodeId, joblet->InputStripeList->TotalDataWeight);

            // Don't rely on static assignment anymore.
            partition->SetAssignedNodeId(InvalidNodeId);

            UpdateTask();

            TSortTaskBase::OnJobStarted(joblet);
        }

        virtual void OnJobLost(TCompletedJobPtr completedJob) override
        {
            if (!Controller_->SimpleSort) {
                auto partitionIndex = *completedJob->InputStripe->PartitionTag;
                auto& partition = Controller_->GetFinalPartition(partitionIndex);
                auto nodeId = completedJob->NodeDescriptor.Id;
                partition->AddLocality(nodeId, -completedJob->DataWeight);
            }

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
                    Controller_->GetFinalPartitionTask()->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    totalInputStatistics);
            }

            if (!IsFinalSort_) {
                auto partitionIndex = *joblet->InputStripeList->PartitionTag;
                const auto& partition = Controller_->GetFinalPartition(partitionIndex);
                if (partition->ChunkPoolOutput->IsCompleted()) {
                    Controller_->SortedMergeTask->OnIntermediateSortCompleted(partitionIndex);
                }
            }

            return result;
        }

        virtual void OnTaskCompleted() override
        {
            TSortTaskBase::OnTaskCompleted();

            if (IsFinalSort_)  {
                for (const auto& partition : Partitions_) {
                    Controller_->OnFinalPartitionCompleted(partition);
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

        TSimpleSortTask(TSortControllerBase* controller, TPartition* partition, std::vector<TStreamDescriptor> streamDescriptors)
            : TSortTaskBase(controller, std::move(streamDescriptors), /*isFinalSort=*/true)
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

            YT_VERIFY(Controller_->GetFinalPartitions().size() == 1);

            if (Controller_->SortedMergeTask) {
                Controller_->SortedMergeTask->Finalize();
            }

            if (IsFinalSort_) {
                Controller_->OnFinalPartitionCompleted(Partition_);
            }
        }

        // TODO(max42, gritukan): this is a dirty way add sorted merge when we
        // finally understand that it is needed. Re-write this.
        void OnSortedMergeNeeded()
        {
            GetJobCounter()->RemoveParent(Controller_->FinalSortJobCounter);
            GetJobCounter()->AddParent(Controller_->IntermediateSortJobCounter);

            SetIsFinalSort(false);
            StreamDescriptors_ = Controller_->GetSortedMergeStreamDescriptors();
        }

        virtual IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId) override
        {
            auto cookie = TSortTaskBase::ExtractCookie(nodeId);

            // NB(gritukan): In some weird cases unordered chunk pool can estimate total
            // number of jobs as 1 after pool creation and >1 after first cookie extraction.
            // For example, this might happen if there is a few data but many slices in the pool.
            // That's why we can understand that simple sort required sorted merge only after first
            // job start.
            Controller_->IsSortedMergeNeeded(Partition_);

            return cookie;
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
            std::vector<TStreamDescriptor> streamDescriptors)
            : TTask(controller, std::move(streamDescriptors))
            , Controller_(controller)
            , MultiChunkPool_(CreateMultiChunkPool({}))
        {
            ChunkPoolInput_ = CreateTaskUpdatingAdapter(MultiChunkPool_, this);

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
            Persist(context, SortedMergeChunkPools_);
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
            const auto& partition = Controller_->GetFinalPartition(partitionIndex);
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
            JobOutputs_[partitionIndex].clear();
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            YT_VERIFY(partition->IsFinal());

            auto partitionIndex = partition->Index;
            auto sortedMergeChunkPool = Controller_->CreateSortedMergeChunkPool(Format("%v(%v)", GetTitle(), partitionIndex));
            YT_VERIFY(SortedMergeChunkPools_.emplace(partitionIndex, sortedMergeChunkPool).second);;
            MultiChunkPool_->AddPool(std::move(sortedMergeChunkPool), partitionIndex);

            Partitions_.push_back(std::move(partition));

            if (partitionIndex >= ActiveJoblets_.size()) {
                ActiveJoblets_.resize(partitionIndex + 1);
                InvalidatedJoblets_.resize(partitionIndex + 1);
                JobOutputs_.resize(partitionIndex + 1);
            }

            Controller_->UpdateTask(this);
        }

        void OnIntermediateSortCompleted(int partitionIndex)
        {
            const auto& chunkPool = GetOrCrash(SortedMergeChunkPools_, partitionIndex);
            chunkPool->Finish();
        }

        void Finalize()
        {
            MultiChunkPool_->Finalize();
        }

        virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // TODO(gritukan): In case of many sort jobs per partition job specs may
            // become huge because of job splitting.
            config->EnableJobSplitting = false;

            return config;
        }

        virtual bool IsJobInterruptible() const override
        {
            return false;
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedMergeTask, 0x4ab19c75);

        TSortControllerBase* Controller_;

        std::vector<TPartitionPtr> Partitions_;

        IMultiChunkPoolPtr MultiChunkPool_;
        IChunkPoolInputPtr ChunkPoolInput_;

        THashMap<int, IChunkPoolPtr> SortedMergeChunkPools_;

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
            VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->SortedMergeJobSpecTemplate);
            auto comparator = GetComparator(Controller_->GetSortedMergeSortColumns());
            AddParallelInputSpec(jobSpec, joblet, comparator);
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

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            RegisterAllOutputs();

            for (const auto& partition : Partitions_) {
                Controller_->OnFinalPartitionCompleted(partition);
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
            std::vector<TStreamDescriptor> streamDescriptors)
            : TTask(controller, std::move(streamDescriptors))
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
            return Controller_->ShuffleMultiChunkPoolInputs.back();
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
            return Controller_->ShuffleMultiInputChunkMappings.back();
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            YT_VERIFY(partition->IsFinal());

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
            VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);

            auto partitionIndex = joblet->InputStripeList->PartitionTag;
            if (partitionIndex) {
                auto partitionTag = *Controller_->GetFinalPartition(*partitionIndex)->ParentPartitionTag;
                auto* mergeJobSpecExt = jobSpec->MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
                mergeJobSpecExt->set_partition_tag(partitionTag);
                auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
                schedulerJobSpecExt->set_partition_tag(partitionTag);
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
                    Controller_->GetFinalPartitionTask()->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    totalInputStatistics);
            }

            return result;
        }

        virtual void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            for (const auto& partition : Partitions_) {
                Controller_->OnFinalPartitionCompleted(partition);
            }
        }

        virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // TODO(gritukan): In case of many sort jobs per partition job specs may
            // become huge because of job splitting.
            config->EnableJobSplitting = false;

            return config;
        }

        virtual bool IsJobInterruptible() const override
        {
            return false;
        }
    };

    // Partition tree helpers.

    const TPartitionTaskPtr& GetRootPartitionTask() const
    {
        return PartitionTasks.front();
    }

    const TPartitionTaskPtr& GetFinalPartitionTask() const
    {
        return PartitionTasks.back();
    }

    const std::vector<TPartitionPtr>& GetFinalPartitions() const
    {
        // This fallback is used if partition tree is not built yet.
        // For example, per-partition statistcs can be requested before partition building.
        if (PartitionsByLevels.empty()) {
            const static std::vector<TPartitionPtr> emptyVector;
            return emptyVector;
        }

        YT_VERIFY(PartitionsByLevels.size() == PartitionTreeDepth + 1);
        return PartitionsByLevels.back();
    }

    const TPartitionPtr& GetRootPartition() const
    {
        return PartitionsByLevels[0][0];
    }

    const TPartitionPtr& GetFinalPartition(int partitionIndex) const
    {
        YT_VERIFY(PartitionsByLevels.size() == PartitionTreeDepth + 1);
        return PartitionsByLevels.back()[partitionIndex];
    }

    void SetupPartitioningCompletedCallbacks()
    {
        if (SimpleSort) {
            return;
        }

        for (const auto& partitions : PartitionsByLevels) {
            for (const auto& partition : partitions) {
                const auto& partitionPool = partition->IsRoot()
                    ? RootPartitionPool
                    : partition->ChunkPoolOutput;
                partitionPool->SubscribeCompleted(BIND([weakPartition = MakeWeak(partition)] {
                    auto partition = weakPartition.Lock();
                    if (!partition) {
                        return;
                    }

                    // Partitioning of #partition data is completed,
                    // so its data can be processed.
                    for (const auto& child : partition->Children) {
                        child->OnPartitioningCompleted();
                    }

                    if (partition->IsIntermediate()) {
                        partition->ShuffleChunkPoolInput->Finish();
                    }
                }));
            }
        }
    }

    IMultiChunkPoolOutputPtr CreateLevelMultiChunkPoolOutput(int level) const
    {
        const auto& partitions = PartitionsByLevels[level];
        std::vector<IChunkPoolOutputPtr> outputs;
        outputs.reserve(partitions.size());
        for (const auto& partition : partitions) {
            outputs.push_back(partition->ChunkPoolOutput);
        }

        auto multiChunkPoolOutput = CreateMultiChunkPoolOutput(std::move(outputs));
        multiChunkPoolOutput->Finalize();
        return multiChunkPoolOutput;
    }

    // Custom bits of preparation pipeline.

    void PrepareSortTasks()
    {
        if (SimpleSort) {
            return;
        }

        IntermediateSortTask = TSortTaskPtr(New<TSortTask>(this, GetSortedMergeStreamDescriptors(), /*isFinalSort=*/false));
        IntermediateSortTask->SetupJobCounters();
        IntermediateSortTask->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(IntermediateSortTask);

        FinalSortTask = TSortTaskPtr(New<TSortTask>(this, GetFinalStreamDescriptors(), /*isFinalSort=*/true));
        FinalSortTask->SetupJobCounters();
        FinalSortTask->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(FinalSortTask);
    }

    void CreateSortedMergeTask()
    {
        SortedMergeTask = New<TSortedMergeTask>(this, GetFinalStreamDescriptors());
    }

    void PrepareSortedMergeTask()
    {
        RegisterTask(SortedMergeTask);
        SortedMergeTask->SetInputVertex(FormatEnum(GetIntermediateSortJobType()));
        SortedMergeTask->RegisterInGraph();
    }

    // Init/finish.

    void AssignPartitions()
    {
        struct TAssignedNode
            : public TRefCounted
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
        for (const auto& partition : GetFinalPartitions()) {
            // We are not interested in one-job partition locality.
            if (!partition->Maniac && !IsSortedMergeNeeded(partition)) {
                continue;
            }
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

            UpdateTask(task);

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
            options.MaxTotalSliceCount = Config->MaxTotalSliceCount;
            options.EnablePeriodicYielder = true;
            options.ShouldSliceByRowIndices = true;
            options.Logger = Logger.WithTag("Name: RootPartition");

            RootPartitionPool = CreateOrderedChunkPool(
                std::move(options),
                IntermediateInputStreamDirectory);
        } else {
            TUnorderedChunkPoolOptions options;
            options.RowBuffer = RowBuffer;
            options.JobSizeConstraints = std::move(jobSizeConstraints);
            options.JobSizeAdjusterConfig = std::move(jobSizeAdjusterConfig);
            options.Logger = Logger.WithTag("Name: RootPartition");

            RootPartitionPool = CreateUnorderedChunkPool(
                std::move(options),
                GetInputStreamDirectory());
        }

        RootPartitionPool->GetDataWeightCounter()->AddParent(SortDataWeightCounter);
    }

    void CreateShufflePools()
    {
        ShuffleMultiChunkPoolInputs.reserve(PartitionTreeDepth);
        ShuffleMultiInputChunkMappings.reserve(PartitionTreeDepth);
        for (int level = 0; level < PartitionTreeDepth; ++level) {
            const auto& partitions = PartitionsByLevels[level];
            std::vector<IChunkPoolInputPtr> shuffleChunkPoolInputs;
            shuffleChunkPoolInputs.reserve(partitions.size());
            for (auto& partition : partitions) {
                IShuffleChunkPoolPtr shuffleChunkPool;
                if (level + 1 == PartitionTreeDepth) {
                    shuffleChunkPool = CreateShuffleChunkPool(
                        partition->Children.size(),
                        Spec->DataWeightPerShuffleJob,
                        Spec->MaxChunkSlicePerShuffleJob);
                } else {
                    shuffleChunkPool = CreateShuffleChunkPool(
                        partition->Children.size(),
                        Spec->DataWeightPerIntermediatePartitionJob,
                        Spec->MaxChunkSlicePerIntermediatePartitionJob);
                }
                partition->ShuffleChunkPool = shuffleChunkPool;
                partition->ShuffleChunkPoolInput = CreateIntermediateLivePreviewAdapter(shuffleChunkPool->GetInput(), this);
                for (int childIndex = 0; childIndex < partition->Children.size(); ++childIndex) {
                    auto& child = partition->Children[childIndex];
                    child->ChunkPoolOutput = shuffleChunkPool->GetOutput(childIndex);
                }
                shuffleChunkPoolInputs.push_back(partition->ShuffleChunkPoolInput);
            }

            auto shuffleMultiChunkPoolInput = CreateMultiChunkPoolInput(std::move(shuffleChunkPoolInputs));
            ShuffleMultiChunkPoolInputs.push_back(std::move(shuffleMultiChunkPoolInput));

            ShuffleMultiInputChunkMappings.push_back(New<TInputChunkMapping>(EChunkMappingMode::Unordered));
        }
    }

    void InitSimpleSortPool(IJobSizeConstraintsPtr jobSizeConstraints)
    {
        TUnorderedChunkPoolOptions options;
        options.RowBuffer = RowBuffer;
        options.JobSizeConstraints = std::move(jobSizeConstraints);
        options.Logger = Logger.WithTag("Name: SimpleSort");

        SimpleSortPool = CreateUnorderedChunkPool(
            std::move(options),
            GetInputStreamDirectory());
    }

    virtual bool IsCompleted() const override
    {
        return CompletedPartitionCount == GetFinalPartitions().size();
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

            if (IsRowCountPreserved() && !(SimpleSort && isNontrivialInput) && !IsSamplingEnabled() && !InputHasDynamicStores()) {
                // We don't check row count for simple sort if nontrivial read limits are specified,
                // since input row count can be estimated inaccurate.
                i64 totalInputRowCount = 0;
                for (const auto& partition : GetFinalPartitions()) {
                    i64 inputRowCount = partition->ChunkPoolOutput->GetRowCounter()->GetTotal();
                    totalInputRowCount += inputRowCount;
                }
                YT_LOG_ERROR_IF(totalInputRowCount != TotalOutputRowCount,
                    "Input/output row count mismatch in sort operation (TotalInputRowCount: %v, TotalOutputRowCount: %v)",
                    totalInputRowCount,
                    TotalOutputRowCount);
                YT_VERIFY(totalInputRowCount == TotalOutputRowCount);
            }

            YT_VERIFY(CompletedPartitionCount == GetFinalPartitions().size());
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

    void OnFinalPartitionCompleted(TPartitionPtr partition)
    {
        YT_VERIFY(partition->IsFinal());

        if (partition->Completed) {
            return;
        }

        partition->Completed = true;

        ++CompletedPartitionCount;

        YT_LOG_DEBUG("Final partition completed (PartitionIndex: %v)", partition->Index);
    }

    virtual bool IsSortedMergeNeeded(const TPartitionPtr& partition) const
    {
        YT_VERIFY(partition->IsFinal());

        if (partition->CachedSortedMergeNeeded) {
            return true;
        }

        if (partition->Maniac) {
            return false;
        }

        if (partition->ChunkPoolOutput->GetJobCounter()->GetTotal() <= 1) {
            return false;
        }

        YT_LOG_DEBUG("Final partition needs sorted merge (PartitionIndex: %v)", partition->Index);
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
            if (SimpleSort) {
                SortStartThresholdReached = true;
            } else if (IsSamplingEnabled() && PartitionTreeDepth == 1) {
                if (GetRootPartitionTask()->IsCompleted()) {
                    SortStartThresholdReached = true;
                }
            } else if (GetFinalPartitionTask()->GetChunkPoolInput()->IsFinished()) {
                auto totalDataWeight = GetFinalPartitionTask()->GetTotalDataWeight();
                auto completedDataWeight = GetFinalPartitionTask()->GetCompletedDataWeight();
                if (completedDataWeight >= totalDataWeight * Spec->ShuffleStartThreshold) {
                    SortStartThresholdReached = true;
                }
            }

            if (SortStartThresholdReached) {
                YT_LOG_INFO("Sort start threshold reached");
            }
        }

        UpdateSortTasks();
    }

    bool IsShuffleCompleted() const
    {
        if (UnorderedMergeTask && !UnorderedMergeTask->IsCompleted()) {
            return false;
        }

        if (SimpleSortTask && !SimpleSortTask->IsCompleted()) {
            return false;
        }

        if (IntermediateSortTask && !IntermediateSortTask->IsCompleted()) {
            return false;
        }

        if (FinalSortTask && !FinalSortTask->IsCompleted()) {
            return false;
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
                if (!GetFinalPartitionTask()->IsCompleted()) {
                    return;
                }
                if (SortDataWeightCounter->GetCompletedTotal() < SortDataWeightCounter->GetTotal() * Spec->MergeStartThreshold) {
                    return;
                }
            }

            YT_LOG_INFO("Merge start threshold reached");

            MergeStartThresholdReached = true;
        }

        UpdateMergeTasks();
    }

    void UpdateSortTasks()
    {
        if (SimpleSort) {
            UpdateTask(SimpleSortTask);
        } else {
            UpdateTask(IntermediateSortTask);
            UpdateTask(FinalSortTask);
        }
    }

    void UpdateMergeTasks()
    {
        if (UnorderedMergeTask) {
            UpdateTask(UnorderedMergeTask);
        }
        if (SortedMergeTask) {
            UpdateTask(SortedMergeTask);
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
        const TChunkStripeStatisticsVector& statistics,
        bool isRoot) const = 0;

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
        for (const auto& partitionTask : PartitionTasks) {
            partitionTask->OnExecNodesUpdated();
        }
    }

    virtual bool EnableNewPartitionsHeuristic() const = 0;

    void ProcessInputs(const TTaskPtr& inputTask, const IJobSizeConstraintsPtr& jobSizeConstraints)
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        inputTask->SetIsInput(true);

        int unversionedSlices = 0;
        int versionedSlices = 0;
        // TODO(max42): use CollectPrimaryInputDataSlices() here?
        for (auto& chunk : CollectPrimaryUnversionedChunks()) {
            const auto& comparator = InputTables_[chunk->GetTableIndex()]->Comparator;

            const auto& dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            if (comparator) {
                dataSlice->TransformToNew(RowBuffer, comparator.GetLength());
                InferLimitsFromBoundaryKeys(dataSlice, RowBuffer, comparator);
            } else {
                dataSlice->TransformToNewKeyless();
            }

            inputTask->AddInput(New<TChunkStripe>(std::move(dataSlice)));
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
        return static_cast<int>(std::clamp<i64>(result, 1, Options->MaxPartitionCount));
    }

    void SuggestPartitionCountAndMaxPartitionFactor(std::optional<int> forcedPartitionCount)
    {
        YT_VERIFY(TotalEstimatedInputDataWeight > 0);
        i64 dataWeightAfterPartition = 1 + static_cast<i64>(TotalEstimatedInputDataWeight * Spec->MapSelectivityFactor);

        if (forcedPartitionCount) {
            PartitionCount = *forcedPartitionCount;
        } else if (Spec->PartitionCount) {
            PartitionCount = *Spec->PartitionCount;
        } else if (Spec->PartitionDataWeight) {
            PartitionCount = DivCeil(dataWeightAfterPartition, *Spec->PartitionDataWeight);
        } else {
            i64 partitionSize = Spec->DataWeightPerShuffleJob * Spec->PartitionSizeFactor;
            PartitionCount = DivCeil(dataWeightAfterPartition, partitionSize);
        }

        PartitionCount = std::clamp(PartitionCount, 1, Options->MaxNewPartitionCount);

        if (Spec->MaxPartitionFactor) {
            MaxPartitionFactor = *Spec->MaxPartitionFactor;
        } else {
            auto maxPartitionsWithDepth = [] (i64 depth, i64 partitionFactor) {
                i64 partitions = 1;
                for (i64 level = 1; level <= depth; ++level) {
                    partitions *= partitionFactor;
                }

                return partitions;
            };

            i64 partitionFactorLimit = std::min<i64>(Options->MaxPartitionFactor, DivCeil(GetMaxPartitionJobBufferSize(), Options->MinUncompressedBlockSize));

            i64 depth = 1;
            while (maxPartitionsWithDepth(depth, partitionFactorLimit) < PartitionCount) {
                ++depth;
            }

            MaxPartitionFactor = 2;
            while (maxPartitionsWithDepth(depth, MaxPartitionFactor) < PartitionCount) {
                ++MaxPartitionFactor;
            }
        }

        MaxPartitionFactor = std::max(MaxPartitionFactor, 2);

        YT_LOG_DEBUG("Suggesting partition count and max partition factor (PartitionCount: %v, MaxPartitionFactor: %v)",
            PartitionCount,
            MaxPartitionFactor);
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

    TPartitionProgress ComputeFinalPartitionProgress() const
    {
        TPartitionProgress result;
        std::vector<i64> sizes(GetFinalPartitions().size());

        const auto& finalPartitions = GetFinalPartitions();
        {
            for (int i = 0; i < finalPartitions.size(); ++i) {
                sizes[i] = GetFinalPartitions()[i]->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
            }
            result.Total = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < finalPartitions.size(); ++i) {
                sizes[i] = GetFinalPartitions()[i]->ChunkPoolOutput->GetDataWeightCounter()->GetRunning();
            }
            result.Runnning = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < finalPartitions.size(); ++i) {
                sizes[i] = GetFinalPartitions()[i]->ChunkPoolOutput->GetDataWeightCounter()->GetCompletedTotal();
            }
            result.Completed = AggregateValues(sizes, MaxProgressBuckets);
        }
        return result;
    }

    const TProgressCounterPtr& GetPartitionJobCounter() const
    {
        return PartitionJobCounter;
    }

    // Partition sizes histogram.

    std::unique_ptr<IHistogram> ComputeFinalPartitionSizeHistogram() const
    {
        auto histogram = CreateHistogram();
        for (const auto& partition : GetFinalPartitions()) {
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
        auto progress = ComputeFinalPartitionProgress();
        auto sizeHistogram = ComputeFinalPartitionSizeHistogram();

        fluent
            .Item("partitions").BeginMap()
                .Item("total").Value(GetFinalPartitions().size())
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

        auto sizeHistogram = ComputeFinalPartitionSizeHistogram();
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
        RootPartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        PartitionJobIOConfig->TableReader->SamplingRate = std::nullopt;
    }

    virtual void CustomMaterialize() override
    {
        TOperationControllerBase::CustomMaterialize();

        ValidateIntermediateDataAccountPermission(EPermission::Use);

        for (const auto& table : InputTables_) {
            for (const auto& sortColumn : Spec->SortBy) {
                if (auto column = table->Schema->FindColumn(sortColumn.Name)) {
                    if (column->Aggregate()) {
                        THROW_ERROR_EXCEPTION("Sort by aggregate column is not allowed")
                            << TErrorAttribute("table_path", table->Path)
                            << TErrorAttribute("column_name", sortColumn.Name);
                    }
                }
            }
        }
    }

    virtual const std::vector<TStreamDescriptor>& GetFinalStreamDescriptors() const
    {
        return GetStandardStreamDescriptors();
    }

    virtual std::vector<TStreamDescriptor> GetSortedMergeStreamDescriptors() const
    {
        // TODO(gritukan): Here should be intermediate stream descriptor, but it breaks complex types.
        auto streamDescriptor = GetFinalStreamDescriptors()[0];

        streamDescriptor.DestinationPool = SortedMergeTask->GetChunkPoolInput();
        streamDescriptor.ChunkMapping = SortedMergeTask->GetChunkMapping();
        streamDescriptor.TableWriterOptions = GetIntermediateTableWriterOptions();
        if (streamDescriptor.TableUploadOptions.TableSchema->GetSortColumns() != Spec->SortBy) {
            streamDescriptor.TableUploadOptions.TableSchema = TTableSchema::FromSortColumns(Spec->SortBy);
        }
        streamDescriptor.RequiresRecoveryInfo = true;
        streamDescriptor.IsFinalOutput = false;
        streamDescriptor.TargetDescriptor = SortedMergeTask->GetVertexDescriptor();

        return {streamDescriptor};
    }

    IChunkPoolPtr CreateSortedMergeChunkPool(TString name)
    {
        TSortedChunkPoolOptions chunkPoolOptions;
        TSortedJobOptions jobOptions;
        jobOptions.EnableKeyGuarantee = GetSortedMergeJobType() == EJobType::SortedReduce;
        jobOptions.PrimaryComparator = GetComparator(GetSortedMergeSortColumns());
        jobOptions.PrimaryPrefixLength = jobOptions.PrimaryComparator.GetLength();
        jobOptions.ShouldSlicePrimaryTableByKeys = GetSortedMergeJobType() == EJobType::SortedReduce;
        jobOptions.MaxTotalSliceCount = Config->MaxTotalSliceCount;

        // NB: otherwise we could easily be persisted during preparing the jobs. Sorted chunk pool
        // can't handle this.
        jobOptions.EnablePeriodicYielder = false;
        chunkPoolOptions.RowBuffer = RowBuffer;
        chunkPoolOptions.SortedJobOptions = jobOptions;
        chunkPoolOptions.JobSizeConstraints = CreatePartitionBoundSortedJobSizeConstraints(
            Spec,
            Options,
            Logger,
            GetOutputTablePaths().size());
        chunkPoolOptions.Logger = Logger.WithTag("Name: %v", name);

        if (Spec->UseNewSortedPool) {
            YT_LOG_DEBUG("Creating new sorted pool");
            return CreateNewSortedChunkPool(chunkPoolOptions, nullptr /* chunkSliceFetcher */, IntermediateInputStreamDirectory);
        } else {
            YT_LOG_DEBUG("Creating legacy sorted pool");
            return CreateLegacySortedChunkPool(chunkPoolOptions, nullptr /* chunkSliceFetcher */, IntermediateInputStreamDirectory);
        }
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

    void BuildPartitionTree(int finalPartitionCount, int maxPartitionFactor)
    {
        YT_LOG_DEBUG("Building partition tree (FinalPartitionCount: %v, MaxPartitionFactor: %v)",
            finalPartitionCount,
            maxPartitionFactor);
        PartitionTreeDepth = 0;
        PartitionsByLevels.resize(1);
        auto buildPartitionTree = [&] (TPartitionTreeSkeleton* partitionTreeSkeleton, int level, auto buildPartitionTree) -> TPartitionPtr {
            if (level > PartitionTreeDepth) {
                PartitionTreeDepth = level;
                PartitionsByLevels.resize(level + 1);
            }

            auto levelPartitionIndex = PartitionsByLevels[level].size();
            auto partition = New<TPartition>(this, level, levelPartitionIndex);
            PartitionsByLevels[level].push_back(partition);
            for (int childIndex = 0; childIndex < partitionTreeSkeleton->Children.size(); ++childIndex) {
                auto* child = partitionTreeSkeleton->Children[childIndex].get();
                auto treeChild = buildPartitionTree(child, level + 1, buildPartitionTree);
                partition->Children.push_back(treeChild);
                treeChild->ParentPartitionTag = childIndex;
            }

            return partition;
        };
        auto partitionTreeSkeleton = BuildPartitionTreeSkeleton(finalPartitionCount, maxPartitionFactor);
        buildPartitionTree(partitionTreeSkeleton.get(), 0, buildPartitionTree);
    }

    std::vector<TPartitionKey> BuildPartitionKeysByPivotKeys()
    {
        std::vector<TPartitionKey> partitionKeys;
        partitionKeys.reserve(Spec->PivotKeys.size());

        for (const auto& key : Spec->PivotKeys) {
            auto upperBound = TKeyBound::FromRow(RowBuffer->Capture(key), /* isInclusive */true, /* isUpper */false);
            partitionKeys.emplace_back(upperBound);
        }

        return partitionKeys;
    }

    void AssignPartitionKeysToPartitions(const std::vector<TPartitionKey>& partitionKeys)
    {
        const auto& finalPartitions = GetFinalPartitions();
        YT_VERIFY(finalPartitions.size() == partitionKeys.size() + 1);
        finalPartitions[0]->LowerBound = TKeyBound::MakeUniversal(/* isUpper */false);
        for (int finalPartitionIndex = 1; finalPartitionIndex < finalPartitions.size(); ++finalPartitionIndex) {
            const auto& partition = finalPartitions[finalPartitionIndex];
            const auto& partitionKey = partitionKeys[finalPartitionIndex - 1];
            YT_LOG_DEBUG("Assigned lower key bound to final partition (FinalPartitionIndex: %v, KeyBound: %v)",
                finalPartitionIndex,
                partitionKey.LowerBound);
            partition->LowerBound = partitionKey.LowerBound;
            if (partitionKey.Maniac) {
                YT_LOG_DEBUG("Final partition is a maniac (FinalPartitionIndex: %v)", finalPartitionIndex);
                partition->Maniac = true;
            }
        }

        for (int level = PartitionTreeDepth - 1; level >= 0; --level) {
            for (const auto& partition : PartitionsByLevels[level]) {
                partition->LowerBound = partition->Children.front()->LowerBound;
                YT_LOG_DEBUG("Assigned lower key bound to partition (PartitionLevel: %v, PartitionIndex: %v, KeyBound: %v",
                    partition->Level,
                    partition->Index,
                    partition->LowerBound);
            }
        }
    }

    virtual EJobType GetPartitionJobType(bool isRoot) const = 0;
    virtual EJobType GetIntermediateSortJobType() const = 0;
    virtual EJobType GetFinalSortJobType() const = 0;
    virtual EJobType GetSortedMergeJobType() const = 0;

    virtual TUserJobSpecPtr GetPartitionUserJobSpec(bool isRoot) const = 0;
    virtual TUserJobSpecPtr GetSortUserJobSpec(bool isFinalSort) const = 0;
    virtual TUserJobSpecPtr GetSortedMergeUserJobSpec() const = 0;

    virtual TSortColumns GetSortedMergeSortColumns() const = 0;
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
                return TStringBuf("data_weight_per_partition_job");
            case EJobType::FinalSort:
                return TStringBuf("partition_data_weight");
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
            table->TableUploadOptions.TableSchema->GetSortColumns() != Spec->SortBy)
        {
            THROW_ERROR_EXCEPTION("sort_by is different from output table key columns")
                << TErrorAttribute("output_table_path", Spec->OutputTablePath)
                << TErrorAttribute("output_table_sort_columns", table->TableUploadOptions.TableSchema->GetSortColumns())
                << TErrorAttribute("sort_by", Spec->SortBy);
        }

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(Spec->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
                    ValidateOutputSchemaCompatibility(true, true);
                    ValidateOutputSchemaComputedColumnsCompatibility();
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(Spec->SortBy);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table->TableUploadOptions.TableSchema = TTableSchema::FromSortColumns(Spec->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
                    ValidateOutputSchemaComputedColumnsCompatibility();
                }
                break;

            default:
                YT_ABORT();
        }
    }

    virtual void CustomMaterialize() override
    {
        TSortControllerBase::CustomMaterialize();

        if (TotalEstimatedInputDataWeight == 0) {
            return;
        }

        if (TotalEstimatedInputDataWeight > Spec->MaxInputDataWeight) {
            THROW_ERROR_EXCEPTION("Failed to initialize sort operation, input data weight is too large")
                << TErrorAttribute("estimated_input_data_weight", TotalEstimatedInputDataWeight)
                << TErrorAttribute("max_input_data_weight", Spec->MaxInputDataWeight);
        }

        InitJobIOConfigs();

        std::vector<TPartitionKey> partitionKeys;

        if (Spec->PivotKeys.empty()) {
            auto samples = FetchSamples();

            YT_LOG_INFO("Suggested partition count (PartitionCount: %v, SampleCount: %v)", PartitionCount, samples.size());

            // Don't create more partitions than we have samples (plus one).
            PartitionCount = std::min(PartitionCount, static_cast<int>(samples.size()) + 1);
            SimpleSort = (PartitionCount == 1);

            auto partitionJobSizeConstraints = CreatePartitionJobSizeConstraints(
                Spec,
                Options,
                Logger,
                TotalEstimatedInputUncompressedDataSize,
                TotalEstimatedInputDataWeight,
                TotalEstimatedInputRowCount,
                InputCompressionRatio);

            if (!EnableNewPartitionsHeuristic()) {
                // Finally adjust partition count wrt block size constraints.
                PartitionCount = AdjustPartitionCountToWriterBufferSize(
                    PartitionCount,
                    partitionJobSizeConstraints->GetJobCount(),
                    PartitionJobIOConfig->TableWriter);

                YT_LOG_INFO("Adjusted partition count (PartitionCount: %v)", PartitionCount);
            }

            YT_LOG_INFO("Building partition keys");

            YT_PROFILE_TIMING("/operations/sort/samples_processing_time") {
                if (!SimpleSort) {
                    auto comparator = OutputTables_[0]->TableUploadOptions.GetUploadSchema()->ToComparator();
                    partitionKeys = BuildPartitionKeysBySamples(
                        samples,
                        PartitionCount,
                        partitionJobSizeConstraints,
                        comparator,
                        RowBuffer);
                }
            }
        } else {
            partitionKeys = BuildPartitionKeysByPivotKeys();
        }

        if (EnableNewPartitionsHeuristic()) {
            SuggestPartitionCountAndMaxPartitionFactor(partitionKeys.size() + 1);
        } else {
            PartitionCount = partitionKeys.size() + 1;
            if (Spec->MaxPartitionFactor) {
                MaxPartitionFactor = *Spec->MaxPartitionFactor;
            } else {
                // Build a flat tree by default.
                MaxPartitionFactor = PartitionCount;
            }
        }

        BuildPartitionTree(PartitionCount, MaxPartitionFactor);

        AssignPartitionKeysToPartitions(partitionKeys);

        CreateShufflePools();

        CreateSortedMergeTask();

        // NB: Here we register tasks in order of descending priority.
        PreparePartitionTasks();

        PrepareSortTasks();

        PrepareSimpleSortTask();

        PrepareUnorderedMergeTask();

        PrepareSortedMergeTask();

        InitJobSpecTemplates();

        SetupPartitioningCompletedCallbacks();
    }

    void PreparePartitionTasks()
    {
        if (SimpleSort) {
            return;
        }

        RootPartitionPoolJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);
        InitPartitionPool(RootPartitionPoolJobSizeConstraints, nullptr, false /* ordered */);

        PartitionTasks.resize(PartitionTreeDepth);
        for (int partitionTaskLevel = PartitionTreeDepth - 1; partitionTaskLevel >= 0; --partitionTaskLevel) {
            TStreamDescriptor shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate();
            shuffleStreamDescriptor.DestinationPool = ShuffleMultiChunkPoolInputs[partitionTaskLevel];
            shuffleStreamDescriptor.ChunkMapping = ShuffleMultiInputChunkMappings[partitionTaskLevel];
            shuffleStreamDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor.TableUploadOptions.TableSchema = OutputTables_[0]->TableUploadOptions.TableSchema;
            if (partitionTaskLevel != PartitionTreeDepth - 1) {
                shuffleStreamDescriptor.TargetDescriptor = PartitionTasks[partitionTaskLevel + 1]->GetVertexDescriptor();
            }
            PartitionTasks[partitionTaskLevel] = New<TPartitionTask>(this, std::vector<TStreamDescriptor>{shuffleStreamDescriptor}, partitionTaskLevel);
        }

        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < PartitionTasks.size(); ++partitionTaskLevel) {
            const auto& partitionTask = PartitionTasks[partitionTaskLevel];
            partitionTask->SetInputVertex(PartitionTasks[partitionTaskLevel - 1]->GetVertexDescriptor());
            partitionTask->RegisterInGraph();
        }

        ProcessInputs(PartitionTasks.front(), RootPartitionPoolJobSizeConstraints);
        FinishTaskInput(PartitionTasks.front());

        YT_LOG_INFO("Sorting with partitioning (PartitionCount: %v, PartitionJobCount: %v, DataWeightPerPartitionJob: %v)",
            GetFinalPartitions().size(),
            RootPartitionPoolJobSizeConstraints->GetJobCount(),
            RootPartitionPoolJobSizeConstraints->GetDataWeightPerJob());
    }

    void PrepareSimpleSortTask()
    {
        if (!SimpleSort) {
            return;
        }

        auto& partition = GetFinalPartition(0);

        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputDataWeight);

        InitSimpleSortPool(jobSizeConstraints);

        SimpleSortTask = New<TSimpleSortTask>(this, partition.Get(), GetFinalStreamDescriptors());
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
            YT_LOG_DEBUG("Final partition needs sorted merge (PartitionIndex: %v)", partition->Index);
        }

        // Kick-start the sort task.
        SortStartThresholdReached = true;
        UpdateSortTasks();
    }

    void PrepareUnorderedMergeTask()
    {
        if (SimpleSort) {
            return;
        }

        UnorderedMergeTask = New<TUnorderedMergeTask>(this, GetFinalStreamDescriptors());
        RegisterTask(UnorderedMergeTask);
        UnorderedMergeTask->SetInputVertex(GetFinalPartitionTask()->GetVertexDescriptor());
        UnorderedMergeTask->RegisterInGraph();
    }

    std::vector<TSample> FetchSamples()
    {
        TFuture<void> asyncSamplesResult;
        YT_PROFILE_TIMING("/operations/sort/input_processing_time") {
            // TODO(gritukan): Should we do it here?
            if (EnableNewPartitionsHeuristic()) {
                SuggestPartitionCountAndMaxPartitionFactor(std::nullopt);
            } else {
                PartitionCount = SuggestPartitionCount();
            }
            i64 sampleCount = static_cast<i64>(PartitionCount) * Spec->SamplesPerPartition;

            FetcherChunkScraper = CreateFetcherChunkScraper();

            auto samplesRowBuffer = New<TRowBuffer>(
                TRowBufferTag(),
                Config->ControllerRowBufferChunkSize);

            SamplesFetcher = New<TSamplesFetcher>(
                Config->Fetcher,
                ESamplingPolicy::Sorting,
                sampleCount,
                GetColumnNames(Spec->SortBy),
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

        YT_PROFILE_TIMING("/operations/sort/samples_processing_time") {
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

    virtual TUserJobSpecPtr GetPartitionUserJobSpec(bool /*isRoot*/) const override
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
        auto intermediateReaderOptions = New<TTableReaderOptions>();

        {
            RootPartitionJobSpecTemplate.set_type(static_cast<int>(EJobType::Partition));
            auto* schedulerJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig)).ToString());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(RootPartitionJobIOConfig).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));
            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_deterministic(Spec->EnableIntermediateOutputRecalculation);
        }

        {
            PartitionJobSpecTemplate.set_type(static_cast<int>(EJobType::Partition));
            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig)).ToString());
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());
            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_deterministic(Spec->EnableIntermediateOutputRecalculation);
        }

        TJobSpec sortJobSpecTemplate;
        {
            auto* schedulerJobSpecExt = sortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            if (SimpleSort) {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).ToString());
                SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));
            } else {
                schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
                SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());
            }

            auto* sortJobSpecExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
        }

        {
            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_type(static_cast<int>(GetIntermediateSortJobType()));
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).ToString());
        }

        {
            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_type(static_cast<int>(GetFinalSortJobType()));
            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).ToString());
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedMerge));
            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).ToString());

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(mergeJobSpecExt->mutable_sort_columns(), Spec->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
            auto* schedulerJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(UnorderedMergeJobIOConfig).ToString());

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
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
        const TChunkStripeStatisticsVector& statistics,
        bool /*isRoot*/) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 outputBufferSize = std::min(
            PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(GetFinalPartitions().size()),
            stat.DataWeight);

        outputBufferSize += THorizontalBlockWriter::MaxReserveSize * static_cast<i64>(GetFinalPartitions().size());

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

    virtual bool EnableNewPartitionsHeuristic() const override
    {
        if (Spec->UseNewPartitionsHeuristic) {
            return true;
        }

        int salt = OperationId.Parts64[1] & 255;
        if (salt < Spec->NewPartitionsHeuristicProbability) {
            return true;
        }

        return false;
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
            GetFinalPartitions().size(),
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

    virtual EJobType GetPartitionJobType(bool /*isRoot*/) const override
    {
        return EJobType::Partition;
    }

    virtual TSortColumns GetSortedMergeSortColumns() const override
    {
        return Spec->SortBy;
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

    void InitStreamDescriptors()
    {
        const auto& streamDescriptors = GetStandardStreamDescriptors();

        MapperSinkEdges_ = std::vector<TStreamDescriptor>(
            streamDescriptors.begin(),
            streamDescriptors.begin() + Spec->MapperOutputTableCount);
        for (int index = 0; index < MapperSinkEdges_.size(); ++index) {
            MapperSinkEdges_[index].TableWriterOptions->TableIndex = index + 1;
        }

        ReducerSinkEdges_ = std::vector<TStreamDescriptor>(
            streamDescriptors.begin() + Spec->MapperOutputTableCount,
            streamDescriptors.end());
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
                return TStringBuf("data_weight_per_map_job");
            case EJobType::PartitionReduce:
            case EJobType::SortedReduce:
                return TStringBuf("partition_data_weight");
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

    // Mapper stream descriptors are for the data that is written from mappers directly to the first
    // `Spec->MapperOutputTableCount` output tables skipping the shuffle and reduce phases.
    std::vector<TStreamDescriptor> MapperSinkEdges_;
    std::vector<TStreamDescriptor> ReducerSinkEdges_;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserFile> ReduceCombinerFiles;
    std::vector<TUserFile> ReducerFiles;

    i64 MapStartRowIndex = 0;
    i64 ReduceStartRowIndex = 0;


    std::vector<TTableSchemaPtr> IntermediateStreamSchemas_;
    TTableSchemaPtr IntermediateChunkSchema_;

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

        if (!CheckKeyColumnsCompatible(GetColumnNames(Spec->SortBy), Spec->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce columns %v are not compatible with sort columns %v",
                Spec->ReduceBy,
                GetColumnNames(Spec->SortBy));
        }

        YT_LOG_DEBUG("ReduceColumns: %v, SortColumns: %v",
            Spec->ReduceBy,
            GetColumnNames(Spec->SortBy));
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

    static bool AreAllEqual(const std::vector<TTableSchemaPtr>& schemas)
    {
        for (const auto& schema : schemas) {
            if (*schemas.front() != *schema) {
                return false;
            }
        }
        return true;
    }

    void InitIntermediateSchemas()
    {
        if (!Spec->HasSchemafulIntermediateStreams()) {
            IntermediateStreamSchemas_ = {New<TTableSchema>()};
            IntermediateChunkSchema_ = TTableSchema::FromSortColumns(Spec->SortBy);
            return;
        }

        auto toStreamSchema = [] (const TTableSchemaPtr& schema, const TSortColumns& sortColumns) {
            auto columns = schema->Columns();
            for (const auto& sortColumn : sortColumns) {
                if (!schema->FindColumn(sortColumn.Name)) {
                    columns.push_back(TColumnSchema(sortColumn.Name, SimpleLogicalType(ESimpleLogicalValueType::Null)));
                }
            }
            return New<TTableSchema>(std::move(columns), schema->GetStrict())->ToSorted(sortColumns);
        };

        auto inferColumnType = [] (const std::vector<TInputTablePtr>& tables, TStringBuf keyColumn) -> TLogicalTypePtr {
            TLogicalTypePtr type;
            bool missingInSomeSchema = false;
            for (const auto& table : tables) {
                auto column = table->Schema->FindColumn(keyColumn);
                if (!column) {
                    missingInSomeSchema = true;
                    continue;
                }
                if (!type) {
                    type = column->LogicalType();
                } else if (*type != *column->LogicalType()) {
                    THROW_ERROR_EXCEPTION("Type mismatch for key column %Qv in input schemas", keyColumn)
                        << TErrorAttribute("lhs_type", type)
                        << TErrorAttribute("rhs_type", column->LogicalType());
                }
            }
            if (!type) {
                return SimpleLogicalType(ESimpleLogicalValueType::Null);
            }
            if (missingInSomeSchema && !type->IsNullable()) {
                return OptionalLogicalType(std::move(type));
            }
            return type;
        };

        std::vector<TColumnSchema> chunkSchemaColumns;
        if (Spec->HasNontrivialMapper()) {
            YT_VERIFY(Spec->Mapper->OutputStreams.size() > Spec->MapperOutputTableCount);
            auto intermediateStreamCount = Spec->Mapper->OutputStreams.size() - Spec->MapperOutputTableCount;
            for (int i = 0; i < intermediateStreamCount; ++i) {
                IntermediateStreamSchemas_.push_back(Spec->Mapper->OutputStreams[i]->Schema);
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Filter(GetColumnNames(Spec->SortBy))->Columns();
            }
        } else {
            YT_VERIFY(!InputTables_.empty());
            for (const auto& inputTable : InputTables_) {
                IntermediateStreamSchemas_.push_back(toStreamSchema(inputTable->Schema, Spec->SortBy));
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                for (const auto& sortColumn : Spec->SortBy) {
                    auto type = inferColumnType(InputTables_, sortColumn.Name);
                    chunkSchemaColumns.emplace_back(sortColumn.Name, std::move(type), sortColumn.SortOrder);
                }
            }
        }

        chunkSchemaColumns.emplace_back(TableIndexColumnName, ESimpleLogicalValueType::Int64);
        IntermediateChunkSchema_ = New<TTableSchema>(std::move(chunkSchemaColumns), /* strict */ false);
    }

    virtual void CustomMaterialize() override
    {
        TSortControllerBase::CustomMaterialize();

        InitIntermediateSchemas();

        if (TotalEstimatedInputDataWeight == 0)
            return;

        MapperFiles = UserJobFiles_[Spec->Mapper];
        ReduceCombinerFiles = UserJobFiles_[Spec->ReduceCombiner];
        ReducerFiles = UserJobFiles_[Spec->Reducer];

        InitJobIOConfigs();
        InitStreamDescriptors();

        std::vector<TPartitionKey> partitionKeys;
        bool usePivotKeys = !Spec->PivotKeys.empty();
        if (usePivotKeys) {
            partitionKeys = BuildPartitionKeysByPivotKeys();
        }

        if (EnableNewPartitionsHeuristic()) {
            std::optional<int> forcedPartitionCount;
            if (usePivotKeys) {
                forcedPartitionCount = partitionKeys.size() + 1;
            }
            SuggestPartitionCountAndMaxPartitionFactor(forcedPartitionCount);
        } else {
            PartitionCount = SuggestPartitionCount();
            YT_LOG_INFO("Suggested partition count %v (PartitionCount: %v)", PartitionCount);
        }

        Spec->Sampling->MaxTotalSliceCount = Spec->Sampling->MaxTotalSliceCount.value_or(Config->MaxTotalSliceCount);

        RootPartitionPoolJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);

        if (!EnableNewPartitionsHeuristic()) {
            PartitionCount = AdjustPartitionCountToWriterBufferSize(
                PartitionCount,
                RootPartitionPoolJobSizeConstraints->GetJobCount(),
                PartitionJobIOConfig->TableWriter);
            if (usePivotKeys) {
                PartitionCount = partitionKeys.size() + 1;
            }

            if (Spec->MaxPartitionFactor) {
                MaxPartitionFactor = *Spec->MaxPartitionFactor;
            } else {
                // Build a flat tree by default.
                MaxPartitionFactor = PartitionCount;
            }
        }

        int maxPartitionFactor;
        if (Spec->MaxPartitionFactor) {
            maxPartitionFactor = *Spec->MaxPartitionFactor;
        } else {
            // Build a flat tree by default.
            maxPartitionFactor = PartitionCount;
        }

        YT_LOG_INFO("Adjusted partition count %v", PartitionCount);

        BuildPartitionTree(PartitionCount, MaxPartitionFactor);

        YT_PROFILE_TIMING("/operations/sort/input_processing_time") {
            if (usePivotKeys) {
                AssignPartitionKeysToPartitions(partitionKeys);
            }
        }

        CreateShufflePools();

        CreateSortedMergeTask();

        // NB: Here we register tasks in order of descending priority.
        PreparePartitionTasks(RootPartitionPoolJobSizeConstraints);

        PrepareSortTasks();

        PrepareSortedMergeTask();

        InitJobSpecTemplates();

        SetupPartitioningCompletedCallbacks();
    }

    void PreparePartitionTasks(const IJobSizeConstraintsPtr& partitionJobSizeConstraints)
    {
        InitPartitionPool(
            partitionJobSizeConstraints,
            Config->EnablePartitionMapJobSizeAdjustment && !Spec->Ordered
            ? Options->PartitionJobSizeAdjuster
            : nullptr,
            Spec->Ordered);

        PartitionTasks.resize(PartitionTreeDepth);
        for (int partitionTaskLevel = PartitionTreeDepth - 1; partitionTaskLevel >= 0; --partitionTaskLevel) {
            std::vector<TStreamDescriptor> partitionStreamDescriptors;
            // Primary stream descriptor for shuffled output of the mapper.
            TStreamDescriptor shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate();
            shuffleStreamDescriptor.DestinationPool = ShuffleMultiChunkPoolInputs[partitionTaskLevel];
            shuffleStreamDescriptor.ChunkMapping = ShuffleMultiInputChunkMappings[partitionTaskLevel];
            shuffleStreamDescriptor.TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor.TableUploadOptions.TableSchema = IntermediateChunkSchema_;
            shuffleStreamDescriptor.StreamSchemas = IntermediateStreamSchemas_;
            if (partitionTaskLevel != PartitionTreeDepth - 1) {
                shuffleStreamDescriptor.TargetDescriptor = PartitionTasks[partitionTaskLevel + 1]->GetVertexDescriptor();
            }
            partitionStreamDescriptors.emplace_back(std::move(shuffleStreamDescriptor));

            if (partitionTaskLevel == 0) {
                partitionStreamDescriptors.insert(
                    partitionStreamDescriptors.end(),
                    MapperSinkEdges_.begin(),
                    MapperSinkEdges_.end());
            }

            PartitionTasks[partitionTaskLevel] = (New<TPartitionTask>(this, std::move(partitionStreamDescriptors), partitionTaskLevel));
        }

        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < PartitionTasks.size(); ++partitionTaskLevel) {
            const auto& partitionTask = PartitionTasks[partitionTaskLevel];
            partitionTask->SetInputVertex(PartitionTasks[partitionTaskLevel - 1]->GetVertexDescriptor());
            partitionTask->RegisterInGraph();
        }

        ProcessInputs(PartitionTasks[0], partitionJobSizeConstraints);
        FinishTaskInput(PartitionTasks[0]);

        YT_LOG_INFO("Map-reducing with partitioning (PartitionCount: %v, PartitionJobCount: %v, PartitionDataWeightPerJob: %v)",
            GetFinalPartitions().size(),
            partitionJobSizeConstraints->GetJobCount(),
            partitionJobSizeConstraints->GetDataWeightPerJob());
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        RootPartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        PartitionJobIOConfig = CloneYsonSerializable(Spec->PartitionJobIO);
        PartitionJobIOConfig->TableReader->SamplingRate = std::nullopt;

        IntermediateSortJobIOConfig = Spec->SortJobIO;

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
        FinalSortJobIOConfig->TableReader = CloneYsonSerializable(Spec->SortJobIO->TableReader);

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonSerializable(Spec->MergeJobIO);
    }

    virtual EJobType GetPartitionJobType(bool isRoot) const override
    {
        if (Spec->HasNontrivialMapper() && isRoot) {
            return EJobType::PartitionMap;
        } else {
            return EJobType::Partition;
        }
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

    virtual const std::vector<TStreamDescriptor>& GetFinalStreamDescriptors() const override
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
            RootPartitionJobSpecTemplate.set_type(static_cast<int>(GetPartitionJobType(/*isRoot=*/true)));

            auto* schedulerJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig)).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, BuildDataSourceDirectoryFromInputTables(InputTables_));

            if (Spec->InputQuery) {
                WriteInputQueryToJobSpec(schedulerJobSpecExt);
            }

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(RootPartitionJobIOConfig).ToString());

            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_deterministic(Spec->EnableIntermediateOutputRecalculation);

            if (Spec->HasNontrivialMapper()) {
                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->Mapper,
                    MapperFiles,
                    Spec->JobNodeAccount);
            }
        }

        {
            PartitionJobSpecTemplate.set_type(static_cast<int>(EJobType::Partition));
            auto* schedulerJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig)).ToString());

            SetDataSourceDirectory(schedulerJobSpecExt, BuildIntermediateDataSourceDirectory());

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).ToString());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_deterministic(Spec->EnableIntermediateOutputRecalculation);
        }

        auto intermediateDataSourceDirectory = BuildIntermediateDataSourceDirectory(IntermediateStreamSchemas_);
        const auto castAnyToComposite = !AreAllEqual(IntermediateStreamSchemas_);

        auto intermediateReaderOptions = New<TTableReaderOptions>();
        {
            auto* schedulerJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            schedulerJobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).ToString());

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, intermediateDataSourceDirectory);

            if (Spec->HasNontrivialReduceCombiner()) {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::ReduceCombiner));

                auto* reduceJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
                reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
                ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);

                InitUserJobSpecTemplate(
                    schedulerJobSpecExt->mutable_user_job_spec(),
                    Spec->ReduceCombiner,
                    ReduceCombinerFiles,
                    Spec->JobNodeAccount);
                schedulerJobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
            } else {
                IntermediateSortJobSpecTemplate.set_type(static_cast<int>(EJobType::IntermediateSort));
                auto* sortJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            }
        }

        {
            FinalSortJobSpecTemplate.set_type(static_cast<int>(EJobType::PartitionReduce));

            auto* schedulerJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* reduceJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, intermediateDataSourceDirectory);

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).ToString());

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->JobNodeAccount);
            schedulerJobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
        }

        {
            SortedMergeJobSpecTemplate.set_type(static_cast<int>(EJobType::SortedReduce));

            auto* schedulerJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
            auto* reduceJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetDataSourceDirectory(schedulerJobSpecExt, intermediateDataSourceDirectory);

            schedulerJobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).ToString());

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);

            InitUserJobSpecTemplate(
                schedulerJobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->JobNodeAccount);
            schedulerJobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
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
        return TCpuResource(Spec->HasNontrivialMapper() ? Spec->Mapper->CpuLimit : 1);
    }

    virtual TCpuResource GetSortCpuLimit() const override
    {
        // At least one CPU, may be more in PartitionReduce job.
        return 1;
    }

    virtual TCpuResource GetMergeCpuLimit() const override
    {
        return TCpuResource(Spec->Reducer->CpuLimit);
    }

    virtual TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool isRoot) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 reserveSize = THorizontalBlockWriter::MaxReserveSize * static_cast<i64>(GetFinalPartitions().size());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * static_cast<i64>(GetFinalPartitions().size()),
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        if (Spec->HasNontrivialMapper() && isRoot) {
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

    virtual bool EnableNewPartitionsHeuristic() const override
    {
        if (Spec->UseNewPartitionsHeuristic) {
            return true;
        }

        if (Spec->HasNontrivialReduceCombiner()) {
            return false;
        }

        int salt = OperationId.Parts64[1] & 255;
        if (salt < Spec->NewPartitionsHeuristicProbability) {
            return true;
        }

        return false;
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
            GetFinalPartitions().size(),
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
            // TODO(gritukan): What should I do here?
            .Item(JobTypeAsKey(GetPartitionJobType(/*isRoot=*/true))).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(GetIntermediateSortJobType())).Value(IntermediateSortJobCounter)
            .Item(JobTypeAsKey(GetFinalSortJobType())).Value(FinalSortJobCounter)
            .Item(JobTypeAsKey(GetSortedMergeJobType())).Value(SortedMergeJobCounter)
            // TODO(ignat): remove when UI migrate to new keys.
            .Item(Spec->HasNontrivialMapper() ? "map_jobs" : "partition_jobs").Value(GetPartitionJobCounter())
            .Item(Spec->HasNontrivialReduceCombiner() ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
    }

    virtual TUserJobSpecPtr GetPartitionUserJobSpec(bool isRoot) const override
    {
        if (Spec->HasNontrivialMapper() && isRoot) {
            return Spec->Mapper;
        } else {
            return nullptr;
        }
    }

    virtual TSortColumns GetSortedMergeSortColumns() const override
    {
        auto sortColumns = Spec->SortBy;
        sortColumns.resize(Spec->ReduceBy.size());
        return sortColumns;
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
