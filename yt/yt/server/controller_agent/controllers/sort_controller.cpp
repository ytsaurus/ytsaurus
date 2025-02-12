#include "sort_controller.h"

#include "chunk_pool_adapters.h"
#include "data_balancer.h"
#include "helpers.h"
#include "job_info.h"
#include "job_memory.h"
#include "operation_controller_detail.h"
#include "task.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation.h>
#include <yt/yt/server/controller_agent/partitioning_parameters_evaluator.h>
#include <yt/yt/server/controller_agent/scheduling_context.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/shuffle_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/chunk_client/proto/data_sink.pb.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/key_set.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>
#include <yt/yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/logging/serializable_logger.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <algorithm>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NComplexTypes;
using namespace NConcurrency;
using namespace NControllerAgent::NProto;
using namespace NCrypto;
using namespace NCypressClient;
using namespace NJobTrackerClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NScheduler;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;

using NTableClient::TLegacyKey;
using NNodeTrackerClient::TNodeId;

using NControllerAgent::NProto::TPartitionJobSpecExt;
using NControllerAgent::NProto::TReduceJobSpecExt;
using NControllerAgent::NProto::TSortJobSpecExt;
using NControllerAgent::NProto::TMergeJobSpecExt;
using NControllerAgent::NProto::TJobSpecExt;

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

    std::pair<NApi::ITransactionPtr, TString> GetIntermediateMediumTransaction() override
    {
        if (GetFastIntermediateMediumLimit() > 0 && !SwitchedToSlowIntermediateMedium) {
            auto medium = GetIntermediateStreamDescriptorTemplate()->TableWriterOptions->MediumName;
            return {OutputTransaction, medium};
        } else {
            return {nullptr, {}};
        }
    }

    void UpdateIntermediateMediumUsage(i64 usage) override
    {
        auto fastIntermediateMediumLimit = GetFastIntermediateMediumLimit();

        if (!GetIntermediateMediumTransaction().first || usage < fastIntermediateMediumLimit) {
            return;
        }

        for (const auto& partitionTask : PartitionTasks) {
            partitionTask->SwitchIntermediateMedium();
        }
        if (SimpleSortTask && !SimpleSortTask->IsFinal()) {
            SimpleSortTask->SwitchIntermediateMedium();
        }
        if (IntermediateSortTask) {
            IntermediateSortTask->SwitchIntermediateMedium();
        }

        YT_LOG_DEBUG("Switching from the fast intermediate medium to the slow one "
            "(FastMediumUsage: %v, FastMediumLimit: %v, UsageToLimitRatio: %v, TransactionId: %v)",
            usage,
            fastIntermediateMediumLimit,
            static_cast<double>(usage) / fastIntermediateMediumLimit,
            OutputTransaction->GetId());

        SwitchedToSlowIntermediateMedium = true;
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

    TTableSchemaPtr IntermediateChunkSchema_ = New<TTableSchema>();
    std::vector<TTableSchemaPtr> IntermediateStreamSchemas_;

    TUnavailableChunksWatcherPtr UnavailableChunksWatcher_;

    // Forward declarations.
    class TPartitionTask;
    using TPartitionTaskPtr = TIntrusivePtr<TPartitionTask>;

    class TSortTask;
    using TSortTaskPtr = TIntrusivePtr<TSortTask>;

    class TSimpleSortTask;
    using TSimpleSortTaskPtr = TIntrusivePtr<TSimpleSortTask>;

    class TSortedMergeTask;
    using TSortedMergeTaskPtr = TIntrusivePtr<TSortedMergeTask>;

    class TUnorderedMergeTask;
    using TUnorderedMergeTaskPtr = TIntrusivePtr<TUnorderedMergeTask>;

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
        IPersistentChunkPoolInputPtr ShuffleChunkPoolInput;

        //! Chunk pool containing data of this partition.
        //! For root partition it is either partition pool or simple sort pool.
        //! For non-root partitions it's an output of parent partition's shuffle pool.
        IPersistentChunkPoolOutputPtr ChunkPoolOutput;

        TSerializableLogger Logger;

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

        bool HasAssignedNode() const
        {
            return AssignedNodeId != InvalidNodeId;
        }

        void AddLocality(TNodeId nodeId, i64 delta) const
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

    private:
        //! The node assigned to this partition, #InvalidNodeId if none.
        TNodeId AssignedNodeId = InvalidNodeId;

        PHOENIX_DECLARE_TYPE(TPartition, 0xec5290d7);
    };

    using TPartitionPtr = TIntrusivePtr<TPartition>;

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
    IPartitioningParametersEvaluatorPtr PartitioningParametersEvaluator_;

    IPersistentChunkPoolPtr RootPartitionPool;
    IPersistentChunkPoolPtr SimpleSortPool;

    //! i-th element is a multi chunk pool built over the inputs of i-th level partitions' shuffle chunk pools.
    std::vector<IMultiChunkPoolInputPtr> ShuffleMultiChunkPoolInputs;

    //! i-th element is a input chunk mapping for i-th shuffle multi chunk pool input.
    std::vector<TInputChunkMappingPtr> ShuffleMultiInputChunkMappings;

    //! i-th element is a partition task of i-th level.
    std::vector<TPartitionTaskPtr> PartitionTasks;

    TSimpleSortTaskPtr SimpleSortTask;

    TSortTaskPtr IntermediateSortTask;
    TSortTaskPtr FinalSortTask;

    TUnorderedMergeTaskPtr UnorderedMergeTask;

    TSortedMergeTaskPtr SortedMergeTask;

    //! True if the operation has switched to the slow intermediate medium (HDD) after producing
    //! more intermediate data than the limit for the fast intermediate medium (SSD).
    bool SwitchedToSlowIntermediateMedium = false;

    //! Implements partition phase for sort operations and map phase for map-reduce operations.
    class TPartitionTask
        : public TTask
    {
    public:
        //! For persistence only.
        TPartitionTask() = default;

        TPartitionTask(
            TSortControllerBase* controller,
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors,
            int level)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , Level_(level)
        {
            if (!IsRoot()) {
                ShuffleMultiChunkOutput_ = Controller_->CreateLevelMultiChunkPoolOutput(Level_);
            }

            GetChunkPoolOutput()->GetJobCounter()->AddParent(Controller_->PartitionJobCounter);

            WirePartitionLowerBoundPrefixes_.reserve(GetInputPartitions().size());
            PartitionLowerBoundInclusivenesses_.reserve(GetInputPartitions().size());
            for (const auto& inputPartition : GetInputPartitions()) {
                auto keySetWriter = New<TKeySetWriter>();
                auto partitionLowerBoundPrefixesWriter = New<TKeySetWriter>();
                std::vector<bool> partitionLowerBoundInclusivenesses;

                int keysWritten = 0;
                for (int childPartitionIndex = 1; childPartitionIndex < std::ssize(inputPartition->Children); ++childPartitionIndex) {
                    const auto& childPartition = inputPartition->Children[childPartitionIndex];
                    auto lowerBound = childPartition->LowerBound;
                    if (lowerBound && !lowerBound.IsUniversal()) {
                        keySetWriter->WriteKey(KeyBoundToLegacyRow(lowerBound));
                        partitionLowerBoundPrefixesWriter->WriteKey(lowerBound.Prefix);
                        partitionLowerBoundInclusivenesses.push_back(lowerBound.IsInclusive);
                        ++keysWritten;
                    }
                }
                YT_VERIFY(keysWritten == 0 || keysWritten + 1 == std::ssize(inputPartition->Children));
                if (keysWritten == 0) {
                    WirePartitionLowerBoundPrefixes_.push_back(std::nullopt);
                    PartitionLowerBoundInclusivenesses_.push_back({});
                } else {
                    WirePartitionLowerBoundPrefixes_.push_back(ToString(partitionLowerBoundPrefixesWriter->Finish()));
                    PartitionLowerBoundInclusivenesses_.push_back(partitionLowerBoundInclusivenesses);
                }
            }
        }

        TString GetTitle() const override
        {
            return Format("Partition(%v)", Level_);
        }

        TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const override
        {
            return Format("%v(%v)", TTask::GetVertexDescriptor(), Level_);
        }

        void FinishInput() override
        {
            // NB: We try to use the value as close to the total data weight of all extracted stripe lists as possible.
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

        TDuration GetLocalityTimeout() const override
        {
            // Locality is not defined for non-root partition tasks since their inputs are shuffled.
            if (!IsRoot()) {
                return TDuration::Zero();
            }

            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec->PartitionLocalityTimeout
                : TDuration::Zero();
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetPartitionResources(joblet->InputStripeList->GetStatistics(), IsRoot());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            if (IsRoot()) {
                return Controller_->RootPartitionPool;
            } else {
                return Controller_->ShuffleMultiChunkPoolInputs[Level_ - 1];
            }
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            if (IsRoot()) {
                return Controller_->RootPartitionPool;
            } else {
                return ShuffleMultiChunkOutput_;
            }
        }

        TInputChunkMappingPtr GetChunkMapping() const override
        {
            if (IsRoot()) {
                return TTask::GetChunkMapping();
            } else {
                return Controller_->ShuffleMultiInputChunkMappings[Level_ - 1];
            }
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetPartitionUserJobSpec(IsRoot());
        }

        EJobType GetJobType() const override
        {
            return Controller_->GetPartitionJobType(IsRoot());
        }

        void OnExecNodesUpdated()
        {
            if (DataBalancer_) {
                DataBalancer_->OnExecNodesUpdated(Controller_->GetOnlineExecNodeDescriptors());
            }
        }

        void PropagatePartitions(
            const std::vector<TOutputStreamDescriptorPtr>& streamDescriptors,
            const TChunkStripeListPtr& inputStripeList,
            std::vector<TChunkStripePtr>* outputStripes) override
        {
            TTask::PropagatePartitions(streamDescriptors, inputStripeList, outputStripes);

            auto& shuffleStripe = outputStripes->front();
            shuffleStripe->PartitionTag = GetPartitionIndex(inputStripeList);
        }

    private:
        TSortControllerBase* Controller_ = nullptr;

        TDataBalancerPtr DataBalancer_;

        int Level_ = -1;

        IMultiChunkPoolOutputPtr ShuffleMultiChunkOutput_;

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

        int GetPartitionIndex(const TChunkStripeListPtr& chunkStripeList) const
        {
            if (IsRoot()) {
                return 0;
            } else {
                return *chunkStripeList->PartitionTag;
            }
        }

        bool CanLoseJobs() const override
        {
            return Controller_->Spec->EnableIntermediateOutputRecalculation;
        }

        std::optional<EScheduleFailReason> GetScheduleFailReason(const TSchedulingContext& context) override
        {
            // We don't have a job at hand here, let's make a guess.
            auto approximateStatistics = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            if (approximateStatistics.empty()) {
                return std::nullopt;
            }

            const auto& node = context.GetNodeDescriptor();

            if (DataBalancer_ && !DataBalancer_->CanScheduleJob(node, approximateStatistics.front().DataWeight)) {
                return EScheduleFailReason::DataBalancingViolation;
            }

            return std::nullopt;
        }

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto statistics = GetChunkPoolOutput()->GetApproximateStripeStatistics();
            auto resources = Controller_->GetPartitionResources(statistics, IsRoot());
            AddFootprintAndUserJobResources(resources);

            return resources;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            if (IsRoot()) {
                jobSpec->CopyFrom(Controller_->RootPartitionJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller_->PartitionJobSpecTemplate);
            }

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            const auto& partition = GetInputPartitions()[partitionIndex];

            auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
            if (auto parentPartitionTag = partition->ParentPartitionTag) {
                jobSpecExt->set_partition_tag(*parentPartitionTag);
            }

            auto* partitionJobSpecExt = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_partition_count(partition->Children.size());
            if (const auto& lowerBoundPrefixes = WirePartitionLowerBoundPrefixes_[partitionIndex]) {
                partitionJobSpecExt->set_wire_partition_lower_bound_prefixes(*lowerBoundPrefixes);
                ToProto(partitionJobSpecExt->mutable_partition_lower_bound_inclusivenesses(), PartitionLowerBoundInclusivenesses_[partitionIndex]);
            }
            partitionJobSpecExt->set_partition_task_level(Level_);

            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            auto dataWeight = joblet->InputStripeList->TotalDataWeight;
            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, +dataWeight);
            }

            TTask::OnJobStarted(joblet);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            if (IsIntermediate()) {
                Controller_->UpdateTask(GetNextPartitionTask().Get());
            } else if (Controller_->SimpleSort) {
                Controller_->UpdateTask(Controller_->SimpleSortTask.Get());
            } else {
                Controller_->UpdateTask(Controller_->UnorderedMergeTask.Get());
                Controller_->UpdateTask(Controller_->IntermediateSortTask.Get());
                Controller_->UpdateTask(Controller_->FinalSortTask.Get());
            }

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            const auto& partition = Controller_->PartitionsByLevels[Level_][partitionIndex];
            const auto& shuffleChunkPool = partition->ShuffleChunkPool;

            if (IsFinal()) {
                for (const auto& finalPartition : partition->Children) {
                    // Check if final partition is large enough to start sorted merge.
                    Controller_->IsSortedMergeNeeded(finalPartition);
                }
            }

            // NB: This should be done not only in OnTaskCompleted:
            // 1) jobs may run after the task has been completed;
            // 2) ShuffleStartThreshold can be less than one.
            // Kick-start sort and unordered merge tasks.
            Controller_->CheckSortStartThreshold();
            Controller_->CheckMergeStartThreshold();

            if (shuffleChunkPool->GetTotalDataSliceCount() > Controller_->Spec->MaxShuffleDataSliceCount) {
                result.OperationFailedError = TError("Too many data slices in shuffle pool, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_data_slice_count", shuffleChunkPool->GetTotalDataSliceCount())
                    << TErrorAttribute("max_shuffle_data_slice_count", Controller_->Spec->MaxShuffleDataSliceCount);
                return result;
            }

            if (shuffleChunkPool->GetTotalJobCount() > Controller_->Spec->MaxShuffleJobCount) {
                result.OperationFailedError = TError("Too many shuffle jobs, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_job_count", shuffleChunkPool->GetTotalJobCount())
                    << TErrorAttribute("max_shuffle_job_count", Controller_->Spec->MaxShuffleJobCount);
                return result;
            }

            return result;
        }

        void OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId) override
        {
            TTask::OnJobLost(completedJob, chunkId);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(completedJob->NodeDescriptor, -completedJob->DataWeight);
            }

            if (!Controller_->IsShuffleCompleted()) {
                // Update task if shuffle is in progress and some partition jobs were lost.
                Controller_->UpdateTask(this);
            }
        }

        TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobFailed(joblet, jobSummary);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
            }

            return result;
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->TotalDataWeight);
            }

            return result;
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            for (const auto& partition : GetInputPartitions()) {
                partition->ShuffleChunkPoolInput->Finish();
            }

            if (IsIntermediate()) {
                const auto& nextPartitionTask = GetNextPartitionTask();
                nextPartitionTask->FinishInput();
                Controller_->UpdateTask(nextPartitionTask.Get());
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

            // NB: This is required at least to mark tasks completed, when there are no pending jobs.
            // This couldn't have been done earlier since we've just finished populating shuffle pool.
            Controller_->CheckSortStartThreshold();
            Controller_->CheckMergeStartThreshold();
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            config->EnableJobSplitting &=
                (IsJobInterruptible() &&
                std::ssize(Controller_->InputManager->GetInputTables()) <= Controller_->Options->JobSplitter->MaxInputTableCount);

            return config;
        }

        bool IsJobInterruptible() const override
        {
            return false;
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPartitionTask, 0x63a4c761);
    };

    //! Base class implementing sort phase for sort operations
    //! and partition reduce phase for map-reduce operations.
    class TSortTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TSortTaskBase() = default;

        TSortTaskBase(
            TSortControllerBase* controller,
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors,
            bool isFinalSort)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , IsFinalSort_(isFinalSort)
        {
            auto config = New<TLogDigestConfig>();
            // LowerLimit - we do not want to adjust memory reserve lower limit for sort jobs - we are pretty sure in our initial estimates.
            config->LowerBound = 1.0;
            config->UpperBound = Controller_->Spec->JobProxyMemoryDigest->UpperBound;
            config->DefaultValue = Controller_->Spec->JobProxyMemoryDigest->DefaultValue.value_or(1.0);
            JobProxyMemoryDigest_ = CreateLogDigest(config);
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

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = GetNeededResourcesForChunkStripe(
                joblet->InputStripeList->GetAggregateStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        EJobType GetJobType() const override
        {
            if (IsFinalSort_) {
                return Controller_->GetFinalSortJobType();
            } else {
                return Controller_->GetIntermediateSortJobType();
            }
        }

        bool IsSimpleTask() const override
        {
            return false;
        }

        void OnStripeRegistrationFailed(
            TError error,
            IChunkPoolInput::TCookie cookie,
            const TChunkStripePtr& stripe,
            const TOutputStreamDescriptorPtr& descriptor) override
        {
            if (IsFinalSort_) {
                // Somehow we failed resuming a lost stripe in a sink. No comments.
                TTask::OnStripeRegistrationFailed(error, cookie, stripe, descriptor);
            }
            Controller_->SortedMergeTask->AbortAllActiveJoblets(error, *stripe->PartitionTag);
            // TODO(max42): maybe moving chunk mapping outside of the pool was not that great idea.
            // Let's live like this a bit, and then maybe move it inside pool.
            descriptor->DestinationPool->Reset(cookie, stripe, descriptor->ChunkMapping);
            descriptor->ChunkMapping->Reset(cookie, stripe);
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // Sort jobs are unsplittable.
            config->EnableJobSplitting = false;

            return config;
        }

        bool IsJobInterruptible() const override
        {
            return false;
        }

        bool IsFinal() const
        {
            return IsFinalSort_;
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

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto stat = AggregateStatistics(GetChunkPoolOutput()->GetApproximateStripeStatistics());
            YT_VERIFY(stat.size() == 1);
            auto result = GetNeededResourcesForChunkStripe(stat.front());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            if (IsFinalSort_) {
                jobSpec->CopyFrom(Controller_->FinalSortJobSpecTemplate);
            } else {
                jobSpec->CopyFrom(Controller_->IntermediateSortJobSpecTemplate);
            }

            AddOutputTableSpecs(jobSpec, joblet);

            auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_is_approximate(joblet->InputStripeList->IsApproximate);

            AddSequentialInputSpec(jobSpec, joblet);

            auto partitionIndex = joblet->InputStripeList->PartitionTag;
            if (partitionIndex) {
                auto partitionTag = *Controller_->GetFinalPartition(*partitionIndex)->ParentPartitionTag;
                auto jobType = GetJobType();
                if (jobType == EJobType::PartitionReduce || jobType == EJobType::ReduceCombiner) {
                    auto* reduceJobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                    jobSpecExt->set_partition_tag(partitionTag);
                    reduceJobSpecExt->set_partition_tag(partitionTag);
                } else {
                    auto* sortJobSpecExt = jobSpec->MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                    jobSpecExt->set_partition_tag(partitionTag);
                    sortJobSpecExt->set_partition_tag(partitionTag);
                }
            }
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            if (IsFinalSort_) {
                Controller_->AccountRows(jobSummary);

                RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);
            } else {
                int inputStreamIndex = CurrentInputStreamIndex_++;

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto& jobResultExt = jobSummary.GetJobResultExt();
                auto stripe = BuildIntermediateChunkStripe(jobResultExt.mutable_output_chunk_specs());

                for (const auto& dataSlice : stripe->DataSlices) {
                    // NB: Intermediate sort uses sort_by as a prefix, while pool expects reduce_by as a prefix.
                    auto keyColumnCount = Controller_->GetSortedMergeSortColumns().size();
                    SetLimitsFromShortenedBoundaryKeys(dataSlice, keyColumnCount, Controller_->RowBuffer);
                    // Transform data slice to legacy if legacy sorted pool is used in sorted merge.
                    if (!Controller_->Spec->UseNewSortedPool) {
                        dataSlice->TransformToLegacy(Controller_->RowBuffer);
                    }
                    dataSlice->SetInputStreamIndex(inputStreamIndex);
                }

                if (Controller_->SimpleSort) {
                    stripe->PartitionTag = 0;
                } else {
                    stripe->PartitionTag = joblet->InputStripeList->PartitionTag;
                }

                std::optional<TMD5Hash> outputDigest;
                if (!jobResultExt.output_digests().empty()) {
                    FromProto(&outputDigest, jobResultExt.output_digests()[0]);
                }

                RegisterStripe(
                    stripe,
                    OutputStreamDescriptors_[0],
                    joblet,
                    TChunkStripeKey(),
                    /*processEmptyStripes*/ false,
                    outputDigest);
            }

            Controller_->CheckMergeStartThreshold();

            if (!IsFinalSort_) {
                Controller_->UpdateTask(Controller_->SortedMergeTask.Get());
            }

            return result;
        }

        void OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId) override
        {
            TTask::OnJobLost(completedJob, chunkId);

            Controller_->UpdateTask(this);
            if (!Controller_->SimpleSort) {
                Controller_->UpdateTask(Controller_->GetFinalPartitionTask().Get());
            }
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (!IsFinalSort_) {
                Controller_->SortedMergeTask->FinishInput();
                Controller_->UpdateMergeTasks();
                Controller_->ValidateMergeDataSliceLimit();
            }
        }

        bool CanLoseJobs() const override
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

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSortTaskBase, 0x184a5af8);
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
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors,
            bool isFinalSort)
            : TSortTaskBase(
                controller,
                std::move(outputStreamDescriptors),
                std::move(inputStreamDescriptors),
                isFinalSort)
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput({}))
        { }

        TDuration GetLocalityTimeout() const override
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

        i64 GetLocality(TNodeId nodeId) const override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                return localityEntry->Locality;
            } else {
                return 0;
            }
        }

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(const TAllocation& allocation) override
        {
            auto nodeId = HasInputLocality() ? NodeIdFromAllocationId(allocation.Id) : InvalidNodeId;
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                auto partitionIndex = localityEntry->PartitionIndex;
                return MultiChunkPoolOutput_->ExtractFromPool(partitionIndex, nodeId);
            } else {
                return MultiChunkPoolOutput_->Extract(nodeId);
            }
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShuffleMultiChunkPoolInputs.back();
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPoolOutput_;
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetSortUserJobSpec(IsFinalSort_);
        }

        TInputChunkMappingPtr GetChunkMapping() const override
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

        void Finalize() const
        {
            MultiChunkPoolOutput_->Finalize();
        }

    private:
        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TPartitionPtr> Partitions_;

        bool IsActive() const override
        {
            return Controller_->SortStartThresholdReached || IsFinalSort_;
        }

        bool HasInputLocality() const override
        {
            return false;
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            auto nodeId = joblet->NodeDescriptor.Id;

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            const auto& partition = Controller_->GetFinalPartition(partitionIndex);

            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            partition->AddLocality(nodeId, joblet->InputStripeList->TotalDataWeight);

            // Don't rely on static assignment anymore.
            partition->SetAssignedNodeId(InvalidNodeId);

            UpdateTask();

            TSortTaskBase::OnJobStarted(joblet);
        }

        void OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId) override
        {
            if (!Controller_->SimpleSort) {
                auto partitionIndex = *completedJob->InputStripe->PartitionTag;
                const auto& partition = Controller_->GetFinalPartition(partitionIndex);
                auto nodeId = completedJob->NodeDescriptor.Id;
                partition->AddLocality(nodeId, -completedJob->DataWeight);
            }

            Controller_->ResetTaskLocalityDelays();

            TSortTaskBase::OnJobLost(completedJob, chunkId);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TSortTaskBase::OnJobCompleted(joblet, jobSummary);

            // TODO(gritukan): It seems to be the easiest way to distinguish data sent from partition task
            // to intermediate sort and to final sort. Do it more generic way.
            if (!jobSummary.Abandoned) {
                YT_VERIFY(jobSummary.TotalInputDataStatistics);
                TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
                    Controller_->GetFinalPartitionTask()->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    *jobSummary.TotalInputDataStatistics);
            }

            if (!IsFinalSort_) {
                auto partitionIndex = *joblet->InputStripeList->PartitionTag;
                const auto& partition = Controller_->GetFinalPartition(partitionIndex);
                if (partition->ChunkPoolOutput->IsCompleted()) {
                    auto error = Controller_->SortedMergeTask->OnIntermediateSortCompleted(partitionIndex);
                    if (!error.IsOK()) {
                        result.OperationFailedError = error;
                        return result;
                    }
                }
            }

            return result;
        }

        void OnTaskCompleted() override
        {
            TSortTaskBase::OnTaskCompleted();

            if (IsFinalSort_)  {
                for (const auto& partition : Partitions_) {
                    Controller_->OnFinalPartitionCompleted(partition);
                }
            }
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSortTask, 0x4f9a6cd9);
    };

    //! Implements simple sort phase for sort operations.
    class TSimpleSortTask
        : public TSortTaskBase
    {
    public:
        //! For persistence only.
        TSimpleSortTask() = default;

        TSimpleSortTask(
            TSortControllerBase* controller,
            TPartition* partition,
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
            : TSortTaskBase(
                controller,
                std::move(outputStreamDescriptors),
                std::move(inputStreamDescriptors),
                /*isFinalSort*/ true)
            , Partition_(partition)
        {
            GetChunkPoolOutput()->GetDataWeightCounter()->AddParent(Controller_->SortDataWeightCounter);
        }

        TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec->SimpleSortLocalityTimeout
                : TDuration::Zero();
        }

        TString GetTitle() const override
        {
            return Format("SimpleSort");
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->SimpleSortPool;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return Controller_->SimpleSortPool;
        }

        void OnTaskCompleted() override
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
            OutputStreamDescriptors_ = {Controller_->GetSortedMergeStreamDescriptor()};
        }

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(const TAllocation& allocation) override
        {
            auto cookie = TSortTaskBase::ExtractCookieForAllocation(allocation);

            // NB(gritukan): In some weird cases unordered chunk pool can estimate total
            // number of jobs as 1 after pool creation and >1 after first cookie extraction.
            // For example, this might happen if there is a few data but many slices in the pool.
            // That's why we can understand that simple sort required sorted merge only after first
            // job start.
            Controller_->IsSortedMergeNeeded(Partition_);

            return cookie;
        }

    private:
        TPartition* Partition_;

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSimpleSortTask, 0xb32d4f02);
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
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors,
            bool enableKeyGuarantee)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , MultiChunkPool_(CreateMultiChunkPool({}))
        {
            ChunkPoolInput_ = CreateTaskUpdatingAdapter(MultiChunkPool_, this);

            MultiChunkPool_->GetJobCounter()->AddParent(Controller_->SortedMergeJobCounter);

            if (enableKeyGuarantee) {
                InputChunkMapping_ = New<TInputChunkMapping>(EChunkMappingMode::Sorted, Logger);
            } else {
                InputChunkMapping_ = New<TInputChunkMapping>(EChunkMappingMode::SortedWithoutKeyGuarantree, Logger);
            }
        }

        TDuration GetLocalityTimeout() const override
        {
            if (!Controller_->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

            return
                Controller_->SimpleSort
                ? Controller_->Spec->SimpleMergeLocalityTimeout
                : Controller_->Spec->MergeLocalityTimeout;
        }

        i64 GetLocality(TNodeId nodeId) const override
        {
            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                return localityEntry->Locality;
            } else {
                return 0;
            }
        }

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(const TAllocation& allocation) override
        {
            auto nodeId = HasInputLocality() ? NodeIdFromAllocationId(allocation.Id) : InvalidNodeId;

            auto localityEntry = Controller_->GetLocalityEntry(nodeId);
            if (localityEntry) {
                auto partitionIndex = localityEntry->PartitionIndex;
                return MultiChunkPool_->ExtractFromPool(partitionIndex, nodeId);
            } else {
                return MultiChunkPool_->Extract(nodeId);
            }
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto resources = Controller_->GetSortedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(resources);
            return resources;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPoolInput_;
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetSortedMergeUserJobSpec();
        }

        EJobType GetJobType() const override
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
            std::vector<TJobletPtr> partitionJoblets(ActiveJoblets_[partitionIndex].begin(), ActiveJoblets_[partitionIndex].end());
            for (const auto& joblet : partitionJoblets) {
                Controller_->AbortJob(joblet->JobId, EAbortReason::ChunkMappingInvalidated);
                InvalidatedJoblets_[partitionIndex].insert(joblet);
            }
            for (const auto& jobOutput : JobOutputs_[partitionIndex]) {
                auto tableIndex = Controller_->GetRowCountLimitTableIndex();
                if (tableIndex && jobOutput.JobSummary.OutputDataStatistics) {
                    auto count = VectorAtOr(*jobOutput.JobSummary.OutputDataStatistics, *tableIndex).row_count();
                    // We have to unregister registered output rows.
                    Controller_->RegisterOutputRows(-count, *tableIndex);
                }
            }
            JobOutputs_[partitionIndex].clear();
        }

        void RegisterPartition(TPartitionPtr partition)
        {
            YT_VERIFY(partition->IsFinal());

            auto partitionIndex = partition->Index;
            auto sortedMergeChunkPool = Controller_->CreateSortedMergeChunkPool(Format("%v(%v)", GetTitle(), partitionIndex));
            YT_VERIFY(SortedMergeChunkPools_.emplace(partitionIndex, sortedMergeChunkPool).second);
            MultiChunkPool_->AddPool(std::move(sortedMergeChunkPool), partitionIndex);

            Partitions_.push_back(std::move(partition));

            if (partitionIndex >= std::ssize(ActiveJoblets_)) {
                ActiveJoblets_.resize(partitionIndex + 1);
                InvalidatedJoblets_.resize(partitionIndex + 1);
                JobOutputs_.resize(partitionIndex + 1);
            }

            Controller_->UpdateTask(this);
        }

        TError OnIntermediateSortCompleted(int partitionIndex)
        {
            YT_LOG_DEBUG(
                "Intermediate sorting completed, finishing sorted merge chunk pool (PartitionIndex: %v)",
                partitionIndex);

            const auto& chunkPool = GetOrCrash(SortedMergeChunkPools_, partitionIndex);

            try {
                chunkPool->Finish();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(
                    ex,
                    "Error while finishing input for sorted chunk pool (PartitionIndex: %v)",
                    partitionIndex);
                return TError("Error while finishing input for sorted chunk pool")
                        << ex
                        << TErrorAttribute("partition_index", partitionIndex);
            }

            return TError();
        }

        void Finalize() const
        {
            MultiChunkPool_->Finalize();
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // TODO(gritukan): In case of many sort jobs per partition job specs may
            // become huge because of job splitting.
            config->EnableJobSplitting = false;

            return config;
        }

        bool IsJobInterruptible() const override
        {
            return false;
        }

    private:
        TSortControllerBase* Controller_;

        std::vector<TPartitionPtr> Partitions_;

        IMultiChunkPoolPtr MultiChunkPool_;
        IPersistentChunkPoolInputPtr ChunkPoolInput_;

        THashMap<int, IPersistentChunkPoolPtr> SortedMergeChunkPools_;

        //! Partition index -> list of active joblets.
        std::vector<THashSet<TJobletPtr>> ActiveJoblets_;

        //! Partition index -> list of invalidated joblets.
        std::vector<THashSet<TJobletPtr>> InvalidatedJoblets_;

        struct TJobOutput
        {
            TJobletPtr Joblet;
            TCompletedJobSummary JobSummary;

            PHOENIX_DECLARE_TYPE(TJobOutput, 0xf2f35a40);
        };

        //! Partition index -> list of job outputs.
        std::vector<std::vector<TJobOutput>> JobOutputs_;

        void RegisterAllOutputs()
        {
            for (auto& jobOutputs : JobOutputs_) {
                for (auto& jobOutput : jobOutputs) {
                    Controller_->AccountRows(jobOutput.JobSummary);
                    RegisterOutput(jobOutput.JobSummary, jobOutput.Joblet->ChunkListIds, jobOutput.Joblet);
                }
            }
        }

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller_->GetSortedMergeResources(
                MultiChunkPool_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPool_;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->SortedMergeJobSpecTemplate);
            auto comparator = GetComparator(Controller_->Spec->SortBy);
            AddParallelInputSpec(jobSpec, joblet, comparator);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            TTask::OnJobStarted(joblet);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].insert(joblet).second);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);
            if (!InvalidatedJoblets_[partitionIndex].contains(joblet)) {
                JobOutputs_[partitionIndex].emplace_back(TJobOutput{joblet, jobSummary});
            }

            return result;
        }

        TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobFailed(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);

            return result;
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->PartitionTag;
            YT_VERIFY(ActiveJoblets_[partitionIndex].erase(joblet) == 1);

            return result;
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            RegisterAllOutputs();

            for (const auto& partition : Partitions_) {
                Controller_->OnFinalPartitionCompleted(partition);
            }
        }

        PHOENIX_DECLARE_FRIEND();
        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSortedMergeTask, 0x4ab19c75);
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
            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
            std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput({}))
        {
            MultiChunkPoolOutput_->GetJobCounter()->AddParent(Controller_->UnorderedMergeJobCounter);
        }

        i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            // Locality is unimportant.
            return 0;
        }

        TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetUnorderedMergeResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShuffleMultiChunkPoolInputs.back();
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPoolOutput_;
        }

        EJobType GetJobType() const override
        {
            return EJobType::UnorderedMerge;
        }

        TInputChunkMappingPtr GetChunkMapping() const override
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

    private:
        TSortControllerBase* Controller_;

        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TPartitionPtr> Partitions_;

        bool IsActive() const override
        {
            return Controller_->MergeStartThresholdReached;
        }

        TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto resources = Controller_->GetUnorderedMergeResources(
                MultiChunkPoolOutput_->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(resources);
            return resources;
        }

        bool HasInputLocality() const override
        {
            return false;
        }

        void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

            jobSpec->CopyFrom(Controller_->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);

            auto partitionIndex = joblet->InputStripeList->PartitionTag;
            if (partitionIndex) {
                auto partitionTag = *Controller_->GetFinalPartition(*partitionIndex)->ParentPartitionTag;
                auto* mergeJobSpecExt = jobSpec->MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);
                mergeJobSpecExt->set_partition_tag(partitionTag);
                auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
                jobSpecExt->set_partition_tag(partitionTag);
            }
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            Controller_->AccountRows(jobSummary);
            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

            // TODO(gritukan): It seems to be the easiest way to distinguish data sent from partition task
            // to intermediate sort and to final sort. Do it more generic way.
            if (!jobSummary.Abandoned) {
                YT_VERIFY(jobSummary.TotalInputDataStatistics);
                TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
                    Controller_->GetFinalPartitionTask()->GetVertexDescriptor(),
                    GetVertexDescriptor(),
                    *jobSummary.TotalInputDataStatistics);
            }

            return result;
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            for (const auto& partition : Partitions_) {
                Controller_->OnFinalPartitionCompleted(partition);
            }
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            // TODO(gritukan): In case of many sort jobs per partition job specs may
            // become huge because of job splitting.
            config->EnableJobSplitting = false;

            return config;
        }

        bool IsJobInterruptible() const override
        {
            return false;
        }

        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUnorderedMergeTask, 0xbba17c0f);
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

        YT_VERIFY(std::ssize(PartitionsByLevels) == PartitionTreeDepth + 1);
        return PartitionsByLevels.back();
    }

    const TPartitionPtr& GetRootPartition() const
    {
        return PartitionsByLevels[0][0];
    }

    const TPartitionPtr& GetFinalPartition(int partitionIndex) const
    {
        YT_VERIFY(std::ssize(PartitionsByLevels) == PartitionTreeDepth + 1);
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

                    if (partition->IsIntermediate()) {
                        partition->ShuffleChunkPoolInput->Finish();
                    }

                    // Partitioning of #partition data is completed,
                    // so its data can be processed.
                    for (const auto& child : partition->Children) {
                        child->OnPartitioningCompleted();
                    }
                }));
            }
        }
    }

    IMultiChunkPoolOutputPtr CreateLevelMultiChunkPoolOutput(int level) const
    {
        const auto& partitions = PartitionsByLevels[level];
        std::vector<IPersistentChunkPoolOutputPtr> outputs;
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

        IntermediateSortTask = New<TSortTask>(
            this,
            std::vector{GetSortedMergeStreamDescriptor()},
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()),
            /*isFinalSort*/ false);
        IntermediateSortTask->SetupJobCounters();
        IntermediateSortTask->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(IntermediateSortTask);

        FinalSortTask = New<TSortTask>(
            this,
            GetFinalStreamDescriptors(),
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()),
            /*isFinalSort*/ true);
        FinalSortTask->SetupJobCounters();
        FinalSortTask->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(FinalSortTask);
    }

    void PrepareSortedMergeTask()
    {
        auto SortedMergeInputStreamDescriptors = BuildInputStreamDescriptorsFromOutputStreamDescriptors(
            SimpleSort
            ? SimpleSortTask->GetOutputStreamDescriptors()
            : IntermediateSortTask->GetOutputStreamDescriptors());

        SortedMergeTask->SetInputStreamDescriptors(SortedMergeInputStreamDescriptors);
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
            TAssignedNode(const TExecNodeDescriptorPtr& descriptor, double weight)
                : Descriptor(descriptor)
                , Weight(weight)
            { }

            TExecNodeDescriptorPtr Descriptor;
            double Weight;
            i64 AssignedDataWeight = 0;
        };

        using TAssignedNodePtr = TIntrusivePtr<TAssignedNode>;

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
            maxResourceLimits = Max(maxResourceLimits, descriptor->ResourceLimits);
            maxIOWeight = std::max(maxIOWeight, descriptor->IOWeight);
        }

        std::vector<TAssignedNodePtr> nodeHeap;
        for (const auto& [nodeId, descriptor] : nodeDescriptors) {
            double weight = 1.0;
            weight = std::min(weight, GetMinResourceRatio(descriptor->ResourceLimits, maxResourceLimits));
            weight = std::min(weight, descriptor->IOWeight > 0 ? descriptor->IOWeight / maxIOWeight : 0);
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
            if (!partition->HasAssignedNode()) {
                partitionsToAssign.push_back(partition);
            }
        }
        std::sort(partitionsToAssign.begin(), partitionsToAssign.end(), comparePartitions);

        // This is actually redundant since all values are 0.
        std::make_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

        YT_LOG_DEBUG("Assigning partitions");

        for (const auto& partition : partitionsToAssign) {
            auto node = nodeHeap.front();
            auto nodeId = node->Descriptor->Id;

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

            UpdateTask(task.Get());

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataWeight += partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            YT_LOG_DEBUG("Partition assigned (Index: %v, DataWeight: %v, Address: %v)",
                partition->Index,
                partition->ChunkPoolOutput->GetDataWeightCounter()->GetTotal(),
                node->Descriptor->Address);
        }

        for (const auto& node : nodeHeap) {
            if (node->AssignedDataWeight > 0) {
                YT_LOG_DEBUG("Node used (Address: %v, Weight: %.4lf, AssignedDataWeight: %v, AdjustedDataWeight: %v)",
                    node->Descriptor->Address,
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
            options.MaxTotalSliceCount = Config->MaxTotalSliceCount;
            options.EnablePeriodicYielder = true;
            options.ShouldSliceByRowIndices = true;
            options.Logger = Logger().WithTag("Name: RootPartition");

            RootPartitionPool = CreateOrderedChunkPool(
                std::move(options),
                IntermediateInputStreamDirectory);
        } else {
            TUnorderedChunkPoolOptions options;
            options.RowBuffer = RowBuffer;
            options.JobSizeConstraints = std::move(jobSizeConstraints);
            options.JobSizeAdjusterConfig = std::move(jobSizeAdjusterConfig);
            options.Logger = Logger().WithTag("Name: RootPartition");

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
            std::vector<IPersistentChunkPoolInputPtr> shuffleChunkPoolInputs;
            shuffleChunkPoolInputs.reserve(partitions.size());
            for (const auto& partition : partitions) {
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
                for (int childIndex = 0; childIndex < std::ssize(partition->Children); ++childIndex) {
                    auto& child = partition->Children[childIndex];
                    child->ChunkPoolOutput = shuffleChunkPool->GetOutput(childIndex);
                }
                shuffleChunkPoolInputs.push_back(partition->ShuffleChunkPoolInput);
            }

            auto shuffleMultiChunkPoolInput = CreateMultiChunkPoolInput(std::move(shuffleChunkPoolInputs));
            ShuffleMultiChunkPoolInputs.push_back(std::move(shuffleMultiChunkPoolInput));

            ShuffleMultiInputChunkMappings.push_back(New<TInputChunkMapping>(EChunkMappingMode::Unordered, Logger));
        }
    }

    void InitSimpleSortPool(IJobSizeConstraintsPtr jobSizeConstraints)
    {
        TUnorderedChunkPoolOptions options;
        options.RowBuffer = RowBuffer;
        options.JobSizeConstraints = std::move(jobSizeConstraints);
        options.Logger = Logger().WithTag("Name: SimpleSort");

        SimpleSortPool = CreateUnorderedChunkPool(
            std::move(options),
            GetInputStreamDirectory());
    }

    bool IsCompleted() const override
    {
        return CompletedPartitionCount == std::ssize(GetFinalPartitions());
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

    void OnOperationCompleted(bool interrupted) override
    {
        // This can happen if operation failed during completion.
        if (IsFinished()) {
            return;
        }

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

            YT_VERIFY(CompletedPartitionCount == std::ssize(GetFinalPartitions()));
        } else {
            // We have to save all output in SortedMergeTask.
            if (SortedMergeTask) {
                SortedMergeTask->CheckCompleted();
                if (!SortedMergeTask->IsCompleted()) {
                    // Dirty hack to save chunks.
                    SortedMergeTask->ForceComplete();
                }
            }
        }

        TOperationControllerBase::OnOperationCompleted(interrupted);
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
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
        partitionJobCount = std::max<i64>(partitionJobCount, 1);
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
        UpdateTask(SimpleSortTask.Get());
        UpdateTask(IntermediateSortTask.Get());
        UpdateTask(FinalSortTask.Get());
    }

    void UpdateMergeTasks()
    {
        UpdateTask(UnorderedMergeTask.Get());
        UpdateTask(SortedMergeTask.Get());
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

    void OnExecNodesUpdated() override
    {
        for (const auto& partitionTask : PartitionTasks) {
            partitionTask->OnExecNodesUpdated();
        }
    }

    void ProcessInputs(const TTaskPtr& inputTask, const IJobSizeConstraintsPtr& jobSizeConstraints)
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        inputTask->SetIsInput(true);

        int unversionedSlices = 0;
        int versionedSlices = 0;
        // TODO(max42): use CollectPrimaryInputDataSlices() here?
        for (auto& chunk : InputManager->CollectPrimaryUnversionedChunks()) {
            const auto& comparator = InputManager->GetInputTables()[chunk->GetTableIndex()]->Comparator;

            const auto& dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(InputStreamDirectory_.GetInputStreamIndex(chunk->GetTableIndex(), chunk->GetRangeIndex()));

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

    void SetAlertIfPartitionHeuristicsDifferSignificantly(int oldEstimation, int newEstimation)
    {
        auto [min, max] = std::minmax(newEstimation, oldEstimation);
        YT_VERIFY(min > 0);
        const auto ratio = static_cast<double>(max) / static_cast<double>(min);
        if (ratio >= Options->CriticalNewPartitionDifferenceRatio) {
            SetOperationAlert(
                EOperationAlertType::NewPartitionsCountIsSignificantlyLarger,
                TError(
                    "Partition count, estimated by the new partition heuristic, is significantly larger "
                    "than old heuristic estimation. This may lead to inadequate number of partitions.")
                    << TErrorAttribute("old_estimation", oldEstimation)
                    << TErrorAttribute("new_estimation", newEstimation));
        }
    }

    // Partition progress.

    struct TPartitionProgress
    {
        std::vector<i64> Total;
        std::vector<i64> Running;
        std::vector<i64> Completed;
    };

    static std::vector<i64> AggregateValues(const std::vector<i64>& values, int maxBuckets)
    {
        if (std::ssize(values) < maxBuckets) {
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
            for (int i = 0; i < std::ssize(finalPartitions); ++i) {
                if (const auto& chunkPoolOutput = GetFinalPartitions()[i]->ChunkPoolOutput) {
                    sizes[i] = chunkPoolOutput->GetDataWeightCounter()->GetTotal();
                } else {
                    sizes[i] = 0;
                }
            }
            result.Total = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < std::ssize(finalPartitions); ++i) {
                if (const auto& chunkPoolOutput = GetFinalPartitions()[i]->ChunkPoolOutput) {
                    sizes[i] = chunkPoolOutput->GetDataWeightCounter()->GetRunning();
                } else {
                    sizes[i] = 0;
                }
            }
            result.Running = AggregateValues(sizes, MaxProgressBuckets);
        }
        {
            for (int i = 0; i < std::ssize(finalPartitions); ++i) {
                if (const auto& chunkPoolOutput = GetFinalPartitions()[i]->ChunkPoolOutput) {
                    sizes[i] = chunkPoolOutput->GetDataWeightCounter()->GetCompletedTotal();
                } else {
                    sizes[i] = 0;
                }
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

    std::unique_ptr<IHistogram> ComputeFinalPartitionSizeHistogram() const override
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
                .Item("running").Value(progress.Running)
                .Item("completed").Value(progress.Completed)
            .EndMap()
            .Item("partition_size_histogram").Value(*sizeHistogram);
    }

    void InitJobIOConfigs()
    {
        RootPartitionJobIOConfig = CloneYsonStruct(Spec->PartitionJobIO);
        PartitionJobIOConfig = CloneYsonStruct(Spec->PartitionJobIO);
        PartitionJobIOConfig->TableReader->SamplingRate = std::nullopt;
    }

    virtual void InitIntermediateSchemas() = 0;

    void CustomMaterialize() override
    {
        TOperationControllerBase::CustomMaterialize();

        ValidateAccountPermission(Spec->IntermediateDataAccount, EPermission::Use);

        for (const auto& table : InputManager->GetInputTables()) {
            for (const auto& sortColumn : Spec->SortBy) {
                if (const auto* column = table->Schema->FindColumn(sortColumn.Name)) {
                    if (column->Aggregate()) {
                        THROW_ERROR_EXCEPTION("Sort by aggregate column is not allowed")
                            << TErrorAttribute("table_path", table->Path)
                            << TErrorAttribute("column_name", sortColumn.Name);
                    }
                }
            }
        }

        InitIntermediateSchemas();
    }

    virtual const std::vector<TOutputStreamDescriptorPtr>& GetFinalStreamDescriptors() const
    {
        return GetStandardStreamDescriptors();
    }

    virtual TOutputStreamDescriptorPtr GetSortedMergeStreamDescriptor() const
    {
        auto streamDescriptor = GetIntermediateStreamDescriptorTemplate()->Clone();

        streamDescriptor->DestinationPool = SortedMergeTask->GetChunkPoolInput();
        streamDescriptor->ChunkMapping = SortedMergeTask->GetChunkMapping();
        streamDescriptor->TableUploadOptions.TableSchema = IntermediateChunkSchema_;
        streamDescriptor->RequiresRecoveryInfo = true;
        streamDescriptor->TargetDescriptor = SortedMergeTask->GetVertexDescriptor();
        streamDescriptor->StreamSchemas = IntermediateStreamSchemas_;

        return streamDescriptor;
    }

    IPersistentChunkPoolPtr CreateSortedMergeChunkPool(TString name)
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
            GetOutputTablePaths().size());
        chunkPoolOptions.Logger = Logger().WithTag("Name: %v", name);

        if (Spec->UseNewSortedPool) {
            YT_LOG_DEBUG("Creating new sorted pool");
            return CreateNewSortedChunkPool(chunkPoolOptions, nullptr /*chunkSliceFetcher*/, IntermediateInputStreamDirectory);
        } else {
            YT_LOG_DEBUG("Creating legacy sorted pool");
            return CreateLegacySortedChunkPool(chunkPoolOptions, nullptr /*chunkSliceFetcher*/, IntermediateInputStreamDirectory);
        }
    }

    void AccountRows(const TCompletedJobSummary& jobSummary)
    {
        if (jobSummary.Abandoned) {
            return;
        }
        YT_VERIFY(jobSummary.TotalOutputDataStatistics);
        TotalOutputRowCount += jobSummary.TotalOutputDataStatistics->row_count();
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
            for (int childIndex = 0; childIndex < std::ssize(partitionTreeSkeleton->Children); ++childIndex) {
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

    std::vector<TPartitionKey> BuildPartitionKeysFromPivotKeys()
    {
        std::vector<TPartitionKey> partitionKeys;
        partitionKeys.reserve(Spec->PivotKeys.size());

        for (const auto& key : Spec->PivotKeys) {
            // Skip empty pivot keys since they coincide with virtual empty pivot of the first partition.
            if (key.GetCount() == 0) {
                continue;
            }
            auto upperBound = TKeyBound::FromRow(RowBuffer->CaptureRow(key), /*isInclusive*/ true, /*isUpper*/ false);
            partitionKeys.emplace_back(upperBound);
        }

        int maxPartitionCount = Spec->UseNewPartitionsHeuristic ?
            Options->MaxNewPartitionCount : Options->MaxPartitionCount;

        THROW_ERROR_EXCEPTION_IF(
            std::ssize(partitionKeys) + 1 > maxPartitionCount,
            "Pivot keys count %v exceeds maximum number of pivot keys %v",
            std::ssize(partitionKeys),
            maxPartitionCount - 1);

        return partitionKeys;
    }

    std::pair<TRowBufferPtr, std::vector<TSample>> FetchSamples(const TTableSchemaPtr& sampleSchema, i64 sampleCount)
    {
        auto samplesRowBuffer = New<TRowBuffer>(
            TRowBufferTag(),
            Config->ControllerRowBufferChunkSize);

        auto [samplesFetcher, unavailableChunksWatcher] = InputManager->CreateSamplesFetcher(
            sampleSchema,
            samplesRowBuffer,
            sampleCount,
            Options->MaxSampleSize);

        {
            UnavailableChunksWatcher_ = std::move(unavailableChunksWatcher);

            auto resetChunkWatcherGuard = Finally([&] {
                UnavailableChunksWatcher_.Reset();
            });

            WaitFor(samplesFetcher->Fetch())
                .ThrowOnError();
        }

        auto samples = samplesFetcher->GetSamples();

        return {std::move(samplesRowBuffer), std::move(samples)};
    }

    std::vector<TPartitionKey> BuildPartitionKeys(
        const TTableSchemaPtr& sampleSchema,
        const TTableSchemaPtr& uploadSchema)
    {
        if (!Spec->PivotKeys.empty()) {
            return BuildPartitionKeysFromPivotKeys();
        }

        YT_VERIFY(PartitioningParametersEvaluator_);

        i64 suggestedSampleCount = PartitioningParametersEvaluator_->SuggestSampleCount();
        YT_LOG_DEBUG("Suggested sample count (SuggestedSampleCount: %v)",
            suggestedSampleCount);

        auto [samplesRowBuffer, samples] = FetchSamples(sampleSchema, suggestedSampleCount);

        YT_LOG_DEBUG("Fetched sample count (FetchedSampleCount: %v)",
            samples.size());

        int suggestedPartitionCount = PartitioningParametersEvaluator_->SuggestPartitionCount(std::ssize(samples));

        YT_LOG_DEBUG("Suggested partition count (SuggestedPartitionCount: %v)",
            suggestedPartitionCount);

        return BuildPartitionKeysFromSamples(
            samples,
            sampleSchema,
            uploadSchema,
            Host->GetClient()->GetNativeConnection()->GetExpressionEvaluatorCache(),
            suggestedPartitionCount,
            RowBuffer,
            Logger);
    }

    void InitPartitioningParametersEvaluator()
    {
        RootPartitionPoolJobSizeConstraints = CreatePartitionJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputRowCount,
            InputCompressionRatio);

        PartitioningParametersEvaluator_ = CreatePartitioningParametersEvaluator(
            Spec,
            Options,
            TotalEstimatedInputDataWeight,
            TotalEstimatedInputUncompressedDataSize,
            TotalEstimatedInputValueCount,
            InputCompressionRatio,
            RootPartitionPoolJobSizeConstraints->GetJobCount());
    }

    // TODO(apollo1321): Enable maniac partitions for map-reduce operations.
    void AssignPartitionKeysToPartitions(const std::vector<TPartitionKey>& partitionKeys, bool setManiac)
    {
        const auto& finalPartitions = GetFinalPartitions();
        YT_VERIFY(finalPartitions.size() == partitionKeys.size() + 1);
        finalPartitions[0]->LowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
        for (int finalPartitionIndex = 1; finalPartitionIndex < std::ssize(finalPartitions); ++finalPartitionIndex) {
            const auto& partition = finalPartitions[finalPartitionIndex];
            const auto& partitionKey = partitionKeys[finalPartitionIndex - 1];
            YT_LOG_DEBUG("Assigned lower key bound to final partition (FinalPartitionIndex: %v, KeyBound: %v)",
                finalPartitionIndex,
                partitionKey.LowerBound);
            partition->LowerBound = partitionKey.LowerBound;
            if (partitionKey.Maniac && setManiac) {
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

    virtual void CreateSortedMergeTask() = 0;

    virtual TSortColumns GetSortedMergeSortColumns() const = 0;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSortControllerBase, 0x4f7dcb2f);
};

void TSortControllerBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TOperationControllerBase>();

    PHOENIX_REGISTER_FIELD(1, Spec);

    PHOENIX_REGISTER_FIELD(2, CompletedPartitionCount);
    PHOENIX_REGISTER_FIELD(3, PartitionJobCounter);
    PHOENIX_REGISTER_FIELD(4, SortedMergeJobCounter);
    PHOENIX_REGISTER_FIELD(5, UnorderedMergeJobCounter);
    PHOENIX_REGISTER_FIELD(6, IntermediateSortJobCounter);
    PHOENIX_REGISTER_FIELD(7, FinalSortJobCounter);
    PHOENIX_REGISTER_FIELD(8, SortDataWeightCounter);

    PHOENIX_REGISTER_FIELD(9, SortStartThresholdReached);
    PHOENIX_REGISTER_FIELD(10, MergeStartThresholdReached);

    PHOENIX_REGISTER_FIELD(11, TotalOutputRowCount);

    PHOENIX_REGISTER_FIELD(12, SimpleSort);
    PHOENIX_REGISTER_FIELD(13, PartitionsByLevels);
    PHOENIX_REGISTER_FIELD(14, PartitionTreeDepth);
    PHOENIX_REGISTER_FIELD(15, PartitionCount);
    PHOENIX_REGISTER_FIELD(16, MaxPartitionFactor);

    PHOENIX_REGISTER_FIELD(17, AssignedPartitionsByNodeId);
    PHOENIX_REGISTER_FIELD(18, PartitionsLocalityByNodeId);

    PHOENIX_REGISTER_FIELD(19, RootPartitionJobSpecTemplate);
    PHOENIX_REGISTER_FIELD(20, PartitionJobSpecTemplate);
    PHOENIX_REGISTER_FIELD(21, IntermediateSortJobSpecTemplate);
    PHOENIX_REGISTER_FIELD(22, FinalSortJobSpecTemplate);
    PHOENIX_REGISTER_FIELD(23, SortedMergeJobSpecTemplate);
    PHOENIX_REGISTER_FIELD(24, UnorderedMergeJobSpecTemplate);

    PHOENIX_REGISTER_FIELD(25, RootPartitionJobIOConfig);
    PHOENIX_REGISTER_FIELD(26, PartitionJobIOConfig);
    PHOENIX_REGISTER_FIELD(27, IntermediateSortJobIOConfig);
    PHOENIX_REGISTER_FIELD(28, FinalSortJobIOConfig);
    PHOENIX_REGISTER_FIELD(29, SortedMergeJobIOConfig);
    PHOENIX_REGISTER_FIELD(30, UnorderedMergeJobIOConfig);

    PHOENIX_REGISTER_FIELD(31, RootPartitionPoolJobSizeConstraints);
    PHOENIX_REGISTER_FIELD(32, RootPartitionPool);
    PHOENIX_REGISTER_FIELD(33, SimpleSortPool);

    PHOENIX_REGISTER_FIELD(34, ShuffleMultiChunkPoolInputs);
    PHOENIX_REGISTER_FIELD(35, ShuffleMultiInputChunkMappings);

    registrar.template VirtualField<36>("IntermediateChunkSchema_", [] (TThis* this_, auto& context) {
        NYT::Load(context, *this_->IntermediateChunkSchema_);
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, *this_->IntermediateChunkSchema_);
    })();
    PHOENIX_REGISTER_FIELD(37, IntermediateStreamSchemas_,
        .template Serializer<TVectorSerializer<TNonNullableIntrusivePtrSerializer<>>>());

    PHOENIX_REGISTER_FIELD(38, PartitionTasks);
    PHOENIX_REGISTER_FIELD(39, SimpleSortTask);
    PHOENIX_REGISTER_FIELD(40, IntermediateSortTask);
    PHOENIX_REGISTER_FIELD(41, FinalSortTask);
    PHOENIX_REGISTER_FIELD(42, UnorderedMergeTask);
    PHOENIX_REGISTER_FIELD(43, SortedMergeTask);

    PHOENIX_REGISTER_FIELD(44, SwitchedToSlowIntermediateMedium);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->SetupPartitioningCompletedCallbacks();
    });
}

PHOENIX_DEFINE_TYPE(TSortControllerBase);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TPartition::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Controller);

    PHOENIX_REGISTER_FIELD(2, Level);
    PHOENIX_REGISTER_FIELD(3, Index);
    PHOENIX_REGISTER_FIELD(4, ParentPartitionTag);

    PHOENIX_REGISTER_FIELD(5, Children);

    PHOENIX_REGISTER_FIELD(6, LowerBound);

    PHOENIX_REGISTER_FIELD(7, Completed);

    PHOENIX_REGISTER_FIELD(8, PartitioningCompleted);

    PHOENIX_REGISTER_FIELD(9, CachedSortedMergeNeeded);

    PHOENIX_REGISTER_FIELD(10, Maniac);

    PHOENIX_REGISTER_FIELD(11, AssignedNodeId);

    PHOENIX_REGISTER_FIELD(12, ShuffleChunkPool);
    PHOENIX_REGISTER_FIELD(13, ShuffleChunkPoolInput);
    PHOENIX_REGISTER_FIELD(14, ChunkPoolOutput);

    PHOENIX_REGISTER_FIELD(15, Logger);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TPartition);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TPartitionTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, DataBalancer_);
    PHOENIX_REGISTER_FIELD(3, Level_);
    PHOENIX_REGISTER_FIELD(4, ShuffleMultiChunkOutput_);
    registrar.template VirtualField<5>("WirePartitionKeys_", [] (TThis* /*this_*/, auto& context) {
        Load<std::vector<std::optional<TString>>>(context);
    })
        .BeforeVersion(ESnapshotVersion::DropLegacyWirePartitionKeys)();
    PHOENIX_REGISTER_FIELD(6, WirePartitionLowerBoundPrefixes_);
    PHOENIX_REGISTER_FIELD(7, PartitionLowerBoundInclusivenesses_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        if (this_->DataBalancer_) {
            this_->DataBalancer_->OnExecNodesUpdated(this_->Controller_->GetOnlineExecNodeDescriptors());
        }
    });
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TPartitionTask);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TSortTaskBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, IsFinalSort_);
    PHOENIX_REGISTER_FIELD(3, CurrentInputStreamIndex_);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TSortTaskBase);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TSortTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSortTaskBase>();

    PHOENIX_REGISTER_FIELD(1, MultiChunkPoolOutput_);
    PHOENIX_REGISTER_FIELD(2, Partitions_);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TSortTask);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TSimpleSortTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSortTaskBase>();

    PHOENIX_REGISTER_FIELD(1, Partition_);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TSimpleSortTask);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TSortedMergeTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, Partitions_);
    PHOENIX_REGISTER_FIELD(3, MultiChunkPool_);
    PHOENIX_REGISTER_FIELD(4, ChunkPoolInput_);
    PHOENIX_REGISTER_FIELD(5, SortedMergeChunkPools_);
    PHOENIX_REGISTER_FIELD(6, ActiveJoblets_,
        .template Serializer<TVectorSerializer<TSetSerializer<TDefaultSerializer, TUnsortedTag>>>());
    PHOENIX_REGISTER_FIELD(7, InvalidatedJoblets_,
        .template Serializer<TVectorSerializer<TSetSerializer<TDefaultSerializer, TUnsortedTag>>>());
    PHOENIX_REGISTER_FIELD(8, JobOutputs_);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TSortedMergeTask);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TSortedMergeTask::TJobOutput::RegisterMetadata(auto&& registrar)
{
    // TODO(max42): this place seems to be the only occurrence of job summary persistence.
    // Do we really need this?

    PHOENIX_REGISTER_FIELD(1, Joblet);
    PHOENIX_REGISTER_FIELD(2, JobSummary);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TSortedMergeTask::TJobOutput);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TUnorderedMergeTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, MultiChunkPoolOutput_);
    PHOENIX_REGISTER_FIELD(3, Partitions_);
}

PHOENIX_DEFINE_TYPE(TSortControllerBase::TUnorderedMergeTask);

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
    TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
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

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::Partition, EJobType::FinalSort};
    }

private:
    TSortOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    void PrepareOutputTables() override
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

        if (auto writeMode = table->TableUploadOptions.VersionedWriteOptions.WriteMode; writeMode != EVersionedIOMode::Default) {
            THROW_ERROR_EXCEPTION("Versioned write mode %Qlv is not supported for sort operation",
                writeMode);
        }

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(Spec->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
                    ValidateOutputSchemaCompatibility({
                        .IgnoreSortOrder = true,
                        .ForbidExtraComputedColumns = false,
                        .IgnoreStableNamesDifference = true,
                    });
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

    void InitIntermediateSchemas() override
    {
        IntermediateChunkSchema_ = OutputTables_[0]->TableUploadOptions.TableSchema->ToSorted(Spec->SortBy);
        IntermediateStreamSchemas_ = {IntermediateChunkSchema_};
    }

    void CustomMaterialize() override
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

        InitPartitioningParametersEvaluator();

        auto partitionKeys = BuildPartitionKeys(
            BuildSampleSchema(),
            OutputTables_[0]->TableUploadOptions.GetUploadSchema());

        PartitionCount = partitionKeys.size() + 1;
        MaxPartitionFactor = PartitioningParametersEvaluator_->SuggestMaxPartitionFactor(PartitionCount);

        SimpleSort = (PartitionCount == 1);

        YT_LOG_DEBUG("Final partitioning parameters (PartitionCount: %v, MaxPartitionFactor: %v, SimpleSort: %v)",
            PartitionCount,
            MaxPartitionFactor,
            SimpleSort);

        if (Spec->UseNewPartitionsHeuristic) {
            SetAlertIfPartitionHeuristicsDifferSignificantly(
                PartitioningParametersEvaluator_->SuggestPartitionCount(
                    /*fetchedSamplesCount*/ std::nullopt,
                    /*forceLegacy*/ true),
                PartitionCount);
        }

        BuildPartitionTree(PartitionCount, MaxPartitionFactor);

        AssignPartitionKeysToPartitions(partitionKeys, /*setManiac*/ true);

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

        InitPartitionPool(RootPartitionPoolJobSizeConstraints, nullptr, false /*ordered*/);

        PartitionTasks.resize(PartitionTreeDepth);
        for (int partitionTaskLevel = PartitionTreeDepth - 1; partitionTaskLevel >= 0; --partitionTaskLevel) {
            auto shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate()->Clone();
            shuffleStreamDescriptor->DestinationPool = ShuffleMultiChunkPoolInputs[partitionTaskLevel];
            shuffleStreamDescriptor->ChunkMapping = ShuffleMultiInputChunkMappings[partitionTaskLevel];
            shuffleStreamDescriptor->TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor->TableUploadOptions.TableSchema = OutputTables_[0]->TableUploadOptions.TableSchema;
            if (partitionTaskLevel != PartitionTreeDepth - 1) {
                shuffleStreamDescriptor->TargetDescriptor = PartitionTasks[partitionTaskLevel + 1]->GetVertexDescriptor();
            }

            PartitionTasks[partitionTaskLevel] = New<TPartitionTask>(
                this,
                std::vector<TOutputStreamDescriptorPtr>{shuffleStreamDescriptor},
                std::vector<TInputStreamDescriptorPtr>{},
                partitionTaskLevel);
        }


        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < std::ssize(PartitionTasks); ++partitionTaskLevel) {
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

        const auto& partition = GetFinalPartition(0);

        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec,
            Options,
            Logger,
            TotalEstimatedInputDataWeight);

        InitSimpleSortPool(jobSizeConstraints);

        SimpleSortTask = New<TSimpleSortTask>(
            this,
            partition.Get(),
            GetFinalStreamDescriptors(),
            std::vector<TInputStreamDescriptorPtr>{});
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

        UnorderedMergeTask = New<TUnorderedMergeTask>(
            this,
            GetFinalStreamDescriptors(),
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()));
        RegisterTask(UnorderedMergeTask);
        UnorderedMergeTask->SetInputVertex(GetFinalPartitionTask()->GetVertexDescriptor());
        UnorderedMergeTask->RegisterInGraph();
    }

    TTableSchemaPtr BuildSampleSchema() const
    {
        THashSet<std::string> uniqueColumns;
        auto schema = OutputTables_[0]->TableUploadOptions.GetUploadSchema();

        for (const auto& sortColumn : Spec->SortBy) {
            const auto& column = schema->GetColumn(sortColumn.Name);

            if (const auto& expression = column.Expression()) {
                THashSet<std::string> references;
                PrepareExpression(*expression, *schema, GetBuiltinTypeInferrers(), &references);

                uniqueColumns.insert(references.begin(), references.end());
            } else {
                uniqueColumns.insert(column.Name());
            }
        }

        std::vector<TColumnSchema> columns;
        columns.reserve(uniqueColumns.size());
        for (const auto& column : uniqueColumns) {
            columns.push_back(schema->GetColumn(column));
        }

        return New<TTableSchema>(std::move(columns));
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        IntermediateSortJobIOConfig = CloneYsonStruct(Spec->SortJobIO);

        // Final sort: reader like sort and output like merge.
        FinalSortJobIOConfig = CloneYsonStruct(Spec->SortJobIO);
        FinalSortJobIOConfig->TableWriter = CloneYsonStruct(Spec->MergeJobIO->TableWriter);

        SortedMergeJobIOConfig = CloneYsonStruct(Spec->MergeJobIO);

        UnorderedMergeJobIOConfig = CloneYsonStruct(Spec->MergeJobIO);
        // Since we're reading from huge number of partition chunks, we must use larger buffers,
        // as we do for sort jobs.
        UnorderedMergeJobIOConfig->TableReader = CloneYsonStruct(Spec->SortJobIO->TableReader);
    }

    EJobType GetIntermediateSortJobType() const override
    {
        return SimpleSort ? EJobType::SimpleSort : EJobType::IntermediateSort;
    }

    EJobType GetFinalSortJobType() const override
    {
        return SimpleSort ? EJobType::SimpleSort : EJobType::FinalSort;
    }

    EJobType GetSortedMergeJobType() const override
    {
        return EJobType::SortedMerge;
    }

    TUserJobSpecPtr GetPartitionUserJobSpec(bool /*isRoot*/) const override
    {
        return nullptr;
    }

    TUserJobSpecPtr GetSortUserJobSpec(bool /*isFinalSort*/) const override
    {
        return nullptr;
    }

    TUserJobSpecPtr GetSortedMergeUserJobSpec() const override
    {
        return nullptr;
    }

    void InitJobSpecTemplates()
    {
        auto intermediateReaderOptions = New<TTableReaderOptions>();

        {
            RootPartitionJobSpecTemplate.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig)).ToString());
            jobSpecExt->set_io_config(ConvertToYsonString(RootPartitionJobIOConfig).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec->EnableIntermediateOutputRecalculation);
        }

        {
            PartitionJobSpecTemplate.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = PartitionJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig)).ToString());
            jobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec->EnableIntermediateOutputRecalculation);
        }

        TJobSpec sortJobSpecTemplate;
        {
            auto* jobSpecExt = sortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);

            if (SimpleSort) {
                jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->PartitionJobIO)).ToString());
                SetProtoExtension<TDataSourceDirectoryExt>(
                    jobSpecExt->mutable_extensions(),
                    BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
            } else {
                jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
                SetProtoExtension<TDataSourceDirectoryExt>(
                    jobSpecExt->mutable_extensions(),
                    BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            }

            auto* sortJobSpecExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
        }

        {
            IntermediateSortJobSpecTemplate = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate.set_type(ToProto(GetIntermediateSortJobType()));
            auto* jobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            jobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).ToString());
        }

        {
            FinalSortJobSpecTemplate = sortJobSpecTemplate;
            FinalSortJobSpecTemplate.set_type(ToProto(GetFinalSortJobType()));
            auto* jobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));
            jobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).ToString());
        }

        {
            SortedMergeJobSpecTemplate.set_type(ToProto(EJobType::SortedMerge));
            auto* jobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* mergeJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(
                    GetSpec()->IntermediateDataAccount,
                    {IntermediateChunkSchema_}));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));

            jobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).ToString());

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(mergeJobSpecExt->mutable_sort_columns(), Spec->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate.set_type(ToProto(EJobType::UnorderedMerge));
            auto* jobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));

            jobSpecExt->set_io_config(ConvertToYsonString(UnorderedMergeJobIOConfig).ToString());

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
        }
    }


    // Resource management.

    TCpuResource GetPartitionCpuLimit() const override
    {
        return 1;
    }

    TCpuResource GetSortCpuLimit() const override
    {
        return 1;
    }

    TCpuResource GetMergeCpuLimit() const override
    {
        return 1;
    }

    TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool /*isRoot*/) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 outputBufferSize = std::min(
            PartitionJobIOConfig->TableWriter->BlockSize * std::ssize(GetFinalPartitions()),
            stat.DataWeight);

        outputBufferSize += THorizontalBlockWriter::MaxReserveSize * std::ssize(GetFinalPartitions());

        outputBufferSize = std::min(
            outputBufferSize,
            PartitionJobIOConfig->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetPartitionCpuLimit());
        result.SetJobProxyMemory(GetInputIOMemorySize(PartitionJobIOConfig, stat)
            + outputBufferSize
            + GetOutputWindowMemorySize(PartitionJobIOConfig));

        // Partition map jobs don't use estimated writer buffer size.
        result.SetJobProxyMemoryWithFixedWriteBufferSize(result.GetJobProxyMemory());
        return result;
    }

    TExtendedJobResources GetSimpleSortResources(const TChunkStripeStatistics& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());

        auto memory = GetSortInputIOMemorySize(stat) +
            // Row data memory footprint inside SchemalessSortingReader.
            16 * stat.ValueCount + 16 * stat.RowCount;

        result.SetJobProxyMemory(
            memory + GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ true));
        result.SetJobProxyMemoryWithFixedWriteBufferSize(
            memory + GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ false));
        return result;
    }

    TExtendedJobResources GetPartitionSortResources(
        bool isFinalSort,
        const TChunkStripeStatistics& stat) const override
    {
        i64 jobProxyMemory =
            GetSortBuffersMemorySize(stat) +
            GetSortInputIOMemorySize(stat);
        i64 jobProxyMemoryWithFixedWriteBufferSize = jobProxyMemory;

        if (isFinalSort) {
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ true);
            jobProxyMemoryWithFixedWriteBufferSize += GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ false);
        } else {
            jobProxyMemory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
            jobProxyMemoryWithFixedWriteBufferSize += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
        }

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());
        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        if (Config->EnableNetworkInOperationDemand) {
            result.SetNetwork(Spec->ShuffleNetworkLimit);
        }

        return result;
    }

    TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        auto jobProxyMemory = GetFinalIOMemorySize(
            SortedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ true,
            stat);
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            SortedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ false,
            stat);

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        return result;
    }

    bool IsRowCountPreserved() const override
    {
        return true;
    }

    i64 GetUnavailableInputChunkCount() const override
    {
        if (UnavailableChunksWatcher_ && State == EControllerState::Preparing) {
            return UnavailableChunksWatcher_->GetUnavailableChunkCount();
        }

        return TOperationControllerBase::GetUnavailableInputChunkCount();
    }

    TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetMergeCpuLimit());
        auto jobProxyMemory = GetFinalIOMemorySize(
            UnorderedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ true,
            stat);
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            UnorderedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ false,
            stat);

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        return result;
    }

    // Progress reporting.

    TString GetLoggingProgress() const override
    {
        const auto& jobCounter = GetTotalJobCounter();
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

    void BuildProgress(TFluentMap fluent) override
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

    EJobType GetPartitionJobType(bool /*isRoot*/) const override
    {
        return EJobType::Partition;
    }

    void CreateSortedMergeTask() override
    {
        SortedMergeTask = New<TSortedMergeTask>(
            this,
            GetFinalStreamDescriptors(),
            // To be filled later.
            std::vector<TInputStreamDescriptorPtr>{},
            /*enableKeyGuarantee*/ false);
    }

    TSortColumns GetSortedMergeSortColumns() const override
    {
        return Spec->SortBy;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec;
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSortController, 0xbca37afe);
};

void TSortController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSortControllerBase>();
}

PHOENIX_DEFINE_TYPE(TSortController);

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

    void BuildBriefSpec(TFluentMap fluent) const override
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

    void InitStreamDescriptors()
    {
        const auto& streamDescriptors = GetStandardStreamDescriptors();

        MapperSinkEdges_ = std::vector<TOutputStreamDescriptorPtr>(
            streamDescriptors.begin(),
            streamDescriptors.begin() + Spec->MapperOutputTableCount);
        for (int index = 0; index < std::ssize(MapperSinkEdges_); ++index) {
            MapperSinkEdges_[index] = MapperSinkEdges_[index]->Clone();
            MapperSinkEdges_[index]->TableWriterOptions->TableIndex = index + 1;
        }

        ReducerSinkEdges_ = std::vector<TOutputStreamDescriptorPtr>(
            streamDescriptors.begin() + Spec->MapperOutputTableCount,
            streamDescriptors.end());
        for (int index = 0; index < std::ssize(ReducerSinkEdges_); ++index) {
            ReducerSinkEdges_[index] = ReducerSinkEdges_[index]->Clone();
            ReducerSinkEdges_[index]->TableWriterOptions->TableIndex = index;
        }
    }

protected:
    TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
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

    std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {
            EJobType::PartitionMap,
            EJobType::Partition,
            EJobType::PartitionReduce,
            EJobType::SortedReduce
        };
    }

private:
    TMapReduceOperationSpecPtr Spec;

    // Mapper stream descriptors are for the data that is written from mappers directly to the first
    // `Spec->MapperOutputTableCount` output tables skipping the shuffle and reduce phases.
    std::vector<TOutputStreamDescriptorPtr> MapperSinkEdges_;
    std::vector<TOutputStreamDescriptorPtr> ReducerSinkEdges_;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserFile> ReduceCombinerFiles;
    std::vector<TUserFile> ReducerFiles;

    i64 MapStartRowIndex = 0;
    i64 ReduceStartRowIndex = 0;

    // Custom bits of preparation pipeline.

    void DoInitialize() override
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

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriter;
    }

    std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriter;
    }

    bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec->EnableCudaGpuCoreDump;
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
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

    static TLogicalTypePtr InferColumnType(const std::vector<TInputTablePtr>& tables, TStringBuf keyColumn)
    {
        if (keyColumn == TableIndexColumnName) {
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        }

        auto chooseMostGenericOrThrow = [&] (const auto& lhs, const auto& rhs) {
            auto [compatibilityRhs, errorRhs] = CheckTypeCompatibility(lhs, rhs);
            if (compatibilityRhs == ESchemaCompatibility::FullyCompatible) {
                return rhs;
            }
            auto [compatibilityLhs, errorLhs] = CheckTypeCompatibility(rhs, lhs);
            if (compatibilityLhs == ESchemaCompatibility::FullyCompatible) {
                return lhs;
            }
            THROW_ERROR_EXCEPTION("Type mismatch for key column %Qv in input schemas", keyColumn)
                << errorLhs
                << TErrorAttribute("lhs_type", lhs)
                << TErrorAttribute("rhs_type", rhs);
        };

        TLogicalTypePtr mostGenericType;
        bool missingInSomeSchema = false;
        for (const auto& table : tables) {
            const auto* column = table->Schema->FindColumn(keyColumn);
            if (!column) {
                missingInSomeSchema = true;
                continue;
            }
            if (!mostGenericType) {
                mostGenericType = column->LogicalType();
            } else {
                mostGenericType = chooseMostGenericOrThrow(mostGenericType, column->LogicalType());
            }
        }
        if (!mostGenericType) {
            return SimpleLogicalType(ESimpleLogicalValueType::Any);
        }
        if (missingInSomeSchema && !mostGenericType->IsNullable()) {
            return OptionalLogicalType(std::move(mostGenericType));
        }
        return mostGenericType;
    }

    void InitIntermediateSchemas() override
    {
        if (!Spec->HasSchemafulIntermediateStreams()) {
            IntermediateChunkSchema_ = TTableSchema::FromSortColumns(Spec->SortBy);
            return;
        }

        std::vector<TColumnSchema> chunkSchemaColumns;
        if (Spec->HasNontrivialMapper()) {
            YT_VERIFY(std::ssize(Spec->Mapper->OutputStreams) > Spec->MapperOutputTableCount);
            int intermediateStreamCount = Spec->Mapper->OutputStreams.size() - Spec->MapperOutputTableCount;
            for (int i = 0; i < intermediateStreamCount; ++i) {
                IntermediateStreamSchemas_.push_back(Spec->Mapper->OutputStreams[i]->Schema);
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Filter(GetColumnNames(Spec->SortBy))->Columns();
            }
        } else {
            YT_VERIFY(!InputManager->GetInputTables().empty());
            for (const auto& inputTable : InputManager->GetInputTables()) {
                auto toStreamSchema = [] (
                    const TTableSchemaPtr& schema,
                    const TSortColumns& sortColumns,
                    const std::optional<std::vector<std::string>>& columnFilter)
                {
                    auto columns = schema->Columns();
                    if (columnFilter) {
                        std::vector<TColumnSchema> newColumns;
                        for (const auto& name : *columnFilter) {
                            if (const auto* foundColumn = schema->FindColumn(name)) {
                                newColumns.push_back(*foundColumn);
                            }
                        }
                        columns = std::move(newColumns);
                    }
                    auto optionalAnyType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
                    for (const auto& sortColumn : sortColumns) {
                        if (schema->FindColumn(sortColumn.Name)) {
                            continue;
                        }
                        if (sortColumn.Name == TableIndexColumnName) {
                            columns.push_back(TColumnSchema(sortColumn.Name, SimpleLogicalType(ESimpleLogicalValueType::Int64)));
                        } else {
                            columns.push_back(TColumnSchema(sortColumn.Name, optionalAnyType));
                        }
                    }
                    return New<TTableSchema>(std::move(columns), schema->GetStrict())->ToSorted(sortColumns);
                };

                const auto& columns = inputTable->Path.GetColumns();
                IntermediateStreamSchemas_.push_back(toStreamSchema(inputTable->Schema, Spec->SortBy, columns));
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                for (const auto& sortColumn : Spec->SortBy) {
                    auto type = InferColumnType(InputManager->GetInputTables(), sortColumn.Name);
                    chunkSchemaColumns.emplace_back(sortColumn.Name, std::move(type), sortColumn.SortOrder);
                }
            }
        }

        if (IntermediateStreamSchemas_.size() > 1) {
            bool isTableIndexColumnSpecified = AnyOf(chunkSchemaColumns, [] (const TColumnSchema& column) {
                return column.Name() == TableIndexColumnName;
            });

            if (!isTableIndexColumnSpecified) {
                chunkSchemaColumns.emplace_back(TableIndexColumnName, ESimpleLogicalValueType::Int64);
            }
        }
        IntermediateChunkSchema_ = New<TTableSchema>(std::move(chunkSchemaColumns), /*strict*/ false);
    }

    TTableSchemaPtr BuildSampleSchema() const
    {
        std::vector<TColumnSchema> columns;
        columns.reserve(Spec->ReduceBy.size());
        for (const auto& reduceColumn : Spec->ReduceBy) {
            const auto& column = IntermediateChunkSchema_->GetColumn(reduceColumn);
            columns.push_back(column);
        }
        return New<TTableSchema>(std::move(columns));
    }

    void CustomMaterialize() override
    {
        TSortControllerBase::CustomMaterialize();

        if (TotalEstimatedInputDataWeight == 0) {
            return;
        }

        MapperFiles = UserJobFiles_[Spec->Mapper];
        ReduceCombinerFiles = UserJobFiles_[Spec->ReduceCombiner];
        ReducerFiles = UserJobFiles_[Spec->Reducer];

        InitJobIOConfigs();
        InitStreamDescriptors();

        InitPartitioningParametersEvaluator();

        // Due to the sampling it is possible that TotalEstimatedInputDataWeight > 0
        // but according to job size constraints there is nothing to do.
        if (RootPartitionPoolJobSizeConstraints->GetJobCount() == 0) {
            PartitionCount = 0;
            return;
        }

        std::vector<TPartitionKey> partitionKeys;
        if (!Spec->PivotKeys.empty() || Spec->ComputePivotKeysFromSamples) {
            auto sampleSchema = BuildSampleSchema();
            partitionKeys = BuildPartitionKeys(sampleSchema, sampleSchema);

            PartitionCount = partitionKeys.size() + 1;
        } else {
            PartitionCount = PartitioningParametersEvaluator_->SuggestPartitionCount();
        }

        MaxPartitionFactor = PartitioningParametersEvaluator_->SuggestMaxPartitionFactor(PartitionCount);
        BuildPartitionTree(PartitionCount, MaxPartitionFactor);

        if (!partitionKeys.empty()) {
            AssignPartitionKeysToPartitions(partitionKeys, /*setManiac*/ false);
        }

        YT_LOG_DEBUG("Final partitioning parameters (PartitionCount: %v, MaxPartitionFactor: %v)",
            PartitionCount,
            MaxPartitionFactor);

        if (Spec->UseNewPartitionsHeuristic) {
            SetAlertIfPartitionHeuristicsDifferSignificantly(
                PartitioningParametersEvaluator_->SuggestPartitionCount(
                    /*fetchedSamplesCount*/ std::nullopt,
                    /*forceLegacy*/ true),
                PartitionCount);
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
            std::vector<TOutputStreamDescriptorPtr> partitionStreamDescriptors;
            // Primary stream descriptor for shuffled output of the mapper.
            auto shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate()->Clone();
            shuffleStreamDescriptor->DestinationPool = ShuffleMultiChunkPoolInputs[partitionTaskLevel];
            shuffleStreamDescriptor->ChunkMapping = ShuffleMultiInputChunkMappings[partitionTaskLevel];
            shuffleStreamDescriptor->TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor->TableUploadOptions.TableSchema = IntermediateChunkSchema_;
            shuffleStreamDescriptor->StreamSchemas = IntermediateStreamSchemas_;
            if (partitionTaskLevel != PartitionTreeDepth - 1) {
                shuffleStreamDescriptor->TargetDescriptor = PartitionTasks[partitionTaskLevel + 1]->GetVertexDescriptor();
            }
            shuffleStreamDescriptor->TableWriterOptions->ComputeDigest =
                partitionTaskLevel == 0 &&
                Spec->EnableIntermediateOutputRecalculation &&
                Spec->HasNontrivialMapper();

            partitionStreamDescriptors.emplace_back(std::move(shuffleStreamDescriptor));

            if (partitionTaskLevel == 0) {
                partitionStreamDescriptors.insert(
                    partitionStreamDescriptors.end(),
                    MapperSinkEdges_.begin(),
                    MapperSinkEdges_.end());
            }
            PartitionTasks[partitionTaskLevel] = New<TPartitionTask>(
                this,
                partitionStreamDescriptors,
                std::vector<TInputStreamDescriptorPtr>{},
                partitionTaskLevel);
        }

        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < std::ssize(PartitionTasks); ++partitionTaskLevel) {
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

    TOutputStreamDescriptorPtr GetSortedMergeStreamDescriptor() const override
    {
        auto streamDescriptor = TSortControllerBase::GetSortedMergeStreamDescriptor();
        streamDescriptor->TableWriterOptions->ComputeDigest =
            Spec->HasNontrivialReduceCombiner() &&
            Spec->EnableIntermediateOutputRecalculation;
        return streamDescriptor;
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        RootPartitionJobIOConfig = CloneYsonStruct(Spec->PartitionJobIO);
        PartitionJobIOConfig = CloneYsonStruct(Spec->PartitionJobIO);
        PartitionJobIOConfig->TableReader->SamplingRate = std::nullopt;

        IntermediateSortJobIOConfig = Spec->SortJobIO;

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig = CloneYsonStruct(Spec->MergeJobIO);
        FinalSortJobIOConfig->TableReader = CloneYsonStruct(Spec->SortJobIO->TableReader);

        // Sorted reduce.
        SortedMergeJobIOConfig = CloneYsonStruct(Spec->MergeJobIO);
    }

    EJobType GetPartitionJobType(bool isRoot) const override
    {
        if (Spec->HasNontrivialMapper() && isRoot) {
            return EJobType::PartitionMap;
        } else {
            return EJobType::Partition;
        }
    }

    EJobType GetIntermediateSortJobType() const override
    {
        return Spec->HasNontrivialReduceCombiner() ? EJobType::ReduceCombiner : EJobType::IntermediateSort;
    }

    EJobType GetFinalSortJobType() const override
    {
        return EJobType::PartitionReduce;
    }

    EJobType GetSortedMergeJobType() const override
    {
        return EJobType::SortedReduce;
    }

    TUserJobSpecPtr GetSortedMergeUserJobSpec() const override
    {
        return Spec->Reducer;
    }

    const std::vector<TOutputStreamDescriptorPtr>& GetFinalStreamDescriptors() const override
    {
        return ReducerSinkEdges_;
    }

    void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            if (Spec->InputQueryOptions->UseSystemColumns) {
                InputManager->AdjustSchemas(ControlAttributesToColumnOptions(*Spec->PartitionJobIO->ControlAttributes));
            }
            ParseInputQuery(
                *Spec->InputQuery,
                Spec->InputSchema,
                Spec->InputQueryFilterOptions);
        }
    }

    void InitJobSpecTemplates()
    {
        {
            RootPartitionJobSpecTemplate.set_type(ToProto(GetPartitionJobType(/*isRoot*/ true)));

            auto* jobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);

            jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig)).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSourceDirectoryFromInputTables(InputManager->GetInputTables()));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForMapper());

            if (Spec->InputQuery) {
                WriteInputQueryToJobSpec(jobSpecExt);
            }

            jobSpecExt->set_io_config(ConvertToYsonString(RootPartitionJobIOConfig).ToString());

            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec->EnableIntermediateOutputRecalculation || Spec->Ordered);

            if (Spec->HasNontrivialMapper()) {
                InitUserJobSpecTemplate(
                    jobSpecExt->mutable_user_job_spec(),
                    Spec->Mapper,
                    MapperFiles,
                    Spec->DebugArtifactsAccount);
            }
        }

        {
            PartitionJobSpecTemplate.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = PartitionJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig)).ToString());

            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));

            jobSpecExt->set_io_config(ConvertToYsonString(PartitionJobIOConfig).ToString());

            auto* partitionJobSpecExt = PartitionJobSpecTemplate.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec->EnableIntermediateOutputRecalculation || Spec->Ordered);
        }

        auto intermediateDataSourceDirectory = BuildIntermediateDataSourceDirectory(
            GetSpec()->IntermediateDataAccount,
            IntermediateStreamSchemas_);
        const auto castAnyToComposite = !AreAllEqual(IntermediateStreamSchemas_);

        auto intermediateReaderOptions = New<TTableReaderOptions>();
        {
            auto* jobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_io_config(ConvertToYsonString(IntermediateSortJobIOConfig).ToString());
            jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));

            if (Spec->HasNontrivialReduceCombiner()) {
                IntermediateSortJobSpecTemplate.set_type(ToProto(EJobType::ReduceCombiner));

                auto* reduceJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
                reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
                ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);

                InitUserJobSpecTemplate(
                    jobSpecExt->mutable_user_job_spec(),
                    Spec->ReduceCombiner,
                    ReduceCombinerFiles,
                    Spec->DebugArtifactsAccount);
                jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
            } else {
                IntermediateSortJobSpecTemplate.set_type(ToProto(EJobType::IntermediateSort));
                auto* sortJobSpecExt = IntermediateSortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            }
        }

        {
            FinalSortJobSpecTemplate.set_type(ToProto(EJobType::PartitionReduce));

            auto* jobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* reduceJobSpecExt = FinalSortJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForReducer());

            jobSpecExt->set_io_config(ConvertToYsonString(FinalSortJobIOConfig).ToString());

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            reduceJobSpecExt->set_disable_sorted_input(Spec->DisableSortedInputInReducer);

            InitUserJobSpecTemplate(
                jobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->DebugArtifactsAccount);
            jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
        }

        {
            SortedMergeJobSpecTemplate.set_type(ToProto(EJobType::SortedReduce));

            auto* jobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* reduceJobSpecExt = SortedMergeJobSpecTemplate.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            auto intermediateDataSourceDirectory = BuildIntermediateDataSourceDirectory(
                GetSpec()->IntermediateDataAccount,
                std::vector<TTableSchemaPtr>(IntermediateStreamSchemas_.size(), IntermediateChunkSchema_));

            jobSpecExt->set_table_reader_options(ConvertToYsonString(intermediateReaderOptions).ToString());
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForReducer());

            jobSpecExt->set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).ToString());

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec->SortBy);
            reduceJobSpecExt->set_disable_sorted_input(Spec->DisableSortedInputInReducer);

            InitUserJobSpecTemplate(
                jobSpecExt->mutable_user_job_spec(),
                Spec->Reducer,
                ReducerFiles,
                Spec->DebugArtifactsAccount);
            jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
        }
    }

    void CustomizeJoblet(const TJobletPtr& joblet) override
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

    ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec->EnableLegacyLivePreview);
    }

    ELegacyLivePreviewMode GetLegacyIntermediateLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec->EnableLegacyLivePreview);
    }

    bool IsIntermediateLivePreviewSupported() const override
    {
        return true;
    }

    bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    // Resource management.

    TCpuResource GetPartitionCpuLimit() const override
    {
        return TCpuResource(Spec->HasNontrivialMapper() ? Spec->Mapper->CpuLimit : 1);
    }

    TCpuResource GetSortCpuLimit() const override
    {
        // At least one CPU, may be more in PartitionReduce job.
        return 1;
    }

    TCpuResource GetMergeCpuLimit() const override
    {
        return TCpuResource(Spec->Reducer->CpuLimit);
    }

    TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool isRoot) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 reserveSize = THorizontalBlockWriter::MaxReserveSize * std::ssize(GetFinalPartitions());
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig->TableWriter->BlockSize * std::ssize(GetFinalPartitions()),
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

        // Partition map jobs don't use estimated writer buffer size.
        result.SetJobProxyMemoryWithFixedWriteBufferSize(result.GetJobProxyMemory());
        return result;
    }

    TExtendedJobResources GetSimpleSortResources(const TChunkStripeStatistics& /*stat*/) const override
    {
        YT_ABORT();
    }

    bool IsSortedMergeNeeded(const TPartitionPtr& partition) const override
    {
        if (Spec->ForceReduceCombiners && !partition->CachedSortedMergeNeeded) {
            partition->CachedSortedMergeNeeded = true;
            SortedMergeTask->RegisterPartition(partition);
            IntermediateSortTask->RegisterPartition(partition);
        }
        return TSortControllerBase::IsSortedMergeNeeded(partition);
    }

    TUserJobSpecPtr GetSortUserJobSpec(bool isFinalSort) const override
    {
        if (isFinalSort) {
            return Spec->Reducer;
        } else if (Spec->HasNontrivialReduceCombiner()) {
            return Spec->ReduceCombiner;
        } else {
            return nullptr;
        }
    }

    TExtendedJobResources GetPartitionSortResources(
        bool isFinalSort,
        const TChunkStripeStatistics& stat) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);

        i64 jobProxyMemory =
            GetSortInputIOMemorySize(stat) +
            GetSortBuffersMemorySize(stat);
        i64 jobProxyMemoryWithFixedWriteBufferSize = jobProxyMemory;

        if (isFinalSort) {
            result.SetCpu(Spec->Reducer->CpuLimit);
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ true);
            jobProxyMemoryWithFixedWriteBufferSize += GetFinalOutputIOMemorySize(FinalSortJobIOConfig, /*useEstimatedBufferSize*/ false);
            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);
        } else {
            auto diff = GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig);
            jobProxyMemory += diff;
            jobProxyMemoryWithFixedWriteBufferSize += diff;
            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

            if (Spec->HasNontrivialReduceCombiner()) {
                result.SetCpu(Spec->ReduceCombiner->CpuLimit);
            } else {
                result.SetCpu(1);
            }
        }

        if (Config->EnableNetworkInOperationDemand) {
            result.SetNetwork(Spec->ShuffleNetworkLimit);
        }

        return result;
    }

    TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(Spec->Reducer->CpuLimit);
        auto jobProxyMemory = GetFinalIOMemorySize(
            SortedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ true,
            statistics);
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            SortedMergeJobIOConfig,
            /*useEstimatedBufferSize*/ false,
            statistics);

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        return result;
    }

    TExtendedJobResources GetUnorderedMergeResources(
        const TChunkStripeStatisticsVector& /*statistics*/) const override
    {
        YT_ABORT();
    }

    // Progress reporting.

    TString GetLoggingProgress() const override
    {
        const auto& jobCounter = GetTotalJobCounter();
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

    void BuildProgress(TFluentMap fluent) override
    {
        TSortControllerBase::BuildProgress(fluent);
        fluent
            .Do(BIND(&TMapReduceController::BuildPartitionsProgressYson, Unretained(this)))
            // TODO(gritukan): What should I do here?
            .Item(JobTypeAsKey(GetPartitionJobType(/*isRoot*/ true))).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(GetIntermediateSortJobType())).Value(IntermediateSortJobCounter)
            .Item(JobTypeAsKey(GetFinalSortJobType())).Value(FinalSortJobCounter)
            .Item(JobTypeAsKey(GetSortedMergeJobType())).Value(SortedMergeJobCounter)
            // TODO(ignat): remove when UI migrate to new keys.
            .Item(Spec->HasNontrivialMapper() ? "map_jobs" : "partition_jobs").Value(GetPartitionJobCounter())
            .Item(Spec->HasNontrivialReduceCombiner() ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
    }

    TUserJobSpecPtr GetPartitionUserJobSpec(bool isRoot) const override
    {
        if (Spec->HasNontrivialMapper() && isRoot) {
            return Spec->Mapper;
        } else {
            return nullptr;
        }
    }

    void CreateSortedMergeTask() override
    {
        SortedMergeTask = New<TSortedMergeTask>(
            this,
            GetFinalStreamDescriptors(),
            // To be filled later.
            std::vector<TInputStreamDescriptorPtr>{},
            /*enableKeyGuarantee*/ true);
    }

    TSortColumns GetSortedMergeSortColumns() const override
    {
        auto sortColumns = Spec->SortBy;
        sortColumns.resize(Spec->ReduceBy.size());
        return sortColumns;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec;
    }

private:
    TDataSinkDirectoryPtr BuildDataSinkDirectoryForMapper() const
    {
        auto dataSinkDirectory = New<TDataSinkDirectory>();
        auto mapperOutputTableCount = static_cast<size_t>(Spec->MapperOutputTableCount);
        dataSinkDirectory->DataSinks().reserve(1 + mapperOutputTableCount);
        dataSinkDirectory->DataSinks().emplace_back(BuildIntermediateDataSink(GetSpec()->IntermediateDataAccount));
        for (size_t index = 0; index < mapperOutputTableCount; ++index) {
            dataSinkDirectory->DataSinks().emplace_back(BuildDataSinkFromOutputTable(OutputTables_[index]));
        }
        return dataSinkDirectory;
    }

    TDataSinkDirectoryPtr BuildDataSinkDirectoryForReducer() const
    {
        return BuildDataSinkDirectoryFromOutputTables(
            std::vector<TOutputTablePtr>(OutputTables_.begin() + Spec->MapperOutputTableCount, OutputTables_.end()));
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TMapReduceController, 0xca7286bd);
};

void TMapReduceController::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TSortControllerBase>();

    PHOENIX_REGISTER_FIELD(1, MapperSinkEdges_);
    PHOENIX_REGISTER_FIELD(2, ReducerSinkEdges_);
}

PHOENIX_DEFINE_TYPE(TMapReduceController);

IOperationControllerPtr CreateMapReduceController(
    TControllerAgentConfigPtr config,
    IOperationControllerHostPtr host,
    TOperation* operation)
{
    auto options = config->MapReduceOperationOptions;
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TMapReduceController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
