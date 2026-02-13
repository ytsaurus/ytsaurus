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
#include <yt/yt/server/lib/chunk_pools/chunk_pool_outputs_merger.h>
#include <yt/yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/shuffle_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
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

#include <yt/yt/core/yson/protobuf_helpers.h>

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

// Hierarchical partitions data model.
//
// Both Sort and MapReduce operations consist of two phases:
// during the first phase some data (input data for Sort and Map result for MapReduce)
// is split into groups called partitions which can be processed independently
// and during the second phase some of the partitions are processed.
// For Sort operation partition is a group of rows forming some contiguous interval of keys.
// For MapReduce operation partition is a group of rows with the same key hash or
// forming some contiguous interval of keys.
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
// Partition task of level 0 is called root partition task.
// For MapReduce operations root partition task is a partition map task, and all other tasks are partition tasks.
// For Sort operations all the tasks are partition tasks.
//
// Partition index is an index of a partition among the partitions at the same level.
// That is, partition is uniquely defined by its level and index.
//
// For Sort operation partition with smaller partition index has keys that are not greater than keys
// of a partition (at the same level) with greater partition index.
// Results of per-partition sorts of final partitions are joined in order of partition indices.

class TPartitionBase
    : public IPersistent
    , public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, Level);
    DEFINE_BYREF_RW_PROPERTY(IPersistentChunkPoolOutputPtr, ChunkPoolOutput);

public:
    TPartitionBase() = default;

    explicit TPartitionBase(int level, IPersistentChunkPoolOutputPtr chunkPoolOutput = nullptr)
        : Level_(level)
        , ChunkPoolOutput_(std::move(chunkPoolOutput))
    { }

protected:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPartitionBase, 0x20b1ca1b);
};

using TPartitionBasePtr = TIntrusivePtr<TPartitionBase>;

////////////////////////////////////////////////////////////////////////////////

class TIntermediatePartition;
using TIntermediatePartitionPtr = TIntrusivePtr<TIntermediatePartition>;

class TIntermediatePartition
    : public TPartitionBase
{
public:
    // A single logical child partition may correspond to multiple physical partitions.
    struct TPhysicalPartitionsBounds final
    {
        std::vector<TKeyBound> KeyBounds;
        std::string WireKeyBounds;
        // Currently non-empty only on the last level of intermediate partitions.
        std::vector<bool> IsPhysicalPartitionManiac;

        PHOENIX_DECLARE_TYPE(TPhysicalPartitionsBounds, 0x3e6c9443);
    };

    DEFINE_BYVAL_RO_PROPERTY(int, Index);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TIntermediatePartitionPtr>, Children);
    DEFINE_BYREF_RW_PROPERTY(IShuffleChunkPoolPtr, ShuffleChunkPool);
    DEFINE_BYREF_RW_PROPERTY(IPersistentChunkPoolInputPtr, ShuffleChunkPoolInput);
    DEFINE_BYVAL_RW_PROPERTY(int, PhysicalPartitionCount);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TPhysicalPartitionsBounds>, PhysicalChildPartitionsBounds);

    // Currently used only on the last level of intermediate partitions.
    DEFINE_BYREF_RW_PROPERTY(THashSet<int>, DispatchedPhysicalPartitions);
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(AllDataCollected, false);

public:
    TIntermediatePartition() = default;

    TIntermediatePartition(int level, int index)
        : TPartitionBase(level)
        , Index_(index)
    { }

    bool IsPhysicalPartitionManiac(int physicalPartitionIndex) const
    {
        YT_ASSERT(physicalPartitionIndex < PhysicalPartitionCount_);
        return PhysicalChildPartitionsBounds_.has_value() &&
            !PhysicalChildPartitionsBounds_->IsPhysicalPartitionManiac.empty() &&
            PhysicalChildPartitionsBounds_->IsPhysicalPartitionManiac[physicalPartitionIndex];
    }

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TIntermediatePartition, 0xd449982f);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPartitionDispatchDecision,
    (Skip)
    (NoSort)
    (SortInSingleJob)
    (IntermediateSortAndMerge)
);

class TFinalPartition
    : public TPartitionBase
{
public:
    TFinalPartition() = default;

    TFinalPartition(
        int level,
        int creationIndex,
        EPartitionDispatchDecision dispatchDecision,
        IPersistentChunkPoolOutputPtr chunkPoolOutput,
        bool maniac)
        : TPartitionBase(level, std::move(chunkPoolOutput))
        , CreationIndex_(creationIndex)
        , DispatchDecision_(dispatchDecision)
        , Maniac_(maniac)
    { }

    DEFINE_BYVAL_RO_PROPERTY(int, CreationIndex);
    DEFINE_BYVAL_RO_PROPERTY(EPartitionDispatchDecision, DispatchDecision);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Maniac, false);
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(ReducingPartitionCompleted, false);

    DEFINE_BYVAL_RW_PROPERTY(TNodeId, AssignedNodeId, InvalidNodeId);

    bool HasAssignedNode() const
    {
        return GetAssignedNodeId() != InvalidNodeId;
    }

private:
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TFinalPartition, 0x06933525);
};

using TFinalPartitionPtr = TIntrusivePtr<TFinalPartition>;

////////////////////////////////////////////////////////////////////////////////

void TPartitionBase::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Level_);
    PHOENIX_REGISTER_FIELD(2, ChunkPoolOutput_);
}

PHOENIX_DEFINE_TYPE(TPartitionBase);

void TIntermediatePartition::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TPartitionBase>();

    PHOENIX_REGISTER_FIELD(1, Index_);
    PHOENIX_REGISTER_FIELD(2, Children_);
    PHOENIX_REGISTER_FIELD(3, ShuffleChunkPool_);
    PHOENIX_REGISTER_FIELD(4, ShuffleChunkPoolInput_);
    PHOENIX_REGISTER_FIELD(5, PhysicalPartitionCount_);
    PHOENIX_REGISTER_FIELD(6, PhysicalChildPartitionsBounds_);
    PHOENIX_REGISTER_FIELD(7, DispatchedPhysicalPartitions_);
    PHOENIX_REGISTER_FIELD(8, AllDataCollected_);
}

PHOENIX_DEFINE_TYPE(TIntermediatePartition);

void TIntermediatePartition::TPhysicalPartitionsBounds::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, KeyBounds);
    PHOENIX_REGISTER_FIELD(2, WireKeyBounds);
    PHOENIX_REGISTER_FIELD(3, IsPhysicalPartitionManiac);
}

PHOENIX_DEFINE_TYPE(TIntermediatePartition::TPhysicalPartitionsBounds);

void TFinalPartition::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TPartitionBase>();

    PHOENIX_REGISTER_FIELD(1, CreationIndex_);
    PHOENIX_REGISTER_FIELD(2, DispatchDecision_);
    PHOENIX_REGISTER_FIELD(3, Maniac_);
    PHOENIX_REGISTER_FIELD(4, ReducingPartitionCompleted_);
    PHOENIX_REGISTER_FIELD(5, AssignedNodeId_);
}

PHOENIX_DEFINE_TYPE(TFinalPartition);

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
            std::move(config),
            options,
            std::move(host),
            operation)
        , Spec_(std::move(spec))
        , Options_(std::move(options))
    { }

    std::pair<NApi::ITransactionPtr, std::string> GetIntermediateMediumTransaction() override
    {
        if (GetFastIntermediateMediumLimit() > 0 && !SwitchedToSlowIntermediateMedium_) {
            auto medium = GetIntermediateStreamDescriptorTemplate()->TableWriterOptions->MediumName;
            return {OutputTransaction_, medium};
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

        for (const auto& partitionTask : PartitionTasks_) {
            partitionTask->SwitchIntermediateMedium();
        }
        if (SimpleSortTask_ && !SimpleSortTask_->IsFinal()) {
            SimpleSortTask_->SwitchIntermediateMedium();
        }
        if (IntermediateSortTask_) {
            IntermediateSortTask_->SwitchIntermediateMedium();
        }

        YT_LOG_DEBUG("Switching from the fast intermediate medium to the slow one "
            "(FastMediumUsage: %v, FastMediumLimit: %v, UsageToLimitRatio: %v, TransactionId: %v)",
            usage,
            fastIntermediateMediumLimit,
            static_cast<double>(usage) / fastIntermediateMediumLimit,
            OutputTransaction_->GetId());

        SwitchedToSlowIntermediateMedium_ = true;
    }

private:
    TSortOperationSpecBasePtr Spec_;

protected:
    TSortOperationOptionsBasePtr Options_;

    // Counters.
    int UnprocessedPhysicalPartitionsCount_ = 0;
    int CompletedPartitionCount_ = 0;
    TProgressCounterPtr PartitionJobCounter_ = New<TProgressCounter>();
    TProgressCounterPtr SortedMergeJobCounter_ = New<TProgressCounter>();
    TProgressCounterPtr UnorderedMergeJobCounter_ = New<TProgressCounter>();

    // Sort job counters.
    TProgressCounterPtr IntermediateSortJobCounter_ = New<TProgressCounter>();
    TProgressCounterPtr FinalSortJobCounter_ = New<TProgressCounter>();
    TProgressCounterPtr SortDataWeightCounter_ = New<TProgressCounter>();

    // Start thresholds.
    bool SortStartThresholdReached_ = false;
    bool MergeStartThresholdReached_ = false;

    i64 TotalOutputRowCount_ = 0;

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

    //! PartitionsByLevels[level][index] is a partition with corresponding
    //! level and index.
    std::vector<std::vector<TIntermediatePartitionPtr>> IntermediatePartitionsByLevels_;
    std::vector<TFinalPartitionPtr> UnorderedFinalPartitions_;

    int PartitionTreeDepth_ = 0;

    // Expected number of final partitions before merging.
    // Actual count (UnorderedFinalPartitions_.size()) may be lower when merging is enabled.
    int ExpectedPartitionCount_ = 0;

    int MaxPartitionFactor_ = 0;

    // Locality stuff.

    struct TLocalityEntry
    {
        int PartitionIndex;

        i64 Locality;
    };

    //! NodeId -> set of final partition indices assigned to it.
    THashMap<TNodeId, THashSet<int>> AssignedPartitionsByNodeId_;

    //! NodeId -> map<final partition index, locality>.
    THashMap<TNodeId, THashMap<int, i64>> PartitionsLocalityByNodeId_;

    //! Spec templates for starting new jobs.
    TJobSpec RootPartitionJobSpecTemplate_;
    TJobSpec PartitionJobSpecTemplate_;
    TJobSpec IntermediateSortJobSpecTemplate_;
    TJobSpec FinalSortJobSpecTemplate_;
    TJobSpec SortedMergeJobSpecTemplate_;
    TJobSpec UnorderedMergeJobSpecTemplate_;

    //! IO configs for various job types.
    TJobIOConfigPtr RootPartitionJobIOConfig_;
    TJobIOConfigPtr PartitionJobIOConfig_;
    TJobIOConfigPtr IntermediateSortJobIOConfig_;
    TJobIOConfigPtr FinalSortJobIOConfig_;
    TJobIOConfigPtr SortedMergeJobIOConfig_;
    TJobIOConfigPtr UnorderedMergeJobIOConfig_;

    IJobSizeConstraintsPtr RootPartitionPoolJobSizeConstraints_;

    IPartitioningParametersEvaluatorPtr PartitioningParametersEvaluator_;

    //! i-th element is a multi chunk pool built over the inputs of i-th level partitions' shuffle chunk pools.
    std::vector<IMultiChunkPoolInputPtr> ShuffleMultiChunkPoolInputs_;

    //! i-th element is a input chunk mapping for i-th shuffle multi chunk pool input.
    std::vector<TInputChunkMappingPtr> ShuffleMultiInputChunkMappings_;

    //! i-th element is a partition task of i-th level.
    std::vector<TPartitionTaskPtr> PartitionTasks_;

    //! Non-null only for sort operation when |Partitions.size() == 1|.
    TSimpleSortTaskPtr SimpleSortTask_;

    TSortTaskPtr IntermediateSortTask_;
    TSortTaskPtr FinalSortTask_;

    TUnorderedMergeTaskPtr UnorderedMergeTask_;

    TSortedMergeTaskPtr SortedMergeTask_;

    //! True if the operation has switched to the slow intermediate medium (HDD) after producing
    //! more intermediate data than the limit for the fast intermediate medium (SSD).
    bool SwitchedToSlowIntermediateMedium_ = false;

    TEnumIndexedArray<EPartitionDispatchDecision, int> PartitionsDispatchStatistics_;

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
            int level,
            IPersistentChunkPoolInputPtr chunkPoolInput,
            IPersistentChunkPoolOutputPtr chunkPoolOutput)
            : TTask(controller, std::move(outputStreamDescriptors), /*inputStreamDescriptors*/ {})
            , Controller_(controller)
            , Level_(level)
            , ChunkPoolInput_(std::move(chunkPoolInput))
            , ChunkPoolOutput_(std::move(chunkPoolOutput))
        {
            GetChunkPoolOutput()->GetJobCounter()->AddParent(Controller_->PartitionJobCounter_);
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
            if (Controller_->Spec_->EnablePartitionedDataBalancing &&
                totalDataWeight >= Controller_->Spec_->MinLocalityInputDataWeight)
            {
                YT_LOG_INFO("Data balancing enabled (TotalDataWeight: %v)", totalDataWeight);
                DataBalancer_ = New<TDataBalancer>(
                    Controller_->Options_->DataBalancer,
                    totalDataWeight,
                    Controller_->GetOnlineSuitableExecNodeDescriptors(),
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
                ? Controller_->Spec_->PartitionLocalityTimeout
                : TDuration::Zero();
        }

        TExtendedJobResources GetJobNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetPartitionResources(joblet->InputStripeList->GetPerStripeStatistics(), IsRoot());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPoolInput_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPoolOutput_;
        }

        TInputChunkMappingPtr GetChunkMapping() const override
        {
            if (IsRoot()) {
                return TTask::GetChunkMapping();
            } else {
                return Controller_->ShuffleMultiInputChunkMappings_[Level_ - 1];
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
                DataBalancer_->OnExecNodesUpdated(Controller_->GetOnlineSuitableExecNodeDescriptors());
            }
        }

        void SetChunkPoolIndexForOutputStripes(
            const std::vector<TOutputStreamDescriptorPtr>& streamDescriptors,
            const TChunkStripeListPtr& inputStripeList,
            std::vector<TChunkStripePtr>* outputStripes) override
        {
            TTask::SetChunkPoolIndexForOutputStripes(streamDescriptors, inputStripeList, outputStripes);

            auto& shuffleStripe = outputStripes->front();
            shuffleStripe->SetInputChunkPoolIndex(GetPartitionIndex(inputStripeList));
        }

    private:
        TSortControllerBase* Controller_ = nullptr;

        TDataBalancerPtr DataBalancer_;

        int Level_ = -1;

        IPersistentChunkPoolInputPtr ChunkPoolInput_;
        IPersistentChunkPoolOutputPtr ChunkPoolOutput_;

        bool IsFinal() const
        {
            return Level_ == Controller_->PartitionTreeDepth_ - 1;
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

            return Controller_->PartitionTasks_[Level_ + 1];
        }

        int GetPartitionIndex(const TChunkStripeListPtr& chunkStripeList) const
        {
            if (IsRoot()) {
                return 0;
            } else {
                return *chunkStripeList->GetOutputChunkPoolIndex();
            }
        }

        bool CanLoseJobs() const override
        {
            return Controller_->Spec_->EnableIntermediateOutputRecalculation;
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
                jobSpec->CopyFrom(Controller_->RootPartitionJobSpecTemplate_);
            } else {
                jobSpec->CopyFrom(Controller_->PartitionJobSpecTemplate_);
            }

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            auto partition = Controller_->IntermediatePartitionsByLevels_[Level_][partitionIndex];

            auto* partitionJobSpecExt = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            YT_VERIFY(partition->GetPhysicalPartitionCount() > 0);
            partitionJobSpecExt->set_partition_count(partition->GetPhysicalPartitionCount());
            if (const auto& partitionsBound = partition->PhysicalChildPartitionsBounds()) {
                partitionJobSpecExt->set_wire_partition_lower_bound_prefixes(partitionsBound->WireKeyBounds);

                auto* lowerBoundInclusiveness = partitionJobSpecExt->mutable_partition_lower_bound_inclusivenesses();
                YT_VERIFY(lowerBoundInclusiveness->empty());
                lowerBoundInclusiveness->Reserve(partitionsBound->KeyBounds.size());
                for (const auto& lowerBound : partitionsBound->KeyBounds) {
                    lowerBoundInclusiveness->Add(lowerBound.IsInclusive);
                }
            }
            partitionJobSpecExt->set_partition_task_level(Level_);

            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            auto dataWeight = joblet->InputStripeList->GetAggregateStatistics().DataWeight;
            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, +dataWeight);
            }

            TTask::OnJobStarted(joblet);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            YT_VERIFY(!Controller_->SimpleSortTask_);

            auto partitionIndex = GetPartitionIndex(joblet->InputStripeList);
            auto partition = Controller_->IntermediatePartitionsByLevels_[Level_][partitionIndex];

            if (IsIntermediate()) {
                Controller_->UpdateTask(GetNextPartitionTask().Get());
            } else {
                Controller_->UpdateTask(Controller_->UnorderedMergeTask_.Get());
                Controller_->UpdateTask(Controller_->IntermediateSortTask_.Get());
                Controller_->UpdateTask(Controller_->FinalSortTask_.Get());
                Controller_->TryDispatchPhysicalPartitionsForProcessing(partition, /*isAllDataCollected*/ false);
            }

            // NB: This should be done not only in OnTaskCompleted:
            // 1) jobs may run after the task has been completed;
            // 2) ShuffleStartThreshold can be less than one.
            // Kick-start sort and unordered merge tasks.
            Controller_->CheckSortStartThreshold();
            Controller_->CheckMergeStartThreshold();

            const auto& shuffleChunkPool = partition->ShuffleChunkPool();
            if (shuffleChunkPool->GetTotalDataSliceCount() > Controller_->Spec_->MaxShuffleDataSliceCount) {
                result.OperationFailedError = TError("Too many data slices in shuffle pool, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_data_slice_count", shuffleChunkPool->GetTotalDataSliceCount())
                    << TErrorAttribute("max_shuffle_data_slice_count", Controller_->Spec_->MaxShuffleDataSliceCount);
                return result;
            }

            if (shuffleChunkPool->GetTotalJobCount() > Controller_->Spec_->MaxShuffleJobCount) {
                result.OperationFailedError = TError("Too many shuffle jobs, try to decrease size of intermediate data or split operation into several smaller ones")
                    << TErrorAttribute("shuffle_job_count", shuffleChunkPool->GetTotalJobCount())
                    << TErrorAttribute("max_shuffle_job_count", Controller_->Spec_->MaxShuffleJobCount);
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
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->GetAggregateStatistics().DataWeight);
            }

            return result;
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            if (DataBalancer_) {
                DataBalancer_->UpdateNodeDataWeight(joblet->NodeDescriptor, -joblet->InputStripeList->GetAggregateStatistics().DataWeight);
            }

            return result;
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            for (const auto& partition : Controller_->IntermediatePartitionsByLevels_[Level_]) {
                partition->ShuffleChunkPoolInput()->Finish();
            }

            if (IsIntermediate()) {
                const auto& nextPartitionTask = GetNextPartitionTask();
                nextPartitionTask->FinishInput();
                Controller_->UpdateTask(nextPartitionTask.Get());
            } else {
                Controller_->FinalSortTask_->Finalize();
                Controller_->FinalSortTask_->FinishInput();

                Controller_->IntermediateSortTask_->Finalize();
                Controller_->IntermediateSortTask_->FinishInput();

                Controller_->SortedMergeTask_->Finalize();

                if (Controller_->UnorderedMergeTask_) {
                    Controller_->UnorderedMergeTask_->FinishInput();
                    Controller_->UnorderedMergeTask_->Finalize();
                }

                Controller_->ValidateMergeDataSliceLimit();

                if (DataBalancer_) {
                    DataBalancer_->LogStatistics();
                }

                Controller_->AssignPartitions();

                YT_LOG_DEBUG(
                    "Partitioning completed (PartitionDispatchStatistics: %v)",
                    Controller_->PartitionsDispatchStatistics_);
            }

            // NB: This is required at least to mark tasks completed, when there are no pending jobs.
            // This couldn't have been done earlier since we've just finished populating shuffle pool.
            Controller_->CheckSortStartThreshold();
            Controller_->CheckMergeStartThreshold();
        }

        TJobSplitterConfigPtr GetJobSplitterConfig() const override
        {
            auto config = TaskHost_->GetJobSplitterConfigTemplate();

            if (!IsJobInterruptible()) {
                config->EnableJobSplitting = false;
            }

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
            std::optional<bool> isFinalSort)
            : TTask(controller, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
            , Controller_(controller)
            , IsFinalSort_(isFinalSort)
        {
            auto config = New<TLogDigestConfig>();
            // LowerLimit - we do not want to adjust memory reserve lower limit for sort jobs - we are pretty sure in our initial estimates.
            config->LowerBound = 1.0;
            config->UpperBound = Controller_->Spec_->JobProxyMemoryDigest->UpperBound;
            config->DefaultValue = Controller_->Spec_->JobProxyMemoryDigest->DefaultValue.value_or(1.0);
            JobProxyMemoryDigest_ = CreateLogDigest(config);
        }

        void SetupJobCounters()
        {
            if (IsFinal()) {
                GetJobCounter()->AddParent(Controller_->FinalSortJobCounter_);
            } else {
                GetJobCounter()->AddParent(Controller_->IntermediateSortJobCounter_);
            }
        }

        TExtendedJobResources GetJobNeededResources(const TJobletPtr& joblet) const override
        {
            const auto& stripeList = joblet->InputStripeList;
            auto statistics = stripeList->GetAggregateStatistics();

            // TODO(apollo1321): Do we really need this logic? There are no tests for this scenario.
            if (stripeList->IsApproximate()) {
                statistics.RowCount *= ApproximateSizesBoostFactor;
                statistics.ValueCount *= ApproximateSizesBoostFactor;
                statistics.DataWeight *= ApproximateSizesBoostFactor;
                statistics.CompressedDataSize *= ApproximateSizesBoostFactor;
            }

            auto result = GetNeededResourcesForChunkStripe(statistics);
            AddFootprintAndUserJobResources(result);
            return result;
        }

        EJobType GetJobType() const override
        {
            if (IsFinal()) {
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
            if (IsFinal()) {
                // Somehow we failed resuming a lost stripe in a sink. No comments.
                TTask::OnStripeRegistrationFailed(error, cookie, stripe, descriptor);
            }
            Controller_->SortedMergeTask_->AbortAllActiveJoblets(error, *stripe->GetInputChunkPoolIndex());
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
            YT_VERIFY(IsFinalSort_.has_value());
            return *IsFinalSort_;
        }

    protected:
        TSortControllerBase* Controller_ = nullptr;

        TExtendedJobResources GetNeededResourcesForChunkStripe(const TChunkStripeStatistics& stat) const
        {
            if (Controller_->SimpleSortTask_) {
                return Controller_->GetSimpleSortResources(stat);
            } else {
                return Controller_->GetPartitionSortResources(IsFinal(), stat);
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

            if (IsFinal()) {
                jobSpec->CopyFrom(Controller_->FinalSortJobSpecTemplate_);
            } else {
                jobSpec->CopyFrom(Controller_->IntermediateSortJobSpecTemplate_);
            }

            AddOutputTableSpecs(jobSpec, joblet);

            AddSequentialInputSpec(jobSpec, joblet);
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            if (IsFinal()) {
                Controller_->AccountRows(jobSummary);

                RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);
            } else {
                int inputStreamIndex = CurrentInputStreamIndex_++;

                // Sort outputs in large partitions are queued for further merge.
                // Construct a stripe consisting of sorted chunks and put it into the pool.
                auto& jobResultExt = jobSummary.GetJobResultExt();
                auto stripe = BuildIntermediateChunkStripe(jobResultExt.mutable_output_chunk_specs());

                for (const auto& dataSlice : stripe->DataSlices()) {
                    // NB: Intermediate sort uses sort_by as a prefix, while pool expects reduce_by as a prefix.
                    auto keyColumnCount = Controller_->GetSortedMergeSortColumns().size();
                    SetLimitsFromShortenedBoundaryKeys(dataSlice, keyColumnCount, Controller_->RowBuffer_);
                    // Transform data slice to legacy if legacy sorted pool is used in sorted merge.
                    if (!Controller_->Spec_->UseNewSortedPool) {
                        dataSlice->TransformToLegacy(Controller_->RowBuffer_);
                    }
                    dataSlice->SetInputStreamIndex(inputStreamIndex);
                }

                stripe->SetInputChunkPoolIndex(Controller_->SimpleSortTask_ ? 0 : joblet->InputStripeList->GetOutputChunkPoolIndex());

                std::optional<TRowsDigest> rowsDigest;
                if (!jobResultExt.output_digests().empty() && jobResultExt.output_digests()[0].has_value()) {
                    rowsDigest.emplace(jobResultExt.output_digests()[0].value());
                }

                RegisterStripe(
                    stripe,
                    OutputStreamDescriptors_[0],
                    joblet,
                    TChunkStripeKey(),
                    /*processEmptyStripes*/ false,
                    rowsDigest);
            }

            Controller_->CheckMergeStartThreshold();

            if (!IsFinal()) {
                Controller_->UpdateTask(Controller_->SortedMergeTask_.Get());
            }

            return result;
        }

        void OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId) override
        {
            TTask::OnJobLost(completedJob, chunkId);

            Controller_->UpdateTask(this);
            if (!Controller_->SimpleSortTask_) {
                Controller_->UpdateTask(Controller_->GetFinalPartitionTask().Get());
            }
        }

        void OnTaskCompleted() override
        {
            TTask::OnTaskCompleted();

            if (!IsFinal()) {
                Controller_->SortedMergeTask_->FinishInput();
                Controller_->UpdateMergeTasks();
                Controller_->ValidateMergeDataSliceLimit();
            }
        }

        bool CanLoseJobs() const override
        {
            return Controller_->Spec_->EnableIntermediateOutputRecalculation;
        }

        void SetIsFinalSort(bool isFinalSort)
        {
            YT_VERIFY(!IsFinalSort_.has_value());
            IsFinalSort_ = isFinalSort;
        }

    private:
        int CurrentInputStreamIndex_ = 0;
        std::optional<bool> IsFinalSort_;

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
            , MultiChunkPoolOutput_(CreateMultiChunkPoolOutput(/*underlyingPools*/ {}))
        { }

        TDuration GetLocalityTimeout() const override
        {
            // Locality for one-job partition is not important.
            if (IsFinal()) {
                return TDuration::Zero();
            }

            if (!Controller_->IsLocalityEnabled()) {
                return TDuration::Zero();
            }

            return Controller_->Spec_->SortLocalityTimeout;
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

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(const TAllocation& /*allocation*/) override
        {
            // Input locality is disabled for sort task.
            return MultiChunkPoolOutput_->Extract();
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShuffleMultiChunkPoolInputs_.back();
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return MultiChunkPoolOutput_;
        }

        TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller_->GetSortUserJobSpec(IsFinal());
        }

        TInputChunkMappingPtr GetChunkMapping() const override
        {
            return Controller_->ShuffleMultiInputChunkMappings_.back();
        }

        void RegisterPartition(TFinalPartitionPtr partition)
        {
            MultiChunkPoolOutput_->AddPoolOutput(partition->ChunkPoolOutput(), partition->GetCreationIndex());
            Partitions_.push_back(std::move(partition));
            Controller_->UpdateTask(this);
        }

        void Finalize() const
        {
            MultiChunkPoolOutput_->Finalize();
        }

    private:
        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TFinalPartitionPtr> Partitions_;

        bool IsActive() const override
        {
            return Controller_->SortStartThresholdReached_ || IsFinal();
        }

        bool HasInputLocality() const final
        {
            return false;
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            auto nodeId = joblet->NodeDescriptor.Id;

            auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
            const auto& partition = Controller_->UnorderedFinalPartitions_[partitionIndex];

            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            Controller_->AddLocalityToPartition(partition, nodeId, joblet->InputStripeList->GetAggregateStatistics().DataWeight);

            // Don't rely on static assignment anymore.
            Controller_->AssignNodeIdToPartition(partition, InvalidNodeId);

            UpdateTask();

            TSortTaskBase::OnJobStarted(joblet);
        }

        void OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId) override
        {
            if (!Controller_->SimpleSortTask_) {
                int partitionIndex = *completedJob->InputStripe->GetInputChunkPoolIndex();
                const auto& partition = Controller_->UnorderedFinalPartitions_[partitionIndex];
                auto nodeId = completedJob->NodeDescriptor.Id;
                Controller_->AddLocalityToPartition(partition, nodeId, -completedJob->DataWeight);
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

            if (!IsFinal()) {
                auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
                const auto& partition = Controller_->UnorderedFinalPartitions_[partitionIndex];
                if (partition->ChunkPoolOutput()->IsCompleted()) {
                    auto error = Controller_->SortedMergeTask_->OnIntermediateSortCompleted(partitionIndex);
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

            if (IsFinal())  {
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
            IJobSizeConstraintsPtr jobSizeConstraints)
            : TSortTaskBase(
                controller,
                /*outputStreamDescriptors*/ {},
                /*inputStreamDescriptors*/ {},
                /*isFinalSort*/ std::nullopt)
            , ChunkPool_(CreateUnorderedChunkPool(
                TUnorderedChunkPoolOptions{
                    .JobSizeConstraints = std::move(jobSizeConstraints),
                    .RowBuffer = controller->RowBuffer_,
                    .Logger = Logger().WithTag("Name: SimpleSort"),
                    // Build the first job from finished input to reliably determine the total job count.
                    // This prevents incorrect strategy selection caused by the unordered chunk pool's
                    // unpredictable behavior, which could otherwise cause job count estimates to
                    // fluctuate between 1 and 2, leading to choosing final sort vs sorted merge incorrectly.
                    .BuildFirstJobOnFinishedInput = true,
                },
                controller->GetInputStreamDirectory()))
        {
            GetChunkPoolOutput()->GetDataWeightCounter()->AddParent(Controller_->SortDataWeightCounter_);
        }

        TDuration GetLocalityTimeout() const override
        {
            return Controller_->IsLocalityEnabled()
                ? Controller_->Spec_->SimpleSortLocalityTimeout
                : TDuration::Zero();
        }

        TString GetTitle() const override
        {
            return Format("SimpleSort");
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return ChunkPool_;
        }

        IPersistentChunkPoolOutputPtr GetChunkPoolOutput() const override
        {
            return ChunkPool_;
        }

        void OnTaskCompleted() override
        {
            TSortTaskBase::OnTaskCompleted();

            YT_VERIFY(std::ssize(Controller_->UnorderedFinalPartitions_) == 1);
            Controller_->SortedMergeTask_->Finalize();
            if (IsFinal()) {
                ++Controller_->CompletedPartitionCount_;
            }
        }

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(
            const TAllocation& allocation) override
        {
            auto cookie = TSortTaskBase::ExtractCookieForAllocation(allocation);
            if (IsFinal()) {
                YT_VERIFY(GetChunkPoolOutput()->GetJobCounter()->GetTotal() == 1);
            } else {
                YT_VERIFY(GetChunkPoolOutput()->GetJobCounter()->GetTotal() > 1);
            }
            return cookie;
        }

        void SetIsFinalSort(bool isFinalSort, std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors)
        {
            TSortTaskBase::SetIsFinalSort(isFinalSort);
            YT_VERIFY(OutputStreamDescriptors_.empty());
            OutputStreamDescriptors_ = std::move(outputStreamDescriptors);
        }

    private:
        IPersistentChunkPoolPtr ChunkPool_;

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
            bool enableKeyGuarantee)
            : TTask(controller, std::move(outputStreamDescriptors), /*inputStreamDescriptors*/ {})
            , Controller_(controller)
            , MultiChunkPool_(CreateMultiChunkPool({}))
        {
            ChunkPoolInput_ = CreateTaskUpdatingAdapter(MultiChunkPool_, this);

            MultiChunkPool_->GetJobCounter()->AddParent(Controller_->SortedMergeJobCounter_);

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
                Controller_->SimpleSortTask_
                ? Controller_->Spec_->SimpleMergeLocalityTimeout
                : Controller_->Spec_->MergeLocalityTimeout;
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

        IChunkPoolOutput::TCookie ExtractCookieForAllocation(
            const TAllocation& allocation) override
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

        TExtendedJobResources GetJobNeededResources(const TJobletPtr& joblet) const override
        {
            auto resources = Controller_->GetSortedMergeResources(
                joblet->InputStripeList->GetPerStripeStatistics());
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
            const auto& partition = Controller_->UnorderedFinalPartitions_[partitionIndex];
            if (partition->IsReducingPartitionCompleted()) {
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

        void RegisterPartition(TFinalPartitionPtr partition)
        {
            auto partitionIndex = partition->GetCreationIndex();
            auto sortedMergeChunkPool = Controller_->CreateSortedMergeChunkPool(Format("%v(%v)", GetTitle(), partitionIndex));
            EmplaceOrCrash(SortedMergeChunkPools_, partitionIndex, sortedMergeChunkPool);
            MultiChunkPool_->AddPool(std::move(sortedMergeChunkPool), partitionIndex);

            Partitions_.push_back(std::move(partition));

            EnsureVectorIndex(ActiveJoblets_, partitionIndex);
            EnsureVectorIndex(InvalidatedJoblets_, partitionIndex);
            EnsureVectorIndex(JobOutputs_, partitionIndex);

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
        TSortControllerBase* Controller_ = nullptr;

        std::vector<TFinalPartitionPtr> Partitions_;

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
            for (const auto& [partitionIndex, jobOutputs] : Enumerate(JobOutputs_)) {
                i64 outputRowCount = 0;
                for (auto& jobOutput : jobOutputs) {
                    outputRowCount += Controller_->AccountRows(jobOutput.JobSummary);
                    RegisterOutput(jobOutput.JobSummary, jobOutput.Joblet->ChunkListIds, jobOutput.Joblet);
                }
                YT_LOG_DEBUG(
                    "Register outputs (PartitionIndex: %v, OutputRowCount: %v)",
                    partitionIndex,
                    outputRowCount);
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

            jobSpec->CopyFrom(Controller_->SortedMergeJobSpecTemplate_);
            auto comparator = GetComparator(Controller_->Spec_->SortBy);
            AddParallelInputSpec(jobSpec, joblet, comparator);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void OnJobStarted(TJobletPtr joblet) override
        {
            TTask::OnJobStarted(joblet);

            auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
            InsertOrCrash(ActiveJoblets_[partitionIndex], std::move(joblet));
        }

        TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobCompleted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
            EraseOrCrash(ActiveJoblets_[partitionIndex], joblet);
            if (!InvalidatedJoblets_[partitionIndex].contains(joblet)) {
                JobOutputs_[partitionIndex].emplace_back(TJobOutput{joblet, jobSummary});
            }

            return result;
        }

        TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobFailed(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
            EraseOrCrash(ActiveJoblets_[partitionIndex], joblet);

            return result;
        }

        TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
        {
            auto result = TTask::OnJobAborted(joblet, jobSummary);

            auto partitionIndex = *joblet->InputStripeList->GetOutputChunkPoolIndex();
            EraseOrCrash(ActiveJoblets_[partitionIndex], joblet);

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
            MultiChunkPoolOutput_->GetJobCounter()->AddParent(Controller_->UnorderedMergeJobCounter_);
        }

        i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            // Locality is unimportant.
            return 0;
        }

        TExtendedJobResources GetJobNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller_->GetUnorderedMergeResources(
                joblet->InputStripeList->GetPerStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        IPersistentChunkPoolInputPtr GetChunkPoolInput() const override
        {
            return Controller_->ShuffleMultiChunkPoolInputs_.back();
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
            return Controller_->ShuffleMultiInputChunkMappings_.back();
        }

        void RegisterPartition(TFinalPartitionPtr partition)
        {
            MultiChunkPoolOutput_->AddPoolOutput(partition->ChunkPoolOutput(), partition->GetCreationIndex());
            Partitions_.push_back(std::move(partition));
        }

        void Finalize()
        {
            MultiChunkPoolOutput_->Finalize();
        }

    private:
        TSortControllerBase* Controller_ = nullptr;

        IMultiChunkPoolOutputPtr MultiChunkPoolOutput_;

        std::vector<TFinalPartitionPtr> Partitions_;

        bool IsActive() const override
        {
            return Controller_->MergeStartThresholdReached_;
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

            jobSpec->CopyFrom(Controller_->UnorderedMergeJobSpecTemplate_);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
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

    const TPartitionTaskPtr& GetFinalPartitionTask() const
    {
        return PartitionTasks_.back();
    }

    std::optional<i64> GetPartitionDataWeightForMerging() const
    {
        YT_VERIFY(!SimpleSortTask_);

        if (Spec_->PartitionCount.has_value()) {
            return std::nullopt;
        } else if (Spec_->PartitionDataWeightForMerging.has_value()) {
            return Spec_->PartitionDataWeightForMerging;
        } else {
            return std::min(Options_->DefaultPartitionDataWeightForMerging, Spec_->DataWeightPerShuffleJob);
        }
    }

    void TryDispatchPhysicalPartitionsForProcessing(
        const TIntermediatePartitionPtr& intermediatePartition,
        TCompactVector<int, 1> physicalPartitionIndices,
        bool isAllDataCollected)
    {
        YT_VERIFY(!physicalPartitionIndices.empty());
        YT_ASSERT(std::ranges::all_of(physicalPartitionIndices, [&] (int physicalPartitionIndex) {
            return !intermediatePartition->DispatchedPhysicalPartitions().contains(physicalPartitionIndex);
        }));

        auto chunkPoolOutput = [&] {
            if (std::ssize(physicalPartitionIndices) == 1) {
                return intermediatePartition->ShuffleChunkPool()->GetOutput(physicalPartitionIndices[0]);
            }

            std::vector<IPersistentChunkPoolOutputPtr> physicalPartitionsToMerge;
            physicalPartitionsToMerge.reserve(std::ssize(physicalPartitionIndices));
            for (int physicalPartitionIndex : physicalPartitionIndices) {
                physicalPartitionsToMerge.push_back(intermediatePartition->ShuffleChunkPool()->GetOutput(physicalPartitionIndex));
            }

            return MergeChunkPoolOutputs(
                std::move(physicalPartitionsToMerge),
                Logger().WithTag(
                    "Name: PartitionsMerger[%v][%v]",
                    intermediatePartition->GetLevel(),
                    intermediatePartition->GetIndex()));
        }();

        bool isManiac = std::ssize(physicalPartitionIndices) == 1 &&
            intermediatePartition->IsPhysicalPartitionManiac(physicalPartitionIndices[0]);

        auto dispatchDecision = [&] () -> std::optional<EPartitionDispatchDecision> {
            if (chunkPoolOutput->GetDataWeightCounter()->GetTotal() == 0 && isAllDataCollected) {
                return EPartitionDispatchDecision::Skip;
            }
            if (ShouldForceSortedMerge() || (!isManiac && chunkPoolOutput->GetJobCounter()->GetTotal() > 1)) {
                return EPartitionDispatchDecision::IntermediateSortAndMerge;
            }
            if (isManiac && isAllDataCollected) {
                return EPartitionDispatchDecision::NoSort;
            }
            if (isAllDataCollected) {
                return EPartitionDispatchDecision::SortInSingleJob;
            }
            return std::nullopt;
        }();

        YT_VERIFY(!isAllDataCollected || dispatchDecision.has_value());

        if (!dispatchDecision.has_value()) {
            YT_VERIFY(std::ssize(physicalPartitionIndices) == 1);
            YT_LOG_TRACE(
                "Physical partition does not meet early dispatch criteria "
                "(ParentPartitionLevel: %v, ParentPartitionIndex: %v, PhysicalPartitionIndex: %v, "
                "PartitionJobCount: %v, PartitionDataWeight: %v, PartitionDataSliceCount: %v)",
                intermediatePartition->GetLevel(),
                intermediatePartition->GetIndex(),
                physicalPartitionIndices[0],
                chunkPoolOutput->GetJobCounter()->GetTotal(),
                chunkPoolOutput->GetDataSliceCounter()->GetTotal(),
                chunkPoolOutput->GetDataWeightCounter()->GetTotal());
            return;
        }

        for (int physicalPartitionIndex : physicalPartitionIndices) {
            bool wasInserted = intermediatePartition->DispatchedPhysicalPartitions().insert(physicalPartitionIndex).second;
            YT_VERIFY(wasInserted);
        }

        auto finalPartition = New<TFinalPartition>(
            PartitionTreeDepth_,
            /*creationIndex*/ std::ssize(UnorderedFinalPartitions_),
            *dispatchDecision,
            std::move(chunkPoolOutput),
            isManiac);

        UnorderedFinalPartitions_.push_back(finalPartition);

        UnprocessedPhysicalPartitionsCount_ -= std::ssize(physicalPartitionIndices);
        YT_VERIFY(UnprocessedPhysicalPartitionsCount_ >= 0);
        ++PartitionsDispatchStatistics_[finalPartition->GetDispatchDecision()];
        YT_VERIFY(Accumulate(PartitionsDispatchStatistics_, 0) == std::ssize(UnorderedFinalPartitions_));

        YT_LOG_DEBUG(
            "Final partition created (ParentPartitionLevel: %v, ParentPartitionIndex: %v, CreationIndex: %v, DispatchDecision: %v, "
            "IsManiac: %v, PartitionJobCount: %v, PartitionDataWeight: %v, PartitionDataSliceCount: %v, "
            "UnprocessedPhysicalPartitionsCount: %v, DispatchStatistics: %v, PhysicalPartitionIndices: %v)",
            intermediatePartition->GetLevel(),
            intermediatePartition->GetIndex(),
            finalPartition->GetCreationIndex(),
            finalPartition->GetDispatchDecision(),
            finalPartition->IsManiac(),
            finalPartition->ChunkPoolOutput()->GetJobCounter()->GetTotal(),
            finalPartition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal(),
            finalPartition->ChunkPoolOutput()->GetDataSliceCounter()->GetTotal(),
            UnprocessedPhysicalPartitionsCount_,
            PartitionsDispatchStatistics_,
            physicalPartitionIndices);

        switch (finalPartition->GetDispatchDecision()) {
            case EPartitionDispatchDecision::Skip:
                OnFinalPartitionCompleted(std::move(finalPartition));
                break;
            case EPartitionDispatchDecision::NoSort:
                UnorderedMergeTask_->RegisterPartition(std::move(finalPartition));
                break;
            case EPartitionDispatchDecision::SortInSingleJob:
                FinalSortTask_->RegisterPartition(std::move(finalPartition));
                break;
            case EPartitionDispatchDecision::IntermediateSortAndMerge:
                SortedMergeTask_->RegisterPartition(finalPartition);
                IntermediateSortTask_->RegisterPartition(std::move(finalPartition));
                break;
        }
    }

    // Attempts to determine and apply a dispatch decision for physical partitions of intermediate partition.
    void TryDispatchPhysicalPartitionsForProcessing(
        const TIntermediatePartitionPtr& partition,
        bool isAllDataCollected)
    {
        // NB(apollo1321): The greedy algorithm is optimal for sequential bin packing,
        // where partitions order must be preserved. However, for hash-based partitioning,
        // we can reorder partitions to achieve better packing.
        i64 accumulatedDataWeight = 0;
        i64 accumulatedDataSliceCount = 0;
        i64 accumulatedRowCount = 0;
        TCompactVector<int, 1> physicalPartitionIndices;

        auto endCurrentMerging = [&] {
            if (physicalPartitionIndices.empty()) {
                return;
            }

            TryDispatchPhysicalPartitionsForProcessing(partition, physicalPartitionIndices, isAllDataCollected);

            accumulatedDataWeight = 0;
            accumulatedDataSliceCount = 0;
            accumulatedRowCount = 0;
            physicalPartitionIndices.clear();
        };

        auto partitionDataWeightForMerging = GetPartitionDataWeightForMerging();

        for (int physicalPartitionIndex : std::views::iota(0, partition->GetPhysicalPartitionCount())) {
            if (partition->DispatchedPhysicalPartitions().contains(physicalPartitionIndex)) {
                // There is no need to end current merging for hash-based partitioning.
                if (partition->PhysicalChildPartitionsBounds().has_value()) {
                    endCurrentMerging();
                }
                continue;
            }

            auto chunkPoolOutput = partition->ShuffleChunkPool()->GetOutput(physicalPartitionIndex);
            i64 dataWeight = chunkPoolOutput->GetDataWeightCounter()->GetTotal();
            i64 dataSliceCount = chunkPoolOutput->GetDataSliceCounter()->GetTotal();
            i64 rowCount = chunkPoolOutput->GetRowCounter()->GetTotal();

            if (!Spec_->EnableFinalPartitionsMerging ||
                !isAllDataCollected ||
                IsPartitionOversized(
                    accumulatedDataWeight + dataWeight,
                    accumulatedRowCount + rowCount,
                    accumulatedDataSliceCount + dataSliceCount,
                    *partitionDataWeightForMerging,
                    Spec_->MaxChunkSlicePerShuffleJob))
            {
                endCurrentMerging();
            }

            physicalPartitionIndices.push_back(physicalPartitionIndex);
            accumulatedDataWeight += dataWeight;
            accumulatedDataSliceCount += dataSliceCount;
            accumulatedRowCount += rowCount;
        }

        endCurrentMerging();
    }

    void OnPartitionProcessedByJobs(TWeakPtr<TIntermediatePartition> weakPartition)
    {
        auto partition = weakPartition.Lock();
        if (!partition) {
            return;
        }

        YT_VERIFY(partition->IsAllDataCollected());

        YT_LOG_DEBUG(
            "Processing partition completed by jobs "
            "(PartitionLevel: %v, PartitionIndex: %v, PhysicalPartitionCount: %v, DataWeight: %v)",
            partition->GetLevel(),
            partition->GetIndex(),
            partition->GetPhysicalPartitionCount(),
            partition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal());

        if (partition->GetLevel() + 1 < std::ssize(IntermediatePartitionsByLevels_)) {
            for (const auto& childPartition : partition->Children()) {
                if (childPartition->IsAllDataCollected()) {
                    continue;
                }

                childPartition->SetAllDataCollected(true);

                YT_LOG_DEBUG(
                    "All data for intermediate partition has been collected "
                    "(PartitionLevel: %v, PartitionIndex: %v, PartitionDataWeight: %v)",
                    childPartition->GetLevel(),
                    childPartition->GetIndex(),
                    childPartition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal());
            }

            // NB(apollo1321): Finish may run callbacks in-place, so Finish() should not
            // be called before marking all child partitions as completed.
            partition->ShuffleChunkPoolInput()->Finish();
        } else {
            YT_LOG_DEBUG(
                "Dispatching physical partitions for final level intermediate partition "
                "(PartitionLevel: %v, PartitionIndex: %v, PhysicalPartitionCount: %v)",
                partition->GetLevel(),
                partition->GetIndex(),
                partition->GetPhysicalPartitionCount());

            partition->ShuffleChunkPoolInput()->Finish();

            TryDispatchPhysicalPartitionsForProcessing(partition, /*isAllDataCollected*/ true);
            YT_VERIFY(std::ssize(partition->DispatchedPhysicalPartitions()) == partition->GetPhysicalPartitionCount());
        }
    }

    void SetupPartitioningCompletedCallbacks()
    {
        YT_VERIFY(!SimpleSortTask_);
        for (const auto& partition : IntermediatePartitionsByLevels_ | std::views::join) {
            partition->ChunkPoolOutput()->SubscribeCompleted(BIND(
                &TSortControllerBase::OnPartitionProcessedByJobs,
                MakeWeak(this),
                MakeWeak(partition)));
        }
    }

    IMultiChunkPoolOutputPtr CreateLevelMultiChunkPoolOutput(int level) const
    {
        const auto& partitions = IntermediatePartitionsByLevels_[level];
        std::vector<IPersistentChunkPoolOutputPtr> outputs;
        outputs.reserve(partitions.size());
        for (const auto& partition : partitions) {
            outputs.push_back(partition->ChunkPoolOutput());
        }

        auto multiChunkPoolOutput = CreateMultiChunkPoolOutput(std::move(outputs));
        multiChunkPoolOutput->Finalize();
        return multiChunkPoolOutput;
    }

    // Custom bits of preparation pipeline.

    void PrepareSortTasks()
    {
        YT_VERIFY(!SimpleSortTask_);

        IntermediateSortTask_ = New<TSortTask>(
            this,
            std::vector{GetSortedMergeStreamDescriptor()},
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()),
            /*isFinalSort*/ false);
        IntermediateSortTask_->SetupJobCounters();
        IntermediateSortTask_->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(IntermediateSortTask_);

        FinalSortTask_ = New<TSortTask>(
            this,
            GetFinalStreamDescriptors(),
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()),
            /*isFinalSort*/ true);
        FinalSortTask_->SetupJobCounters();
        FinalSortTask_->RegisterInGraph(GetFinalPartitionTask()->GetVertexDescriptor());
        RegisterTask(FinalSortTask_);
    }

    void PrepareSortedMergeTask()
    {
        auto sortedMergeInputStreamDescriptors = BuildInputStreamDescriptorsFromOutputStreamDescriptors(
            SimpleSortTask_
            ? SimpleSortTask_->GetOutputStreamDescriptors()
            : IntermediateSortTask_->GetOutputStreamDescriptors());

        SortedMergeTask_->SetInputStreamDescriptors(sortedMergeInputStreamDescriptors);
        RegisterTask(SortedMergeTask_);
        SortedMergeTask_->SetInputVertex(FormatEnum(GetIntermediateSortJobType()));
        SortedMergeTask_->RegisterInGraph();
    }

    // Init/finish.

    void AssignNodeIdToPartition(const TFinalPartitionPtr& partition, TNodeId nodeId)
    {
        if (partition->GetAssignedNodeId() != InvalidNodeId) {
            EraseOrCrash(AssignedPartitionsByNodeId_[partition->GetAssignedNodeId()], partition->GetCreationIndex());
        }

        partition->SetAssignedNodeId(nodeId);

        if (partition->GetAssignedNodeId() != InvalidNodeId) {
            EmplaceOrCrash(AssignedPartitionsByNodeId_[partition->GetAssignedNodeId()], partition->GetCreationIndex());
        }
    }

    void AddLocalityToPartition(const TFinalPartitionPtr& partition, TNodeId nodeId, i64 delta)
    {
        auto& localityMap = PartitionsLocalityByNodeId_[nodeId];
        localityMap[partition->GetCreationIndex()] += delta;
        YT_VERIFY(localityMap[partition->GetCreationIndex()] >= 0);
        if (localityMap[partition->GetCreationIndex()] == 0) {
            EraseOrCrash(localityMap, partition->GetCreationIndex());
        }
    }

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

        auto comparePartitions = [&] (const TPartitionBasePtr& lhs, const TPartitionBasePtr& rhs) {
            return lhs->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal() >
                   rhs->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
        };

        YT_LOG_DEBUG("Examining online nodes");

        const auto& nodeDescriptors = GetOnlineSuitableExecNodeDescriptors();
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

        std::vector<TFinalPartitionPtr> partitionsToAssign;
        for (const auto& partition : UnorderedFinalPartitions_) {
            // We are not interested in one-job partition locality.
            if (!partition->IsManiac() && partition->GetDispatchDecision() != EPartitionDispatchDecision::IntermediateSortAndMerge) {
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

            AssignNodeIdToPartition(partition, nodeId);

            auto task = [&] () -> TTaskPtr {
                if (partition->IsManiac()) {
                    return UnorderedMergeTask_;
                } else if (SimpleSortTask_) {
                    return SimpleSortTask_;
                } else if (partition->GetDispatchDecision() == EPartitionDispatchDecision::IntermediateSortAndMerge) {
                    return IntermediateSortTask_;
                } else {
                    return FinalSortTask_;
                }
            }();

            UpdateTask(task.Get());

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataWeight += partition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            YT_LOG_DEBUG("Partition assigned (CreationIndex: %v, DataWeight: %v, Address: %v)",
                partition->GetCreationIndex(),
                partition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal(),
                GetDefaultAddress(node->Descriptor->Addresses));
        }

        for (const auto& node : nodeHeap) {
            if (node->AssignedDataWeight > 0) {
                YT_LOG_DEBUG("Node used (Address: %v, Weight: %.4lf, AssignedDataWeight: %v, AdjustedDataWeight: %v)",
                    GetDefaultAddress(node->Descriptor->Addresses),
                    node->Weight,
                    node->AssignedDataWeight,
                    static_cast<i64>(node->AssignedDataWeight / node->Weight));
            }
        }

        YT_LOG_DEBUG("Partitions assigned");
    }

    IPersistentChunkPoolPtr CreateRootPartitionPool(
        TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig,
        bool ordered) const
    {
        YT_VERIFY(RootPartitionPoolJobSizeConstraints_);

        if (ordered) {
           return CreateOrderedChunkPool(
                TOrderedChunkPoolOptions{
                    .MaxTotalSliceCount = Config_->MaxTotalSliceCount,
                    .JobSizeConstraints = RootPartitionPoolJobSizeConstraints_,
                    .EnablePeriodicYielder = true,
                    .ShouldSliceByRowIndices = true,
                    .Logger = Logger().WithTag("Name: RootPartition"),
                    .JobSizeAdjusterConfig = std::move(jobSizeAdjusterConfig),
                },
                IntermediateInputStreamDirectory);
        }

        return CreateUnorderedChunkPool(
            TUnorderedChunkPoolOptions{
                .JobSizeAdjusterConfig = std::move(jobSizeAdjusterConfig),
                .JobSizeConstraints = RootPartitionPoolJobSizeConstraints_,
                .RowBuffer = RowBuffer_,
                .Logger = Logger().WithTag("Name: RootPartition"),
            },
            GetInputStreamDirectory());
    }

    void CreateShufflePools()
    {
        YT_VERIFY(!SimpleSortTask_);

        ShuffleMultiChunkPoolInputs_.reserve(PartitionTreeDepth_);
        ShuffleMultiInputChunkMappings_.reserve(PartitionTreeDepth_);
        for (int level = 0; level < PartitionTreeDepth_; ++level) {
            const auto& partitions = IntermediatePartitionsByLevels_[level];
            std::vector<IPersistentChunkPoolInputPtr> shuffleChunkPoolInputs;
            shuffleChunkPoolInputs.reserve(partitions.size());
            for (const auto& intermediatePartition : partitions) {
                IShuffleChunkPoolPtr shuffleChunkPool;
                if (level + 1 == PartitionTreeDepth_) {
                    shuffleChunkPool = CreateShuffleChunkPool(
                        intermediatePartition->GetPhysicalPartitionCount(),
                        Spec_->DataWeightPerShuffleJob,
                        Spec_->MaxChunkSlicePerShuffleJob);
                } else {
                    shuffleChunkPool = CreateShuffleChunkPool(
                        intermediatePartition->GetPhysicalPartitionCount(),
                        Spec_->DataWeightPerIntermediatePartitionJob,
                        Spec_->MaxChunkSlicePerIntermediatePartitionJob);
                }
                intermediatePartition->ShuffleChunkPool() = shuffleChunkPool;
                intermediatePartition->ShuffleChunkPoolInput() = CreateIntermediateLivePreviewAdapter(shuffleChunkPool->GetInput(), this);
                for (int childIndex = 0; childIndex < std::ssize(intermediatePartition->Children()); ++childIndex) {
                    auto& child = intermediatePartition->Children()[childIndex];
                    child->ChunkPoolOutput() = shuffleChunkPool->GetOutput(childIndex);
                }
                shuffleChunkPoolInputs.push_back(intermediatePartition->ShuffleChunkPoolInput());
            }

            auto shuffleMultiChunkPoolInput = CreateMultiChunkPoolInput(std::move(shuffleChunkPoolInputs));
            ShuffleMultiChunkPoolInputs_.push_back(std::move(shuffleMultiChunkPoolInput));

            ShuffleMultiInputChunkMappings_.push_back(New<TInputChunkMapping>(EChunkMappingMode::Unordered, Logger));
        }
    }

    bool IsCompleted() const override
    {
        return CompletedPartitionCount_ == std::ssize(UnorderedFinalPartitions_) && UnprocessedPhysicalPartitionsCount_ == 0;
    }

    bool IsSamplingEnabled() const
    {
        for (const auto& jobIOConfig : {
            PartitionJobIOConfig_,
            IntermediateSortJobIOConfig_,
            FinalSortJobIOConfig_,
            SortedMergeJobIOConfig_,
            UnorderedMergeJobIOConfig_,
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

            if (IsRowCountPreserved() && !(SimpleSortTask_ && isNontrivialInput) && !IsSamplingEnabled() && !InputHasDynamicStores()) {
                // We don't check row count for simple sort if nontrivial read limits are specified,
                // since input row count can be estimated inaccurate.
                i64 totalInputRowCount = 0;
                for (const auto& partition : UnorderedFinalPartitions_) {
                    i64 inputRowCount = partition->ChunkPoolOutput()->GetRowCounter()->GetTotal();
                    totalInputRowCount += inputRowCount;
                }
                YT_LOG_ERROR_IF(totalInputRowCount != TotalOutputRowCount_,
                    "Input/output row count mismatch in sort operation (TotalInputRowCount: %v, TotalOutputRowCount: %v)",
                    totalInputRowCount,
                    TotalOutputRowCount_);
                YT_VERIFY(totalInputRowCount == TotalOutputRowCount_);
            }

            YT_VERIFY(CompletedPartitionCount_ == std::ssize(UnorderedFinalPartitions_));
        } else {
            // We have to save all output in SortedMergeTask.
            if (SortedMergeTask_) {
                SortedMergeTask_->CheckCompleted();
                if (!SortedMergeTask_->IsCompleted()) {
                    // Dirty hack to save chunks.
                    SortedMergeTask_->ForceComplete();
                }
            }
        }

        TOperationControllerBase::OnOperationCompleted(interrupted);
    }

    TError GetUseChunkSliceStatisticsError() const override
    {
        return TError();
    }

    void OnFinalPartitionCompleted(const TFinalPartitionPtr& partition)
    {
        if (partition->IsReducingPartitionCompleted()) {
            return;
        }

        partition->SetReducingPartitionCompleted(true);

        ++CompletedPartitionCount_;

        YT_LOG_DEBUG("Final partition completed (PartitionCreationIndex: %v)", partition->GetCreationIndex());
    }

    void CheckSortStartThreshold()
    {
        if (!SortStartThresholdReached_) {
            if (SimpleSortTask_) {
                SortStartThresholdReached_ = true;
            } else if (IsSamplingEnabled() && PartitionTreeDepth_ == 1) {
                if (PartitionTasks_.front()->IsCompleted()) {
                    SortStartThresholdReached_ = true;
                }
            } else if (GetFinalPartitionTask()->GetChunkPoolInput()->IsFinished()) {
                auto totalDataWeight = GetFinalPartitionTask()->GetTotalDataWeight();
                auto completedDataWeight = GetFinalPartitionTask()->GetCompletedDataWeight();
                if (completedDataWeight >= totalDataWeight * Spec_->ShuffleStartThreshold) {
                    SortStartThresholdReached_ = true;
                }
            }

            if (SortStartThresholdReached_) {
                YT_LOG_INFO("Sort start threshold reached");
            }
        }

        UpdateSortTasks();
    }

    bool IsShuffleCompleted() const
    {
        YT_VERIFY(!SimpleSortTask_);

        if (UnorderedMergeTask_ && !UnorderedMergeTask_->IsCompleted()) {
            return false;
        }

        return IntermediateSortTask_->IsCompleted() && FinalSortTask_->IsCompleted();
    }

    void CheckMergeStartThreshold()
    {
        if (!MergeStartThresholdReached_) {
            if (!SimpleSortTask_) {
                if (!GetFinalPartitionTask()->IsCompleted()) {
                    return;
                }
                if (SortDataWeightCounter_->GetCompletedTotal() < SortDataWeightCounter_->GetTotal() * Spec_->MergeStartThreshold) {
                    return;
                }
            }

            YT_LOG_INFO("Merge start threshold reached");

            MergeStartThresholdReached_ = true;
        }

        UpdateMergeTasks();
    }

    void UpdateSortTasks()
    {
        UpdateTask(SimpleSortTask_.Get());
        UpdateTask(IntermediateSortTask_.Get());
        UpdateTask(FinalSortTask_.Get());
    }

    void UpdateMergeTasks()
    {
        UpdateTask(UnorderedMergeTask_.Get());
        UpdateTask(SortedMergeTask_.Get());
    }

    std::optional<TLocalityEntry> GetLocalityEntry(TNodeId nodeId) const
    {
        if (const auto* partitions = AssignedPartitionsByNodeId_.FindPtr(nodeId)) {
            if (!partitions->empty()) {
                return TLocalityEntry{
                    .PartitionIndex = *partitions->begin(),
                    .Locality = 1
                };
            }
        }

        if (const auto* partitionLocalities = PartitionsLocalityByNodeId_.FindPtr(nodeId)) {
            if (!partitionLocalities->empty()) {
                return std::make_from_tuple<TLocalityEntry>(*partitionLocalities->begin());
            }
        }

        return std::nullopt;
    }

    virtual bool ShouldForceSortedMerge() const
    {
        return false;
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
        for (const auto& partitionTask : PartitionTasks_) {
            partitionTask->OnExecNodesUpdated();
        }
    }

    // TODO(apollo1321): Separate processing inputs and collecting primary input slices.
    void ProcessInputs(const TTaskPtr& inputTask, const IJobSizeConstraintsPtr& jobSizeConstraints)
    {
        auto yielder = CreatePeriodicYielder(PrepareYieldPeriod);

        inputTask->SetIsInput(true);

        for (auto& dataSlice : CollectPrimaryInputDataSlices(jobSizeConstraints->GetInputSliceDataWeight())) {
            inputTask->AddInput(New<TChunkStripe>(std::move(dataSlice)));
            yielder.TryYield();
        }

        YT_LOG_INFO("Processed inputs");
    }

    // Unsorted helpers.

    i64 GetSortBuffersMemorySize(const TChunkStripeStatistics& stat) const
    {
        // Calculate total size of buffers, presented in TSchemalessPartitionSortReader.
        return
            (i64) 16 * Spec_->SortBy.size() * stat.RowCount + // KeyBuffer
            (i64) 12 * stat.RowCount +                        // RowDescriptorBuffer
            (i64) 4 * stat.RowCount +                         // Buckets
            (i64) 4 * stat.RowCount;                          // SortedIndexes
    }

    // Partition progress.

    struct TPartitionProgress
    {
        std::vector<i64> Total;
        std::vector<i64> Running;
        std::vector<i64> Completed;
    };

    TPartitionProgress ComputeFinalPartitionProgressByDataWeight() const
    {
        const auto& finalPartitions = UnorderedFinalPartitions_;
        int partitionCount = std::ssize(finalPartitions);
        int bucketCount = std::min(partitionCount, MaxProgressBuckets);

        TPartitionProgress result;
        result.Total.resize(bucketCount);
        result.Running.resize(bucketCount);
        result.Completed.resize(bucketCount);

        auto partitionIt = finalPartitions.begin();
        int partitionIndex = 0;
        int lo = 0;

        for (int i = 0; i < bucketCount; ++i) {
            int hi = (i + 1) * partitionCount / bucketCount;

            i64 totalSum = 0;
            i64 runningSum = 0;
            i64 completedSum = 0;

            while (partitionIndex < hi) {
                const auto& counter = (*partitionIt)->ChunkPoolOutput()->GetDataWeightCounter();
                totalSum += counter->GetTotal();
                runningSum += counter->GetRunning();
                completedSum += counter->GetCompletedTotal();

                ++partitionIt;
                ++partitionIndex;
            }

            result.Total[i] = totalSum * partitionCount / (hi - lo) / bucketCount;
            result.Running[i] = runningSum * partitionCount / (hi - lo) / bucketCount;
            result.Completed[i] = completedSum * partitionCount / (hi - lo) / bucketCount;

            lo = hi;
        }

        return result;
    }

    const TProgressCounterPtr& GetPartitionJobCounter() const
    {
        return PartitionJobCounter_;
    }

    // Partition sizes histogram.

    std::unique_ptr<IHistogram> ComputeFinalPartitionSizeHistogram() const override
    {
        auto histogram = CreateHistogram();
        for (const auto& partition : UnorderedFinalPartitions_) {
            i64 size = partition->ChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
            if (size != 0) {
                histogram->AddValue(size);
            }
        }
        histogram->BuildHistogramView();
        return histogram;
    }

    void BuildPartitionsProgressYson(TFluentMap fluent) const
    {
        auto progress = ComputeFinalPartitionProgressByDataWeight();
        auto sizeHistogram = ComputeFinalPartitionSizeHistogram();

        fluent
            .Item("partitions").BeginMap()
                .Item("total").Value(std::ssize(UnorderedFinalPartitions_))
                .Item("completed").Value(CompletedPartitionCount_)
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
        RootPartitionJobIOConfig_ = CloneYsonStruct(Spec_->PartitionJobIO);
        PartitionJobIOConfig_ = CloneYsonStruct(Spec_->PartitionJobIO);
        PartitionJobIOConfig_->TableReader->SamplingRate = std::nullopt;
    }

    virtual void InitIntermediateSchemas() = 0;

    void CustomMaterialize() override
    {
        TOperationControllerBase::CustomMaterialize();

        ValidateAccountPermission(Spec_->IntermediateDataAccount, EPermission::Use);

        for (const auto& table : InputManager_->GetInputTables()) {
            for (const auto& sortColumn : Spec_->SortBy) {
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

        streamDescriptor->DestinationPool = SortedMergeTask_->GetChunkPoolInput();
        streamDescriptor->ChunkMapping = SortedMergeTask_->GetChunkMapping();
        streamDescriptor->TableUploadOptions.TableSchema = IntermediateChunkSchema_;
        streamDescriptor->RequiresRecoveryInfo = true;
        streamDescriptor->TargetDescriptor = SortedMergeTask_->GetVertexDescriptor();
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
        jobOptions.MaxTotalSliceCount = Config_->MaxTotalSliceCount;

        // NB: otherwise we could easily be persisted during preparing the jobs. Sorted chunk pool
        // can't handle this.
        jobOptions.EnablePeriodicYielder = false;
        chunkPoolOptions.RowBuffer = RowBuffer_;
        chunkPoolOptions.SortedJobOptions = jobOptions;
        chunkPoolOptions.JobSizeConstraints = CreatePartitionBoundSortedJobSizeConstraints(
            Spec_,
            Options_,
            GetOutputTablePaths().size(),
            ExpectedPartitionCount_);
        chunkPoolOptions.Logger = Logger().WithTag("Name: %v", name);
        if (Config_->EnableSortedMergeInSortJobSizeAdjustment) {
            chunkPoolOptions.JobSizeAdjusterConfig = Options_->SortedMergeJobSizeAdjuster;
        }

        if (Spec_->UseNewSortedPool) {
            YT_LOG_DEBUG("Creating new sorted pool");
            return CreateNewSortedChunkPool(chunkPoolOptions, nullptr /*chunkSliceFetcher*/, IntermediateInputStreamDirectory);
        } else {
            YT_LOG_DEBUG("Creating legacy sorted pool");
            return CreateLegacySortedChunkPool(chunkPoolOptions, nullptr /*chunkSliceFetcher*/, IntermediateInputStreamDirectory);
        }
    }

    i64 AccountRows(const TCompletedJobSummary& jobSummary)
    {
        if (jobSummary.Abandoned) {
            return 0;
        }
        YT_VERIFY(jobSummary.TotalOutputDataStatistics);
        auto outputRowCount = jobSummary.TotalOutputDataStatistics->row_count();
        TotalOutputRowCount_ += outputRowCount;
        return outputRowCount;
    }

    void ValidateMergeDataSliceLimit()
    {
        i64 dataSliceCount = 0;

        if (SortedMergeTask_) {
            dataSliceCount += SortedMergeTask_->GetInputDataSliceCount();
        }

        if (UnorderedMergeTask_) {
            dataSliceCount += UnorderedMergeTask_->GetInputDataSliceCount();
        }

        if (dataSliceCount > Spec_->MaxMergeDataSliceCount) {
            OnOperationFailed(TError("Too many data slices in merge pools, try to decrease size of "
                "intermediate data or split operation into several smaller ones")
                << TErrorAttribute("merge_data_slice_count", dataSliceCount)
                << TErrorAttribute("max_merge_data_slice_count", Spec_->MaxMergeDataSliceCount));
        }
    }

    void BuildPartitionTree()
    {
        YT_VERIFY(!SimpleSortTask_);

        auto buildPartitionTree = [&] (TPartitionTreeSkeletonNode* partitionTreeSkeleton, int level, auto buildPartitionTree) -> TIntermediatePartitionPtr {
            YT_VERIFY(level < PartitionTreeDepth_);
            int levelPartitionIndex = std::ssize(IntermediatePartitionsByLevels_[level]);

            auto partition = New<TIntermediatePartition>(level, levelPartitionIndex);
            partition->SetPhysicalPartitionCount(std::ssize(partitionTreeSkeleton->Children));
            IntermediatePartitionsByLevels_[level].push_back(partition);

            if (level + 1 < PartitionTreeDepth_) {
                for (int childIndex = 0; childIndex < std::ssize(partitionTreeSkeleton->Children); ++childIndex) {
                    auto* child = partitionTreeSkeleton->Children[childIndex].get();
                    auto treeChild = buildPartitionTree(child, level + 1, buildPartitionTree);
                    partition->Children().push_back(std::move(treeChild));
                }
            } else {
                UnprocessedPhysicalPartitionsCount_ += partition->GetPhysicalPartitionCount();
            }

            return partition;
        };

        auto partitionTreeSkeleton = BuildPartitionTreeSkeleton(ExpectedPartitionCount_, MaxPartitionFactor_);
        PartitionTreeDepth_ = partitionTreeSkeleton.Depth;
        IntermediatePartitionsByLevels_.resize(PartitionTreeDepth_);

        YT_LOG_DEBUG("Building partition tree (FinalPartitionCount: %v, MaxPartitionFactor: %v, PartitionTreeDepth: %v)",
            ExpectedPartitionCount_,
            MaxPartitionFactor_,
            PartitionTreeDepth_);

        buildPartitionTree(partitionTreeSkeleton.Root.get(), 0, buildPartitionTree);
    }

    std::vector<TPartitionKey> BuildPartitionKeysFromPivotKeys()
    {
        std::vector<TPartitionKey> partitionKeys;
        partitionKeys.reserve(Spec_->PivotKeys.size());

        for (const auto& key : Spec_->PivotKeys) {
            // Skip empty pivot keys since they coincide with virtual empty pivot of the first partition.
            if (key.GetCount() == 0) {
                continue;
            }
            auto upperBound = TKeyBound::FromRow(RowBuffer_->CaptureRow(key), /*isInclusive*/ true, /*isUpper*/ false);
            partitionKeys.emplace_back(upperBound);
        }

        THROW_ERROR_EXCEPTION_IF(
            std::ssize(partitionKeys) + 1 > Options_->MaxPartitionCount,
            "Pivot keys count %v exceeds maximum number of pivot keys %v",
            std::ssize(partitionKeys),
            Options_->MaxPartitionCount - 1);

        return partitionKeys;
    }

    std::pair<TRowBufferPtr, std::vector<TSample>> FetchSamples(const TTableSchemaPtr& sampleSchema, i64 sampleCount)
    {
        auto samplesRowBuffer = New<TRowBuffer>(
            TRowBufferTag(),
            Config_->ControllerRowBufferChunkSize);

        auto [samplesFetcher, unavailableChunksWatcher] = InputManager_->CreateSamplesFetcher(
            sampleSchema,
            samplesRowBuffer,
            sampleCount,
            Options_->MaxSampleSize);

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
        if (!Spec_->PivotKeys.empty()) {
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
            Host_->GetClient()->GetNativeConnection()->GetExpressionEvaluatorCache(),
            suggestedPartitionCount,
            RowBuffer_,
            Logger);
    }

    void InitPartitioningParametersEvaluator()
    {
        RootPartitionPoolJobSizeConstraints_ = CreatePartitionJobSizeConstraints(
            Spec_,
            Options_,
            Logger,
            EstimatedInputStatistics_->DataWeight,
            EstimatedInputStatistics_->RowCount,
            EstimatedInputStatistics_->CompressedDataSize);

        PartitioningParametersEvaluator_ = CreatePartitioningParametersEvaluator(
            Spec_,
            Options_,
            EstimatedInputStatistics_->DataWeight,
            EstimatedInputStatistics_->UncompressedDataSize,
            EstimatedInputStatistics_->ValueCount,
            EstimatedInputStatistics_->CompressionRatio,
            RootPartitionPoolJobSizeConstraints_->GetJobCount());
    }

    void AssignPartitionKeysToPartitions(const std::vector<TPartitionKey>& partitionKeys, bool setManiac)
    {
        YT_VERIFY(!SimpleSortTask_);
        YT_VERIFY(ExpectedPartitionCount_ == std::ssize(partitionKeys) + 1);

        int currentKeyIndex = 0;

        // In-order DFS to assign bounds.
        auto visitPartition = [&] (const TIntermediatePartitionPtr& intermediatePartition, auto visitPartition) -> void {
            YT_VERIFY(currentKeyIndex <= std::ssize(partitionKeys));

            YT_LOG_DEBUG(
                "Assigning partition keys to intermediate partition "
                "(Level: %v, Index: %v, PhysicalPartitionCount: %v, CurrentKeyIndex: %v)",
                intermediatePartition->GetLevel(),
                intermediatePartition->GetIndex(),
                intermediatePartition->GetPhysicalPartitionCount(),
                currentKeyIndex);

            std::vector<TKeyBound> keyBounds;
            auto keySetWriter = New<TKeySetWriter>();

            if (!intermediatePartition->Children().empty()) {
                // Intermediate partition merging is not currently supported.
                YT_VERIFY(intermediatePartition->GetPhysicalPartitionCount() == std::ssize(intermediatePartition->Children()));
            }

            std::vector<bool> isPhysicalPartitionManiac;
            if (setManiac && intermediatePartition->GetLevel() + 1 == PartitionTreeDepth_) {
                isPhysicalPartitionManiac.resize(intermediatePartition->GetPhysicalPartitionCount());
            }

            for (int childIndex : std::views::iota(0, intermediatePartition->GetPhysicalPartitionCount())) {
                // Before visiting each child (except the first), record the boundary.
                if (childIndex > 0) {
                    YT_VERIFY(currentKeyIndex > 0 && currentKeyIndex <= std::ssize(partitionKeys));
                    const auto& lowerBound = partitionKeys[currentKeyIndex - 1].LowerBound;
                    YT_VERIFY(lowerBound);
                    YT_VERIFY(!lowerBound.IsUniversal());

                    keyBounds.push_back(lowerBound);
                    keySetWriter->WriteKey(lowerBound.Prefix);

                    YT_LOG_DEBUG(
                        "Added key bound for intermediate partition child "
                        "(PartitionLevel: %v, PartitionIndex: %v, ChildIndex: %v, KeyBound: %v)",
                        intermediatePartition->GetLevel(),
                        intermediatePartition->GetIndex(),
                        childIndex,
                        lowerBound);
                }

                if (!isPhysicalPartitionManiac.empty() && currentKeyIndex > 0) {
                    isPhysicalPartitionManiac[childIndex] = partitionKeys[currentKeyIndex - 1].Maniac;
                }

                if (intermediatePartition->Children().empty()) {
                    ++currentKeyIndex;
                } else {
                    visitPartition(intermediatePartition->Children()[childIndex], visitPartition);
                }
            }

            YT_VERIFY(intermediatePartition->GetPhysicalPartitionCount() == std::ssize(keyBounds) + 1);

            if (intermediatePartition->GetPhysicalPartitionCount() == 1) {
                // Fallback to hash-based partitioning with partition count = 1.
                // TODO(apollo1321): This scenario should be prohibited.
                YT_LOG_DEBUG(
                    "Skipping physical partitions bound for intermediate partition with single child "
                    "(PartitionLevel: %v, PartitionIndex: %v, PhysicalPartitionCount: %v)",
                    intermediatePartition->GetLevel(),
                    intermediatePartition->GetIndex(),
                    intermediatePartition->GetPhysicalPartitionCount());
                return;
            }

            intermediatePartition->PhysicalChildPartitionsBounds() = TIntermediatePartition::TPhysicalPartitionsBounds{
                .KeyBounds = std::move(keyBounds),
                .WireKeyBounds = ToString(keySetWriter->Finish()),
                .IsPhysicalPartitionManiac = std::move(isPhysicalPartitionManiac),
            };

            YT_LOG_DEBUG(
                "Set physical partitions bound for intermediate partition "
                "(PartitionLevel: %v, PartitionIndex: %v, PhysicalPartitionCount: %v, BoundCount: %v)",
                intermediatePartition->GetLevel(),
                intermediatePartition->GetIndex(),
                intermediatePartition->GetPhysicalPartitionCount(),
                std::ssize(intermediatePartition->PhysicalChildPartitionsBounds()->KeyBounds));
        };

        YT_VERIFY(!IntermediatePartitionsByLevels_.empty() && !IntermediatePartitionsByLevels_[0].empty());
        visitPartition(IntermediatePartitionsByLevels_[0][0], visitPartition);

        YT_VERIFY(currentKeyIndex == ExpectedPartitionCount_);
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

    PHOENIX_REGISTER_FIELD(1, Spec_);

    PHOENIX_REGISTER_FIELD(2, CompletedPartitionCount_);
    PHOENIX_REGISTER_FIELD(3, PartitionJobCounter_);
    PHOENIX_REGISTER_FIELD(4, SortedMergeJobCounter_);
    PHOENIX_REGISTER_FIELD(5, UnorderedMergeJobCounter_);
    PHOENIX_REGISTER_FIELD(6, IntermediateSortJobCounter_);
    PHOENIX_REGISTER_FIELD(7, FinalSortJobCounter_);
    PHOENIX_REGISTER_FIELD(8, SortDataWeightCounter_);

    PHOENIX_REGISTER_FIELD(9, SortStartThresholdReached_);
    PHOENIX_REGISTER_FIELD(10, MergeStartThresholdReached_);

    PHOENIX_REGISTER_FIELD(11, TotalOutputRowCount_);

    PHOENIX_REGISTER_FIELD(13, IntermediatePartitionsByLevels_);
    PHOENIX_REGISTER_FIELD(14, PartitionTreeDepth_);
    PHOENIX_REGISTER_FIELD(15, ExpectedPartitionCount_);
    PHOENIX_REGISTER_FIELD(16, MaxPartitionFactor_);

    PHOENIX_REGISTER_FIELD(17, AssignedPartitionsByNodeId_);
    PHOENIX_REGISTER_FIELD(18, PartitionsLocalityByNodeId_);

    PHOENIX_REGISTER_FIELD(19, RootPartitionJobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(20, PartitionJobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(21, IntermediateSortJobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(22, FinalSortJobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(23, SortedMergeJobSpecTemplate_);
    PHOENIX_REGISTER_FIELD(24, UnorderedMergeJobSpecTemplate_);

    PHOENIX_REGISTER_FIELD(25, RootPartitionJobIOConfig_);
    PHOENIX_REGISTER_FIELD(26, PartitionJobIOConfig_);
    PHOENIX_REGISTER_FIELD(27, IntermediateSortJobIOConfig_);
    PHOENIX_REGISTER_FIELD(28, FinalSortJobIOConfig_);
    PHOENIX_REGISTER_FIELD(29, SortedMergeJobIOConfig_);
    PHOENIX_REGISTER_FIELD(30, UnorderedMergeJobIOConfig_);

    PHOENIX_REGISTER_FIELD(31, RootPartitionPoolJobSizeConstraints_);

    PHOENIX_REGISTER_FIELD(34, ShuffleMultiChunkPoolInputs_);
    PHOENIX_REGISTER_FIELD(35, ShuffleMultiInputChunkMappings_);

    registrar.template VirtualField<36>("IntermediateChunkSchema_", [] (TThis* this_, auto& context) {
        NYT::Load(context, *this_->IntermediateChunkSchema_);
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, *this_->IntermediateChunkSchema_);
    })();
    PHOENIX_REGISTER_FIELD(37, IntermediateStreamSchemas_,
        .template Serializer<TVectorSerializer<TNonNullableIntrusivePtrSerializer<>>>());

    PHOENIX_REGISTER_FIELD(38, PartitionTasks_);
    PHOENIX_REGISTER_FIELD(39, SimpleSortTask_);
    PHOENIX_REGISTER_FIELD(40, IntermediateSortTask_);
    PHOENIX_REGISTER_FIELD(41, FinalSortTask_);
    PHOENIX_REGISTER_FIELD(42, UnorderedMergeTask_);
    PHOENIX_REGISTER_FIELD(43, SortedMergeTask_);

    PHOENIX_REGISTER_FIELD(44, SwitchedToSlowIntermediateMedium_);

    PHOENIX_REGISTER_FIELD(45, UnorderedFinalPartitions_);
    PHOENIX_REGISTER_FIELD(46, UnprocessedPhysicalPartitionsCount_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        if (!this_->SimpleSortTask_) {
            this_->SetupPartitioningCompletedCallbacks();
        }
    });
}

PHOENIX_DEFINE_TYPE(TSortControllerBase);

////////////////////////////////////////////////////////////////////////////////

void TSortControllerBase::TPartitionTask::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TTask>();

    PHOENIX_REGISTER_FIELD(1, Controller_);
    PHOENIX_REGISTER_FIELD(2, DataBalancer_);
    PHOENIX_REGISTER_FIELD(3, Level_);
    PHOENIX_REGISTER_FIELD(4, ChunkPoolInput_);
    PHOENIX_REGISTER_FIELD(5, ChunkPoolOutput_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        if (this_->DataBalancer_) {
            this_->DataBalancer_->OnExecNodesUpdated(this_->Controller_->GetOnlineSuitableExecNodeDescriptors());
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

    PHOENIX_REGISTER_FIELD(1, ChunkPool_);
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
            std::move(config),
            std::move(options),
            std::move(host),
            operation)
        , Spec_(std::move(spec))
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
    TSortOperationSpecPtr Spec_;

    // Custom bits of preparation pipeline.

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec_->OutputTablePath);
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

        ValidateSchemaInferenceMode(Spec_->SchemaInferenceMode);

        if ((table->Dynamic || table->TableUploadOptions.UpdateMode == EUpdateMode::Append) &&
            table->TableUploadOptions.TableSchema->GetSortColumns() != Spec_->SortBy)
        {
            THROW_ERROR_EXCEPTION("sort_by is different from output table key columns")
                << TErrorAttribute("output_table_path", Spec_->OutputTablePath)
                << TErrorAttribute("output_table_sort_columns", table->TableUploadOptions.TableSchema->GetSortColumns())
                << TErrorAttribute("sort_by", Spec_->SortBy);
        }

        if (auto writeMode = table->TableUploadOptions.VersionedWriteOptions.WriteMode; writeMode != EVersionedIOMode::Default) {
            THROW_ERROR_EXCEPTION("Versioned write mode %Qlv is not supported for sort operation",
                writeMode);
        }

        switch (Spec_->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    InferSchemaFromInput(Spec_->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec_->SortBy);
                    ValidateOutputSchemaCompatibility({
                        .TypeCompatibilityOptions = {
                            .AllowStructFieldRenaming = false,
                            .AllowStructFieldRemoval = false,
                            .IgnoreUnknownRemovedFieldNames = false,
                        },
                        .IgnoreSortOrder = true,
                        .ForbidExtraComputedColumns = false,
                        .IgnoreStableNamesDifference = true,
                    });
                    ValidateOutputSchemaComputedColumnsCompatibility();
                }
                break;

            case ESchemaInferenceMode::FromInput:
                InferSchemaFromInput(Spec_->SortBy);
                break;

            case ESchemaInferenceMode::FromOutput:
                if (table->TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    table->TableUploadOptions.TableSchema = TTableSchema::FromSortColumns(Spec_->SortBy);
                } else {
                    table->TableUploadOptions.TableSchema = table->TableUploadOptions.TableSchema->ToSorted(Spec_->SortBy);
                    ValidateOutputSchemaComputedColumnsCompatibility();
                }
                break;

            default:
                YT_ABORT();
        }
    }

    void InitIntermediateSchemas() override
    {
        IntermediateChunkSchema_ = OutputTables_[0]->TableUploadOptions.TableSchema->ToSorted(Spec_->SortBy);
        IntermediateStreamSchemas_ = {IntermediateChunkSchema_};
    }

    void CustomMaterialize() override
    {
        TSortControllerBase::CustomMaterialize();

        if (EstimatedInputStatistics_->DataWeight == 0) {
            return;
        }

        if (EstimatedInputStatistics_->DataWeight > Spec_->MaxInputDataWeight) {
            THROW_ERROR_EXCEPTION("Failed to initialize sort operation, input data weight is too large")
                << TErrorAttribute("estimated_input_data_weight", EstimatedInputStatistics_->DataWeight)
                << TErrorAttribute("max_input_data_weight", Spec_->MaxInputDataWeight);
        }

        InitJobIOConfigs();

        InitPartitioningParametersEvaluator();

        auto partitionKeys = BuildPartitionKeys(
            BuildSampleSchema(),
            OutputTables_[0]->TableUploadOptions.GetUploadSchema());

        ExpectedPartitionCount_ = partitionKeys.size() + 1;
        MaxPartitionFactor_ = PartitioningParametersEvaluator_->SuggestMaxPartitionFactor(ExpectedPartitionCount_);

        YT_LOG_DEBUG("Final partitioning parameters (ExpectedPartitionCount: %v, MaxPartitionFactor: %v)",
            ExpectedPartitionCount_,
            MaxPartitionFactor_);

        CreateSortedMergeTask();

        if (ExpectedPartitionCount_ == 1) {
            PrepareSimpleSortTask();
        } else {
            BuildPartitionTree();
            AssignPartitionKeysToPartitions(partitionKeys, /*setManiac*/ true);

            CreateShufflePools();

            PreparePartitionTasks();
            PrepareSortTasks();
            PrepareUnorderedMergeTask();

            SetupPartitioningCompletedCallbacks();
        }

        PrepareSortedMergeTask();

        InitJobSpecTemplates();
    }

    void PreparePartitionTasks()
    {
        YT_VERIFY(!SimpleSortTask_);
        PartitionTasks_.resize(PartitionTreeDepth_);
        for (int partitionTaskLevel = PartitionTreeDepth_ - 1; partitionTaskLevel >= 0; --partitionTaskLevel) {
            auto shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate()->Clone();
            shuffleStreamDescriptor->DestinationPool = ShuffleMultiChunkPoolInputs_[partitionTaskLevel];
            shuffleStreamDescriptor->ChunkMapping = ShuffleMultiInputChunkMappings_[partitionTaskLevel];
            shuffleStreamDescriptor->TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor->TableUploadOptions.TableSchema = OutputTables_[0]->TableUploadOptions.TableSchema;
            if (partitionTaskLevel != PartitionTreeDepth_ - 1) {
                shuffleStreamDescriptor->TargetDescriptor = PartitionTasks_[partitionTaskLevel + 1]->GetVertexDescriptor();
            }

            auto createPartitionTask = [&, this_ = this] (auto chunkPoolInput, auto chunkPoolOutput) {
                return New<TPartitionTask>(
                    this_,
                    /*outputStreamDescriptors*/ std::vector<TOutputStreamDescriptorPtr>{shuffleStreamDescriptor},
                    partitionTaskLevel,
                    std::move(chunkPoolInput),
                    std::move(chunkPoolOutput));
            };

            if (partitionTaskLevel == 0) {
                auto chunkPool = CreateRootPartitionPool(/*jobSizeAdjusterConfig*/ nullptr, /*ordered*/ false);
                auto chunkPoolOutput = chunkPool;
                PartitionTasks_[partitionTaskLevel] = createPartitionTask(std::move(chunkPool), std::move(chunkPoolOutput));;
            } else {
                PartitionTasks_[partitionTaskLevel] = createPartitionTask(
                    ShuffleMultiChunkPoolInputs_[partitionTaskLevel - 1],
                    CreateLevelMultiChunkPoolOutput(partitionTaskLevel));
            }
        }

        const auto& rootPartition = IntermediatePartitionsByLevels_[0][0];
        rootPartition->ChunkPoolOutput() = PartitionTasks_.front()->GetChunkPoolOutput();
        rootPartition->SetAllDataCollected(true);

        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth_; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks_[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < std::ssize(PartitionTasks_); ++partitionTaskLevel) {
            const auto& partitionTask = PartitionTasks_[partitionTaskLevel];
            partitionTask->SetInputVertex(PartitionTasks_[partitionTaskLevel - 1]->GetVertexDescriptor());
            partitionTask->RegisterInGraph();
        }

        ProcessInputs(PartitionTasks_.front(), RootPartitionPoolJobSizeConstraints_);
        FinishTaskInput(PartitionTasks_.front());

        YT_LOG_INFO(
            "Sorting with partitioning (ExpectedPartitionCount: %v, PartitionJobCount: %v, DataWeightPerPartitionJob: %v)",
            ExpectedPartitionCount_,
            RootPartitionPoolJobSizeConstraints_->GetJobCount(),
            RootPartitionPoolJobSizeConstraints_->GetDataWeightPerJob());
    }

    void PrepareSimpleSortTask()
    {
        // Choose sort job count and initialize the pool.
        auto jobSizeConstraints = CreateSimpleSortJobSizeConstraints(
            Spec_,
            Options_,
            Logger,
            EstimatedInputStatistics_->DataWeight,
            EstimatedInputStatistics_->CompressedDataSize);

        SimpleSortTask_ = New<TSimpleSortTask>(
            this,
            jobSizeConstraints);

        RegisterTask(SimpleSortTask_);

        ProcessInputs(SimpleSortTask_, jobSizeConstraints);

        SimpleSortTask_->FinishInput();

        auto decision = SimpleSortTask_->GetChunkPoolOutput()->GetJobCounter()->GetTotal() <= 1
            ? EPartitionDispatchDecision::SortInSingleJob
            : EPartitionDispatchDecision::IntermediateSortAndMerge;

        YT_LOG_INFO(
            "Sorting without partitioning (SortJobCount: %v, DataWeightPerJob: %v, DispatchDecision: %v)",
            jobSizeConstraints->GetJobCount(),
            jobSizeConstraints->GetDataWeightPerJob(),
            decision);

        UnorderedFinalPartitions_.push_back(New<TFinalPartition>(
            /*level*/ 0,
            /*creationIndex*/ 0,
            decision,
            SimpleSortTask_->GetChunkPoolOutput(),
            /*maniac*/ false));

        const auto& partition = UnorderedFinalPartitions_[0];

        if (decision == EPartitionDispatchDecision::IntermediateSortAndMerge) {
            SortedMergeTask_->RegisterPartition(partition);

            SimpleSortTask_->SetIsFinalSort(false, {GetSortedMergeStreamDescriptor()});
            SortedMergeTask_->SetInputVertex(FormatEnum(GetIntermediateSortJobType()));
        } else {
            SimpleSortTask_->SetIsFinalSort(true, GetFinalStreamDescriptors());
        }

        SimpleSortTask_->SetupJobCounters();

        SimpleSortTask_->RegisterInGraph(TDataFlowGraph::SourceDescriptor);
        UpdateTask(SimpleSortTask_.Get());
    }

    void PrepareUnorderedMergeTask()
    {
        YT_VERIFY(!SimpleSortTask_);

        UnorderedMergeTask_ = New<TUnorderedMergeTask>(
            this,
            GetFinalStreamDescriptors(),
            BuildInputStreamDescriptorsFromOutputStreamDescriptors(
                GetFinalPartitionTask()->GetOutputStreamDescriptors()));
        RegisterTask(UnorderedMergeTask_);
        UnorderedMergeTask_->SetInputVertex(GetFinalPartitionTask()->GetVertexDescriptor());
        UnorderedMergeTask_->RegisterInGraph();
    }

    TTableSchemaPtr BuildSampleSchema() const
    {
        THashSet<std::string> uniqueColumns;
        auto schema = OutputTables_[0]->TableUploadOptions.GetUploadSchema();

        for (const auto& sortColumn : Spec_->SortBy) {
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

        IntermediateSortJobIOConfig_ = CloneYsonStruct(Spec_->SortJobIO);

        // Final sort: reader like sort and output like merge.
        FinalSortJobIOConfig_ = CloneYsonStruct(Spec_->SortJobIO);
        FinalSortJobIOConfig_->TableWriter = CloneYsonStruct(Spec_->MergeJobIO->TableWriter);

        SortedMergeJobIOConfig_ = CloneYsonStruct(Spec_->MergeJobIO);

        UnorderedMergeJobIOConfig_ = CloneYsonStruct(Spec_->MergeJobIO);
        // Since we're reading from huge number of partition chunks, we must use larger buffers,
        // as we do for sort jobs.
        UnorderedMergeJobIOConfig_->TableReader = CloneYsonStruct(Spec_->SortJobIO->TableReader);
    }

    EJobType GetIntermediateSortJobType() const override
    {
        return SimpleSortTask_ ? EJobType::SimpleSort : EJobType::IntermediateSort;
    }

    EJobType GetFinalSortJobType() const override
    {
        return SimpleSortTask_ ? EJobType::SimpleSort : EJobType::FinalSort;
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
            RootPartitionJobSpecTemplate_.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = RootPartitionJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig_))));
            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(RootPartitionJobIOConfig_)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSourceDirectoryFromInputTables(InputManager_->GetInputTables()));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate_.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec_->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec_->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec_->EnableIntermediateOutputRecalculation);
        }

        {
            PartitionJobSpecTemplate_.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = PartitionJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig_))));
            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(PartitionJobIOConfig_)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            auto* partitionJobSpecExt = PartitionJobSpecTemplate_.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec_->SortBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec_->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec_->EnableIntermediateOutputRecalculation);
        }

        TJobSpec sortJobSpecTemplate;
        {
            auto* jobSpecExt = sortJobSpecTemplate.MutableExtension(TJobSpecExt::job_spec_ext);

            if (SimpleSortTask_) {
                jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(CreateTableReaderOptions(Spec_->PartitionJobIO))));
                SetProtoExtension<TDataSourceDirectoryExt>(
                    jobSpecExt->mutable_extensions(),
                    BuildDataSourceDirectoryFromInputTables(InputManager_->GetInputTables()));
            } else {
                jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
                SetProtoExtension<TDataSourceDirectoryExt>(
                    jobSpecExt->mutable_extensions(),
                    BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            }

            auto* sortJobSpecExt = sortJobSpecTemplate.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
            ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
        }

        {
            IntermediateSortJobSpecTemplate_ = sortJobSpecTemplate;
            IntermediateSortJobSpecTemplate_.set_type(ToProto(GetIntermediateSortJobType()));
            auto* jobSpecExt = IntermediateSortJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));
            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(IntermediateSortJobIOConfig_)));
        }

        {
            FinalSortJobSpecTemplate_ = sortJobSpecTemplate;
            FinalSortJobSpecTemplate_.set_type(ToProto(GetFinalSortJobType()));
            auto* jobSpecExt = FinalSortJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));
            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(FinalSortJobIOConfig_)));
        }

        {
            SortedMergeJobSpecTemplate_.set_type(ToProto(EJobType::SortedMerge));
            auto* jobSpecExt = SortedMergeJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* mergeJobSpecExt = SortedMergeJobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(
                    GetSpec()->IntermediateDataAccount,
                    {IntermediateChunkSchema_}));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(SortedMergeJobIOConfig_)));

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
            ToProto(mergeJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
        }

        {
            UnorderedMergeJobSpecTemplate_.set_type(ToProto(EJobType::UnorderedMerge));
            auto* jobSpecExt = UnorderedMergeJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* mergeJobSpecExt = UnorderedMergeJobSpecTemplate_.MutableExtension(TMergeJobSpecExt::merge_job_spec_ext);

            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryFromOutputTables(OutputTables_));

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(UnorderedMergeJobIOConfig_)));

            ToProto(mergeJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
        }
    }


    // Resource management.

    TCpuResource GetPartitionCpuLimit() const override
    {
        return TCpuResource(1L);
    }

    TCpuResource GetSortCpuLimit() const override
    {
        return TCpuResource(1L);
    }

    TCpuResource GetMergeCpuLimit() const override
    {
        return TCpuResource(1L);
    }

    // TODO(apollo1321): Is this really correct?
    TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool /*isRoot*/) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        i64 outputBufferSize = std::min(
            PartitionJobIOConfig_->TableWriter->BlockSize * ExpectedPartitionCount_,
            stat.DataWeight);

        outputBufferSize += THorizontalBlockWriter::MaxReserveSize * ExpectedPartitionCount_;

        outputBufferSize = std::min(
            outputBufferSize,
            PartitionJobIOConfig_->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetPartitionCpuLimit());
        result.SetJobProxyMemory(GetInputIOMemorySize(PartitionJobIOConfig_, stat)
            + outputBufferSize
            + GetOutputWindowMemorySize(PartitionJobIOConfig_));

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
            memory + GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ true));
        result.SetJobProxyMemoryWithFixedWriteBufferSize(
            memory + GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ false));
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
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ true);
            jobProxyMemoryWithFixedWriteBufferSize += GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ false);
        } else {
            jobProxyMemory += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig_);
            jobProxyMemoryWithFixedWriteBufferSize += GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig_);
        }

        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetSortCpuLimit());
        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        if (Config_->EnableNetworkInOperationDemand) {
            result.SetNetwork(Spec_->ShuffleNetworkLimit);
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
            SortedMergeJobIOConfig_,
            /*useEstimatedBufferSize*/ true,
            stat);
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            SortedMergeJobIOConfig_,
            /*useEstimatedBufferSize*/ false,
            stat);

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        return result;
    }

    bool IsRowCountPreserved() const override
    {
        return !InputManager_->HasRowLevelAcl();
    }

    i64 GetUnavailableInputChunkCount() const override
    {
        if (UnavailableChunksWatcher_ && State_ == EControllerState::Preparing) {
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
            UnorderedMergeJobIOConfig_,
            /*useEstimatedBufferSize*/ true,
            AggregateStatistics(stat));
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            UnorderedMergeJobIOConfig_,
            /*useEstimatedBufferSize*/ false,
            AggregateStatistics(stat));

        result.SetJobProxyMemory(jobProxyMemory);
        result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

        return result;
    }

    // Progress reporting.

    TString GetLoggingProgress() const override
    {
        return Format(
            "{"
            "Jobs: %v, "
            "Partitions: {T: %v, C: %v}, "
            "DispatchedPartitions: %v, "
            "PartitionJobs: %v, "
            "IntermediateSortJobs: %v, "
            "FinalSortJobs: %v, "
            "SortedMergeJobs: %v, "
            "UnorderedMergeJobs: %v, "
            "ControllerPendingJobCount: %v, "
            "UnavailableInputChunks: %v"
            "}",
            // Jobs
            GetTotalJobCounter(),
            // Partitions
            std::ssize(UnorderedFinalPartitions_),
            CompletedPartitionCount_,
            PartitionsDispatchStatistics_,
            // PartitionJobs
            GetPartitionJobCounter(),
            // IntermediateSortJobs
            IntermediateSortJobCounter_,
            // FinalSortJobs
            FinalSortJobCounter_,
            // SortedMergeJobs
            SortedMergeJobCounter_,
            // UnorderedMergeJobs
            UnorderedMergeJobCounter_,
            GetPendingJobCount(),
            GetUnavailableInputChunkCount());
    }

    void BuildProgress(TFluentMap fluent) override
    {
        TSortControllerBase::BuildProgress(fluent);
        fluent
            .Do(BIND(&TSortController::BuildPartitionsProgressYson, Unretained(this)))
            .Item(JobTypeAsKey(EJobType::Partition)).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(EJobType::IntermediateSort)).Value(IntermediateSortJobCounter_)
            .Item(JobTypeAsKey(EJobType::FinalSort)).Value(FinalSortJobCounter_)
            .Item(JobTypeAsKey(EJobType::SortedMerge)).Value(SortedMergeJobCounter_)
            .Item(JobTypeAsKey(EJobType::UnorderedMerge)).Value(UnorderedMergeJobCounter_)
            // TODO(ignat): remove when UI migrate to new keys.
            .Item("partition_jobs").Value(GetPartitionJobCounter())
            .Item("intermediate_sort_jobs").Value(IntermediateSortJobCounter_)
            .Item("final_sort_jobs").Value(FinalSortJobCounter_)
            .Item("sorted_merge_jobs").Value(SortedMergeJobCounter_)
            .Item("unordered_merge_jobs").Value(UnorderedMergeJobCounter_);
    }

    EJobType GetPartitionJobType(bool /*isRoot*/) const override
    {
        return EJobType::Partition;
    }

    void CreateSortedMergeTask() override
    {
        SortedMergeTask_ = New<TSortedMergeTask>(
            this,
            /*outputStreamDescriptors*/ GetFinalStreamDescriptors(),
            /*enableKeyGuarantee*/ false);
    }

    TSortColumns GetSortedMergeSortColumns() const override
    {
        return Spec_->SortBy;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TSortOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TSortOperationSpec>();
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
    auto options = CreateOperationOptions(config->SortOperationOptions, operation->GetOptionsPatch());
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
            std::move(config),
            std::move(options),
            std::move(host),
            operation)
        , Spec_(std::move(spec))
    { }

    void BuildBriefSpec(TFluentMap fluent) const override
    {
        TSortControllerBase::BuildBriefSpec(fluent);
        fluent
            .DoIf(Spec_->HasNontrivialMapper(), [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec_->Mapper->Command))
                    .EndMap();
            })
            .Item("reducer").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec_->Reducer->Command))
            .EndMap()
            .DoIf(Spec_->HasNontrivialReduceCombiner(), [&] (TFluentMap fluent) {
                fluent
                    .Item("reduce_combiner").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec_->ReduceCombiner->Command))
                    .EndMap();
            });
    }

    void InitStreamDescriptors()
    {
        const auto& streamDescriptors = GetStandardStreamDescriptors();

        MapperSinkEdges_ = std::vector<TOutputStreamDescriptorPtr>(
            streamDescriptors.begin(),
            streamDescriptors.begin() + Spec_->MapperOutputTableCount);
        for (int index = 0; index < std::ssize(MapperSinkEdges_); ++index) {
            MapperSinkEdges_[index] = MapperSinkEdges_[index]->Clone();
            MapperSinkEdges_[index]->TableWriterOptions->TableIndex = index + 1;
        }

        ReducerSinkEdges_ = std::vector<TOutputStreamDescriptorPtr>(
            streamDescriptors.begin() + Spec_->MapperOutputTableCount,
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
    TMapReduceOperationSpecPtr Spec_;

    // Mapper stream descriptors are for the data that is written from mappers directly to the first
    // `Spec->MapperOutputTableCount` output tables skipping the shuffle and reduce phases.
    std::vector<TOutputStreamDescriptorPtr> MapperSinkEdges_;
    std::vector<TOutputStreamDescriptorPtr> ReducerSinkEdges_;

    std::vector<TUserFile> MapperFiles_;
    std::vector<TUserFile> ReduceCombinerFiles_;
    std::vector<TUserFile> ReducerFiles_;

    i64 MapStartRowIndex_ = 0;
    i64 ReduceStartRowIndex_ = 0;

    // Custom bits of preparation pipeline.

    void DoInitialize() override
    {
        TSortControllerBase::DoInitialize();

        if (Spec_->HasNontrivialMapper()) {
            ValidateUserFileCount(Spec_->Mapper, "mapper");
        }
        ValidateUserFileCount(Spec_->Reducer, "reducer");
        if (Spec_->HasNontrivialReduceCombiner()) {
            ValidateUserFileCount(Spec_->ReduceCombiner, "reduce combiner");
        }

        if (!CheckKeyColumnsCompatible(GetColumnNames(Spec_->SortBy), Spec_->ReduceBy)) {
            THROW_ERROR_EXCEPTION("Reduce columns %v are not compatible with sort columns %v",
                Spec_->ReduceBy,
                GetColumnNames(Spec_->SortBy));
        }

        YT_LOG_DEBUG("ReduceColumns: %v, SortColumns: %v",
            Spec_->ReduceBy,
            GetColumnNames(Spec_->SortBy));
    }

    std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec_->InputTablePaths;
    }

    std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec_->OutputTablePaths;
    }

    std::optional<TRichYPath> GetStderrTablePath() const override
    {
        return Spec_->StderrTablePath;
    }

    TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec_->StderrTableWriter;
    }

    std::optional<TRichYPath> GetCoreTablePath() const override
    {
        return Spec_->CoreTablePath;
    }

    TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec_->CoreTableWriter;
    }

    bool GetEnableCudaGpuCoreDump() const override
    {
        return Spec_->EnableCudaGpuCoreDump;
    }

    std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        std::vector<TUserJobSpecPtr> result = {Spec_->Reducer};
        if (Spec_->HasNontrivialMapper()) {
            result.emplace_back(Spec_->Mapper);
        }
        if (Spec_->HasNontrivialReduceCombiner()) {
            result.emplace_back(Spec_->ReduceCombiner);
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
            // TODO(s-berdnikov): Relax constraints?
            static constexpr TTypeCompatibilityOptions TypeCompatibilityOptions{
                .AllowStructFieldRenaming = false,
                .AllowStructFieldRemoval = false,
                .IgnoreUnknownRemovedFieldNames = false,
            };
            auto [compatibilityRhs, errorRhs] = CheckTypeCompatibility(lhs, rhs, TypeCompatibilityOptions);
            if (compatibilityRhs == ESchemaCompatibility::FullyCompatible) {
                return rhs;
            }
            auto [compatibilityLhs, errorLhs] = CheckTypeCompatibility(rhs, lhs, TypeCompatibilityOptions);
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
        if (!Spec_->HasSchemafulIntermediateStreams()) {
            IntermediateChunkSchema_ = TTableSchema::FromSortColumns(Spec_->SortBy);
            return;
        }

        std::vector<TColumnSchema> chunkSchemaColumns;
        if (Spec_->HasNontrivialMapper()) {
            YT_VERIFY(std::ssize(Spec_->Mapper->OutputStreams) > Spec_->MapperOutputTableCount);
            int intermediateStreamCount = Spec_->Mapper->OutputStreams.size() - Spec_->MapperOutputTableCount;
            for (int i = 0; i < intermediateStreamCount; ++i) {
                IntermediateStreamSchemas_.push_back(Spec_->Mapper->OutputStreams[i]->Schema);
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Filter(GetColumnNames(Spec_->SortBy))->Columns();
            }
        } else {
            YT_VERIFY(!InputManager_->GetInputTables().empty());
            for (const auto& inputTable : InputManager_->GetInputTables()) {
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
                    return New<TTableSchema>(std::move(columns), schema->IsStrict())->ToSorted(sortColumns);
                };

                const auto& columns = inputTable->Path.GetColumns();
                IntermediateStreamSchemas_.push_back(toStreamSchema(inputTable->Schema, Spec_->SortBy, columns));
            }
            if (AreAllEqual(IntermediateStreamSchemas_)) {
                chunkSchemaColumns = IntermediateStreamSchemas_.front()->Columns();
            } else {
                for (const auto& sortColumn : Spec_->SortBy) {
                    auto type = InferColumnType(InputManager_->GetInputTables(), sortColumn.Name);
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
        columns.reserve(Spec_->ReduceBy.size());
        for (const auto& reduceColumn : Spec_->ReduceBy) {
            const auto& column = IntermediateChunkSchema_->GetColumn(reduceColumn);
            columns.push_back(column);
        }
        return New<TTableSchema>(std::move(columns));
    }

    void CustomMaterialize() override
    {
        TSortControllerBase::CustomMaterialize();

        if (EstimatedInputStatistics_->DataWeight == 0) {
            return;
        }

        MapperFiles_ = UserJobFiles_[Spec_->Mapper];
        ReduceCombinerFiles_ = UserJobFiles_[Spec_->ReduceCombiner];
        ReducerFiles_ = UserJobFiles_[Spec_->Reducer];

        InitJobIOConfigs();
        InitStreamDescriptors();

        InitPartitioningParametersEvaluator();

        // Due to the sampling it is possible that TotalEstimatedInputDataWeight > 0
        // but according to job size constraints there is nothing to do.
        if (RootPartitionPoolJobSizeConstraints_->GetJobCount() == 0) {
            ExpectedPartitionCount_ = 0;
            return;
        }

        std::vector<TPartitionKey> partitionKeys;
        if (!Spec_->PivotKeys.empty() || Spec_->ComputePivotKeysFromSamples) {
            auto sampleSchema = BuildSampleSchema();
            partitionKeys = BuildPartitionKeys(sampleSchema, sampleSchema);

            ExpectedPartitionCount_ = partitionKeys.size() + 1;
        } else {
            ExpectedPartitionCount_ = PartitioningParametersEvaluator_->SuggestPartitionCount();
        }

        MaxPartitionFactor_ = PartitioningParametersEvaluator_->SuggestMaxPartitionFactor(ExpectedPartitionCount_);
        BuildPartitionTree();

        if (!partitionKeys.empty()) {
            AssignPartitionKeysToPartitions(partitionKeys, /*setManiac*/ false);
        }

        YT_LOG_DEBUG(
            "Final partitioning parameters (ExpectedPartitionCount: %v, MaxPartitionFactor: %v)",
            ExpectedPartitionCount_,
            MaxPartitionFactor_);

        CreateShufflePools();
        CreateSortedMergeTask();

        PreparePartitionTasks();
        PrepareSortTasks();
        PrepareSortedMergeTask();

        InitJobSpecTemplates();

        SetupPartitioningCompletedCallbacks();
    }

    void PreparePartitionTasks()
    {
        PartitionTasks_.resize(PartitionTreeDepth_);
        for (int partitionTaskLevel = PartitionTreeDepth_ - 1; partitionTaskLevel >= 0; --partitionTaskLevel) {
            // Primary stream descriptor for shuffled output of the mapper.
            auto shuffleStreamDescriptor = GetIntermediateStreamDescriptorTemplate()->Clone();
            shuffleStreamDescriptor->DestinationPool = ShuffleMultiChunkPoolInputs_[partitionTaskLevel];
            shuffleStreamDescriptor->ChunkMapping = ShuffleMultiInputChunkMappings_[partitionTaskLevel];
            shuffleStreamDescriptor->TableWriterOptions->ReturnBoundaryKeys = false;
            shuffleStreamDescriptor->TableUploadOptions.TableSchema = IntermediateChunkSchema_;
            shuffleStreamDescriptor->StreamSchemas = IntermediateStreamSchemas_;
            if (partitionTaskLevel != PartitionTreeDepth_ - 1) {
                shuffleStreamDescriptor->TargetDescriptor = PartitionTasks_[partitionTaskLevel + 1]->GetVertexDescriptor();
            }
            shuffleStreamDescriptor->TableWriterOptions->ComputeDigest =
                partitionTaskLevel == 0 &&
                Spec_->EnableIntermediateOutputRecalculation &&
                Spec_->HasNontrivialMapper();

            std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors{std::move(shuffleStreamDescriptor)};

            if (partitionTaskLevel == 0) {
                outputStreamDescriptors.insert(
                    outputStreamDescriptors.end(),
                    MapperSinkEdges_.begin(),
                    MapperSinkEdges_.end());

                bool useJobSizeAdjuster = Spec_->Ordered
                    ? Config_->EnableOrderedPartitionMapJobSizeAdjustment
                    : Config_->EnablePartitionMapJobSizeAdjustment;

                auto chunkPool = CreateRootPartitionPool(
                    useJobSizeAdjuster
                        ? Options_->PartitionJobSizeAdjuster
                        : nullptr,
                    Spec_->Ordered);

                auto chunkPoolOutput = chunkPool;
                PartitionTasks_[partitionTaskLevel] = New<TPartitionTask>(
                    this,
                    /*outputStreamDescriptors*/ std::move(outputStreamDescriptors),
                    partitionTaskLevel,
                    std::move(chunkPool),
                    std::move(chunkPoolOutput));
            } else {
                PartitionTasks_[partitionTaskLevel] = New<TPartitionTask>(
                    this,
                    /*outputStreamDescriptors*/ std::move(outputStreamDescriptors),
                    partitionTaskLevel,
                    ShuffleMultiChunkPoolInputs_[partitionTaskLevel - 1],
                    CreateLevelMultiChunkPoolOutput(partitionTaskLevel));
            }
        }

        const auto& rootPartition = IntermediatePartitionsByLevels_[0][0];
        rootPartition->ChunkPoolOutput() = PartitionTasks_.front()->GetChunkPoolOutput();
        rootPartition->SetAllDataCollected(true);

        for (int partitionTaskLevel = 0; partitionTaskLevel < PartitionTreeDepth_; ++partitionTaskLevel) {
            RegisterTask(PartitionTasks_[partitionTaskLevel]);
        }

        for (int partitionTaskLevel = 1; partitionTaskLevel < std::ssize(PartitionTasks_); ++partitionTaskLevel) {
            const auto& partitionTask = PartitionTasks_[partitionTaskLevel];
            partitionTask->SetInputVertex(PartitionTasks_[partitionTaskLevel - 1]->GetVertexDescriptor());
            partitionTask->RegisterInGraph();
        }

        ProcessInputs(PartitionTasks_[0], RootPartitionPoolJobSizeConstraints_);
        FinishTaskInput(PartitionTasks_[0]);

        YT_LOG_INFO("Map-reducing with partitioning (ExpectedPartitionCount: %v, PartitionJobCount: %v, PartitionDataWeightPerJob: %v)",
            ExpectedPartitionCount_,
            RootPartitionPoolJobSizeConstraints_->GetJobCount(),
            RootPartitionPoolJobSizeConstraints_->GetDataWeightPerJob());
    }

    TOutputStreamDescriptorPtr GetSortedMergeStreamDescriptor() const override
    {
        auto streamDescriptor = TSortControllerBase::GetSortedMergeStreamDescriptor();
        streamDescriptor->TableWriterOptions->ComputeDigest =
            Spec_->HasNontrivialReduceCombiner() &&
            Spec_->EnableIntermediateOutputRecalculation;
        return streamDescriptor;
    }

    void InitJobIOConfigs()
    {
        TSortControllerBase::InitJobIOConfigs();

        RootPartitionJobIOConfig_ = CloneYsonStruct(Spec_->PartitionJobIO);
        PartitionJobIOConfig_ = CloneYsonStruct(Spec_->PartitionJobIO);
        PartitionJobIOConfig_->TableReader->SamplingRate = std::nullopt;

        IntermediateSortJobIOConfig_ = Spec_->SortJobIO;

        // Partition reduce: writer like in merge and reader like in sort.
        FinalSortJobIOConfig_ = CloneYsonStruct(Spec_->MergeJobIO);
        FinalSortJobIOConfig_->TableReader = CloneYsonStruct(Spec_->SortJobIO->TableReader);

        // Sorted reduce.
        SortedMergeJobIOConfig_ = CloneYsonStruct(Spec_->MergeJobIO);
    }

    EJobType GetPartitionJobType(bool isRoot) const override
    {
        if (Spec_->HasNontrivialMapper() && isRoot) {
            return EJobType::PartitionMap;
        } else {
            return EJobType::Partition;
        }
    }

    EJobType GetIntermediateSortJobType() const override
    {
        return Spec_->HasNontrivialReduceCombiner() ? EJobType::ReduceCombiner : EJobType::IntermediateSort;
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
        return Spec_->Reducer;
    }

    const std::vector<TOutputStreamDescriptorPtr>& GetFinalStreamDescriptors() const override
    {
        return ReducerSinkEdges_;
    }

    void PrepareInputQuery() override
    {
        if (HasInputQuery(*Spec_)) {
            if (Spec_->InputQueryOptions->UseSystemColumns) {
                InputManager_->AdjustSchemas(ControlAttributesToColumnOptions(*Spec_->PartitionJobIO->ControlAttributes));
            }
            ParseInputQuery(*Spec_);
        }
    }

    void InitJobSpecTemplates()
    {
        {
            RootPartitionJobSpecTemplate_.set_type(ToProto(GetPartitionJobType(/*isRoot*/ true)));

            auto* jobSpecExt = RootPartitionJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);

            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(CreateTableReaderOptions(RootPartitionJobIOConfig_))));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSourceDirectoryFromInputTables(InputManager_->GetInputTables()));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForMapper());

            if (InputQuerySpec_) {
                WriteInputQueryToJobSpec(jobSpecExt);
            }

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(RootPartitionJobIOConfig_)));

            auto* partitionJobSpecExt = RootPartitionJobSpecTemplate_.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec_->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec_->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec_->EnableIntermediateOutputRecalculation || Spec_->Ordered);

            if (Spec_->HasNontrivialMapper()) {
                InitUserJobSpecTemplate(
                    jobSpecExt->mutable_user_job_spec(),
                    Spec_->Mapper,
                    MapperFiles_,
                    Spec_->DebugArtifactsAccount);
            }
        }

        {
            PartitionJobSpecTemplate_.set_type(ToProto(EJobType::Partition));
            auto* jobSpecExt = PartitionJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(CreateTableReaderOptions(PartitionJobIOConfig_))));

            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSourceDirectory(GetSpec()->IntermediateDataAccount));
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(PartitionJobIOConfig_)));

            auto* partitionJobSpecExt = PartitionJobSpecTemplate_.MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
            partitionJobSpecExt->set_reduce_key_column_count(Spec_->ReduceBy.size());
            ToProto(partitionJobSpecExt->mutable_sort_key_columns(), GetColumnNames(Spec_->SortBy));
            ToProto(partitionJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            partitionJobSpecExt->set_use_sequential_reader(Spec_->EnableIntermediateOutputRecalculation || Spec_->Ordered);
        }

        auto intermediateDataSourceDirectory = BuildIntermediateDataSourceDirectory(
            GetSpec()->IntermediateDataAccount,
            IntermediateStreamSchemas_);
        const auto castAnyToComposite = !AreAllEqual(IntermediateStreamSchemas_);

        auto intermediateReaderOptions = New<TTableReaderOptions>();
        {
            auto* jobSpecExt = IntermediateSortJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(IntermediateSortJobIOConfig_)));
            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildIntermediateDataSinkDirectory(GetSpec()->IntermediateDataAccount));

            if (Spec_->HasNontrivialReduceCombiner()) {
                IntermediateSortJobSpecTemplate_.set_type(ToProto(EJobType::ReduceCombiner));

                auto* reduceJobSpecExt = IntermediateSortJobSpecTemplate_.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
                reduceJobSpecExt->set_reduce_key_column_count(Spec_->ReduceBy.size());
                ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec_->SortBy);

                InitUserJobSpecTemplate(
                    jobSpecExt->mutable_user_job_spec(),
                    Spec_->ReduceCombiner,
                    ReduceCombinerFiles_,
                    Spec_->DebugArtifactsAccount);
                jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
            } else {
                IntermediateSortJobSpecTemplate_.set_type(ToProto(EJobType::IntermediateSort));
                auto* sortJobSpecExt = IntermediateSortJobSpecTemplate_.MutableExtension(TSortJobSpecExt::sort_job_spec_ext);
                ToProto(sortJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
            }
        }

        {
            FinalSortJobSpecTemplate_.set_type(ToProto(EJobType::PartitionReduce));

            auto* jobSpecExt = FinalSortJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* reduceJobSpecExt = FinalSortJobSpecTemplate_.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);

            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForReducer());

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(FinalSortJobIOConfig_)));

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec_->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            reduceJobSpecExt->set_disable_sorted_input(Spec_->DisableSortedInputInReducer);

            InitUserJobSpecTemplate(
                jobSpecExt->mutable_user_job_spec(),
                Spec_->Reducer,
                ReducerFiles_,
                Spec_->DebugArtifactsAccount);
            jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
        }

        {
            SortedMergeJobSpecTemplate_.set_type(ToProto(EJobType::SortedReduce));

            auto* jobSpecExt = SortedMergeJobSpecTemplate_.MutableExtension(TJobSpecExt::job_spec_ext);
            auto* reduceJobSpecExt = SortedMergeJobSpecTemplate_.MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
            auto intermediateDataSourceDirectory = BuildIntermediateDataSourceDirectory(
                GetSpec()->IntermediateDataAccount,
                std::vector<TTableSchemaPtr>(IntermediateStreamSchemas_.size(), IntermediateChunkSchema_));

            jobSpecExt->set_table_reader_options(ToProto(ConvertToYsonString(intermediateReaderOptions)));
            SetProtoExtension<TDataSourceDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                intermediateDataSourceDirectory);
            SetProtoExtension<TDataSinkDirectoryExt>(
                jobSpecExt->mutable_extensions(),
                BuildDataSinkDirectoryForReducer());

            jobSpecExt->set_io_config(ToProto(ConvertToYsonString(SortedMergeJobIOConfig_)));

            ToProto(reduceJobSpecExt->mutable_key_columns(), GetColumnNames(Spec_->SortBy));
            reduceJobSpecExt->set_reduce_key_column_count(Spec_->ReduceBy.size());
            ToProto(reduceJobSpecExt->mutable_sort_columns(), Spec_->SortBy);
            reduceJobSpecExt->set_disable_sorted_input(Spec_->DisableSortedInputInReducer);

            InitUserJobSpecTemplate(
                jobSpecExt->mutable_user_job_spec(),
                Spec_->Reducer,
                ReducerFiles_,
                Spec_->DebugArtifactsAccount);
            jobSpecExt->mutable_user_job_spec()->set_cast_input_any_to_composite(castAnyToComposite);
        }
    }

    void CustomizeJoblet(const TJobletPtr& joblet, const TAllocation& /*allocation*/) override
    {
        switch (joblet->JobType) {
            case EJobType::PartitionMap:
                joblet->StartRowIndex = MapStartRowIndex_;
                MapStartRowIndex_ += joblet->InputStripeList->GetAggregateStatistics().RowCount;
                break;

            case EJobType::PartitionReduce:
            case EJobType::SortedReduce:
                joblet->StartRowIndex = ReduceStartRowIndex_;
                ReduceStartRowIndex_ += joblet->InputStripeList->GetAggregateStatistics().RowCount;
                break;

            default:
                break;
        }
    }

    ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec_->EnableLegacyLivePreview);
    }

    ELegacyLivePreviewMode GetLegacyIntermediateLivePreviewMode() const override
    {
        return ToLegacyLivePreviewMode(Spec_->EnableLegacyLivePreview);
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
        return TCpuResource(Spec_->HasNontrivialMapper() ? GetCpuLimit(Spec_->Mapper) : 1);
    }

    TCpuResource GetSortCpuLimit() const override
    {
        // At least one CPU, may be more in PartitionReduce job.
        return TCpuResource(1L);
    }

    TCpuResource GetMergeCpuLimit() const override
    {
        return TCpuResource(GetCpuLimit(Spec_->Reducer));
    }

    TExtendedJobResources GetPartitionResources(
        const TChunkStripeStatisticsVector& statistics,
        bool isRoot) const override
    {
        auto stat = AggregateStatistics(statistics).front();

        // TODO(apollo1321): Current memory estimation is not correct for multilevel shuffle.
        // It should account for the number of output partitions for a given job.
        i64 reserveSize = THorizontalBlockWriter::MaxReserveSize * ExpectedPartitionCount_;
        i64 bufferSize = std::min(
            reserveSize + PartitionJobIOConfig_->TableWriter->BlockSize * ExpectedPartitionCount_,
            PartitionJobIOConfig_->TableWriter->MaxBufferSize);

        TExtendedJobResources result;
        result.SetUserSlots(1);
        if (Spec_->HasNontrivialMapper() && isRoot) {
            result.SetCpu(GetCpuLimit(Spec_->Mapper));
            result.SetJobProxyMemory(
                GetInputIOMemorySize(PartitionJobIOConfig_, stat) +
                GetOutputWindowMemorySize(PartitionJobIOConfig_) +
                bufferSize);
        } else {
            result.SetCpu(1);
            bufferSize = std::min(bufferSize, stat.DataWeight + reserveSize);
            result.SetJobProxyMemory(
                GetInputIOMemorySize(PartitionJobIOConfig_, stat) +
                GetOutputWindowMemorySize(PartitionJobIOConfig_) +
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

    TUserJobSpecPtr GetSortUserJobSpec(bool isFinalSort) const override
    {
        if (isFinalSort) {
            return Spec_->Reducer;
        } else if (Spec_->HasNontrivialReduceCombiner()) {
            return Spec_->ReduceCombiner;
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
            result.SetCpu(GetCpuLimit(Spec_->Reducer));
            jobProxyMemory += GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ true);
            jobProxyMemoryWithFixedWriteBufferSize += GetFinalOutputIOMemorySize(FinalSortJobIOConfig_, /*useEstimatedBufferSize*/ false);
            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);
        } else {
            auto diff = GetIntermediateOutputIOMemorySize(IntermediateSortJobIOConfig_);
            jobProxyMemory += diff;
            jobProxyMemoryWithFixedWriteBufferSize += diff;
            result.SetJobProxyMemory(jobProxyMemory);
            result.SetJobProxyMemoryWithFixedWriteBufferSize(jobProxyMemoryWithFixedWriteBufferSize);

            if (Spec_->HasNontrivialReduceCombiner()) {
                result.SetCpu(GetCpuLimit(Spec_->ReduceCombiner));
            } else {
                result.SetCpu(1);
            }
        }

        if (Config_->EnableNetworkInOperationDemand) {
            result.SetNetwork(Spec_->ShuffleNetworkLimit);
        }

        return result;
    }

    TExtendedJobResources GetSortedMergeResources(
        const TChunkStripeStatisticsVector& statistics) const override
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetCpuLimit(Spec_->Reducer));
        auto jobProxyMemory = GetFinalIOMemorySize(
            SortedMergeJobIOConfig_,
            /*useEstimatedBufferSize*/ true,
            statistics);
        auto jobProxyMemoryWithFixedWriteBufferSize = GetFinalIOMemorySize(
            SortedMergeJobIOConfig_,
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
        return Format(
            "{"
            "Jobs: %v, "
            "Partitions: {T: %v, C: %v}, "
            "DispatchedPartitions: %v, "
            "MapJobs: %v, "
            "SortJobs: %v, "
            "PartitionReduceJobs: %v, "
            "SortedReduceJobs: %v, "
            "ControllerPendingJobCount: %v, "
            "UnavailableInputChunks: %v"
            "}",
            // Jobs
            GetTotalJobCounter(),
            // Partitions
            std::ssize(UnorderedFinalPartitions_),
            CompletedPartitionCount_,
            PartitionsDispatchStatistics_,
            // MapJobs
            GetPartitionJobCounter(),
            // SortJobs
            IntermediateSortJobCounter_,
            // PartitionReduceJobs
            FinalSortJobCounter_,
            // SortedReduceJobs
            SortedMergeJobCounter_,
            GetPendingJobCount(),
            GetUnavailableInputChunkCount());
    }

    void BuildProgress(TFluentMap fluent) override
    {
        TSortControllerBase::BuildProgress(fluent);
        fluent
            .Do(BIND(&TMapReduceController::BuildPartitionsProgressYson, Unretained(this)))
            // TODO(gritukan): What should I do here?
            .Item(JobTypeAsKey(GetPartitionJobType(/*isRoot*/ true))).Value(GetPartitionJobCounter())
            .Item(JobTypeAsKey(GetIntermediateSortJobType())).Value(IntermediateSortJobCounter_)
            .Item(JobTypeAsKey(GetFinalSortJobType())).Value(FinalSortJobCounter_)
            .Item(JobTypeAsKey(GetSortedMergeJobType())).Value(SortedMergeJobCounter_)
            // TODO(ignat): remove when UI migrate to new keys.
            .Item(Spec_->HasNontrivialMapper() ? "map_jobs" : "partition_jobs").Value(GetPartitionJobCounter())
            .Item(Spec_->HasNontrivialReduceCombiner() ? "reduce_combiner_jobs" : "sort_jobs").Value(IntermediateSortJobCounter_)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter_)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter_);
    }

    TUserJobSpecPtr GetPartitionUserJobSpec(bool isRoot) const override
    {
        if (Spec_->HasNontrivialMapper() && isRoot) {
            return Spec_->Mapper;
        } else {
            return nullptr;
        }
    }

    void CreateSortedMergeTask() override
    {
        SortedMergeTask_ = New<TSortedMergeTask>(
            this,
            /*outputStreamDescriptors*/ GetFinalStreamDescriptors(),
            /*enableKeyGuarantee*/ true);
    }

    TSortColumns GetSortedMergeSortColumns() const override
    {
        auto sortColumns = Spec_->SortBy;
        sortColumns.resize(Spec_->ReduceBy.size());
        return sortColumns;
    }

    TYsonStructPtr GetTypedSpec() const override
    {
        return Spec_;
    }

    TOperationSpecBasePtr ParseTypedSpec(const INodePtr& spec) const override
    {
        return ParseOperationSpec<TMapReduceOperationSpec>(spec);
    }

    TOperationSpecBaseConfigurator GetOperationSpecBaseConfigurator() const override
    {
        return TConfigurator<TMapReduceOperationSpec>();
    }

    bool ShouldForceSortedMerge() const override
    {
        return Spec_->ForceReduceCombiners;
    }

private:
    TDataSinkDirectoryPtr BuildDataSinkDirectoryForMapper() const
    {
        auto dataSinkDirectory = New<TDataSinkDirectory>();
        auto mapperOutputTableCount = static_cast<size_t>(Spec_->MapperOutputTableCount);
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
            std::vector<TOutputTablePtr>(OutputTables_.begin() + Spec_->MapperOutputTableCount, OutputTables_.end()));
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
    auto options = CreateOperationOptions(config->MapReduceOperationOptions, operation->GetOptionsPatch());
    auto spec = ParseOperationSpec<TMapReduceOperationSpec>(UpdateSpec(options->SpecTemplate, operation->GetSpec()));
    AdjustSamplingFromConfig(spec, config);
    return New<TMapReduceController>(spec, config, options, host, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
