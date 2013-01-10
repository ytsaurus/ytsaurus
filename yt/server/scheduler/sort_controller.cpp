#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"
#include "chunk_info_collector.h"
#include "job_resources.h"
#include "helpers.h"

#include <ytlib/misc/string.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/helpers.h>
#include <ytlib/table_client/channel_writer.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/job_proxy/config.h>

#include <ytlib/transaction_client/transaction.h>

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
using namespace NScheduler::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationLogger);
static NProfiling::TProfiler Profiler("/operations/sort");

//! Maximum number of buckets for partition sizes aggregation.
static const int MaxAggregatedPartitionBuckets = 100;

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
        : TOperationControllerBase(config, spec, host, operation)
        , Spec(spec)
        , Config(config)
        , CompletedPartitionCount(0)
        , SortDataSizeCounter(0)
        , SortStartThresholdReached(false)
        , MergeStartThresholdReached(false)
        , SortedMergeJobCounter(0)
        , UnorderedMergeJobCounter(0)
        , SimpleSort(false)
    { }

private:
    TSortOperationSpecBasePtr Spec;

protected:
    TSchedulerConfigPtr Config;

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
            , CachedSortedMergeNeeded(false)
            , Maniac(false)
            , SortTask(
                controller->SimpleSort
                ? TSortTaskPtr(New<TSimpleSortTask>(controller, this))
                : TSortTaskPtr(New<TPartitionSortTask>(controller, this)))
            , SortedMergeTask(New<TSortedMergeTask>(controller, this))
            , UnorderedMergeTask(New<TUnorderedMergeTask>(controller, this))
            , ChunkPoolOutput(nullptr)
        { }

        //! Sequential index (zero based).
        int Index;

        //! Is partition completed?
        bool Completed;

        //! Do we need to run merge tasks for this partition?
        //! Cached value, updated by #IsSortedMergeNeeded.
        bool CachedSortedMergeNeeded;

        //! Does the partition consist of rows with the same key?
        bool Maniac;

        //! Number of sorted bytes residing at a given node.
        yhash_map<Stroka, i64> AddressToLocality;

        // Tasks.
        TSortTaskPtr SortTask;
        TSortedMergeTaskPtr SortedMergeTask;
        TUnorderedMergeTaskPtr UnorderedMergeTask;

        // Chunk pool output obtained from the shuffle pool.
        IChunkPoolOutput* ChunkPoolOutput;

        //! A statically assigned partition address, if any.
        TNullable<Stroka> AssignedAddress;
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
            ChunkPool = CreateUnorderedChunkPool(Controller->PartitionJobCounter.GetTotal());
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

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->PartitionJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->PartitionJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet, Controller->IsTableIndexEnabled());
            AddIntermediateOutputSpec(jobSpec, joblet);
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

            auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_chunks());
            Controller->ShufflePool->GetInput()->Add(stripe);


            // Kick-start sort and unordered merge tasks.
            // Compute sort data size delta.
            i64 oldSortDataSize = Controller->SortDataSizeCounter.GetTotal();
            i64 newSortDataSize = 0;
            FOREACH (auto partition, Controller->Partitions) {
                if (partition->Maniac) {
                    Controller->AddTaskPendingHint(partition->UnorderedMergeTask);
                } else {
                    newSortDataSize += partition->ChunkPoolOutput->GetTotalDataSize();
                    Controller->AddTaskPendingHint(partition->SortTask);
                }
            }
            LOG_DEBUG("Sort data size updated: %" PRId64 " -> %" PRId64,
                oldSortDataSize,
                newSortDataSize);
            Controller->SortDataSizeCounter.Increment(newSortDataSize - oldSortDataSize);

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

            Controller->AssignPartitions();
            Controller->CheckMergeStartThreshold();

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
        TPartitionBoundTask(TSortControllerBase* controller, TPartition* partition)
            : TTask(controller)
            , Controller(controller)
            , Partition(partition)
        { }

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
        TSortTask(TSortControllerBase* controller, TPartition* partition)
            : TPartitionBoundTask(controller, partition)
        { }

        virtual int GetPriority() const override
        {
            return 1;
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

    protected:
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

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->IsSortedMergeNeeded(Partition)
                ? 1
                : Controller->OutputTables.size();
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
                AddIntermediateOutputSpec(jobSpec, joblet);
            } else {
                jobSpec->CopyFrom(Controller->FinalSortJobSpecTemplate);
                AddFinalOutputSpecs(jobSpec, joblet);
            }

            AddSequentialInputSpec(jobSpec, joblet, Controller->IsTableIndexEnabled());
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
                auto* resultExt = joblet->Job->Result().MutableExtension(TSortJobResultExt::sort_job_result_ext);
                auto stripe = BuildIntermediateChunkStripe(resultExt->mutable_chunks());
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
        TPartitionSortTask(TSortControllerBase* controller, TPartition* partition)
            : TSortTask(controller, partition)
        { }

        virtual Stroka GetId() const override
        {
            return Sprintf("Sort(%d)", Partition->Index);
        }

        virtual int GetPendingJobCount() const override
        {
            if (!Controller->SortStartThresholdReached) {
                return 0;
            }

            if (Partition->Maniac) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return
                Partition->AssignedAddress
                ? Controller->Spec->SortAssignmentTimeout
                : Controller->Spec->SortLocalityTimeout;
        }

        virtual i64 GetLocality(const Stroka& address) const override
        {
            if (Partition->AssignedAddress && Partition->AssignedAddress.Get() == address) {
                // Handle initially assigned address.
                return 1;
            } else {
                // Handle data-driven locality.
                auto it = Partition->AddressToLocality.find(address);
                return it == Partition->AddressToLocality.end() ? 0 : it->second;
            }
        }

    private:
        virtual bool HasInputLocality() override
        {
            return false;
        }

        virtual void OnJobStarted(TJobletPtr joblet) override
        {
            // Increase data size for this address to ensure subsequent sort jobs
            // to be scheduled to this very node.
            auto address = joblet->Job->GetNode()->GetAddress();
            Partition->AddressToLocality[address] += joblet->InputStripeList->TotalDataSize;

            // Don't rely on static assignment anymore.
            Partition->AssignedAddress = Null;

            // Also add a hint to ensure that subsequent jobs are also scheduled here.
            AddLocalityHint(address);

            TSortTask::OnJobStarted(joblet);
        }

    };

    //! Implements simple sort phase for sort operations.
    class TSimpleSortTask
        : public TSortTask
    {
    public:
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

    private:
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

            if (Partition->Maniac) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return
                Controller->SimpleSort
                ? Controller->Spec->SimpleMergeLocalityTimeout
                : Controller->Spec->MergeLocalityTimeout;
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

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->SortedMergeJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->SortedMergeJobSpecTemplate);
            AddParallelInputSpec(jobSpec, joblet, Controller->IsTableIndexEnabled());
            AddFinalOutputSpecs(jobSpec, joblet);
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

            if (!Partition->Maniac) {
                return 0;
            }

            return TTask::GetPendingJobCount();
        }

        virtual i64 GetLocality(const Stroka& address) const override
        {
            // Locality is unimportant.
            return 0;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            // Makes no sense to wait.
            return TDuration::Zero();
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

        virtual bool HasInputLocality() override
        {
            return false;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return 1;
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->UnorderedMergeJobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->UnorderedMergeJobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet, Controller->IsTableIndexEnabled());
            AddFinalOutputSpecs(jobSpec, joblet);

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

    void AssignPartitions()
    {
        struct TAssignedNode
            : public TIntrinsicRefCounted
        {
            TAssignedNode(TExecNodePtr node, double weight)
                : Node(node)
                , Weight(weight)
                , AssignedDataSize(0)
            { }

            TExecNodePtr Node;
            double Weight;
            i64 AssignedDataSize;
        };

        typedef TIntrusivePtr<TAssignedNode> TAssignedNodePtr;

        auto compareNodes = [&] (const TAssignedNodePtr& lhs, const TAssignedNodePtr& rhs) {
            return lhs->AssignedDataSize / lhs->Weight > rhs->AssignedDataSize / rhs->Weight;
        };

        auto comparePartitions = [&] (const TPartitionPtr& lhs, const TPartitionPtr& rhs) {
            return lhs->ChunkPoolOutput->GetTotalDataSize() > rhs->ChunkPoolOutput->GetTotalDataSize();
        };

        LOG_DEBUG("Examining online nodes");

        std::vector<TAssignedNodePtr> nodeHeap;
        FOREACH (auto node, Host->GetExecNodes()) {
            const auto& resourceUsage = node->ResourceUsage();
            const auto& resourceLimits = node->ResourceLimits();
            double weight = GetMinResourceRatio(resourceLimits - resourceUsage, resourceLimits);
            if (weight > 0) {
                auto assignedNode = New<TAssignedNode>(node, weight);
                nodeHeap.push_back(assignedNode);
            }
        }

        std::vector<TPartitionPtr> partitionsToAssign;
        FOREACH (auto partition, Partitions) {
            // Only take partitions for which no jobs are launched yet.
            if (partition->AddressToLocality.empty()) {
                partitionsToAssign.push_back(partition);
            }
        }
        std::sort(partitionsToAssign.begin(), partitionsToAssign.end(), comparePartitions);

        // This is actually redundant since all values are 0.
        std::make_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

        LOG_DEBUG("Assigning partitions");

        FOREACH (auto partition, partitionsToAssign) {
            auto node = nodeHeap.front();
            auto address = node->Node->GetAddress();

            partition->AssignedAddress = address;
            AddTaskLocalityHint(partition->SortTask, address);

            std::pop_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);
            node->AssignedDataSize += partition->ChunkPoolOutput->GetTotalDataSize();
            std::push_heap(nodeHeap.begin(), nodeHeap.end(), compareNodes);

            LOG_DEBUG("Partition assigned (Index: %d, DataSize: %" PRId64 ", Address: %s)",
                partition->Index,
                partition->ChunkPoolOutput->GetTotalDataSize(),
                ~address);
        }

        FOREACH (auto node, nodeHeap) {
            if (node->AssignedDataSize > 0) {
                LOG_DEBUG("Node used (Address: %s, Weight: %.4lf, AssignedDataSize: %" PRId64 ", AdjustedDataSize: %" PRId64 ")",
                    ~node->Node->GetAddress(),
                    node->Weight,
                    node->AssignedDataSize,
                    static_cast<i64>(node->AssignedDataSize / node->Weight));
            }
        }

        LOG_DEBUG("Partitions assigned");
    }

    void InitShufflePool()
    {
        std::vector<i64> dataSizeThresholds;
        for (int index = 0; index < static_cast<int>(Partitions.size()); ++index) {
            dataSizeThresholds.push_back(
                Partitions[index]->Maniac
                ? std::numeric_limits<i64>::max()
                : Spec->MaxDataSizePerSortJob);
        }
        ShufflePool = CreateShuffleChunkPool(dataSizeThresholds);

        FOREACH (auto partition, Partitions) {
            partition->ChunkPoolOutput = ShufflePool->GetOutput(partition->Index);
        }
    }

    void InitSimpleSortPool(int sortJobCount)
    {
        SimpleSortPool = CreateUnorderedChunkPool(sortJobCount);
    }

    virtual void OnOperationCompleted() override
    {
        YCHECK(CompletedPartitionCount == Partitions.size());
        TOperationControllerBase::OnOperationCompleted();
    }

    void RegisterOutputChunkTrees(TJobletPtr joblet, TPartition* partition)
    {
        const TUserJobResult* userJobResult = nullptr;
        if (joblet->Job->Result().HasExtension(TReduceJobResultExt::reduce_job_result_ext)) {
            userJobResult = &joblet->Job->Result()
                .GetExtension(TReduceJobResultExt::reduce_job_result_ext)
                .reducer_result();
        }

        TOperationControllerBase::RegisterOutputChunkTrees(joblet, partition->Index, userJobResult);
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
        if (SimpleSort) {
            return false;
        }

        if (partition->Maniac) {
            return false;
        }

        if (partition->CachedSortedMergeNeeded) {
            return true;
        }

        if (partition->SortTask->GetPendingJobCount() == 0) {
            return false;
        }

        if (partition->ChunkPoolOutput->GetTotalJobCount() <= 1 && PartitionTask->IsCompleted()) {
            return false;
        }

        LOG_DEBUG("Partition needs sorted merge (Partition: %d)", partition->Index);
        SortedMergeJobCounter.Increment(1);
        partition->CachedSortedMergeNeeded = true;
        return true;
    }

    void CheckSortStartThreshold()
    {
        if (SimpleSort)
            return;

        if (SortStartThresholdReached)
            return;

        if (PartitionTask->GetCompletedDataSize() < PartitionTask->GetTotalDataSize() * Spec->ShuffleStartThreshold)
            return;

        LOG_INFO("Sort start threshold reached");

        SortStartThresholdReached = true;
        AddSortTasksPendingHints();
    }

    static void CheckPartitionWriterBuffer(int partitionCount, NTableClient::TTableWriterConfigPtr config)
    {
        auto averageBufferSize = config->MaxBufferSize / partitionCount / 2;
        if (averageBufferSize < NTableClient::TChannelWriter::MinUpperReserveLimit) {
            i64 minAppropriateSize = partitionCount * 2 * NTableClient::TChannelWriter::MinUpperReserveLimit;
            THROW_ERROR_EXCEPTION(
                "Too small table writer buffer size for partitioner (MaxBufferSize: %"PRId64"). Min appropriate buffer size is %"PRId64,
                averageBufferSize,
                minAppropriateSize);
        }
    }

    void CheckMergeStartThreshold()
    {
        if (SimpleSort)
            return;

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

    virtual bool IsTableIndexEnabled() const
    {
        return false;
    }


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
        return static_cast<i64>((double) TotalInputValueCount * dataSize / TotalInputDataSize);
    }

    int SuggestPartitionCount() const
    {
        YCHECK(TotalInputDataSize > 0);
        i64 minSuggestion = static_cast<i64>(ceil((double) TotalInputDataSize / Spec->MaxPartitionDataSize));
        i64 maxSuggestion = static_cast<i64>(ceil((double) TotalInputDataSize / Spec->MinPartitionDataSize));
        i64 result = Spec->PartitionCount.Get(minSuggestion);
        result = std::min(result, maxSuggestion);
        result = std::max(result, (i64)1);
        return static_cast<int>(result);
    }

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

    std::vector<i64> GetAggregatedTotalPartitionSizes() const
    {
        std::vector<i64> sizes(Partitions.size());
        for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
            sizes[i] = Partitions[i]->ChunkPoolOutput->GetTotalDataSize();
        }
        return AggregateValues(sizes, MaxAggregatedPartitionBuckets);
    }

    std::vector<i64> GetAggregatedCompletedPartitionSizes() const
    {
        std::vector<i64> sizes(Partitions.size());
        for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
            sizes[i] = Partitions[i]->ChunkPoolOutput->GetCompletedDataSize();
        }
        return AggregateValues(sizes, MaxAggregatedPartitionBuckets);
    }

    std::vector<i64> GetAggregatedRunningPartitionSizes() const
    {
        std::vector<i64> sizes(Partitions.size());
        for (int i = 0; i < static_cast<int>(Partitions.size()); ++i) {
            sizes[i] = Partitions[i]->ChunkPoolOutput->GetRunningDataSize();
        }
        return AggregateValues(sizes, MaxAggregatedPartitionBuckets);
    }

    void BuildPartitionsProgressYson(IYsonConsumer* consumer) const
    {
        BuildYsonMapFluently(consumer)
            .Item("partitions").BeginMap()
                .Item("total").Value(Partitions.size())
                .Item("completed").Value(CompletedPartitionCount)
            .EndMap()
            .Item("partition_sizes").BeginMap()
                .Item("total").Value(GetAggregatedTotalPartitionSizes())
                .Item("running").Value(GetAggregatedRunningPartitionSizes())
                .Item("completed").Value(GetAggregatedCompletedPartitionSizes())
            .EndMap();
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
        PROFILE_TIMING ("/input_processing_time") {
            SamplesFetcher = New<TSamplesFetcher>(
                Config,
                Spec,
                Operation->GetOperationId());

            SamplesCollector = New<TSamplesCollector>(
                SamplesFetcher,
                Host->GetBackgroundInvoker());

            auto chunks = CollectInputChunks();
            FOREACH (auto chunk, chunks) {
                SamplesCollector->AddChunk(chunk);
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

        CheckPartitionWriterBuffer(partitionCount, PartitionJobIOConfig->TableWriter);

        if (SimpleSort) {
            BuildSinglePartition();
        } else {
            BuildMulitplePartitions(partitionCount);
        }
    }

    void BuildSinglePartition()
    {
        auto stripes = SliceInputChunks(
            Spec->SortJobCount,
            Spec->SortJobSliceDataSize);

        // Initialize counters.
        PartitionJobCounter.Set(0);
        SortDataSizeCounter.Set(TotalInputDataSize);

        // Choose sort job count and initialize the pool.
        int sortJobCount = SuggestJobCount(
            TotalInputDataSize,
            Spec->MinDataSizePerSortJob,
            Spec->MaxDataSizePerSortJob,
            Spec->SortJobCount,
            static_cast<int>(stripes.size()));
        InitSimpleSortPool(sortJobCount);

        // Create the fake partition.
        auto partition = New<TPartition>(this, 0);
        Partitions.push_back(partition);
        partition->ChunkPoolOutput = ~SimpleSortPool;
        partition->SortTask->AddInput(stripes);
        partition->SortTask->FinishInput();

        LOG_INFO("Sorting without partitioning (SortJobCount: %d)",
            sortJobCount);

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

                lastPartition->Maniac = true;
                UnorderedMergeJobCounter.Increment(1);
                YCHECK(skippedCount >= 1);

                auto successorKey = GetSuccessorKey(*sampleKey);
                AddPartition(successorKey);
            }
        }

        InitShufflePool();

        auto stripes = SliceInputChunks(
            Spec->PartitionJobCount,
            Spec->PartitionJobSliceDataSize);

        PartitionJobCounter.Set(SuggestJobCount(
            TotalInputDataSize,
            Spec->MinDataSizePerPartitionJob,
            Spec->MaxDataSizePerPartitionJob,
            Spec->PartitionJobCount,
            static_cast<int>(stripes.size())));

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();

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
            bufferSize + NTableClient::TChannelWriter::MaxUpperReserveLimit * static_cast<i64>(Partitions.size()),
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

    virtual Stroka GetLoggingProgress() override
    {
        return Sprintf(
            "Jobs = {R: % " PRId64 ", C: %" PRId64 ", P: %d, F: %" PRId64 ", A: %" PRId64 ", L: %" PRId64 "}, "
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
            .Do(BIND(&TSortController::BuildPartitionsProgressYson, Unretained(this)))
            .Item("partition_jobs").Value(PartitionJobCounter)
            .Item("intermediate_sort_jobs").Value(IntermediateSortJobCounter)
            .Item("final_sort_jobs").Value(FinalSortJobCounter)
            .Item("sorted_merge_jobs").Value(SortedMergeJobCounter)
            .Item("unordered_merge_jobs").Value(UnorderedMergeJobCounter);
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
        , MapStartRowIndex(0)
        , ReduceStartRowIndex(0)
    { }

private:
    TMapReduceOperationSpecPtr Spec;

    std::vector<TUserFile> MapperFiles;
    std::vector<TUserTableFile> MapperTableFiles;

    std::vector<TUserFile> ReducerFiles;
    std::vector<TUserTableFile> ReducerTableFiles;

    i64 MapStartRowIndex;
    i64 ReduceStartRowIndex;


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

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        // Combine mapper and reducer files into a single collection.
        std::vector<TPathWithStage> result;
        if (Spec->Mapper) {
            FOREACH (const auto& path, Spec->Mapper->FilePaths) {
                result.push_back(std::make_pair(path, EOperationStage::Map));
            }
        }
        FOREACH (const auto& path, Spec->Reducer->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Reduce));
        }
        return result;
    }

    virtual void OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp) override
    {
        FOREACH (const auto& file, Files) {
            if (file.Stage == EOperationStage::Map) {
                MapperFiles.push_back(file);
            } else {
                ReducerFiles.push_back(file);
            }
        }

        FOREACH (const auto& file, TableFiles) {
            if (file.Stage == EOperationStage::Map) {
                MapperTableFiles.push_back(file);
            } else {
                ReducerTableFiles.push_back(file);
            }
        }
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline->Add(BIND(&TMapReduceController::ProcessInputs, MakeStrong(this)));
    }

    TFuture<void> ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
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

        InitJobIOConfigs();

        CheckPartitionWriterBuffer(partitionCount, PartitionJobIOConfig->TableWriter);

        BuildMultiplePartitions(partitionCount);
    }

    void BuildMultiplePartitions(int partitionCount)
    {
        for (int index = 0; index < partitionCount; ++index) {
            Partitions.push_back(New<TPartition>(this, index));
        }

        InitShufflePool();

        auto stripes = SliceInputChunks(
            Spec->PartitionJobCount,
            Spec->PartitionJobSliceDataSize);

        PartitionJobCounter.Set(SuggestJobCount(
            TotalInputDataSize,
            Spec->MinDataSizePerPartitionJob,
            Spec->MaxDataSizePerPartitionJob,
            Spec->PartitionJobCount,
            static_cast<int>(stripes.size())));

        PartitionTask = New<TPartitionTask>(this);
        PartitionTask->AddInput(stripes);
        PartitionTask->FinishInput();

        LOG_INFO("Map-reducing with partitioning (PartitionCount: %d, PartitionJobCount: %" PRId64 ")",
            partitionCount,
            PartitionJobCounter.GetTotal());

        // Kick-start the partition task.
        AddTaskPendingHint(PartitionTask);
    }

    void InitJobIOConfigs()
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
                    MapperFiles,
                    MapperTableFiles);
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
                ReducerFiles,
                ReducerTableFiles);

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
                ReducerFiles,
                ReducerTableFiles);

            SortedMergeJobSpecTemplate.set_io_config(ConvertToYsonString(SortedMergeJobIOConfig).Data());
        }
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        switch (joblet->Job->GetType()) {
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

    virtual void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec) override
    {
        switch (jobSpec->type()) {
            case EJobType::PartitionMap: {
                auto* jobSpecExt = jobSpec->MutableExtension(TPartitionJobSpecExt::partition_job_spec_ext);
                AddUserJobEnvironment(jobSpecExt->mutable_mapper_spec(), joblet);
                break;
            }

            case EJobType::PartitionReduce:
            case EJobType::SortedReduce: {
                auto* jobSpecExt = jobSpec->MutableExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                AddUserJobEnvironment(jobSpecExt->mutable_reducer_spec(), joblet);
                break;
            }

            default:
                break;
        }
    }

    virtual bool IsTableIndexEnabled() const override
    {
        return Spec->EnableTableIndex;
    }

    // Resource management.

    virtual bool IsPartitionJobNonexpanding() const
    {
        return false;
    }

    virtual TNodeResources GetPartitionResources(
        i64 dataSize) const override
    {
        i64 reserveSize = NTableClient::TChannelWriter::MaxUpperReserveLimit * static_cast<i64>(Partitions.size());
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

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }


    // Progress reporting.

    virtual Stroka GetLoggingProgress() override
    {
        return Sprintf(
            "Jobs = {R: %" PRId64 ", C: %" PRId64 ", P: %d, F: %" PRId64", A: %" PRId64 ", L: %" PRId64 "}, "
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
            JobCounter.GetLost(),
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
            .Do(BIND(&TMapReduceController::BuildPartitionsProgressYson, Unretained(this)))
            .Item("map_jobs").Value(PartitionJobCounter)
            .Item("sort_jobs").Value(IntermediateSortJobCounter)
            .Item("partition_reduce_jobs").Value(FinalSortJobCounter)
            .Item("sorted_reduce_jobs").Value(SortedMergeJobCounter);
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

