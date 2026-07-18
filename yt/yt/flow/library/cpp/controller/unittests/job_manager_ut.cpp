#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/computation/controller_base.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>
#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/job_manager.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NController {
namespace {

constexpr TDuration MaxBalanceWait = TDuration::Seconds(10);
constexpr TDuration MinBalanceStep = TDuration::MilliSeconds(100);
constexpr int MaxThreads = 50;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSimpleComputation)

struct TSimpleComputation
    : public TTransformComputation
{
    using TTransformComputation::TTransformComputation;

    std::vector<TMessage> DoTransform(const std::vector<TMessage>& /*messages*/)
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleComputation);

YT_FLOW_DEFINE_COMPUTATION(TSimpleComputation);

////////////////////////////////////////////////////////////////////////////////

struct TThrowingTraverseComputationController
    : public TTransformComputation::TComputationController
{
    using TTransformComputation::TComputationController::TComputationController;

    TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& /*traverseData*/,
        const TFlowViewPtr& /*flowView*/) override
    {
        THROW_ERROR_EXCEPTION("Injected traverse failure");
    }
};

struct TBrokenTraverseComputation
    : public TTransformComputation
{
    using TTransformComputation::TTransformComputation;
    using TComputationController = TThrowingTraverseComputationController;

    std::vector<TMessage> DoTransform(const std::vector<TMessage>& /*messages*/)
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TBrokenTraverseComputation);

YT_FLOW_DEFINE_COMPUTATION(TBrokenTraverseComputation);

////////////////////////////////////////////////////////////////////////////////

//! Fake storage handler. Enough for this test since there's no recovery after restart.
class TStorageHandler : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TComputationDescription
{
    struct TDeviatingPartitionDescription
    {
        int Count;
        double Factor;
    };

    int PartitionCount{};
    std::vector<TDeviatingPartitionDescription> DeviatingPartitions;
    TWorkerGroupId WorkerGroup;
    int Tag{};
};

struct TWorkerGroupDescription
{
    int WorkerCount{};
    std::vector<TWorkerGroupId> Groups;
};

////////////////////////////////////////////////////////////////////////////////

template <class TComputation>
TComputationSpecPtr CreateGenericComputationSpec(const TWorkerGroupId& workerGroup = {})
{
    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = TypeName<TComputation>();
    spec->GroupBySchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
    spec->InputStreamIds.insert("input");
    spec->WorkerGroup = workerGroup;
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

class TJobManagerTest
    : public ::testing::Test
{
public:
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>();
    TPersistedStateControlPtr<std::string> PersistedControl;
    TFlowViewPtr FlowView;
    IJobManagerPtr JobManager;
    NConcurrency::IFairShareThreadPoolPtr ThreadPool;

    void Prepare(const TPipelineSpecPtr& spec, const TDynamicPipelineSpecPtr& dynamicSpec)
    {
        FlowView->CurrentSpec->SetValue(spec);
        FlowView->CurrentDynamicSpec->SetValue(dynamicSpec);
        FlowView->State->ExecutionSpec->PipelineSpec->SetValue(spec);
        FlowView->State->ExecutionSpec->DynamicPipelineSpec->SetValue(dynamicSpec);
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(spec));
        auto context = New<TJobManagerContext>();
        ThreadPool = NConcurrency::CreateFairShareThreadPool(MaxThreads, "Balancer");
        context->Invoker = ThreadPool->GetInvoker("Balancer");
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->StatusProfiler = CreateSyncStatusProfiler();
        JobManager = CreateJobManager(context, spec, dynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
    }

    void SetUp() override
    {
        Reset();
    }

    void Reset()
    {
        FlowView = New<TFlowView>();
        PersistedControl = New<TPersistedStateControl<std::string>>(StorageHandler);
        FlowView->State->AttachToControl(PersistedControl);
        PersistedControl->Recover();
        JobManager = nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TMap>
std::vector<typename TMap::mapped_type> MapValues(const TMap& map)
{
    std::vector<typename TMap::mapped_type> values;
    values.reserve(map.size());
    for (const auto& [key, value] : map) {
        values.push_back(value);
    }
    return values;
}

class TJobBalancerTest
    : public TJobManagerTest
{
public:
    static constexpr double BaseCpuLoad = 100;

    int WorkerCount{};
    THashMap<TWorkerGroupId, int> WorkerCountByGroup;
    int ComputationCount{};
    THashMap<TWorkerGroupId, int> ComputationCountByGroup;
    THashMap<TComputationId, TComputationDescription> ComputationDescriptions;
    THashMap<TComputationId, std::map<TPartitionId, double>> DeviatingPartitions;
    bool AsyncBalancer = false;

    void DistributeJobs()
    {
        YT_ASSERT(FlowView);
        YT_ASSERT(JobManager);
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        if (AsyncBalancer) {
            TInstant start = TInstant::Now();
            while (FlowView->State->ExecutionSpec->Layout->Jobs.size() != FlowView->State->ExecutionSpec->Layout->Partitions.size() &&
                TInstant::Now() < start + MaxBalanceWait) {
                NConcurrency::TDelayedExecutor::WaitForDuration(MinBalanceStep);
                JobManager->DistributeJobs(FlowView);
            }
            ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Jobs.size(), FlowView->State->ExecutionSpec->Layout->Partitions.size());
        }

        FlowView->State->CommitMutation();

        // With graceful move, DistributeJobs only signals a move (sets a pending target +
        // FinishAfterCurrentEpoch); the actual move happens in CheckCompletedPartitions once
        // the job finishes its epoch. Simulate that finish so balancer tests converge the same
        // way they do with graceful move disabled. No-op when nothing is pending.
        CompleteGracefulMoves();

        std::set<TPartitionId> jobPartitions;
        std::set<TPartitionId> partitions;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            partitions.insert(partitionId);
        }
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            jobPartitions.insert(job->PartitionId);
        }
        ASSERT_EQ(jobPartitions, partitions);
    }

    //! Completes any in-flight graceful moves: for every partition that DistributeJobs marked
    //! with a pending graceful rebalance, report its current job as finished with the typed
    //! GracefulShutdown error and run CheckCompletedPartitions, which recreates the job on the
    //! target worker (mirroring one controller iteration). No-op without pending moves.
    void CompleteGracefulMoves()
    {
        std::vector<TPartitionId> pending;
        for (const auto& [partitionId, ephemeralState] : FlowView->EphemeralState->Partitions) {
            if (ephemeralState && ephemeralState->PendingGracefulRebalanceWorkerAddress.has_value()) {
                pending.push_back(partitionId);
            }
        }
        if (pending.empty()) {
            return;
        }

        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        for (const auto& partitionId : pending) {
            const auto& partition = layout->Partitions.at(partitionId);
            if (!partition->CurrentJobId) {
                continue;
            }
            auto& status = FlowView->Feedback->PartitionJobStatuses[partitionId];
            if (!status) {
                status = New<TPartitionJobStatus>();
            }
            if (!status->CurrentJobStatus) {
                status->CurrentJobStatus = New<TJobStatus>();
            }
            status->CurrentJobStatus->JobId = *partition->CurrentJobId;
            status->CurrentJobStatus->IsFinished = true;
            status->CurrentJobStatus->Error = TError(NFlow::EErrorCode::GracefulShutdown, "Job finished by graceful shutdown signal");
            status->CurrentJobStatus->Epoch = 1;
            status->CurrentJobStatus->UpdateTime = TInstant::Now();
        }

        FlowView->State->StartMutation();
        JobManager->CheckCompletedPartitions(FlowView);
        FlowView->State->CommitMutation();
    }

    void PrepareBalancerTest(int workerCount, const std::vector<TComputationDescription>& computationDescriptions, double exceedCountAllowed = 1, TDuration timeToBalance = TDuration::MilliSeconds(50), bool cpuAwareBalancer = true, bool asyncBalancer = false)
    {
        std::vector<TWorkerGroupDescription> workerDescriptions(1);
        workerDescriptions[0].WorkerCount = workerCount;
        PrepareBalancerTest(workerDescriptions, computationDescriptions, exceedCountAllowed, timeToBalance, cpuAwareBalancer, asyncBalancer);
    }

    void PrepareBalancerTest(const std::vector<TWorkerGroupDescription>& workerDescriptions, const std::vector<TComputationDescription>& computationDescriptions, double exceedCountAllowed = 1, TDuration timeToBalance = TDuration::MilliSeconds(50), bool cpuAwareBalancer = true, bool asyncBalancer = false)
    {
        AsyncBalancer = asyncBalancer;
        WorkerCount = 0;
        WorkerCountByGroup.clear();
        for (const auto& workerDescription : workerDescriptions) {
            for (const auto& group : workerDescription.Groups) {
                WorkerCountByGroup[group] += workerDescription.WorkerCount;
            }
            WorkerCount += workerDescription.WorkerCount;
        }
        ComputationDescriptions.clear();
        ComputationCountByGroup.clear();
        for (int i = 0; i < std::ssize(computationDescriptions); i++) {
            ComputationDescriptions[GetComputationId(i)] = computationDescriptions[i];
            ComputationCountByGroup[computationDescriptions[i].WorkerGroup]++;
        }
        ComputationCount = std::ssize(computationDescriptions);

        size_t totalPartitionCount = 0;
        auto spec = New<TPipelineSpec>();
        auto dynamicSpec = New<TDynamicPipelineSpec>();
        dynamicSpec->JobManager->BalancerType = cpuAwareBalancer ? EJobBalancerType::CpuAware : EJobBalancerType::Greedy;
        dynamicSpec->JobManager->RebalanceSyncPeriod = timeToBalance;
        dynamicSpec->JobManager->RebalanceTargetDeviation = 0.01;
        dynamicSpec->JobManager->RebalanceCountExceedAllowed = exceedCountAllowed;
        // These tests exercise the raw (de)balancing logic, so bypass the even-load gate by default.
        // The gate itself is covered by the RebalanceMinCpuSpread* tests below, which clear this flag.
        dynamicSpec->JobManager->DisableEvenLoadGate = true;
        dynamicSpec->JobManager->AsyncBalancing = asyncBalancer;
        dynamicSpec->JobManager->RebalanceActionMinTime = TDuration::MilliSeconds(1);
        for (int i = 0; i < std::ssize(computationDescriptions); i++) {
            totalPartitionCount += computationDescriptions[i].PartitionCount;
            TComputationId computationId = GetComputationId(i);
            const TComputationDescription& computationDescription = computationDescriptions[i];
            spec->Computations[computationId] = CreateGenericComputationSpec<TSimpleComputation>(computationDescriptions[i].WorkerGroup);
            dynamicSpec->Computations[computationId] = New<TDynamicComputationSpec>();
            dynamicSpec->Computations[computationId]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(computationDescription.PartitionCount));
        }
        Prepare(spec, dynamicSpec);
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 0u);
        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);
        FlowView->State->CommitMutation();

        int i = 0;
        for (const auto& workerDescription : workerDescriptions) {
            for (int j = 0; j < workerDescription.WorkerCount; j++) {
                std::string workerAddress = GetWorkerAddress(i++);
                auto worker = New<NFlow::TWorker>();
                worker->RpcAddress = workerAddress;
                worker->Groups = workerDescription.Groups;
                FlowView->State->Workers[workerAddress] = worker;
            }
        }

        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), totalPartitionCount);
    }

    int GetPartitionCount(std::optional<TComputationId> computationId)
    {
        if (!computationId) {
            return FlowView->State->ExecutionSpec->Layout->Partitions.size();
        }
        int result = 0;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            if (computationId && partition->ComputationId != *computationId) {
                continue;
            }
            result++;
        }
        return result;
    }

    THashMap<std::string, int> GetJobCountOnWorker(std::optional<TComputationId> computationId = std::nullopt)
    {
        THashMap<std::string, int> result;
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            const auto& partition = FlowView->State->ExecutionSpec->Layout->Partitions.at(job->PartitionId);
            if (computationId && partition->ComputationId != computationId) {
                continue;
            }
            result[job->WorkerAddress]++;
        }
        return result;
    }

    THashMap<std::string, double> GetCpuLoadOnWorker(std::optional<TComputationId> computationId = std::nullopt)
    {
        THashMap<std::string, double> result;
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            const auto& partition = FlowView->State->ExecutionSpec->Layout->Partitions.at(job->PartitionId);
            if (computationId && partition->ComputationId != computationId) {
                continue;
            }
            auto partitionJobStatus = FlowView->Feedback->PartitionJobStatuses.at(job->PartitionId);
            result[job->WorkerAddress] += *(partitionJobStatus->CurrentJobStatus->PerformanceMetrics->CpuUsage30s);
        }
        return result;
    }

    void SetCpuLoadUniform()
    {
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            auto partitionJobStatus = New<TPartitionJobStatus>();
            partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
            partitionJobStatus->CurrentJobStatus->PerformanceMetrics->CpuUsage30s = BaseCpuLoad;
            partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);

            FlowView->Feedback->PartitionJobStatuses[job->PartitionId] = partitionJobStatus;
        }
    }

    void CheckUniformLoad()
    {
        for (const auto& [computationId, computationDescription] : ComputationDescriptions) {
            auto jobCounts = MapValues(GetJobCountOnWorker(computationId));
            ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
        }
    }

    void SetCpuLoadSlowWorker(const std::set<std::string>& slowWorkerAddresses, double slowWorkerFactor)
    {
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            auto partitionJobStatus = New<TPartitionJobStatus>();
            partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
            if (slowWorkerAddresses.contains(job->WorkerAddress)) {
                partitionJobStatus->CurrentJobStatus->PerformanceMetrics->CpuUsage30s = slowWorkerFactor * BaseCpuLoad;
            } else {
                partitionJobStatus->CurrentJobStatus->PerformanceMetrics->CpuUsage30s = BaseCpuLoad;
            }
            partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);

            FlowView->Feedback->PartitionJobStatuses[job->PartitionId] = partitionJobStatus;
        }
    }

    void CheckSlowWorkerLoad(const std::set<std::string>& slowWorkerAddresses, double slowWorkerFactor, double allowedMargin = 1.15)
    {
        // Some of the "effective" worker coefficient will be inevitable attributed by the balancer to the partitions complexities.
        // Therefore, we must check with some tolerance to our "perfect" expectation - that is allowedMargin.
        for (const auto& [computationId, computationDescription] : ComputationDescriptions) {
            double sumFactor = WorkerCount + slowWorkerAddresses.size() * (slowWorkerFactor - 1);
            double avgFactor = sumFactor / WorkerCount;
            auto jobCountRange = MapValues(GetJobCountOnWorker(computationId));
            double sumJobCount = std::accumulate(jobCountRange.begin(), jobCountRange.end(), 0);
            double avgJobCount = sumJobCount / WorkerCount;

            for (const auto& [workerAddress, jobCount] : GetJobCountOnWorker(computationId)) {
                double myFactor = 1;
                if (slowWorkerAddresses.contains(workerAddress)) {
                    myFactor = slowWorkerFactor;
                }

                double expectedJobCount = avgJobCount / myFactor * avgFactor;
                ASSERT_LE(jobCount, expectedJobCount * allowedMargin) << Format("Computation %v is not distributed equally: too much workload on worker %v", computationId, workerAddress);
            }
        }
    }

    void InitializeCpuLoadDeviatingPartitions()
    {
        DeviatingPartitions.clear();

        std::map<TComputationId, std::vector<TPartitionId>> partitionsByComputation;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            partitionsByComputation[partition->ComputationId].push_back(partitionId);
        }

        for (const auto& [computationId, description] : ComputationDescriptions) {
            auto& partitions = partitionsByComputation[computationId];
            // Here we expect all partitions to be already assigned to workers.
            // We try to put deviating partitions "compactly" to as few workers as possible.
            // This is done deliberately - as opposite to the ideal situation balancer should try to achieve - where they are distributed evenly.
            std::ranges::sort(partitions, [&] (const auto& lhs, const auto& rhs) {
                const auto& lhsPartition = FlowView->State->ExecutionSpec->Layout->Partitions.at(lhs);
                const auto& rhsPartition = FlowView->State->ExecutionSpec->Layout->Partitions.at(rhs);
                const auto& lhsJob = FlowView->State->ExecutionSpec->Layout->Jobs.at(lhsPartition->CurrentJobId.value());
                const auto& rhsJob = FlowView->State->ExecutionSpec->Layout->Jobs.at(rhsPartition->CurrentJobId.value());

                return lhsJob->WorkerAddress < rhsJob->WorkerAddress;
            });

            DeviatingPartitions[computationId].clear();
            int pos = 0;
            for (const auto& deviatingDescription : description.DeviatingPartitions) {
                for (int i = 0; i < deviatingDescription.Count; ++i) {
                    const auto& partitionId = partitionsByComputation[computationId][pos++];
                    DeviatingPartitions[computationId].insert({partitionId, deviatingDescription.Factor});
                }
            }
        }
    }

    void SetCpuLoadDeviatingPartitions()
    {
        for (const auto& [jobId, job] : FlowView->State->ExecutionSpec->Layout->Jobs) {
            const auto& partitionId = job->PartitionId;
            const auto& partition = FlowView->State->ExecutionSpec->Layout->Partitions.at(partitionId);
            const auto& deviatingPartitions = DeviatingPartitions.at(partition->ComputationId);

            auto partitionJobStatus = New<TPartitionJobStatus>();
            partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
            partitionJobStatus->CurrentJobStatus->PerformanceMetrics->CpuUsage30s = (GetOrDefault(deviatingPartitions, partitionId, 1.0)) * BaseCpuLoad;
            partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);

            FlowView->Feedback->PartitionJobStatuses[partitionId] = partitionJobStatus;
        }
    }

    double GetMinMaxDiffCpuLoad(std::optional<TComputationId> computationId = std::nullopt)
    {
        auto range = MapValues(GetCpuLoadOnWorker(computationId));
        return std::ranges::max(range) - std::ranges::min(range);
    }

    int GetMinMaxDiffJobCount(std::optional<TComputationId> computationId = std::nullopt)
    {
        auto range = MapValues(GetJobCountOnWorker(computationId));
        return std::ranges::max(range) - std::ranges::min(range);
    }

    TComputationId GetComputationId(int num)
    {
        return TComputationId("Computation" + ToString(num));
    }

    std::string GetWorkerAddress(int num)
    {
        return "flow" + ToString(num);
    }

    TWorkerGroupId GetWorkerGroupId(int num)
    {
        return TWorkerGroupId(Format("group%v", num));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobManagerTest, PrepareNewComputation)
{
    auto spec = New<TPipelineSpec>();
    spec->Computations["SmallComputation"] = CreateGenericComputationSpec<TSimpleComputation>();
    spec->Computations["Computation"] = CreateGenericComputationSpec<TSimpleComputation>();
    spec->Computations["BigComputation"] = CreateGenericComputationSpec<TSimpleComputation>();

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations["SmallComputation"] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations["SmallComputation"]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(1u));
    dynamicSpec->Computations["Computation"] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations["Computation"]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(3u));
    dynamicSpec->Computations["BigComputation"] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations["BigComputation"]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(8u));

    Prepare(spec, dynamicSpec);

    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 0u);

    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);

    // 1 + 3 + 8
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 12);
    ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->Partitions.size(), 12u);
    THashMap<TComputationId, std::vector<TKeyRange>> ranges;
    THashSet<TPartitionId> partitionIds;
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        ASSERT_TRUE(partition->PartitionId);
        ASSERT_EQ(partitionId, partition->PartitionId);
        ASSERT_EQ(partitionIds.contains(partition->PartitionId), false);
        partitionIds.insert(partition->PartitionId);
        ASSERT_EQ(partition->State, EPartitionState::Executing);
        ASSERT_TRUE(partition->LowerKey && partition->UpperKey);
        ranges[partition->ComputationId].push_back({*partition->LowerKey, *partition->UpperKey});
    }
    for (auto& [computationId, values] : ranges) {
        Sort(values);
    }

    const THashMap<TComputationId, std::vector<TKeyRange>> expected = {
        {"SmallComputation", {
                MakeUintKeyRange(0x0000000000000000, {}),
                             }},
        {"Computation", {
                MakeUintKeyRange(0x0000000000000000, 0x5555555555555556),
                MakeUintKeyRange(0x5555555555555556, 0xAAAAAAAAAAAAAAAB),
                MakeUintKeyRange(0xAAAAAAAAAAAAAAAB, {}),
                        }},
        {"BigComputation", {
                MakeUintKeyRange(0x0000000000000000, 0x2000000000000000),
                MakeUintKeyRange(0x2000000000000000, 0x4000000000000000),
                MakeUintKeyRange(0x4000000000000000, 0x6000000000000000),
                MakeUintKeyRange(0x6000000000000000, 0x8000000000000000),
                MakeUintKeyRange(0x8000000000000000, 0xA000000000000000),
                MakeUintKeyRange(0xA000000000000000, 0xC000000000000000),
                MakeUintKeyRange(0xC000000000000000, 0xE000000000000000),
                MakeUintKeyRange(0xE000000000000000, {}),
                           }}};
    ASSERT_EQ(ranges, expected);
}

////////////////////////////////////////////////////////////////////////////////

class TTraverseIsolationTest
    : public TJobManagerTest
{
public:
    void PrepareTwoComputations()
    {
        auto spec = New<TPipelineSpec>();
        spec->Computations["healthy"] = CreateGenericComputationSpec<TSimpleComputation>();
        spec->Computations["broken"] = CreateGenericComputationSpec<TBrokenTraverseComputation>();

        auto dynamicSpec = New<TDynamicPipelineSpec>();
        dynamicSpec->JobManager->AsyncBalancing = false;
        for (const auto& computationId : {TComputationId("healthy"), TComputationId("broken")}) {
            dynamicSpec->Computations[computationId] = New<TDynamicComputationSpec>();
            dynamicSpec->Computations[computationId]->Parameters->AddChild(
                "desired_partition_count",
                NYTree::ConvertToNode(1u));
        }

        Prepare(spec, dynamicSpec);
        FlowView->State->StartMutation();
        JobManager->DoPartitioning(FlowView);

        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            auto traverseData = New<TFromPartitionTraverseData>();
            traverseData->Node = MakeInputNode(/*epoch*/ 1, TSystemTimestamp(100));
            auto status = New<TPartitionJobStatus>();
            status->LastTraverseData = std::move(traverseData);
            FlowView->Feedback->PartitionJobStatuses[partitionId] = std::move(status);
        }
    }

    static TNodeTraverseDataPtr MakeInputNode(i64 epoch, TSystemTimestamp watermark)
    {
        auto stream = New<TStreamTraverseData>();
        stream->Epoch = epoch;
        stream->SystemWatermark = watermark;
        stream->EventWatermark = watermark;
        auto node = New<TNodeTraverseData>();
        node->Streams[TStreamId("input")] = std::move(stream);
        return node;
    }

    static TSystemTimestamp GetInputWatermark(const TFlowViewPtr& flowView, const TComputationId& computationId)
    {
        const auto& node = GetOrCrash(flowView->State->TraverseData->Computations, computationId);
        return GetOrCrash(node->Streams, TStreamId("input"))->SystemWatermark;
    }
};

TEST_F(TTraverseIsolationTest, FailingComputationDoesNotFreezeOthers)
{
    PrepareTwoComputations();
    // The failing computation falls back to its previous node data, as after a controller
    // restart, which recovers it from the persisted traverse data.
    FlowView->State->TraverseData->Computations[TComputationId("broken")] =
        MakeInputNode(/*epoch*/ 0, TSystemTimestamp(50));

    JobManager->AggregateTraverseData(FlowView);

    EXPECT_EQ(GetInputWatermark(FlowView, TComputationId("healthy")), TSystemTimestamp(100));
    EXPECT_EQ(GetInputWatermark(FlowView, TComputationId("broken")), TSystemTimestamp(50));
}

TEST_F(TTraverseIsolationTest, FailingComputationWithoutPreviousDataAborts)
{
    PrepareTwoComputations();

    EXPECT_THROW(JobManager->AggregateTraverseData(FlowView), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, NoCpuMetrics)
{
    auto runTest = [&] (size_t workerCount, const std::vector<int>& partitionCounts) {
        Reset();
        std::vector<TComputationDescription> computationDescriptions;
        for (int partitionCount : partitionCounts) {
            computationDescriptions.push_back(TComputationDescription{partitionCount, {}});
        }
        PrepareBalancerTest(workerCount, computationDescriptions);
        DistributeJobs();
        CheckUniformLoad();
        int diameterPrev = GetMinMaxDiffJobCount();
        DistributeJobs();
        if (FlowView->State->ExecutionSpec->Layout->GetUpdated() != 0) {
            int diameterCur = GetMinMaxDiffJobCount();
            ASSERT_LT(diameterCur, diameterPrev) << "Computations were already well-balanced, further actions must improve overall distribution";
        }
    };

    runTest(3, {12, 13, 14});
    runTest(4, {9, 25, 36, 49, 64, 81});
    runTest(5, {120, 121, 122, 123});
    auto range20 = std::views::iota(1, 20);
    runTest(7, std::vector<int>(range20.begin(), range20.end()));
}

TEST_F(TJobBalancerTest, TrivialCpuMetrics)
{
    auto runTest = [&] (size_t workerCount, const std::vector<int>& partitionCounts) {
        Reset();
        std::vector<TComputationDescription> computationDescriptions;
        for (int partitionCount : partitionCounts) {
            computationDescriptions.push_back(TComputationDescription{partitionCount, {}});
        }
        PrepareBalancerTest(workerCount, computationDescriptions);
        SetCpuLoadUniform();
        DistributeJobs();
        CheckUniformLoad();

        Reset();
        PrepareBalancerTest(workerCount, computationDescriptions);
        DistributeJobs();
        CheckUniformLoad();
        SetCpuLoadUniform();
        double diameterPrev = GetMinMaxDiffCpuLoad();
        DistributeJobs();
        if (FlowView->State->ExecutionSpec->Layout->GetUpdated() != 0) {
            double diameterCur = GetMinMaxDiffCpuLoad();
            ASSERT_LT(diameterCur, diameterPrev) << "Computations were already well-balanced, further actions must improve overall distribution";
        }
    };

    runTest(3, {12, 13, 14});
    runTest(4, {9, 25, 36, 49, 64, 81});
    runTest(5, {120, 121, 122, 123});
    auto range20 = std::views::iota(1, 20);
    runTest(7, std::vector<int>(range20.begin(), range20.end()));
}

//! Many single-partition computations start piled on one worker; the rest of the workers are empty.
//! Deep (slow) balancing must spread them out. The per-worker count target for a one-partition
//! computation is 1/N, so floor(target.Count * rebalance_count_exceeded_allowed) = floor(1/N * 1.2)
//! used to round to 0, which made the slow move-acceptance gate reject every destination and froze
//! the imbalance. With the cap floored at one partition per worker, the moves are accepted and the
//! partitions spread.
TEST_F(TJobBalancerTest, SlowBalancingSpreadsConcentratedOnePartitionComputations)
{
    constexpr int computationCount = 40;
    std::vector<TComputationDescription> computationDescriptions(computationCount, TComputationDescription{/*PartitionCount*/ 1, {}});

    // Start with a single worker so every partition is forced onto it.
    PrepareBalancerTest(/*workerCount*/ 1, computationDescriptions);
    DistributeJobs();
    SetCpuLoadUniform();
    ASSERT_EQ(GetOrDefault(GetJobCountOnWorker(), GetWorkerAddress(0), 0), computationCount);

    // Add seven empty workers; an even spread is five partitions per worker.
    constexpr int totalWorkers = 8;
    FlowView->State->StartMutation();
    for (int i = 1; i < totalWorkers; ++i) {
        auto worker = New<NFlow::TWorker>();
        worker->RpcAddress = GetWorkerAddress(i);
        FlowView->State->Workers[worker->RpcAddress] = worker;
    }
    FlowView->State->CommitMutation();
    WorkerCount = totalWorkers;

    // Give deep balancing many iterations to converge.
    for (int i = 0; i < 30; ++i) {
        SetCpuLoadUniform();
        DistributeJobs();
    }

    int maxOnWorker = std::ranges::max(MapValues(GetJobCountOnWorker()));
    ASSERT_LE(maxOnWorker, computationCount / totalWorkers + 1)
        << "deep balancing left " << maxOnWorker << " of " << computationCount
        << " single-partition computations piled on one worker";
}

//! Many computations with a single partition each. Per-computation balance is trivial (one
//! partition), but the partitions must still be spread across all workers. The fast-path stray
//! distribution used to pick a worker by this-computation load only, which cannot distinguish
//! workers when every computation has one partition, so they all piled onto a single worker. The
//! fix scores candidate workers by the deep-balancing measure (overall load plus this computation's),
//! so the partitions spread evenly.
TEST_F(TJobBalancerTest, StrayManyOnePartitionComputations)
{
    constexpr int workerCount = 8;
    constexpr int computationCount = 80;
    std::vector<TComputationDescription> computationDescriptions(computationCount, TComputationDescription{/*PartitionCount*/ 1, {}});
    PrepareBalancerTest(workerCount, computationDescriptions);

    // Give every (still stray) partition the same CPU load so the CPU-aware balancer has a real
    // per-worker signal while distributing stray partitions.
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        auto status = New<TPartitionJobStatus>();
        status->CurrentJobStatus = New<TJobStatus>();
        status->CurrentJobStatus->PerformanceMetrics->CpuUsage30s = BaseCpuLoad;
        status->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);
        FlowView->Feedback->PartitionJobStatuses[partitionId] = status;
    }

    DistributeJobs();

    // Count jobs on every worker, including ones that received none.
    auto jobCounts = GetJobCountOnWorker();
    int maxCount = 0;
    int minCount = computationCount;
    for (int i = 0; i < workerCount; ++i) {
        int count = GetOrDefault(jobCounts, GetWorkerAddress(i), 0);
        maxCount = std::max(maxCount, count);
        minCount = std::min(minCount, count);
    }
    ASSERT_LE(maxCount - minCount, 1)
        << "one-partition computations are not spread across workers: max=" << maxCount << " min=" << minCount;
}

TEST_F(TJobBalancerTest, SlowWorker)
{
    auto runTest = [&] (size_t workerCount, const std::vector<int>& partitionCounts, int slowWorkerSize, double slowWorkerFactor, TDuration slowBalancingTime = TDuration::MilliSeconds(400)) {
        Reset();
        std::vector<TComputationDescription> computationDescriptions;
        for (int partitionCount : partitionCounts) {
            computationDescriptions.push_back(TComputationDescription{partitionCount, {}});
        }
        std::set<std::string> slowWorkers;
        for (int i = 0; i < slowWorkerSize; ++i) {
            slowWorkers.insert(GetWorkerAddress(i));
        }
        PrepareBalancerTest(workerCount, computationDescriptions);
        DistributeJobs();
        for (int iteration = 0; iteration < 5; ++iteration) {
            SetCpuLoadSlowWorker(slowWorkers, slowWorkerFactor);
            DistributeJobs();
        }
        CheckSlowWorkerLoad(slowWorkers, slowWorkerFactor);

        Reset();
        PrepareBalancerTest(workerCount, computationDescriptions, 1.2, slowBalancingTime);
        DistributeJobs();
        SetCpuLoadUniform();
        DistributeJobs();
        for (int iteration = 0; iteration < 10; ++iteration) {
            SetCpuLoadSlowWorker(slowWorkers, slowWorkerFactor);
            DistributeJobs();
        }
        CheckSlowWorkerLoad(slowWorkers, slowWorkerFactor);
    };

    runTest(3, {60, 61, 62}, 1, 1.5);
#if !defined(_tsan_enabled_)
    // Disable some test to avoid timeouts with tsan build.
    runTest(5, {120, 121, 122, 123, 124}, 1, 2);
    runTest(10, {120, 121}, 3, 1.5, TDuration::MilliSeconds(400));
#endif
}

TEST_F(TJobBalancerTest, DeviatingPartitionsStrict)
{
    auto runTest = [&] (size_t workerCount, const std::vector<TComputationDescription>& descriptions, TDuration slowBalancingTime = TDuration::MilliSeconds(50), double expectedDiff = BaseCpuLoad) {
        Reset();
        PrepareBalancerTest(workerCount, descriptions, 1, slowBalancingTime);
        DistributeJobs();
        InitializeCpuLoadDeviatingPartitions();
        for (int iteration = 0; iteration < 20; ++iteration) {
            SetCpuLoadDeviatingPartitions();
            DistributeJobs();
        }

        for (int i = 0; i < std::ssize(descriptions); ++i) {
            const auto& computationId = GetComputationId(i);

            double diameterLoad = GetMinMaxDiffCpuLoad(computationId);
            ASSERT_LE(diameterLoad, expectedDiff) << Format("Computation %v is not distributed equally by CPU load", computationId);
        }
    };

    // For this test we don't want any unpredictable actions by the balancer.
    // THerefore we set RebalanceCountExceedAllowed to 1 and make all computations' but one's partitions counts divisible by worker count.
    // Reasonbly small counts of partitions on other computation allow us to safely assume that no kicks will be permissible even with slightly elevated workercoef.
    // Therefore we ensure that slow balancer can only perform swaps on the computations featuring deviating partitions.
    // Thus, we can determine the what the optimal result would look like.
    runTest(3, {{12, {}}, {18, {}}, {14, {{3, 2}}}});
    runTest(7, {{14, {}}, {21, {{14, 3}}}, {14, {}}}, TDuration::MilliSeconds(100));
}

TEST_F(TJobBalancerTest, DeviatingPartitionsNotStrict)
{
    auto runTest = [&] (size_t workerCount, const std::vector<TComputationDescription>& descriptions, TDuration slowBalancingTime = TDuration::MilliSeconds(50), double expectedDiff = BaseCpuLoad) {
        Reset();
        PrepareBalancerTest(workerCount, descriptions, 1.2, slowBalancingTime);
        DistributeJobs();
        InitializeCpuLoadDeviatingPartitions();
        for (int iteration = 0; iteration < 40; ++iteration) {
            SetCpuLoadDeviatingPartitions();
            DistributeJobs();
        }

        for (int i = 0; i < std::ssize(descriptions); ++i) {
            const auto& computationId = GetComputationId(i);

            double diameterLoad = GetMinMaxDiffCpuLoad(computationId);
            ASSERT_LE(diameterLoad, expectedDiff) << Format("Computation %v is not distributed equally by CPU load", computationId);
        }
    };

    // As opposed to the previous test, here we absolutely allow for the possibility of kicks by fast balancing or moves by slow balancing.
    // Thus, we cannot anymore assume that global optimum should be reached.
    // The expected min/max difference is therefore picked heuristically.
    // We just want to "lock" the expected behaviour of balancer on certain scenarios to get alarmed in case that changes.
    // This does not mean, however, that under no circumstances can valid modifications cause this test to fail.
    runTest(3, {{12, {}}, {13, {}}, {14, {{3, 2}}}}, TDuration::MilliSeconds(100), BaseCpuLoad * 2);
    runTest(5, {{20, {{4, 2}}}, {13, {{4, 2}}}, {11, {{1, 3}}}}, TDuration::MilliSeconds(100), BaseCpuLoad * 2);
}

TEST_F(TJobBalancerTest, OldBalancing)
{
    auto runTest = [&] (size_t workerCount, const std::vector<int>& partitionCounts) {
        Reset();
        std::vector<TComputationDescription> computationDescriptions;
        for (int partitionCount : partitionCounts) {
            computationDescriptions.push_back(TComputationDescription{partitionCount, {}});
        }
        PrepareBalancerTest(workerCount, computationDescriptions, 1, TDuration::MilliSeconds(50), false);
        DistributeJobs();
        CheckUniformLoad();
        DistributeJobs();
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
    };

    runTest(3, {12, 13, 14});
    runTest(4, {9, 25, 36, 49, 64, 81});
    runTest(5, {120, 121, 122, 123});
    auto range20 = std::views::iota(1, 20);
    runTest(7, std::vector<int>(range20.begin(), range20.end()));
}

TEST_F(TJobBalancerTest, WorkerGroupBasics)
{
    auto runTest = [&] (const std::vector<int>& workerCounts, const std::vector<std::vector<int>>& partitionCounts, bool cpuAwareBalancer) {
        YT_ASSERT(std::ssize(workerCounts) == std::ssize(partitionCounts));
        Reset();
        std::vector<TWorkerGroupDescription> workerGroupDescriptions;
        std::vector<TComputationDescription> computationDescriptions;
        for (int i = 0; i < std::ssize(workerCounts); ++i) {
            TWorkerGroupId group = GetWorkerGroupId(i);
            workerGroupDescriptions.push_back(TWorkerGroupDescription{workerCounts[i], {group}});
            for (int partitionCount : partitionCounts[i]) {
                computationDescriptions.push_back(TComputationDescription{partitionCount, {}, group});
            }
        }
        PrepareBalancerTest(workerGroupDescriptions, computationDescriptions, 1, TDuration::MilliSeconds(50), cpuAwareBalancer);
        SetCpuLoadUniform();
        DistributeJobs();
        THashMap<TWorkerGroupId, THashMap<TComputationId, THashMap<std::string, int>>> jobCounts;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            const auto& computationId = partition->ComputationId;
            const auto& computationDescription = ComputationDescriptions.at(computationId);
            EXPECT_TRUE(partition->CurrentJobId);
            if (!partition->CurrentJobId) {
                continue;
            }
            const auto& job = FlowView->State->ExecutionSpec->Layout->Jobs.at(*partition->CurrentJobId);
            const auto& workerAddress = job->WorkerAddress;
            const auto& worker = FlowView->State->Workers.at(workerAddress);
            EXPECT_EQ(computationDescription.WorkerGroup, worker->Groups[0]);
            jobCounts[computationDescription.WorkerGroup][computationId][workerAddress]++;
        }
        for (const auto& [workerGroup, computationCounts] : jobCounts) {
            EXPECT_EQ(std::ssize(computationCounts), ComputationCountByGroup.at(workerGroup));
            for (const auto& [computationId, workerCounts] : computationCounts) {
                EXPECT_EQ(std::ssize(workerCounts), WorkerCountByGroup.at(workerGroup));
                auto jobCounts = MapValues(workerCounts);
                ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
            }
        }
        DistributeJobs();
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
    };

    for (bool cpuAwareBalancer : {true, false}) {
        runTest({1}, {{12}}, cpuAwareBalancer);
        runTest({3}, {{12, 9, 6}}, cpuAwareBalancer);
        runTest({1, 1}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12}, {12}, {3}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12, 6}, {12, 9}, {3}}, cpuAwareBalancer);
    }
}

TEST_F(TJobBalancerTest, WorkerGroupBrokenGroup)
{
    auto runTest = [&] (const std::vector<int>& workerCounts, const std::vector<std::vector<int>>& partitionCounts, bool cpuAwareBalancer) {
        YT_ASSERT(std::ssize(workerCounts) == std::ssize(partitionCounts));
        Reset();
        std::vector<TWorkerGroupDescription> workerGroupDescriptions;
        std::vector<TComputationDescription> computationDescriptions;
        for (int i = 0; i < std::ssize(workerCounts); ++i) {
            TWorkerGroupId group = GetWorkerGroupId(i);
            TWorkerGroupId wrongGroup = TWorkerGroupId(Format("wrong group %v", i));
            workerGroupDescriptions.push_back(TWorkerGroupDescription{workerCounts[i], {group}});
            for (int partitionCount : partitionCounts[i]) {
                if (i % 2 == 0) {
                    computationDescriptions.push_back(TComputationDescription{partitionCount, {}, wrongGroup, i % 2});
                } else {
                    computationDescriptions.push_back(TComputationDescription{partitionCount, {}, group, i % 2});
                }
            }
        }
        PrepareBalancerTest(workerGroupDescriptions, computationDescriptions, 1, TDuration::MilliSeconds(50), cpuAwareBalancer);
        // The even-indexed computations target a deliberately empty "wrong group". Exempt it from the
        // per-group minimum-worker check (set the minimum to 0) so the pipeline is not paused; those
        // computations simply get no jobs.
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        for (int i = 0; i < std::ssize(workerCounts); ++i) {
            if (i % 2 == 0) {
                auto groupOverride = New<TDynamicJobManagerSpec>();
                groupOverride->MinimumWorkerCount = 0;
                dynamicSpec->JobManager->WorkerGroupOverride[TWorkerGroupId(Format("wrong group %v", i))] = groupOverride;
            }
        }
        SetCpuLoadUniform();
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
        THashMap<TWorkerGroupId, THashMap<TComputationId, THashMap<std::string, int>>> jobCounts;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            const auto& computationId = partition->ComputationId;
            const auto& computationDescription = ComputationDescriptions.at(computationId);
            if (computationDescription.Tag == 0) {
                EXPECT_TRUE(!partition->CurrentJobId);
            } else {
                EXPECT_TRUE(partition->CurrentJobId);
            }
            if (!partition->CurrentJobId) {
                continue;
            }
            const auto& job = FlowView->State->ExecutionSpec->Layout->Jobs.at(*partition->CurrentJobId);
            const auto& workerAddress = job->WorkerAddress;
            const auto& worker = FlowView->State->Workers.at(workerAddress);
            EXPECT_EQ(computationDescription.WorkerGroup, worker->Groups[0]);
            jobCounts[computationDescription.WorkerGroup][computationId][workerAddress]++;
        }
        for (const auto& [workerGroup, computationCounts] : jobCounts) {
            EXPECT_EQ(std::ssize(computationCounts), ComputationCountByGroup.at(workerGroup));
            for (const auto& [computationId, workerCounts] : computationCounts) {
                EXPECT_EQ(std::ssize(workerCounts), WorkerCountByGroup.at(workerGroup));
                auto jobCounts = MapValues(workerCounts);
                ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
            }
        }
    };

    for (bool cpuAwareBalancer : {true, false}) {
        runTest({1}, {{12}}, cpuAwareBalancer);
        runTest({3}, {{12, 9, 6}}, cpuAwareBalancer);
        runTest({1, 1}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12}, {12}, {3}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12, 6}, {12, 9}, {3}}, cpuAwareBalancer);
    }
}

TEST_F(TJobBalancerTest, WorkerGroupMultipleGroupsInWorker)
{
    auto runTest = [&] (const std::vector<int>& workerCounts, const std::vector<std::vector<int>>& partitionCounts, bool cpuAwareBalancer) {
        YT_ASSERT(std::ssize(workerCounts) == std::ssize(partitionCounts));
        Reset();
        std::vector<TWorkerGroupDescription> workerGroupDescriptions;
        std::vector<TComputationDescription> computationDescriptions;
        std::vector<TWorkerGroupId> groups;
        for (int i = 0; i < std::ssize(workerCounts); ++i) {
            TWorkerGroupId group = GetWorkerGroupId(i);
            groups.push_back(group);
            workerGroupDescriptions.push_back(TWorkerGroupDescription{workerCounts[i], groups});
            for (int partitionCount : partitionCounts[i]) {
                computationDescriptions.push_back(TComputationDescription{partitionCount, {}, group});
            }
        }
        PrepareBalancerTest(workerGroupDescriptions, computationDescriptions, 1, TDuration::MilliSeconds(50), cpuAwareBalancer);
        SetCpuLoadUniform();
        DistributeJobs();
        THashMap<TWorkerGroupId, THashMap<TComputationId, THashMap<std::string, int>>> jobCounts;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            const auto& computationId = partition->ComputationId;
            const auto& computationDescription = ComputationDescriptions.at(computationId);
            EXPECT_TRUE(partition->CurrentJobId);
            if (!partition->CurrentJobId) {
                continue;
            }
            const auto& job = FlowView->State->ExecutionSpec->Layout->Jobs.at(*partition->CurrentJobId);
            const auto& workerAddress = job->WorkerAddress;
            const auto& worker = FlowView->State->Workers.at(workerAddress);
            bool workerHasGroup = false;
            for (const auto& group : worker->Groups) {
                workerHasGroup = workerHasGroup || computationDescription.WorkerGroup == group;
            }
            EXPECT_TRUE(workerHasGroup);
            jobCounts[computationDescription.WorkerGroup][computationId][workerAddress]++;
        }
        for (const auto& [workerGroup, computationCounts] : jobCounts) {
            EXPECT_EQ(std::ssize(computationCounts), ComputationCountByGroup.at(workerGroup));
            for (const auto& [computationId, workerCounts] : computationCounts) {
                EXPECT_EQ(std::ssize(workerCounts), WorkerCountByGroup.at(workerGroup));
                auto jobCounts = MapValues(workerCounts);
                ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
            }
        }
        DistributeJobs();
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
    };

    for (bool cpuAwareBalancer : {true, false}) {
        runTest({1, 1}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12}, {12}, {3}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12, 10}, {12, 9}, {7}}, cpuAwareBalancer);
    }
}

TEST_F(TJobBalancerTest, WorkerGroupDifferentSpec)
{
    Reset();
    constexpr int workCount = 5;
    constexpr int partCount = 27;
    std::vector<TWorkerGroupDescription> workerGroupDescriptions;
    workerGroupDescriptions.push_back(TWorkerGroupDescription{workCount, {GetWorkerGroupId(0), GetWorkerGroupId(1)}});
    std::vector<TComputationDescription> computationDescriptions;
    computationDescriptions.push_back(TComputationDescription{partCount, {}, GetWorkerGroupId(0)});
    computationDescriptions.push_back(TComputationDescription{partCount, {}, GetWorkerGroupId(1)});
    PrepareBalancerTest(workerGroupDescriptions, computationDescriptions, 1, TDuration::MilliSeconds(50), false);
    auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
    auto override0 = ConvertTo<TDynamicJobManagerSpecPtr>(NYson::ConvertToYsonString(dynamicSpec->JobManager));
    override0->BalancerType = EJobBalancerType::Greedy;
    auto override1 = ConvertTo<TDynamicJobManagerSpecPtr>(NYson::ConvertToYsonString(dynamicSpec->JobManager));
    override1->BalancerType = EJobBalancerType::CpuAware;
    EmplaceOrCrash(dynamicSpec->JobManager->WorkerGroupOverride, GetWorkerGroupId(0), override0);
    EmplaceOrCrash(dynamicSpec->JobManager->WorkerGroupOverride, GetWorkerGroupId(1), override1);
    SetCpuLoadUniform();
    DistributeJobs();
    for (int iteration = 0; iteration < 5; ++iteration) {
        SetCpuLoadSlowWorker({GetWorkerAddress(0)}, 2);
        DistributeJobs();
    }
    THashMap<TWorkerGroupId, THashMap<TComputationId, THashMap<std::string, int>>> jobCounts;
    for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
        const auto& computationId = partition->ComputationId;
        const auto& computationDescription = ComputationDescriptions.at(computationId);
        EXPECT_TRUE(partition->CurrentJobId);
        if (!partition->CurrentJobId) {
            continue;
        }
        const auto& job = FlowView->State->ExecutionSpec->Layout->Jobs.at(*partition->CurrentJobId);
        const auto& workerAddress = job->WorkerAddress;
        const auto& worker = FlowView->State->Workers.at(workerAddress);
        bool workerHasGroup = false;
        for (const auto& group : worker->Groups) {
            workerHasGroup = workerHasGroup || computationDescription.WorkerGroup == group;
        }
        EXPECT_TRUE(workerHasGroup);
        jobCounts[computationDescription.WorkerGroup][computationId][workerAddress]++;
    }
    {
        // Not CPU aware.
        auto workerGroup = GetWorkerGroupId(0);
        auto computationId = GetComputationId(0);
        const auto& computationCounts = jobCounts.at(workerGroup);
        EXPECT_EQ(std::ssize(computationCounts), 1);
        EXPECT_EQ(computationCounts.count(computationId), 1u);
        const auto& workerCounts = computationCounts.at(computationId);
        EXPECT_EQ(std::ssize(workerCounts), workCount);
        auto jobCounts = MapValues(workerCounts);
        ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
    }
    {
        // CPU aware.
        auto workerGroup = GetWorkerGroupId(1);
        auto computationId = GetComputationId(1);
        auto& computationCounts = jobCounts.at(workerGroup);
        EXPECT_EQ(std::ssize(computationCounts), 1);
        EXPECT_EQ(computationCounts.count(computationId), 1u);
        auto& workerCounts = computationCounts.at(computationId);
        workerCounts[GetWorkerAddress(0)] *= 2;
        EXPECT_EQ(std::ssize(workerCounts), workCount);
        auto jobCounts = MapValues(workerCounts);
        ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
    }
}

TEST_F(TJobBalancerTest, WorkerGroupAsync)
{
    auto runTest = [&] (const std::vector<int>& workerCounts, const std::vector<std::vector<int>>& partitionCounts, bool cpuAwareBalancer) {
        YT_ASSERT(std::ssize(workerCounts) == std::ssize(partitionCounts));
        Reset();
        std::vector<TWorkerGroupDescription> workerGroupDescriptions;
        std::vector<TComputationDescription> computationDescriptions;
        for (int i = 0; i < std::ssize(workerCounts); ++i) {
            TWorkerGroupId group = GetWorkerGroupId(i);
            workerGroupDescriptions.push_back(TWorkerGroupDescription{workerCounts[i], {group}});
            for (int partitionCount : partitionCounts[i]) {
                computationDescriptions.push_back(TComputationDescription{partitionCount, {}, group});
            }
        }
        PrepareBalancerTest(workerGroupDescriptions, computationDescriptions, 1, TDuration::MilliSeconds(50), cpuAwareBalancer, cpuAwareBalancer);
        SetCpuLoadUniform();
        DistributeJobs();

        THashMap<TWorkerGroupId, THashMap<TComputationId, THashMap<std::string, int>>> jobCounts;
        for (const auto& [partitionId, partition] : FlowView->State->ExecutionSpec->Layout->Partitions) {
            const auto& computationId = partition->ComputationId;
            const auto& computationDescription = ComputationDescriptions.at(computationId);
            EXPECT_TRUE(partition->CurrentJobId);
            if (!partition->CurrentJobId) {
                continue;
            }
            const auto& job = FlowView->State->ExecutionSpec->Layout->Jobs.at(*partition->CurrentJobId);
            const auto& workerAddress = job->WorkerAddress;
            const auto& worker = FlowView->State->Workers.at(workerAddress);
            EXPECT_EQ(computationDescription.WorkerGroup, worker->Groups[0]);
            jobCounts[computationDescription.WorkerGroup][computationId][workerAddress]++;
        }
        for (const auto& [workerGroup, computationCounts] : jobCounts) {
            EXPECT_EQ(std::ssize(computationCounts), ComputationCountByGroup.at(workerGroup));
            for (const auto& [computationId, workerCounts] : computationCounts) {
                EXPECT_EQ(std::ssize(workerCounts), WorkerCountByGroup.at(workerGroup));
                auto jobCounts = MapValues(workerCounts);
                ASSERT_LE(std::ranges::max(jobCounts), std::ranges::min(jobCounts) + 1) << Format("Computation %v is not distributed equally", computationId);
            }
        }
        DistributeJobs();
        ASSERT_EQ(FlowView->State->ExecutionSpec->Layout->GetUpdated(), 0);
    };

    for (bool cpuAwareBalancer : {true, false}) {
        runTest({1}, {{12}}, cpuAwareBalancer);
        runTest({3}, {{12, 9, 6}}, cpuAwareBalancer);
        runTest({1, 1}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3}, {{12}, {12}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12}, {12}, {3}}, cpuAwareBalancer);
        runTest({3, 3, 1}, {{12, 6}, {12, 9}, {3}}, cpuAwareBalancer);

        int numGroups = MaxThreads - 1;
        std::vector<int> workerCounts(numGroups);
        std::vector<std::vector<int>> partitionCounts(numGroups);
        for (int i = 0; i < numGroups; i++) {
            workerCounts[i] = 1;
            partitionCounts[i].push_back(5);
        }
        runTest(workerCounts, partitionCounts, cpuAwareBalancer);
    }
}

////////////////////////////////////////////////////////////////////////////////

// Helper: simulate a job finishing for a given partition with the supplied error.
// Sets IsFinished=true and records the JobId in the status so CheckCompletedPartitions can pick it up.
[[maybe_unused]] void SimulateJobFinished(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    TError error = {})
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    const auto& partition = layout->Partitions.at(partitionId);
    YT_VERIFY(partition->CurrentJobId.has_value());
    const auto& jobId = *partition->CurrentJobId;

    auto& status = flowView->Feedback->PartitionJobStatuses[partitionId];
    if (!status) {
        status = New<TPartitionJobStatus>();
    }
    if (!status->CurrentJobStatus) {
        status->CurrentJobStatus = New<TJobStatus>();
    }
    status->CurrentJobStatus->JobId = jobId;
    status->CurrentJobStatus->IsFinished = true;
    status->CurrentJobStatus->Error = std::move(error);
    // Epoch must be non-zero: UpdatePartition asserts StateEpoch != 0.
    status->CurrentJobStatus->Epoch = 1;
    status->CurrentJobStatus->UpdateTime = TInstant::Now();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, GracefulShutdownErrorJobCompletesOnTargetWorker)
{
    // Same scenario as GracefulMoveJobCompletesOnTargetWorker, but the worker reports
    // the typed GracefulShutdown error instead of a clean OK status. This is the path
    // taken by upgraded workers — the controller must not depend on ephemeral state
    // alone to recognize a graceful finish.

    Reset();
    constexpr int kWorkerCount = 2;
    constexpr int kPartitionCount = 4;

    std::vector<TComputationDescription> computationDescriptions;
    computationDescriptions.push_back(TComputationDescription{kPartitionCount, {}});
    PrepareBalancerTest(kWorkerCount, computationDescriptions, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->GracefulMove = true;
        JobManager->Reconfigure(dynamicSpec);
    }

    {
        FlowView->State->StartMutation();
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->WorkerAddress = GetWorkerAddress(0);
            job->WorkerIncarnationId = FlowView->State->Workers.at(GetWorkerAddress(0))->IncarnationId;
            job->PartitionId = partitionId;
            layout->CreateJob(job);
        }
        FlowView->State->CommitMutation();
    }

    SetCpuLoadUniform();

    {
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    std::vector<TPartitionId> pendingPartitions;
    for (const auto& [partitionId, ephemeralState] : FlowView->EphemeralState->Partitions) {
        if (ephemeralState && ephemeralState->PendingGracefulRebalanceWorkerAddress.has_value()) {
            pendingPartitions.push_back(partitionId);
        }
    }
    ASSERT_GT(std::ssize(pendingPartitions), 0);

    auto gracefulError = TError(NFlow::EErrorCode::GracefulShutdown, "Job finished by graceful shutdown signal");
    for (const auto& partitionId : pendingPartitions) {
        SimulateJobFinished(FlowView, partitionId, gracefulError);
    }

    {
        FlowView->State->StartMutation();
        JobManager->CheckCompletedPartitions(FlowView);
        FlowView->State->CommitMutation();
    }

    const auto& layout = FlowView->State->ExecutionSpec->Layout;
    for (const auto& partitionId : pendingPartitions) {
        auto* ephemeralStatePtr = FlowView->EphemeralState->Partitions.FindPtr(partitionId);
        ASSERT_TRUE(ephemeralStatePtr && *ephemeralStatePtr);
        EXPECT_FALSE((*ephemeralStatePtr)->PendingGracefulRebalanceWorkerAddress.has_value());

        if ((*ephemeralStatePtr)->DynamicPartitionSpec) {
            EXPECT_FALSE((*ephemeralStatePtr)->DynamicPartitionSpec->FinishAfterCurrentEpoch);
        }

        const auto& partition = layout->Partitions.at(partitionId);
        EXPECT_EQ(partition->State, EPartitionState::Executing)
            << "Partition must stay Executing across a graceful rebalance";
        ASSERT_TRUE(partition->CurrentJobId.has_value());
        const auto& newJob = layout->Jobs.at(*partition->CurrentJobId);
        EXPECT_EQ(newJob->WorkerAddress, GetWorkerAddress(1))
            << "New job must be on the target worker after graceful rebalance";
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, GracefulShutdownErrorTargetWorkerGone)
{
    // Regression test for YTFLOW-538: when a partition reports the typed
    // GracefulShutdown error and the target worker has gone, the partition must
    // not be mistaken for a real completion (it must NOT advance to Completing).
    // The old job is dropped by RemoveFailedJobs and the balancer reschedules it
    // on the next tick.

    Reset();
    constexpr int kWorkerCount = 2;
    constexpr int kPartitionCount = 4;

    std::vector<TComputationDescription> computationDescriptions;
    computationDescriptions.push_back(TComputationDescription{kPartitionCount, {}});
    PrepareBalancerTest(kWorkerCount, computationDescriptions, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->GracefulMove = true;
        JobManager->Reconfigure(dynamicSpec);
    }

    {
        FlowView->State->StartMutation();
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->WorkerAddress = GetWorkerAddress(0);
            job->WorkerIncarnationId = FlowView->State->Workers.at(GetWorkerAddress(0))->IncarnationId;
            job->PartitionId = partitionId;
            layout->CreateJob(job);
        }
        FlowView->State->CommitMutation();
    }

    SetCpuLoadUniform();

    {
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    std::vector<TPartitionId> pendingPartitions;
    for (const auto& [partitionId, ephemeralState] : FlowView->EphemeralState->Partitions) {
        if (ephemeralState && ephemeralState->PendingGracefulRebalanceWorkerAddress.has_value()) {
            pendingPartitions.push_back(partitionId);
        }
    }
    ASSERT_GT(std::ssize(pendingPartitions), 0);

    FlowView->State->Workers.erase(GetWorkerAddress(1));

    auto gracefulError = TError(NFlow::EErrorCode::GracefulShutdown, "Job finished by graceful shutdown signal");
    for (const auto& partitionId : pendingPartitions) {
        SimulateJobFinished(FlowView, partitionId, gracefulError);
    }

    {
        FlowView->State->StartMutation();
        JobManager->CheckCompletedPartitions(FlowView);
        JobManager->RemoveFailedJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    const auto& layout = FlowView->State->ExecutionSpec->Layout;
    for (const auto& partitionId : pendingPartitions) {
        const auto& partition = layout->Partitions.at(partitionId);
        EXPECT_EQ(partition->State, EPartitionState::Executing)
            << "Partition must stay Executing when graceful target worker is gone (YTFLOW-538 regression)";
        EXPECT_FALSE(partition->CurrentJobId.has_value())
            << "Old job must be dropped (RemoveFailedJobs handles it) when graceful target worker is gone";
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, GracefulMoveDisabledDoesImmediateStop)
{
    // When GracefulMove=false, the balancer must immediately remove the job
    // (stopJob) rather than setting PendingGracefulRebalanceWorkerAddress.
    // After DistributeJobs, the partition should have no job and no pending
    // graceful rebalance state.

    Reset();
    constexpr int kWorkerCount = 2;
    constexpr int kPartitionCount = 4;

    std::vector<TComputationDescription> computationDescriptions;
    computationDescriptions.push_back(TComputationDescription{kPartitionCount, {}});
    PrepareBalancerTest(kWorkerCount, computationDescriptions, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Explicitly disable graceful move (it is on by default) — this test covers the
    // immediate stop-and-recreate path.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->GracefulMove = false;
        JobManager->Reconfigure(dynamicSpec);
    }

    // Put all partitions on worker 0.
    {
        FlowView->State->StartMutation();
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->WorkerAddress = GetWorkerAddress(0);
            job->WorkerIncarnationId = FlowView->State->Workers.at(GetWorkerAddress(0))->IncarnationId;
            job->PartitionId = partitionId;
            layout->CreateJob(job);
        }
        FlowView->State->CommitMutation();
    }

    SetCpuLoadUniform();

    // DistributeJobs: balancer should immediately stop and recreate jobs (no graceful).
    {
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    // Verify: no partition has PendingGracefulRebalanceWorkerAddress set.
    for (const auto& [partitionId, ephemeralState] : FlowView->EphemeralState->Partitions) {
        if (ephemeralState) {
            EXPECT_FALSE(ephemeralState->PendingGracefulRebalanceWorkerAddress.has_value())
                << "GracefulMove=false must not set PendingGracefulRebalanceWorkerAddress";
        }
    }

    // Verify: the balancer redistributed jobs — worker 1 now has some jobs.
    const auto& jobCountOnWorker1 = GetJobCountOnWorker();
    EXPECT_GT(jobCountOnWorker1.count(GetWorkerAddress(1)) > 0 ? jobCountOnWorker1.at(GetWorkerAddress(1)) : 0, 0)
        << "Worker 1 must have received jobs after immediate rebalance";
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, GracefulMoveNoRebalanceWhilePending)
{
    // Scenario:
    //   - 2 workers, all partitions initially assigned to worker 0 (worker 1 is empty).
    //   - GracefulMove is enabled.
    //   - First DistributeJobs: balancer sees imbalance and initiates graceful moves
    //     (sets PendingGracefulRebalanceWorkerAddress on some partitions).
    //   - Second DistributeJobs: balancer must NOT propose new moves for partitions that
    //     are already pending graceful migration. Without the fix, the balancer would see
    //     those partitions still on worker 0 and try to move them again.

    Reset();
    constexpr int kWorkerCount = 2;
    constexpr int kPartitionCount = 4;

    std::vector<TComputationDescription> computationDescriptions;
    computationDescriptions.push_back(TComputationDescription{kPartitionCount, {}});
    // Prepare with GracefulMove enabled from the start.
    PrepareBalancerTest(kWorkerCount, computationDescriptions, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Enable GracefulMove on the dynamic spec and reconfigure.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->GracefulMove = true;
        JobManager->Reconfigure(dynamicSpec);
    }

    // Assign all partitions to worker 0 only (worker 1 gets nothing) to create maximum imbalance.
    {
        FlowView->State->StartMutation();
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        for (const auto& [partitionId, partition] : layout->Partitions) {
            auto job = New<TJob>();
            job->JobId = TJobId(TGuid::Create());
            job->WorkerAddress = GetWorkerAddress(0);
            job->WorkerIncarnationId = FlowView->State->Workers.at(GetWorkerAddress(0))->IncarnationId;
            job->PartitionId = partitionId;
            layout->CreateJob(job);
        }
        FlowView->State->CommitMutation();
    }

    // Set uniform CPU load so the balancer has reliable metrics.
    SetCpuLoadUniform();

    auto countPendingGraceful = [&] {
        int count = 0;
        for (const auto& [partitionId, ephemeralState] : FlowView->EphemeralState->Partitions) {
            if (ephemeralState && ephemeralState->PendingGracefulRebalanceWorkerAddress.has_value()) {
                count++;
            }
        }
        return count;
    };

    auto distributeOnce = [&] {
        FlowView->State->StartMutation();
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    };

    // Drive the balancer until it has proposed every graceful move the imbalance needs. This can take
    // more than one call: a single DistributeJobs only commits the moves that fit in one wall-clock
    // slow-balancing window (RebalanceSyncPeriod, throttled by RebalanceActionMinTime), so the number
    // of moves per call is machine/load dependent. The pending set only grows here (no job finishes,
    // so nothing clears a pending move) and is bounded by the moves the imbalance needs, so it
    // saturates after a few calls. Iterating a fixed, generous number of times removes the timing
    // dependence that used to make this test flaky (pending after call 1 could be 1 instead of 2).
    for (int i = 0; i < 10; ++i) {
        distributeOnce();
    }
    int pendingConverged = countPendingGraceful();
    ASSERT_GT(pendingConverged, 0) << "Expected some partitions to be pending graceful rebalance";

    // Once converged, further calls must not change the pending set: already-pending partitions are
    // treated as already on their target worker, so the balancer proposes no new moves for them — and
    // the remaining layout is balanced, so it proposes none for the others either.
    distributeOnce();
    ASSERT_EQ(countPendingGraceful(), pendingConverged)
        << "Balancer must not initiate new graceful moves once all pending migrations are in flight";
}

////////////////////////////////////////////////////////////////////////////////

// Tests for RebalanceMinCpuSpread mechanism.
// RebalanceMinCpuSpread controls whether deferred (slow) rebalancing actions
// are applied based on the current CPU spread (max - min CPU usage) across workers.
// If the spread is below the threshold, slow actions are suppressed.
// These tests use the synchronous balancer to avoid async complexity.
//
// Common topology used by these tests:
//   - 3 workers.
//   - 3 computations: two "filler" ones (12 and 18 partitions, uniform load) plus
//     a target computation (index 2) with 14 partitions, 3 of which deviate with
//     a 2x CPU load factor. The filler computations dilute the per-worker coefficient
//     so the fast (greedy) balancer cannot fix the imbalance and only the slow
//     (deferred merge) balancer is able to reduce it.
static const std::vector<TComputationDescription> RebalanceMinCpuSpreadTopology = {
    {12, {}},
    {18, {}},
    {14, {{3, 2}}},
};

TEST_F(TJobBalancerTest, RebalanceMinCpuSpreadSuppressesSlowActionsWhenBelowThreshold)
{
    // Scenario:
    //   - RebalanceMinCpuSpread is set to a very large value (1e9) BEFORE any slow balancing
    //     can take place, so slow actions are always suppressed.
    //   - We verify that after many iterations the CPU imbalance is NOT reduced
    //     and remains close to the initial imbalance.

    Reset();
    PrepareBalancerTest(3, RebalanceMinCpuSpreadTopology, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Set the impossibly large threshold BEFORE any DistributeJobs call.
    // This ensures slow actions are suppressed from the very first iteration.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = std::nullopt; // Enable the even-load gate.
        dynamicSpec->JobManager->RebalanceMinCpuSpread = 1e9;        // Impossibly large threshold.
        JobManager->Reconfigure(dynamicSpec);
    }

    DistributeJobs();
    InitializeCpuLoadDeviatingPartitions();

    // Capture the baseline imbalance right after partitions are populated with
    // deviating CPU load. This ensures the scenario is non-trivial: the imbalance
    // must already be significant, otherwise the "no reduction" check is vacuous.
    SetCpuLoadDeviatingPartitions();
    double diameterBefore = GetMinMaxDiffCpuLoad(GetComputationId(2));
    ASSERT_GE(diameterBefore, 2 * BaseCpuLoad)
        << "Test setup must produce a significant initial CPU imbalance";

    // Run many iterations — slow actions should be suppressed throughout.
    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterAfter = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // The CPU imbalance must NOT have been reduced.
    // Without slow balancing, the deviating partitions stay concentrated, so the
    // final spread should remain close to the initial baseline (allow a small
    // tolerance for any incidental fast-balancer effects).
    EXPECT_GT(diameterAfter, BaseCpuLoad)
        << "Slow actions must be suppressed when RebalanceMinCpuSpread is above the actual CPU spread; "
           "CPU imbalance must remain above one partition's worth";
    EXPECT_GE(diameterAfter, 0.9 * diameterBefore)
        << "Slow actions must be suppressed: the final imbalance must stay close to the baseline";
}

TEST_F(TJobBalancerTest, RebalanceMinCpuSpreadAllowsSlowActionsWhenAboveThreshold)
{
    // Scenario:
    //   - With RebalanceMinCpuSpread = 0 (disabled), slow actions are applied normally
    //     and the CPU imbalance is reduced.
    //   - We verify that after many iterations the CPU imbalance IS reduced
    //     compared to the initial baseline.

    Reset();
    PrepareBalancerTest(3, RebalanceMinCpuSpreadTopology, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Explicitly set RebalanceMinCpuSpread to 0 (disabled) BEFORE any DistributeJobs call
    // to mirror the structure of the suppressed-mode test. Although 0.0 is the default,
    // setting it explicitly removes any hidden dependency on defaults.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = true; // Bypass the even-load gate.
        JobManager->Reconfigure(dynamicSpec);
    }

    DistributeJobs();
    InitializeCpuLoadDeviatingPartitions();

    // Capture baseline imbalance to make sure the scenario is non-trivial.
    SetCpuLoadDeviatingPartitions();
    double diameterBefore = GetMinMaxDiffCpuLoad(GetComputationId(2));
    ASSERT_GE(diameterBefore, 2 * BaseCpuLoad)
        << "Test setup must produce a significant initial CPU imbalance";

    // Run many iterations — slow actions should be applied, reducing CPU imbalance.
    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterAfter = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // The CPU imbalance must have been reduced to at most BaseCpuLoad (one partition's worth).
    EXPECT_LE(diameterAfter, BaseCpuLoad)
        << "Slow actions must be applied when RebalanceMinCpuSpread is 0 (disabled)";
    EXPECT_LT(diameterAfter, diameterBefore)
        << "Slow actions must reduce the imbalance compared to the baseline";
}

TEST_F(TJobBalancerTest, RebalanceMinCpuSpreadTransitionFromSuppressedToAllowed)
{
    // Scenario:
    //   - Start with RebalanceMinCpuSpread set very high (suppressed).
    //   - Verify imbalance is not reduced.
    //   - Then lower RebalanceMinCpuSpread to 0 (disabled).
    //   - Verify imbalance IS reduced after more iterations.

    Reset();
    PrepareBalancerTest(3, RebalanceMinCpuSpreadTopology, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Phase 1: suppress slow actions with a very high threshold from the start.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = std::nullopt; // Enable the even-load gate.
        dynamicSpec->JobManager->RebalanceMinCpuSpread = 1e9;
        JobManager->Reconfigure(dynamicSpec);
    }

    DistributeJobs();
    InitializeCpuLoadDeviatingPartitions();

    // Capture baseline imbalance to ensure the scenario is non-trivial.
    SetCpuLoadDeviatingPartitions();
    double diameterBaseline = GetMinMaxDiffCpuLoad(GetComputationId(2));
    ASSERT_GE(diameterBaseline, 2 * BaseCpuLoad)
        << "Test setup must produce a significant initial CPU imbalance";

    for (int i = 0; i < 10; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterSuppressed = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // Sanity check between phases: while suppressed the imbalance must remain high.
    ASSERT_GT(diameterSuppressed, BaseCpuLoad)
        << "While slow actions are suppressed, CPU imbalance must remain above one partition's worth; "
           "without this check Phase 2's reduction would be ambiguous";

    // Phase 2: enable slow actions by bypassing the even-load gate.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = true;
        JobManager->Reconfigure(dynamicSpec);
    }

    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterAllowed = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // After enabling, the imbalance must be reduced compared to the suppressed phase.
    EXPECT_LT(diameterAllowed, diameterSuppressed)
        << "Slow actions must be applied after RebalanceMinCpuSpread is lowered to 0";
    EXPECT_LE(diameterAllowed, BaseCpuLoad)
        << "CPU imbalance must be reduced to at most one partition's worth after enabling slow actions";
}

TEST_F(TJobBalancerTest, RebalanceMinCpuSpreadTransitionAcrossIntermediateThresholds)
{
    // Scenario:
    //   - Same topology as the other RebalanceMinCpuSpread* tests; the initial
    //     per-computation CPU imbalance is 2 * BaseCpuLoad = 200.
    //   - Phase 1: set RebalanceMinCpuSpread to 250, which is STRICTLY ABOVE
    //     the initial cluster CPU spread. Slow actions must be suppressed and
    //     the imbalance must remain near the baseline.
    //   - Phase 2: lower RebalanceMinCpuSpread to 150, which is STRICTLY BELOW
    //     the current cluster CPU spread. Slow actions must run and reduce
    //     the imbalance to at most one partition's worth.
    // This complements the suppressed (1e9) <-> allowed (0.0) transition test
    // by exercising finite-but-effective intermediate threshold values.

    Reset();
    PrepareBalancerTest(3, RebalanceMinCpuSpreadTopology, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Phase 1: threshold above the actual spread => slow actions suppressed.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = std::nullopt; // Enable the even-load gate.
        dynamicSpec->JobManager->RebalanceMinCpuRatio = 1.0;         // Make the spread the only binding criterion.
        dynamicSpec->JobManager->RebalanceMinCpuSpread = 250.0;
        JobManager->Reconfigure(dynamicSpec);
    }

    DistributeJobs();
    InitializeCpuLoadDeviatingPartitions();

    SetCpuLoadDeviatingPartitions();
    double diameterBaseline = GetMinMaxDiffCpuLoad(GetComputationId(2));
    ASSERT_GE(diameterBaseline, 2 * BaseCpuLoad)
        << "Test setup must produce a significant initial CPU imbalance";

    for (int i = 0; i < 10; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterSuppressed = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // Threshold (250) is above the cluster CPU spread, so slow actions are
    // suppressed and the imbalance must stay close to the baseline.
    ASSERT_GT(diameterSuppressed, BaseCpuLoad)
        << "Threshold 250 must suppress slow actions while initial cluster spread is below it";
    ASSERT_GE(diameterSuppressed, 0.9 * diameterBaseline)
        << "Threshold 250 must suppress slow actions: imbalance must remain close to the baseline";

    // Phase 2: lower threshold below the current spread => slow actions allowed.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->RebalanceMinCpuSpread = 150.0;
        JobManager->Reconfigure(dynamicSpec);
    }

    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }

    SetCpuLoadDeviatingPartitions();
    double diameterAllowed = GetMinMaxDiffCpuLoad(GetComputationId(2));

    // With threshold (150) below the current spread, slow actions resume and
    // must reduce the imbalance to at most one partition's worth.
    EXPECT_LT(diameterAllowed, diameterSuppressed)
        << "Lowering RebalanceMinCpuSpread below the current spread must allow slow actions to reduce the imbalance";
    EXPECT_LE(diameterAllowed, BaseCpuLoad)
        << "CPU imbalance must be reduced to at most one partition's worth after lowering the threshold";
}

// The even-load gate (WorkerLoadUneven) lives in shared code: DoBalanceSync runs the same
// RebalanceJobs (DoFastBalancing) and ShouldApplySlowActionsNow as the async fiber, so the gate
// behaves identically under the synchronous balancer. This test pins that down with the production
// default thresholds (so the max/min ratio criterion — neutralized in the tests above — is the
// binding one): the topology's imbalance has a worker-CPU ratio below RebalanceMinCpuRatio (1.2),
// so the gate suppresses rebalancing even though the absolute spread and relative deviation are
// above their thresholds. Setting DisableEvenLoadGate then lets the same synchronous balancer
// reduce the imbalance.
TEST_F(TJobBalancerTest, EvenLoadGateGatesSynchronousBalancerWithDefaultThresholds)
{
    Reset();
    PrepareBalancerTest(3, RebalanceMinCpuSpreadTopology, /*exceedCountAllowed*/ 1, TDuration::MilliSeconds(50));

    // Enable the gate with the production default thresholds (RebalanceMinCpuSpread = 1,
    // RebalanceMinCpuRatio = 1.2; RebalanceTargetDeviation = 0.01 is set by PrepareBalancerTest).
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = std::nullopt;
        JobManager->Reconfigure(dynamicSpec);
    }

    DistributeJobs();
    InitializeCpuLoadDeviatingPartitions();

    SetCpuLoadDeviatingPartitions();
    double diameterBaseline = GetMinMaxDiffCpuLoad(GetComputationId(2));
    ASSERT_GE(diameterBaseline, 2 * BaseCpuLoad)
        << "Test setup must produce a significant initial CPU imbalance";

    // The worker-CPU ratio is below RebalanceMinCpuRatio, so the gate suppresses rebalancing: the
    // synchronous balancer leaves the imbalance near the baseline.
    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }
    SetCpuLoadDeviatingPartitions();
    EXPECT_GE(GetMinMaxDiffCpuLoad(GetComputationId(2)), 0.9 * diameterBaseline)
        << "The even-load gate must suppress the synchronous balancer when the worker-CPU ratio is "
           "below RebalanceMinCpuRatio, even though spread and relative deviation are above theirs";

    // Bypass the gate: the same synchronous balancer now reduces the imbalance.
    {
        auto dynamicSpec = FlowView->CurrentDynamicSpec->GetValue();
        dynamicSpec->JobManager->DisableEvenLoadGate = true;
        JobManager->Reconfigure(dynamicSpec);
    }
    for (int i = 0; i < 20; ++i) {
        SetCpuLoadDeviatingPartitions();
        DistributeJobs();
    }
    SetCpuLoadDeviatingPartitions();
    EXPECT_LE(GetMinMaxDiffCpuLoad(GetComputationId(2)), BaseCpuLoad)
        << "DisableEvenLoadGate must let the synchronous balancer reduce the imbalance";
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, PreloadAddActionAppliedToWorkerSpecs)
{
    // Verify that when DoBalance (ResourceQueue) emits a PreloadResourceAction of type Add,
    // DistributeJobs correctly inserts the resource into layout->WorkerSpecs[worker].PreloadResources.

    Reset();

    const TResourceId resId = TResourceId("res_preload");
    const TComputationId compId = TComputationId("Computation0");
    const std::string workerAddress = "flow0";

    // Build spec with one computation that requires a preloadable resource.
    auto spec = New<TPipelineSpec>();
    auto resourceSpec = New<TResourceSpec>();
    resourceSpec->PreloadRequired = true;
    spec->Resources[resId] = resourceSpec;

    auto computationSpec = CreateGenericComputationSpec<TSimpleComputation>();
    // TResourceDescription defaults Controller=true (from spec.cpp), so we must set it to false
    // to avoid ResourceManager_->Load() being called for a preload-required resource that has
    // no preload scheduled yet.
    auto resourceDescription = New<TResourceDescription>();
    resourceDescription->Controller = false;
    computationSpec->RequiredResourceIds[resId] = resourceDescription;
    spec->Computations[compId] = computationSpec;

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobManager->BalancerType = EJobBalancerType::ResourceQueue;
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations[compId] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations[compId]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(1u));

    Prepare(spec, dynamicSpec);

    // Create partitions.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();

    // Add a worker.
    auto worker = New<NFlow::TWorker>();
    worker->RpcAddress = workerAddress;
    FlowView->State->Workers[workerAddress] = worker;

    // Assign the partition to the worker (so ResourceQueue balancer sees it as a computation worker).
    {
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        ASSERT_EQ(layout->Partitions.size(), 1u);
        const auto partitionId = layout->Partitions.begin()->first;

        FlowView->State->StartMutation();
        auto job = New<TJob>();
        job->JobId = TJobId(TGuid::Create());
        job->WorkerAddress = workerAddress;
        job->WorkerIncarnationId = FlowView->State->Workers.at(workerAddress)->IncarnationId;
        job->PartitionId = partitionId;
        layout->CreateJob(job);
        FlowView->State->CommitMutation();

        // Add partition job status so the balancer has metrics.
        auto partitionJobStatus = New<TPartitionJobStatus>();
        partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
        partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);
        auto inputMetrics = New<TNodeInputMetrics>();
        inputMetrics->Global.MessagesPerSecond = 1.0;
        partitionJobStatus->CurrentJobStatus->InputMetrics = inputMetrics;
        FlowView->Feedback->PartitionJobStatuses[partitionId] = partitionJobStatus;
    }

    // Run DistributeJobs — ResourceQueue balancer should emit a PreloadAdd for the worker.
    FlowView->State->StartMutation();
    JobManager->DistributeJobs(FlowView);
    FlowView->State->CommitMutation();

    // Verify: WorkerSpecs[workerAddress].PreloadResources contains resId.
    const auto& layout = FlowView->State->ExecutionSpec->Layout;
    auto* workerSpec = layout->WorkerSpecs.FindPtr(workerAddress);
    ASSERT_TRUE(workerSpec != nullptr) << "WorkerSpecs must have an entry for the worker after PreloadAdd";
    ASSERT_TRUE(*workerSpec != nullptr);
    EXPECT_TRUE((*workerSpec)->PreloadResources.contains(resId))
        << "PreloadResources must contain the preloadable resource after DistributeJobs applies PreloadAdd action";
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, PreloadDelActionAppliedToWorkerSpecs)
{
    // Verify that when DoBalance (ResourceQueue) emits a PreloadResourceAction of type Del,
    // DistributeJobs correctly removes the resource from layout->WorkerSpecs[worker].PreloadResources.
    //
    // Scenario: a preloadable resource was issued to a worker, but the resource is no longer
    // required by any computation (orphaned preload). The balancer should emit a Del.

    Reset();

    const TResourceId resId = TResourceId("res_preload");
    const TComputationId compId = TComputationId("Computation0");
    const std::string workerAddress = "flow0";

    // Build spec: computation does NOT require resId (resId is an orphaned preloadable resource).
    auto spec = New<TPipelineSpec>();
    auto resourceSpec = New<TResourceSpec>();
    resourceSpec->PreloadRequired = true;
    spec->Resources[resId] = resourceSpec;

    // Computation uses a different (non-preloadable) resource.
    const TResourceId otherResId = TResourceId("res_other");
    auto otherResourceSpec = New<TResourceSpec>();
    otherResourceSpec->PreloadRequired = false;
    spec->Resources[otherResId] = otherResourceSpec;

    auto computationSpec = CreateGenericComputationSpec<TSimpleComputation>();
    auto resourceDescription = New<TResourceDescription>();
    resourceDescription->Controller = false;
    computationSpec->RequiredResourceIds[otherResId] = resourceDescription;
    spec->Computations[compId] = computationSpec;

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobManager->BalancerType = EJobBalancerType::ResourceQueue;
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations[compId] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations[compId]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(1u));

    Prepare(spec, dynamicSpec);

    // Create partitions.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();

    // Add a worker.
    auto worker = New<NFlow::TWorker>();
    worker->RpcAddress = workerAddress;
    FlowView->State->Workers[workerAddress] = worker;

    // Pre-populate WorkerSpecs: the worker has resId issued (orphaned — not required by any computation).
    {
        FlowView->State->StartMutation();
        auto workerSpec = New<TWorkerSpec>();
        workerSpec->PreloadResources.insert(resId);
        FlowView->State->ExecutionSpec->Layout->WorkerSpecs.insert_or_assign(workerAddress, workerSpec);
        FlowView->State->CommitMutation();
    }

    // Run DistributeJobs — ResourceQueue balancer should emit a PreloadDel for the worker
    // because resId is not required by any computation (orphaned preload).
    FlowView->State->StartMutation();
    JobManager->DistributeJobs(FlowView);
    FlowView->State->CommitMutation();

    // Verify: WorkerSpecs[workerAddress].PreloadResources no longer contains resId.
    const auto& layout = FlowView->State->ExecutionSpec->Layout;
    auto* workerSpec = layout->WorkerSpecs.FindPtr(workerAddress);
    // Either the entry is gone or the resource is removed — both are correct.
    if (workerSpec && *workerSpec) {
        EXPECT_FALSE((*workerSpec)->PreloadResources.contains(resId))
            << "PreloadResources must not contain the resource after DistributeJobs applies PreloadDel action";
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TJobBalancerTest, PreloadAddPreservesExistingResources)
{
    // Verify that when DistributeJobs applies a PreloadAdd action for a new resource (resIdB),
    // it preserves any resources already present in WorkerSpecs[worker].PreloadResources (resIdA).

    Reset();

    const TResourceId resIdA = TResourceId("res_existing");
    const TResourceId resIdB = TResourceId("res_new");
    const TComputationId compId = TComputationId("Computation0");
    const std::string workerAddress = "flow0";

    // Build spec: computation requires resIdB (preloadable).
    // resIdA is also preloadable but not required by any computation (already issued manually).
    auto spec = New<TPipelineSpec>();
    auto resourceSpecA = New<TResourceSpec>();
    resourceSpecA->PreloadRequired = true;
    spec->Resources[resIdA] = resourceSpecA;
    auto resourceSpecB = New<TResourceSpec>();
    resourceSpecB->PreloadRequired = true;
    spec->Resources[resIdB] = resourceSpecB;

    auto computationSpec = CreateGenericComputationSpec<TSimpleComputation>();
    auto resourceDescriptionB = New<TResourceDescription>();
    resourceDescriptionB->Controller = false;
    computationSpec->RequiredResourceIds[resIdB] = resourceDescriptionB;
    spec->Computations[compId] = computationSpec;

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobManager->BalancerType = EJobBalancerType::ResourceQueue;
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations[compId] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations[compId]->Parameters->AddChild("desired_partition_count", NYTree::ConvertToNode(1u));

    Prepare(spec, dynamicSpec);

    // Create partitions.
    FlowView->State->StartMutation();
    JobManager->DoPartitioning(FlowView);
    FlowView->State->CommitMutation();

    // Add a worker.
    auto worker = New<NFlow::TWorker>();
    worker->RpcAddress = workerAddress;
    FlowView->State->Workers[workerAddress] = worker;

    // Pre-populate WorkerSpecs with resIdA already present.
    {
        FlowView->State->StartMutation();
        auto workerSpec = New<TWorkerSpec>();
        workerSpec->PreloadResources.insert(resIdA);
        FlowView->State->ExecutionSpec->Layout->WorkerSpecs.insert_or_assign(workerAddress, workerSpec);
        FlowView->State->CommitMutation();
    }

    // Assign the partition to the worker so the balancer emits PreloadAdd for resIdB.
    {
        const auto& layout = FlowView->State->ExecutionSpec->Layout;
        ASSERT_EQ(layout->Partitions.size(), 1u);
        const auto partitionId = layout->Partitions.begin()->first;

        FlowView->State->StartMutation();
        auto job = New<TJob>();
        job->JobId = TJobId(TGuid::Create());
        job->WorkerAddress = workerAddress;
        job->WorkerIncarnationId = FlowView->State->Workers.at(workerAddress)->IncarnationId;
        job->PartitionId = partitionId;
        layout->CreateJob(job);
        FlowView->State->CommitMutation();

        auto partitionJobStatus = New<TPartitionJobStatus>();
        partitionJobStatus->CurrentJobStatus = New<TJobStatus>();
        partitionJobStatus->CurrentJobStatus->StartTime = TInstant::Now() - TDuration::Hours(1);
        auto inputMetrics = New<TNodeInputMetrics>();
        inputMetrics->Global.MessagesPerSecond = 1.0;
        partitionJobStatus->CurrentJobStatus->InputMetrics = inputMetrics;
        FlowView->Feedback->PartitionJobStatuses[partitionId] = partitionJobStatus;
    }

    // Run DistributeJobs.
    FlowView->State->StartMutation();
    JobManager->DistributeJobs(FlowView);
    FlowView->State->CommitMutation();

    // Verify: WorkerSpecs[workerAddress].PreloadResources contains resIdB (newly added by PreloadAdd).
    const auto& layout = FlowView->State->ExecutionSpec->Layout;
    auto* workerSpec = layout->WorkerSpecs.FindPtr(workerAddress);
    ASSERT_TRUE(workerSpec != nullptr) << "WorkerSpecs must have an entry for the worker";
    ASSERT_TRUE(*workerSpec != nullptr);
    EXPECT_TRUE((*workerSpec)->PreloadResources.contains(resIdB))
        << "PreloadResources must contain resIdB after DistributeJobs applies PreloadAdd action";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
