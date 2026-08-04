#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <thread>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

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

TEST(TFlowLayoutMutationTest, CreatePartition)
{
    auto keeper = New<TFlowViewKeeper>();
    keeper->Init(New<TFlowState>(), New<TFlowEphemeralState>(), New<TVersionedPipelineSpec>(), New<TVersionedDynamicPipelineSpec>());
    EXPECT_EQ(keeper->GetFlowView()->State->ExecutionSpec->Layout->Partitions.size(), 0ull);

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = "abc";
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = 1;
    partition->StateTimestamp = TInstant::Now();
    partition->LowerKey = MakeUintKey(1);
    partition->UpperKey = MakeUintKey(10);
    auto flowState = New<TFlowState>();
    auto storageHandler = New<TStorageHandler>();
    auto persistedControl = New<TPersistedStateControl<std::string>>(storageHandler);
    flowState->AttachToControl(persistedControl);
    persistedControl->Recover();
    flowState->StartMutation();
    flowState->ExecutionSpec->Layout->CreatePartition(partition);
    EXPECT_EQ(flowState->ExecutionSpec->Layout->GetUpdated(), 1);
    flowState->CommitMutation();

    keeper->SetStates(flowState, {});

    EXPECT_EQ(keeper->GetFlowView()->State->ExecutionSpec->Layout->Partitions.size(), 1ull);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowViewSerializeTest, RendersLayoutButKeepsDefaultEmpty)
{
    auto flowState = New<TFlowState>();
    auto storageHandler = New<TStorageHandler>();
    auto persistedControl = New<TPersistedStateControl<std::string>>(storageHandler);
    flowState->AttachToControl(persistedControl);
    persistedControl->Recover();
    flowState->StartMutation();

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = "comp";
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = 1;
    partition->StateTimestamp = TInstant::Seconds(100);
    flowState->ExecutionSpec->Layout->CreatePartition(partition);

    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->WorkerAddress = "worker";
    job->PartitionId = partition->PartitionId;
    flowState->ExecutionSpec->Layout->CreateJob(job);
    flowState->CommitMutation();

    auto keeper = New<TFlowViewKeeper>();
    keeper->Init(New<TFlowState>(), New<TFlowEphemeralState>(), New<TVersionedPipelineSpec>(), New<TVersionedDynamicPipelineSpec>());
    keeper->SetStates(flowState, New<TFlowEphemeralState>());
    auto flowView = keeper->GetFlowView();

    // The rendered (UI) form streams the layout as partitions/jobs.
    auto rendered = flowView->SerializeAsYsonString();
    EXPECT_TRUE(TryGetAny(rendered.AsStringBuf(), "/state/execution_spec/layout/partitions/" + ToString(partition->PartitionId)));
    EXPECT_TRUE(TryGetAny(rendered.AsStringBuf(), "/state/execution_spec/layout/jobs/" + ToString(job->JobId)));

    // Invariant: the layout's default (persisted) serialization stays empty — the render above must
    // not leak into how the state struct serializes by default.
    EXPECT_FALSE(TryGetAny(ConvertToYsonString(flowView->State->ExecutionSpec->Layout).AsStringBuf(), "/partitions"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowLayoutMutationTest, Multithread)
{
    constexpr ssize_t numPartitions = 8;
    constexpr ssize_t numReaders = 10;
    constexpr ssize_t waitTime = 10;

    std::string someWorker = "worker";
    TComputationId someComputation = "computation";
    TIncarnationId someIncarnationId = TIncarnationId(TGuid::Create());
    TPartitionId partitionIds[numPartitions];
    for (auto& partitionId : partitionIds) {
        partitionId = TPartitionId(TGuid::Create());
    }
    TJobId jobIds[numPartitions];
    for (auto& jobId : jobIds) {
        jobId = TJobId(TGuid::Create());
    }

    auto flowState = New<TFlowState>();
    auto storageHandler = New<TStorageHandler>();
    auto persistedControl = New<TPersistedStateControl<std::string>>(storageHandler);
    flowState->AttachToControl(persistedControl);
    persistedControl->Recover();
    auto ephemeralState = New<TFlowEphemeralState>();
    ephemeralState->WorkerIncarnationsJobs[someIncarnationId];
    auto keeper = New<TFlowViewKeeper>();
    keeper->Init(flowState, ephemeralState, New<TVersionedPipelineSpec>(), New<TVersionedDynamicPipelineSpec>());

    std::atomic<bool> done = false;

    auto writerFunc = [&] {
        while (!done) {
            auto currentFlowView = keeper->GetFlowView();

            auto newFlowView = currentFlowView->CopyPtr();
            newFlowView->State = newFlowView->State->Clone();
            newFlowView->EphemeralState = CloneYsonStruct(newFlowView->EphemeralState);
            newFlowView->State->StartMutation();

            const auto& layout = newFlowView->State->ExecutionSpec->Layout;
            int i = RandomNumber<ui64>(numPartitions);
            if (layout->Partitions.contains(partitionIds[i])) {
                layout->RemoveJob(jobIds[i], EJobFinishReason::Unknown);
                layout->RemovePartition(partitionIds[i]);
                EraseOrCrash(newFlowView->EphemeralState->WorkerIncarnationsJobs[someIncarnationId], jobIds[i]);
            } else {
                auto partition = New<TPartition>();
                partition->PartitionId = partitionIds[i];
                partition->ComputationId = someComputation;
                partition->State = EPartitionState::Executing;
                partition->StateEpoch = 1;
                partition->StateTimestamp = TInstant::Now();
                layout->CreatePartition(partition);
                auto job = New<TJob>();
                job->JobId = jobIds[i];
                job->WorkerAddress = someWorker;
                job->WorkerIncarnationId = someIncarnationId;
                job->PartitionId = partitionIds[i];
                layout->CreateJob(job);
                EmplaceOrCrash(newFlowView->EphemeralState->WorkerIncarnationsJobs[someIncarnationId], jobIds[i]);
            }
            newFlowView->State->CommitMutation();
            keeper->SetStates(newFlowView->State, newFlowView->EphemeralState);
        }
    };

    auto readerFunc = [&] {
        while (!done) {
            auto currentFlowView = keeper->GetFlowView();
            for (const auto& id : currentFlowView->EphemeralState->WorkerIncarnationsJobs[someIncarnationId]) {
                GetOrCrash(currentFlowView->State->ExecutionSpec->Layout->Jobs, id);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.push_back(std::thread(writerFunc));
    for (size_t i = 0; i < numReaders; i++) {
        threads.push_back(std::thread(readerFunc));
    }

    sleep(waitTime);
    done = true;
    for (auto& thread : threads) {
        thread.join();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowViewKeeperTest, SetFeedbackFencesOnStaleSpecVersion)
{
    auto keeper = New<TFlowViewKeeper>();
    auto specV0 = New<TVersionedPipelineSpec>();
    keeper->Init(New<TFlowState>(), New<TFlowEphemeralState>(), specV0, New<TVersionedDynamicPipelineSpec>());

    // Reader captures spec version under which CollectFeedback is computed.
    auto snapshot = keeper->GetFlowView();
    auto expectedSpecVersion = snapshot->CurrentSpec->GetVersion();

    // Barrier crossed: a new static spec is installed, dropping the previous feedback.
    auto specV1 = New<TVersionedPipelineSpec>();
    specV1->Bump(TestVersionProvider());
    keeper->SetSpecs(specV1, std::nullopt);
    EXPECT_NE(expectedSpecVersion, keeper->GetFlowView()->CurrentSpec->GetVersion());
    EXPECT_TRUE(keeper->GetFlowView()->Feedback->PartitionJobStatuses.empty());

    // Stale feedback computed before the barrier must not clobber the post-barrier empty feedback.
    auto staleFeedback = New<TFlowFeedback>();
    staleFeedback->PartitionJobStatuses.emplace(TPartitionId(TGuid::Create()), New<TPartitionJobStatus>());
    EXPECT_FALSE(keeper->SetFeedback(staleFeedback, expectedSpecVersion));
    EXPECT_TRUE(keeper->GetFlowView()->Feedback->PartitionJobStatuses.empty());

    // Feedback computed against the current spec version installs cleanly.
    auto freshFeedback = New<TFlowFeedback>();
    freshFeedback->PartitionJobStatuses.emplace(TPartitionId(TGuid::Create()), New<TPartitionJobStatus>());
    EXPECT_TRUE(keeper->SetFeedback(freshFeedback, keeper->GetFlowView()->CurrentSpec->GetVersion()));
    EXPECT_EQ(keeper->GetFlowView()->Feedback->PartitionJobStatuses.size(), 1ull);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TInputMetricsAggregationTest, IgnoresStaleJobStatuses)
{
    const TComputationId computationId("computation");
    const TInstant staleUpdateTime = TInstant::Seconds(50);
    const TInstant minUpdateTime = TInstant::Seconds(100);
    const TInstant freshUpdateTime = TInstant::Seconds(200);

    auto flowState = New<TFlowState>();
    auto storageHandler = New<TStorageHandler>();
    auto persistedControl = New<TPersistedStateControl<std::string>>(storageHandler);
    flowState->AttachToControl(persistedControl);
    persistedControl->Recover();
    flowState->StartMutation();

    auto addPartition = [&] {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = computationId;
        partition->State = EPartitionState::Executing;
        partition->StateEpoch = 1;
        partition->StateTimestamp = TInstant::Seconds(1);
        flowState->ExecutionSpec->Layout->CreatePartition(partition);
        return partition->PartitionId;
    };

    auto stalePartitionId = addPartition();
    auto freshPartitionId = addPartition();
    flowState->CommitMutation();

    auto flowView = New<TFlowView>();
    flowView->State = flowState;
    flowView->Feedback = New<TFlowFeedback>();

    auto addStatus = [&] (TPartitionId partitionId, TInstant updateTime, double messagesPerSecond) {
        auto inputMetrics = New<TNodeInputMetrics>();
        inputMetrics->Global.MessagesPerSecond = messagesPerSecond;

        auto jobStatus = New<TJobStatus>();
        jobStatus->InputMetrics = std::move(inputMetrics);

        auto partitionJobStatus = New<TPartitionJobStatus>();
        partitionJobStatus->CurrentJobStatusUpdateTime = updateTime;
        partitionJobStatus->CurrentJobStatus = std::move(jobStatus);
        flowView->Feedback->PartitionJobStatuses.emplace(partitionId, std::move(partitionJobStatus));
    };

    addStatus(stalePartitionId, staleUpdateTime, 100.0);
    addStatus(freshPartitionId, freshUpdateTime, 5.0);

    auto metrics = AggregateInputMetricsByComputation(flowView, minUpdateTime);
    ASSERT_EQ(metrics.size(), 1u);
    const auto& computationMetrics = GetOrCrash(metrics, computationId);
    ASSERT_TRUE(computationMetrics->Global);
    EXPECT_DOUBLE_EQ(computationMetrics->Global->Total.MessagesPerSecond, 5.0);
    EXPECT_DOUBLE_EQ(computationMetrics->Global->Avg.MessagesPerSecond, 5.0);
    EXPECT_DOUBLE_EQ(computationMetrics->Global->Max.MessagesPerSecond, 5.0);

    EXPECT_TRUE(AggregateInputMetricsByComputation(flowView, freshUpdateTime + TDuration::Seconds(1)).empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExecutionSpecEpochTest, AdvancesForEveryVersionComponent)
{
    auto versions = New<TExecutionSpecVersions>();
    i64 nextClockVersion = static_cast<i64>(
        NTransactionClient::TimestampFromUnixTime(1'784'633'264).Underlying());

    auto updateClockVersion = [&] (TVersion& version) {
        const auto previousEpoch = versions->GetEpoch();
        version = TVersion(++nextClockVersion);
        EXPECT_GT(versions->GetEpoch(), previousEpoch);
    };

    updateClockVersion(versions->PipelineStateVersion);
    updateClockVersion(versions->PipelineSpecVersion);
    updateClockVersion(versions->ExtendedPipelineSpecVersion);
    updateClockVersion(versions->DynamicPipelineSpecVersion);

    const auto previousEpoch = versions->GetEpoch();
    versions->LayoutVersion = TVersion(1);
    EXPECT_GT(versions->GetEpoch(), previousEpoch);
}

TEST(TExecutionSpecEpochTest, RemainsPositivePastTheOld2038OverflowBoundary)
{
    const i64 clockVersion = static_cast<i64>(
        NTransactionClient::TimestampFromUnixTime(1ULL << 31).Underlying());
    auto versions = New<TExecutionSpecVersions>();
    versions->PipelineStateVersion = TVersion(clockVersion + 1);
    versions->PipelineSpecVersion = TVersion(clockVersion + 2);
    versions->ExtendedPipelineSpecVersion = TVersion(clockVersion + 3);
    versions->DynamicPipelineSpecVersion = TVersion(clockVersion + 4);
    versions->LayoutVersion = TVersion(5);

    EXPECT_EQ(versions->GetEpoch(), clockVersion + 9);
    EXPECT_GT(versions->GetEpoch(), 0);
}

////////////////////////////////////////////////////////////////////////////////

//! Must return synced FlowView for testing.
TFlowViewPtr CreateSyncedFlowView()
{
    return New<TFlowView>();
}

TEST(TFlowCoreTargetTest, EmptyTargetPasses)
{
    auto flowView = CreateSyncedFlowView();
    EXPECT_TRUE(flowView->State->ExecutionSpec->FlowCoreTarget->GetValue().Underlying().empty());
    EXPECT_TRUE(CheckFlowCoreTarget(flowView, GetBinaryChecksum()));
}

TEST(TFlowCoreTargetTest, MatchingTargetPasses)
{
    auto flowView = CreateSyncedFlowView();
    flowView->State->ExecutionSpec->FlowCoreTarget->TrySetValue(TFlowCoreTarget(GetBinaryChecksum()), TestVersionProvider());
    EXPECT_TRUE(CheckFlowCoreTarget(flowView, GetBinaryChecksum()));
}

TEST(TFlowCoreTargetTest, MismatchingTargetFails)
{
    auto flowView = CreateSyncedFlowView();
    flowView->State->ExecutionSpec->FlowCoreTarget->TrySetValue(TFlowCoreTarget("mismatch_version"), TestVersionProvider());
    EXPECT_FALSE(CheckFlowCoreTarget(flowView, GetBinaryChecksum()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
