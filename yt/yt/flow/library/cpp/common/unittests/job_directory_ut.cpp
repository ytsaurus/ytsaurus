#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage_state.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

IPayloadConverterCachePtr MakeConverterCache()
{
    return CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
}

//! A pipeline spec whose computations carry the given group-by schemas (empty when keys are not computed).
TPipelineSpecPtr MakePipelineSpec(const THashMap<TComputationId, TTableSchemaPtr>& groupBySchemas = {})
{
    auto pipelineSpec = New<TPipelineSpec>();
    for (const auto& [computationId, groupBySchema] : groupBySchemas) {
        auto computationSpec = New<TComputationSpec>();
        computationSpec->GroupBySchema = groupBySchema;
        pipelineSpec->Computations[computationId] = std::move(computationSpec);
    }
    return pipelineSpec;
}

////////////////////////////////////////////////////////////////////////////////

class TStorageHandler
    : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TJobDirectoryTest, CheckOutbandKey)
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
    partition->LowerKey = MakeUintKey(5);
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

    auto job = New<TJob>();
    job->JobId = TJobId(TJobId::TUnderlying::Create());
    job->PartitionId = partition->PartitionId;
    flowState->StartMutation();
    flowState->ExecutionSpec->Layout->CreateJob(job);
    flowState->CommitMutation();
    keeper->SetStates(flowState, {});

    auto jobDirectory = CreateJobDirectory(MakeConverterCache(), NLogging::TLogger());
    jobDirectory->Reconfigure(flowState->ExecutionSpec->Layout, MakePipelineSpec());

    ASSERT_TRUE(jobDirectory->FindRouteByKey("abc", MakeUintKey(8)));
    ASSERT_FALSE(jobDirectory->FindRouteByKey("abc", MakeUintKey(1)));
    ASSERT_FALSE(jobDirectory->FindRouteByKey("abc", MakeUintKey(15)));
}

////////////////////////////////////////////////////////////////////////////////

TFlowStatePtr MakeFlowState()
{
    auto flowState = New<TFlowState>();
    auto storageHandler = New<TStorageHandler>();
    auto persistedControl = New<TPersistedStateControl<std::string>>(storageHandler);
    flowState->AttachToControl(persistedControl);
    persistedControl->Recover();
    return flowState;
}

TJobId AddExecutingJob(
    const TFlowStatePtr& flowState,
    TComputationId computationId,
    std::optional<TKey> lowerKey,
    std::optional<TKey> upperKey)
{
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
    partition->ComputationId = computationId;
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = 1;
    partition->StateTimestamp = TInstant::Now();
    partition->LowerKey = lowerKey;
    partition->UpperKey = upperKey;

    auto job = New<TJob>();
    job->JobId = TJobId(TJobId::TUnderlying::Create());
    job->PartitionId = partition->PartitionId;
    job->WorkerAddress = "worker";

    flowState->StartMutation();
    flowState->ExecutionSpec->Layout->CreatePartition(partition);
    flowState->ExecutionSpec->Layout->CreateJob(job);
    flowState->CommitMutation();

    return job->JobId;
}

TEST(TJobDirectoryTest, AliveJobsVsRoutableJobs)
{
    auto flowState = MakeFlowState();
    // A routable job with a key range.
    auto routableJobId = AddExecutingJob(flowState, "abc", MakeUintKey(5), MakeUintKey(10));
    // A source-like job without a key range: alive but not routable.
    auto sourcelikeJobId = AddExecutingJob(flowState, "src", std::nullopt, std::nullopt);

    auto jobDirectory = CreateJobDirectory(MakeConverterCache(), NLogging::TLogger());
    jobDirectory->Reconfigure(flowState->ExecutionSpec->Layout, MakePipelineSpec());

    auto snapshot = jobDirectory->GetSnapshot();

    EXPECT_TRUE(snapshot->IsJobAlive(routableJobId));
    EXPECT_TRUE(snapshot->IsJobAlive(sourcelikeJobId));
    EXPECT_TRUE(snapshot->RoutableJobs().contains(routableJobId));
    EXPECT_FALSE(snapshot->RoutableJobs().contains(sourcelikeJobId));
    EXPECT_EQ(snapshot->GetPartitionCount("abc"), 1);
    EXPECT_EQ(snapshot->GetPartitionCount("src"), 1);
}

TEST(TJobDirectoryTest, ComputeDiff)
{
    auto jobDirectory = CreateJobDirectory(MakeConverterCache(), NLogging::TLogger());
    auto emptySnapshot = jobDirectory->GetSnapshot();

    auto flowState = MakeFlowState();
    auto jobA = AddExecutingJob(flowState, "abc", MakeUintKey(0), MakeUintKey(10));
    jobDirectory->Reconfigure(flowState->ExecutionSpec->Layout, MakePipelineSpec());
    auto snapshotA = jobDirectory->GetSnapshot();

    {
        auto diff = ComputeJobDirectoryDiff(emptySnapshot, snapshotA);
        ASSERT_EQ(diff.AddedJobs.size(), 1ull);
        EXPECT_EQ(diff.AddedJobs[0], jobA);
        EXPECT_TRUE(diff.RemovedJobs.empty());
    }

    // Remove jobA, add jobB.
    flowState->StartMutation();
    flowState->ExecutionSpec->Layout->RemoveJob(jobA, EJobFinishReason::Stopped);
    flowState->CommitMutation();
    auto jobB = AddExecutingJob(flowState, "abc", MakeUintKey(10), MakeUintKey(20));
    jobDirectory->Reconfigure(flowState->ExecutionSpec->Layout, MakePipelineSpec());
    auto snapshotB = jobDirectory->GetSnapshot();

    {
        auto diff = ComputeJobDirectoryDiff(snapshotA, snapshotB);
        ASSERT_EQ(diff.AddedJobs.size(), 1ull);
        EXPECT_EQ(diff.AddedJobs[0], jobB);
        ASSERT_EQ(diff.RemovedJobs.size(), 1ull);
        EXPECT_EQ(diff.RemovedJobs[0], jobA);
    }
}

////////////////////////////////////////////////////////////////////////////////

// The snapshot computes routing keys from the group-by schema captured at its build time (from the layout's pipeline
// spec), so a later reconfigure that swaps a computation's group-by schema cannot change an older in-flight snapshot's
// key. A group-by schema change always bumps the epoch, so the layout and the schema it pairs with stay consistent.
TEST(TJobDirectoryTest, ComputeMessageKeyPinsGroupBySchema)
{
    auto sourceSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="order_id"; type="uint64";};
            {name="banner_id"; type="uint64";};
        ]
    )"""")));

    auto orderKeySchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64"; sort_order="ascending";};
        ]
    )""")));

    auto bannerKeySchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="banner_id"; type="uint64"; sort_order="ascending";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedUint64Value(111, 0));
    builder.Payload().SetValue(MakeUnversionedUint64Value(222, 1));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    const auto message = builder.Finish();

    auto jobDirectory = CreateJobDirectory(MakeConverterCache(), NLogging::TLogger());

    // Epoch 0: group by order_id.
    jobDirectory->Reconfigure(MakeFlowState()->ExecutionSpec->Layout, MakePipelineSpec({{"cid", orderKeySchema}}));
    auto oldSnapshot = jobDirectory->GetSnapshot();

    auto oldKey = oldSnapshot->ComputeMessageKey("cid", message);
    ASSERT_TRUE(oldKey);
    // The key is exactly the payload projected onto the group-by schema.
    EXPECT_EQ(*oldKey, *oldSnapshot->ComputeMessageKey("cid", message));

    // New epoch: same computation now groups by banner_id.
    jobDirectory->Reconfigure(MakeFlowState()->ExecutionSpec->Layout, MakePipelineSpec({{"cid", bannerKeySchema}}));
    auto newSnapshot = jobDirectory->GetSnapshot();

    auto newKey = newSnapshot->ComputeMessageKey("cid", message);
    ASSERT_TRUE(newKey);

    // The new snapshot uses the new schema; the old, still-referenced snapshot keeps using the old one.
    EXPECT_NE(*oldKey, *newKey);
    EXPECT_EQ(*oldSnapshot->ComputeMessageKey("cid", message), *oldKey);

    // Unknown computations yield no key rather than throwing.
    EXPECT_FALSE(newSnapshot->ComputeMessageKey("unknown", message));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
