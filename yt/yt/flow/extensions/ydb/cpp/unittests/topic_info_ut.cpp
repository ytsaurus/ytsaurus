#include <yt/yt/flow/extensions/ydb/cpp/client_factory.h>
#include <yt/yt/flow/extensions/ydb/cpp/helpers.h>
#include <yt/yt/flow/extensions/ydb/cpp/spec.h>
#include <yt/yt/flow/extensions/ydb/cpp/state.h>
#include <yt/yt/flow/extensions/ydb/cpp/topic_info.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("YdbTopicInfoTest");

struct TRecordedTopologyState
    : public TRefCounted
{
    TYdbTopologySnapshot Snapshot;
    int WriteCount = 0;
};

using TRecordedTopologyStatePtr = TIntrusivePtr<TRecordedTopologyState>;

TYdbStateAccess<TYdbTopologySnapshot> MakeRecordedAccess(const TRecordedTopologyStatePtr& state)
{
    return {
        .Init = BIND([] (IInitContextPtr /*initContext*/) {
        }),
        .Read = BIND([state] {
            return state->Snapshot;
        }),
        .Write = BIND([state] (const TYdbTopologySnapshot& snapshot) {
            state->Snapshot = snapshot;
            ++state->WriteCount;
        }),
    };
}

class TFailingDiscoveryClientFactory
    : public IYdbClientFactory
{
public:
    explicit TFailingDiscoveryClientFactory(std::function<void()> onDiscovery)
        : OnDiscovery_(std::move(onDiscovery))
    { }

    const TPromise<void> DiscoveryAttempted = NewPromise<void>();

    std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& /*endpoint*/,
        const std::string& /*database*/) override
    {
        YT_ABORT();
    }

    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& /*endpoint*/,
        const std::string& /*database*/) override
    {
        OnDiscovery_();
        DiscoveryAttempted.TrySet();
        THROW_ERROR_EXCEPTION("Discovery is unavailable");
    }

    IYdbWriterPtr CreateWriter(
        const std::string& /*endpoint*/,
        const std::string& /*database*/,
        NYdb::NTopic::TWriteSessionSettings /*settings*/,
        IInvokerPtr /*invoker*/,
        NLogging::TLogger /*logger*/,
        IStatusProfilerPtr /*statusProfiler*/) override
    {
        YT_ABORT();
    }

    IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        const std::string& /*endpoint*/,
        const std::string& /*database*/) override
    {
        YT_ABORT();
    }

    TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& /*options*/,
        TDuration /*timeout*/,
        std::optional<TYdbWriteTarget> /*preferredTarget*/,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> /*excludedTargets*/) override
    {
        YT_ABORT();
    }

private:
    const std::function<void()> OnDiscovery_;
};

class TYdbTopicInfoControllerTest
    : public ::testing::Test
{
protected:
    const NConcurrency::TActionQueuePtr Queue = New<NConcurrency::TActionQueue>("TopicInfoTest");
    const IInvokerPtr Invoker = NConcurrency::CreateSerializedInvoker(Queue->GetInvoker());
    const TRecordedTopologyStatePtr State = New<TRecordedTopologyState>();

    TYdbTopicInfoControllerPtr CreateController(IYdbClientFactoryPtr clientFactory = nullptr)
    {
        return New<TYdbTopicInfoController>(
            TYdbTopologyOptions{
                .DiscoveryEntries = {TYdbWriteTarget{.Endpoint = "localhost:2135", .Database = "/Root/db"}},
                .Database = "/Root/db",
                .Topic = "topic",
                .ClientTimeout = TDuration::Seconds(1),
                .UpdatePartitionCountPeriod = TDuration::Hours(1),
            },
            std::move(clientFactory),
            Invoker,
            Logger,
            CreateSyncStatusProfiler(Logger),
            MakeRecordedAccess(State));
    }

    THashMap<std::string, TYdbClusterSnapshot> GetClusters(const TYdbTopicInfoControllerPtr& controller)
    {
        return BIND(&TYdbTopicInfoController::GetClusters, controller)
            .AsyncVia(Invoker)
            .Run()
            .BlockingGet()
            .ValueOrThrow();
    }
};

TEST_F(TYdbTopicInfoControllerTest, BorrowedPartitionCountIsReturnedButNeverPersisted)
{
    State->Snapshot.Clusters["a:2135"] = {.PartitionCount = 3};
    State->Snapshot.Clusters["b:2135"] = {};
    auto controller = CreateController();

    auto clusters = GetClusters(controller);
    EXPECT_EQ(clusters["a:2135"].PartitionCount, 3);
    EXPECT_EQ(clusters["b:2135"].PartitionCount, 3);
    EXPECT_FALSE(clusters["b:2135"].DescriptionFailed);
    EXPECT_EQ(State->WriteCount, 0);
    EXPECT_FALSE(State->Snapshot.Clusters["b:2135"].PartitionCount);

    clusters["a:2135"].PartitionCount = 7;
    EXPECT_EQ(GetClusters(controller)["a:2135"].PartitionCount, 3);
    EXPECT_EQ(State->Snapshot.Clusters["a:2135"].PartitionCount, 3);
}

TEST_F(TYdbTopicInfoControllerTest, DisagreeingClustersDoNotLendTheirCount)
{
    State->Snapshot.Clusters["a:2135"] = {.PartitionCount = 3};
    State->Snapshot.Clusters["b:2135"] = {};
    State->Snapshot.Clusters["c:2135"] = {.PartitionCount = 5};
    auto controller = CreateController();

    auto clusters = GetClusters(controller);
    EXPECT_FALSE(clusters["b:2135"].PartitionCount);
    EXPECT_EQ(clusters["a:2135"].PartitionCount, 3);
    EXPECT_EQ(clusters["c:2135"].PartitionCount, 5);
    EXPECT_EQ(State->WriteCount, 0);
}

TEST_F(TYdbTopicInfoControllerTest, ClustersAreReadFromTheCurrentState)
{
    auto readOnlyAt = TInstant::Seconds(1700000000);
    State->Snapshot.Clusters["a:2135"] = {.PartitionCount = 3, .ReadOnlyAt = readOnlyAt};
    auto controller = CreateController();
    EXPECT_EQ(GetClusters(controller)["a:2135"].ReadOnlyAt, readOnlyAt);

    State->Snapshot.Clusters["b:2135"] = {.PartitionCount = 4};
    auto clusters = GetClusters(controller);
    EXPECT_EQ(clusters.size(), 2u);
    EXPECT_EQ(clusters["b:2135"].PartitionCount, 4);
    EXPECT_EQ(clusters["a:2135"].ReadOnlyAt, readOnlyAt);
}

TEST_F(TYdbTopicInfoControllerTest, FailedDiscoveryKeepsKnownClustersAndConcurrentStateChanges)
{
    auto readOnlyAt = TInstant::Seconds(1700000000);
    State->Snapshot.Clusters["a:2135"] = {.PartitionCount = 3, .ReadOnlyAt = readOnlyAt};
    auto clientFactory = New<TFailingDiscoveryClientFactory>([state = State] {
        state->Snapshot.Clusters["c:2135"] = {.PartitionCount = 3};
    });
    auto controller = CreateController(clientFactory);
    BIND(&TYdbTopicInfoController::Init, controller, /*initContext*/ nullptr)
        .AsyncVia(Invoker)
        .Run()
        .BlockingGet()
        .ThrowOnError();
    clientFactory->DiscoveryAttempted.ToFuture().BlockingGet().ThrowOnError();

    auto clusters = GetClusters(controller);
    ASSERT_EQ(clusters.size(), 2u);
    EXPECT_EQ(clusters["a:2135"].PartitionCount, 3);
    EXPECT_EQ(clusters["a:2135"].ReadOnlyAt, readOnlyAt);
    EXPECT_EQ(clusters["c:2135"].PartitionCount, 3);
    EXPECT_EQ(State->Snapshot.Clusters.size(), 2u);
    EXPECT_EQ(State->Snapshot.Clusters["a:2135"].ReadOnlyAt, readOnlyAt);
}

////////////////////////////////////////////////////////////////////////////////

const std::string GraphEndpoint = "a:2135";
const std::string GraphSourceIdentity = "identity";

struct TDescribedPartition
{
    i64 Id;
    bool Active = true;
    std::vector<i64> ParentIds = {};
    std::vector<i64> ChildIds = {};
    std::optional<i64> EndOffsetExclusive = {};
};

NYdb::NTopic::TTopicDescription MakeDescription(const std::vector<TDescribedPartition>& partitions)
{
    Ydb::Topic::DescribeTopicResult result;
    for (const auto& partition : partitions) {
        auto* info = result.add_partitions();
        info->set_partition_id(partition.Id);
        info->set_active(partition.Active);
        for (auto parentId : partition.ParentIds) {
            info->add_parent_partition_ids(parentId);
        }
        for (auto childId : partition.ChildIds) {
            info->add_child_partition_ids(childId);
        }
        if (partition.EndOffsetExclusive) {
            info->mutable_partition_stats()->mutable_partition_offsets()->set_end(*partition.EndOffsetExclusive);
        }
    }
    return NYdb::NTopic::TTopicDescription(std::move(result));
}

TYdbPartitionGraphSnapshot Restart(const TYdbPartitionGraphSnapshot& snapshot)
{
    auto state = New<TYdbPartitionGraphState>();
    WriteYdbStateSnapshot(snapshot, state.Get());
    return ReadYdbStateSnapshot(*NYTree::ConvertTo<TYdbPartitionGraphStatePtr>(NYson::ConvertToYsonString(state)));
}

THashMap<i64, TYdbSourcePartitionSpecPtr> ListSpecs(
    TYdbPartitionGraphSnapshot* snapshot,
    const std::optional<std::vector<std::pair<int, int>>>& partitionFilter = std::nullopt,
    bool finite = false,
    bool descriptionFailed = false)
{
    THashMap<i64, TYdbSourcePartitionSpecPtr> specs;
    auto keys = ListYdbPartitionKeys(GraphEndpoint, &snapshot->Clusters.at(GraphEndpoint), GraphSourceIdentity, partitionFilter, finite, descriptionFailed);
    for (const auto& [key, spec] : keys.Keys) {
        specs[UnpackYdbTopicKey(key).second] = NYTree::ConvertTo<TYdbSourcePartitionSpecPtr>(spec);
    }
    return specs;
}

std::vector<std::pair<std::string, i64>> ReportState(
    TYdbPartitionGraphSnapshot* snapshot,
    i64 partitionId,
    EPartitionState state,
    const std::string& sourceIdentity = GraphSourceIdentity)
{
    auto status = New<TExtendedSourcePartitionStatus>();
    status->PartitionState = state;
    return RecordYdbPartitionStatuses(
        snapshot,
        {{GenerateYdbTopicKey(sourceIdentity, GraphEndpoint, partitionId), status}},
        GraphSourceIdentity);
}

void ReportInputExhausted(TYdbPartitionGraphSnapshot* snapshot, i64 partitionId, bool inputExhausted = true)
{
    auto sourceStatus = New<TYdbSourcePartitionStatus>();
    sourceStatus->InputExhausted = inputExhausted;
    auto status = New<TExtendedSourcePartitionStatus>();
    status->PartitionState = EPartitionState::Executing;
    status->PartitionStatus = NYTree::ConvertTo<NYTree::IMapNodePtr>(sourceStatus);
    auto completed = RecordYdbPartitionStatuses(
        snapshot,
        {{GenerateYdbTopicKey(GraphSourceIdentity, GraphEndpoint, partitionId), status}},
        GraphSourceIdentity);
    EXPECT_TRUE(completed.empty());
}

TEST(TYdbPartitionGraphTest, SplitChildrenAreListedOnlyAfterTheirParentCompletes)
{
    TYdbPartitionGraphSnapshot snapshot;
    const std::vector<TDescribedPartition> partitions = {
        {.Id = 10, .Active = false, .ChildIds = {20, 30}, .EndOffsetExclusive = 5},
        {.Id = 20, .ParentIds = {10}},
        {.Id = 30},
    };
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(partitions));
    snapshot = Restart(snapshot);

    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_FALSE(specs[10]->ReadBlocked);
    EXPECT_EQ(specs[10]->FinalOffsetExclusive, 5);
    EXPECT_FALSE(AreAllYdbPartitionsScheduled(snapshot.Clusters[GraphEndpoint], std::nullopt));

    for (auto state : {EPartitionState::Executing, EPartitionState::Completing}) {
        EXPECT_TRUE(ReportState(&snapshot, 10, state).empty());
    }
    EXPECT_TRUE(ReportState(&snapshot, 10, EPartitionState::Completed, "other-identity").empty());
    snapshot = Restart(snapshot);
    EXPECT_EQ(ListSpecs(&snapshot).size(), 1u);

    EXPECT_EQ(ReportState(&snapshot, 10, EPartitionState::Completed), (std::vector<std::pair<std::string, i64>>{{GraphEndpoint, 10}}));
    snapshot = Restart(snapshot);
    specs = ListSpecs(&snapshot);
    EXPECT_FALSE(specs.contains(10));
    EXPECT_FALSE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs[30]->ReadBlocked);
    EXPECT_FALSE(specs[30]->FinalOffsetExclusive);
    EXPECT_TRUE(AreAllYdbPartitionsScheduled(snapshot.Clusters[GraphEndpoint], std::nullopt));
}

TEST(TYdbPartitionGraphTest, OnlyBlockedPartitionsOfAClusterFailingToDescribeAreUnavailable)
{
    TYdbPartitionGraphSnapshot snapshot;
    SeedLegacyYdbPartitions(&snapshot.Clusters[GraphEndpoint], 2);
    for (bool descriptionFailed : {false, true}) {
        auto specs = ListSpecs(&snapshot, std::nullopt, /*finite*/ false, descriptionFailed);
        ASSERT_EQ(specs.size(), 2u);
        for (const auto& [id, spec] : specs) {
            EXPECT_TRUE(spec->ReadBlocked);
            EXPECT_EQ(spec->TopologyUnavailable, descriptionFailed);
        }
    }

    const std::vector<TDescribedPartition> partitions = {
        {.Id = 0, .Active = false, .ChildIds = {1}, .EndOffsetExclusive = 5},
        {.Id = 1, .ParentIds = {0}},
    };
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(partitions));
    snapshot = Restart(snapshot);
    for (bool descriptionFailed : {false, true}) {
        auto specs = ListSpecs(&snapshot, std::nullopt, /*finite*/ false, descriptionFailed);
        ASSERT_EQ(specs.size(), 2u);
        EXPECT_FALSE(specs[0]->ReadBlocked);
        EXPECT_FALSE(specs[0]->TopologyUnavailable);
        EXPECT_TRUE(specs[1]->ReadBlocked);
        EXPECT_EQ(specs[1]->TopologyUnavailable, descriptionFailed);
    }
}

TEST(TYdbPartitionGraphTest, RepeatedSplitsWaitForEverySelectedAncestor)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    const std::vector<TDescribedPartition> firstSplit = {
        {.Id = 10, .Active = false, .ChildIds = {20}},
        {.Id = 20, .ParentIds = {10}},
    };
    MergeYdbTopicDescription(&graph, MakeDescription(firstSplit));
    const std::vector<TDescribedPartition> secondSplit = {
        {.Id = 10, .Active = false, .ChildIds = {20}},
        {.Id = 20, .Active = false, .ParentIds = {10}, .ChildIds = {40}},
        {.Id = 40, .ParentIds = {20}},
    };
    MergeYdbTopicDescription(&graph, MakeDescription(secondSplit));
    snapshot = Restart(snapshot);
    auto unfiltered = snapshot;
    auto onlyGrandchild = snapshot;

    const std::vector<std::pair<int, int>> withoutIntermediate = {{10, 11}, {40, 41}};
    auto specs = ListSpecs(&snapshot, withoutIntermediate);
    EXPECT_EQ(specs.size(), 1u);
    EXPECT_FALSE(specs[10]->ReadBlocked);
    EXPECT_FALSE(ListSpecs(&onlyGrandchild, std::vector<std::pair<int, int>>{{40, 41}})[40]->ReadBlocked);

    ReportState(&snapshot, 10, EPartitionState::Completed);
    ReportState(&unfiltered, 10, EPartitionState::Completed);
    EXPECT_FALSE(ListSpecs(&snapshot, withoutIntermediate)[40]->ReadBlocked);
    specs = ListSpecs(&unfiltered);
    EXPECT_FALSE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs.contains(40));
}

TEST(TYdbPartitionGraphTest, LegacyPartitionsStayListedWhileTheirReadsWait)
{
    TYdbPartitionGraphSnapshot snapshot;
    SeedLegacyYdbPartitions(&snapshot.Clusters[GraphEndpoint], 3);
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 3u);
    for (const auto& [id, spec] : specs) {
        EXPECT_TRUE(spec->ReadBlocked);
    }

    const std::vector<TDescribedPartition> partitions = {
        {.Id = 0, .Active = false, .ChildIds = {1, 2, 3}, .EndOffsetExclusive = 7},
        {.Id = 1, .ParentIds = {0}},
        {.Id = 2, .ParentIds = {0}},
        {.Id = 3, .ParentIds = {0}},
    };
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(partitions));
    snapshot = Restart(snapshot);
    auto keys = ListYdbPartitionKeys(GraphEndpoint, &snapshot.Clusters[GraphEndpoint], GraphSourceIdentity, std::nullopt, /*finite*/ false, /*descriptionFailed*/ false);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 3u);
    EXPECT_FALSE(specs[0]->ReadBlocked);
    EXPECT_EQ(specs[0]->FinalOffsetExclusive, 7);
    EXPECT_TRUE(specs[1]->ReadBlocked);
    EXPECT_TRUE(specs[2]->ReadBlocked);
    EXPECT_EQ(
        keys.WaitingForOtherPartitions,
        (THashSet<TKey>{
            GenerateYdbTopicKey(GraphSourceIdentity, GraphEndpoint, 1),
            GenerateYdbTopicKey(GraphSourceIdentity, GraphEndpoint, 2),
        }));
}

TEST(TYdbPartitionGraphTest, UnrelatedLegacyPartitionMissingFromTheTopicHoldsReadsUntilDrained)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    SeedLegacyYdbPartitions(&graph, 4);
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 0}, {.Id = 1}, {.Id = 2}, {.Id = 5}}));
    EXPECT_EQ(GetUnresolvedYdbPartitions(graph), std::vector<i64>{3});
    snapshot = Restart(snapshot);
    auto specs = ListSpecs(&snapshot);
    EXPECT_FALSE(specs.contains(5));
    for (const auto& [id, spec] : specs) {
        EXPECT_TRUE(spec->ReadBlocked) << id;
        EXPECT_FALSE(spec->Retired) << id;
    }

    snapshot.Clusters[GraphEndpoint].Partitions[3].Removed = true;
    snapshot = Restart(snapshot);
    specs = ListSpecs(&snapshot);
    EXPECT_TRUE(specs[3]->Retired);
    EXPECT_FALSE(specs[3]->ReadBlocked);
    EXPECT_TRUE(specs[0]->ReadBlocked);
    EXPECT_FALSE(specs.contains(5));

    ReportState(&snapshot, 3, EPartitionState::Completed);
    specs = ListSpecs(&snapshot);
    EXPECT_FALSE(specs.contains(3));
    EXPECT_FALSE(specs[0]->ReadBlocked);
    EXPECT_FALSE(specs[5]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, RemovedParentDrainsBeforeItsChild)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    const std::vector<TDescribedPartition> partitions = {
        {.Id = 10, .Active = false, .ChildIds = {20}, .EndOffsetExclusive = 100},
        {.Id = 20, .ParentIds = {10}},
    };
    MergeYdbTopicDescription(&graph, MakeDescription(partitions));
    EXPECT_EQ(ListSpecs(&snapshot).size(), 1u);
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 20, .ParentIds = {10}}}));
    EXPECT_EQ(GetUnresolvedYdbPartitions(graph), std::vector<i64>{10});
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_TRUE(specs[10]->ReadBlocked);

    graph.Partitions[10].Removed = true;
    snapshot = Restart(snapshot);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_TRUE(specs[10]->Retired);
    EXPECT_EQ(specs[10]->FinalOffsetExclusive, 100);

    ReportState(&snapshot, 10, EPartitionState::Completed);
    EXPECT_FALSE(ListSpecs(&snapshot)[20]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, MissingAncestorWithoutAnOwnerIsDrainedBeforeItsChild)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 20, .ParentIds = {10}}}));
    EXPECT_EQ(GetUnresolvedYdbPartitions(graph), std::vector<i64>{10});
    EXPECT_TRUE(ListSpecs(&snapshot).empty());
    EXPECT_FALSE(AreAllYdbPartitionsScheduled(graph, std::nullopt));

    graph.Partitions[10].Removed = true;
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_TRUE(specs[10]->Retired);

    ReportState(&snapshot, 10, EPartitionState::Completed);
    EXPECT_FALSE(ListSpecs(&snapshot)[20]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, ExhaustedParentListsPausedChildrenBeforeItMayComplete)
{
    TYdbPartitionGraphSnapshot snapshot;
    const std::vector<TDescribedPartition> partitions = {
        {.Id = 10, .Active = false, .ChildIds = {20, 30}, .EndOffsetExclusive = 5},
        {.Id = 20, .ParentIds = {10}},
        {.Id = 30},
    };
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(partitions));

    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_FALSE(specs[10]->CompletionAllowed);
    ReportInputExhausted(&snapshot, 10, /*inputExhausted*/ false);
    EXPECT_FALSE(ListSpecs(&snapshot)[10]->CompletionAllowed);

    ReportInputExhausted(&snapshot, 10);
    snapshot = Restart(snapshot);
    auto keys = ListYdbPartitionKeys(GraphEndpoint, &snapshot.Clusters[GraphEndpoint], GraphSourceIdentity, std::nullopt, /*finite*/ false, /*descriptionFailed*/ false);
    EXPECT_EQ(
        keys.WaitingForOtherPartitions,
        (THashSet<TKey>{
            GenerateYdbTopicKey(GraphSourceIdentity, GraphEndpoint, 20),
            GenerateYdbTopicKey(GraphSourceIdentity, GraphEndpoint, 30),
        }));
    snapshot = Restart(snapshot);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 3u);
    EXPECT_TRUE(specs[10]->CompletionAllowed);
    EXPECT_FALSE(specs[10]->ReadBlocked);
    for (auto childId : {20, 30}) {
        EXPECT_TRUE(specs[childId]->ReadBlocked) << childId;
        EXPECT_FALSE(specs[childId]->CompletionAllowed) << childId;
    }

    ReportState(&snapshot, 10, EPartitionState::Completed);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_FALSE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs[30]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, EndedParentWaitsUntilItsChildrenAreDescribed)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 10, .Active = false, .EndOffsetExclusive = 5}}));
    ReportInputExhausted(&snapshot, 10);
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_FALSE(specs[10]->CompletionAllowed);
    EXPECT_TRUE(ListSpecs(&snapshot, std::nullopt, /*finite*/ true)[10]->CompletionAllowed);

    const std::vector<TDescribedPartition> describedChild = {
        {.Id = 10, .Active = false, .ChildIds = {20}, .EndOffsetExclusive = 5},
        {.Id = 20, .ParentIds = {10}},
    };
    MergeYdbTopicDescription(&graph, MakeDescription(describedChild));
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_TRUE(specs[10]->CompletionAllowed);
    EXPECT_TRUE(specs[20]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, SuccessorsBehindExcludedPartitionsProtectTheirAncestor)
{
    const std::vector<std::pair<int, int>> filter = {{10, 11}, {40, 60}};
    {
        TYdbPartitionGraphSnapshot snapshot;
        const std::vector<TDescribedPartition> describedGrandchildren = {
            {.Id = 10, .Active = false, .ChildIds = {20}},
            {.Id = 20, .Active = false, .ParentIds = {10}, .ChildIds = {40, 50}},
            {.Id = 40, .ParentIds = {20}},
            {.Id = 50, .ParentIds = {20}},
        };
        MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(describedGrandchildren));
        ReportInputExhausted(&snapshot, 10);
        auto specs = ListSpecs(&snapshot, filter);
        ASSERT_EQ(specs.size(), 3u);
        EXPECT_TRUE(specs[10]->CompletionAllowed);
        EXPECT_TRUE(specs[40]->ReadBlocked);
        EXPECT_TRUE(specs[50]->ReadBlocked);
    }
    {
        TYdbPartitionGraphSnapshot snapshot;
        const std::vector<TDescribedPartition> undescribedGrandchildren = {
            {.Id = 10, .Active = false, .ChildIds = {20}},
            {.Id = 20, .Active = false, .ParentIds = {10}},
        };
        MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(undescribedGrandchildren));
        ReportInputExhausted(&snapshot, 10);
        auto specs = ListSpecs(&snapshot, filter);
        ASSERT_EQ(specs.size(), 1u);
        EXPECT_FALSE(specs[10]->CompletionAllowed);
        EXPECT_TRUE(ListSpecs(&snapshot, filter, /*finite*/ true)[10]->CompletionAllowed);
    }
    {
        TYdbPartitionGraphSnapshot snapshot;
        const std::vector<TDescribedPartition> childWithoutKnownChildren = {
            {.Id = 10, .Active = false, .ChildIds = {20}},
            {.Id = 20, .ParentIds = {10}},
        };
        MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(childWithoutKnownChildren));
        ReportInputExhausted(&snapshot, 10);
        auto specs = ListSpecs(&snapshot, filter);
        ASSERT_EQ(specs.size(), 1u);
        EXPECT_FALSE(specs[10]->CompletionAllowed);
        EXPECT_TRUE(ListSpecs(&snapshot, filter, /*finite*/ true)[10]->CompletionAllowed);

        const std::vector<TDescribedPartition> splitChild = {
            {.Id = 10, .Active = false, .ChildIds = {20}},
            {.Id = 20, .Active = false, .ParentIds = {10}, .ChildIds = {40}},
            {.Id = 40, .ParentIds = {20}},
        };
        MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(splitChild));
        specs = ListSpecs(&snapshot, filter);
        ASSERT_EQ(specs.size(), 2u);
        EXPECT_TRUE(specs[10]->CompletionAllowed);
        EXPECT_TRUE(specs[40]->ReadBlocked);
    }
}

TEST(TYdbPartitionGraphTest, RepeatedSplitsHandTheWatermarkDownEveryGeneration)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    const std::vector<TDescribedPartition> repeatedSplits = {
        {.Id = 10, .Active = false, .ChildIds = {20}},
        {.Id = 20, .Active = false, .ParentIds = {10}, .ChildIds = {40}},
        {.Id = 40, .ParentIds = {20}},
    };
    MergeYdbTopicDescription(&graph, MakeDescription(repeatedSplits));
    ReportInputExhausted(&snapshot, 10);
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_TRUE(specs[10]->CompletionAllowed);
    EXPECT_TRUE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs[20]->CompletionAllowed);

    ReportState(&snapshot, 10, EPartitionState::Completed);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_FALSE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs[20]->CompletionAllowed);

    ReportInputExhausted(&snapshot, 20);
    snapshot = Restart(snapshot);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_TRUE(specs[20]->CompletionAllowed);
    EXPECT_TRUE(specs[40]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, OnlyFiniteSourceCompletesAPartitionWithoutChildren)
{
    TYdbPartitionGraphSnapshot snapshot;
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription({{.Id = 0}}));
    EXPECT_FALSE(ListSpecs(&snapshot)[0]->CompletionAllowed);
    EXPECT_TRUE(ListSpecs(&snapshot, std::nullopt, /*finite*/ true)[0]->CompletionAllowed);
}

TEST(TYdbPartitionGraphTest, UnrelatedLegacyPartitionListsEveryPossibleSuccessorBeforeItCompletes)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    SeedLegacyYdbPartitions(&graph, 4);
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 0}, {.Id = 1}, {.Id = 2}, {.Id = 5}}));
    graph.Partitions[3].Removed = true;

    auto specs = ListSpecs(&snapshot);
    EXPECT_FALSE(specs.contains(5));
    EXPECT_TRUE(specs[3]->Retired);
    EXPECT_FALSE(specs[3]->CompletionAllowed);

    ReportInputExhausted(&snapshot, 3);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 5u);
    EXPECT_TRUE(specs[3]->CompletionAllowed);
    for (auto id : {0, 1, 2, 5}) {
        EXPECT_TRUE(specs[id]->ReadBlocked) << id;
    }
}

TEST(TYdbPartitionGraphTest, ChildKnownOnlyFromItsParentDoesNotBlockOtherReads)
{
    TYdbPartitionGraphSnapshot snapshot;
    const std::vector<TDescribedPartition> childKnownOnlyFromItsParent = {
        {.Id = 10, .Active = false, .ChildIds = {20}, .EndOffsetExclusive = 5},
        {.Id = 30},
    };
    MergeYdbTopicDescription(&snapshot.Clusters[GraphEndpoint], MakeDescription(childKnownOnlyFromItsParent));
    auto specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_FALSE(specs[10]->ReadBlocked);
    EXPECT_FALSE(specs[30]->ReadBlocked);

    ReportInputExhausted(&snapshot, 10);
    specs = ListSpecs(&snapshot);
    ASSERT_EQ(specs.size(), 3u);
    EXPECT_TRUE(specs[10]->CompletionAllowed);
    EXPECT_TRUE(specs[20]->ReadBlocked);
    EXPECT_FALSE(specs[30]->ReadBlocked);
}

TEST(TYdbPartitionGraphTest, ListingStartsWithTheFirstSelectedPartition)
{
    TYdbPartitionGraphSnapshot snapshot;
    auto& graph = snapshot.Clusters[GraphEndpoint];
    EXPECT_FALSE(HasYdbPartitionListingStarted(graph, std::nullopt));
    MergeYdbTopicDescription(&graph, MakeDescription({{.Id = 0}, {.Id = 1}}));
    EXPECT_FALSE(HasYdbPartitionListingStarted(graph, std::nullopt));
    EXPECT_FALSE(HasYdbPartitionListingStarted(graph, std::vector<std::pair<int, int>>{{5, 6}}));
    ListSpecs(&snapshot);
    EXPECT_TRUE(HasYdbPartitionListingStarted(graph, std::nullopt));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
