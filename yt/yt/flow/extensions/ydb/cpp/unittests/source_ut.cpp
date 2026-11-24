#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/fakes.h>
#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/ordinary_sink_contract_helpers.h>
#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/source_contract.h>

#include <yt/yt/flow/extensions/ydb/cpp/helpers.h>
#include <yt/yt/flow/extensions/ydb/cpp/options.h>
#include <yt/yt/flow/extensions/ydb/cpp/payload_splitter.h>
#include <yt/yt/flow/extensions/ydb/cpp/source.h>
#include <yt/yt/flow/extensions/ydb/cpp/source_engine.h>
#include <yt/yt/flow/extensions/ydb/cpp/state.h>
#include <yt/yt/flow/extensions/ydb/cpp/topic_info.h>
#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/source_context.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>

#include <deque>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NYdbTesting;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

ISourceControllerPtr CreatePublicController(const std::string& database, const std::string& topic)
{
    auto resourceContext = New<TResourceContext>();
    resourceContext->ResourceSpec = New<TResourceSpec>();
    resourceContext->ResourceSpec->ResourceClassName = TypeName<TYdbTopicDriver>();
    resourceContext->Logger = TestLogger;
    auto dynamicResourceContext = New<TDynamicResourceContext>();
    dynamicResourceContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    auto resource = New<TFakeYdbResource>(
        resourceContext,
        dynamicResourceContext,
        std::make_shared<TFakeYdbWriteState>(),
        New<TFakeDeferredPublicationClient>());

    auto context = New<TSourceControllerContext>();
    context->SourceSpec = New<TSourceSpec>();
    context->SourceSpec->SourceClassName = TypeName<TYdbSource>();
    context->SourceSpec->Parameters = BuildYsonNodeFluently()
        .BeginMap()
        .Item("endpoint")
        .Value("localhost:2135")
        .Item("database")
        .Value(database)
        .Item("topic")
        .Value(topic)
        .Item("consumer")
        .Value("consumer")
        .EndMap()
        ->AsMap();
    context->SourceStreamId = TStreamId("input");
    context->Invoker = GetSyncInvoker();
    context->Logger = TestLogger;
    context->StatusProfiler = CreateSyncStatusProfiler(TestLogger);
    context->StaticResources.emplace(YdbDriverDefaultResourceId, resource);
    auto dynamicContext = New<TDynamicSourceControllerContext>();
    dynamicContext->DynamicSourceSpec = New<TDynamicSourceSpec>();
    return TRegistry::Get()->CreateSourceController(context, dynamicContext);
}

TEST(TYdbSourceControllerTest, IdentityIsNamespacedByConfiguredAddress)
{
    auto controller = CreatePublicController("/Root/database", "folder/topic");
    EXPECT_EQ(
        controller->GetSourceIdentity(),
        MakeSourceIdentity({"ydb-topic/v1", "/Root/database", "folder/topic"}));
    EXPECT_NE(
        controller->GetSourceIdentity(),
        MakeSourceIdentity({"/Root/database", "folder/topic"}));
    EXPECT_NE(
        controller->GetSourceIdentity(),
        CreatePublicController("/Root/other", "folder/topic")->GetSourceIdentity());
}

TEST(TYdbSourceControllerTest, KeysRetainThreeColumnShape)
{
    const auto identity = MakeSourceIdentity({"ydb-topic/v1", "/Root", "topic"});
    const auto key = GenerateYdbTopicKey(identity, "localhost:2135", 3);
    EXPECT_EQ(key.Underlying().GetCount(), 3);
    EXPECT_EQ(UnpackYdbTopicKey(key), std::pair(std::string("localhost:2135"), 3));
}

class TYdbSourceControllerLifecycleTest
    : public ::testing::Test
{
protected:
    NOrdinarySinkContract::TFakeTopicServer Server;
    const TActionQueuePtr Queue = New<TActionQueue>("SourceController");
    const IInvokerPtr Invoker = CreateSerializedInvoker(Queue->GetInvoker());
    TStateManagerMockPtr StateManager = New<TStateManagerMock>();
    const NOrdinarySinkContract::TTopicServerResourcePtr Resource =
        NOrdinarySinkContract::CreateTopicServerResource(TypeName<TYdbTopicDriver>(), Server.GetEndpoint());
    const std::string SourceIdentity = MakeSourceIdentity({"ydb-topic/v1", "/Root", "topic"});

    template <class TFunction>
    auto Run(TFunction function)
    {
        return BIND(std::move(function))
            .AsyncVia(Invoker)
            .Run()
            .BlockingGet()
            .ValueOrThrow();
    }

    using TGraphPredicate = std::function<bool(const TYdbPartitionGraph&)>;

    static bool IsTopologyKnown(const TYdbPartitionGraph& graph)
    {
        return graph.TopologyKnown;
    }

    ISourceControllerPtr StartController(bool finite = false, TGraphPredicate isDescribed = IsTopologyKnown)
    {
        auto context = New<TSourceControllerContext>();
        context->SourceSpec = New<TSourceSpec>();
        context->SourceSpec->SourceClassName = TypeName<TYdbSource>();
        // clang-format off
        context->SourceSpec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
                .Item("endpoint").Value(Server.GetEndpoint())
                .Item("database").Value("/Root")
                .Item("topic").Value("topic")
                .Item("consumer").Value("consumer")
                .Item("finite").Value(finite)
                .Item("update_partition_count_period").Value(TDuration::Hours(1))
            .EndMap()
            ->AsMap();
        // clang-format on
        context->SourceStreamId = TStreamId("input");
        context->ComputationSpec = New<TComputationSpec>();
        context->TimeProvider = New<NOrdinarySinkContract::TControlledTimeProvider>();
        context->Invoker = Invoker;
        context->Logger = TestLogger;
        context->StatusProfiler = CreateSyncStatusProfiler(TestLogger);
        context->StaticResources.emplace(YdbDriverDefaultResourceId, Resource);
        auto dynamicContext = New<TDynamicSourceControllerContext>();
        dynamicContext->DynamicSourceSpec = New<TDynamicSourceSpec>();
        auto controller = TRegistry::Get()->CreateSourceController(context, dynamicContext);

        const int descriptionCount = Server.GetTopicDescriptionCount();
        Run([&] {
            controller->Init(StateManager->CreateContext());
            return true;
        });
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (Server.GetTopicDescriptionCount() == descriptionCount) {
            EXPECT_LT(TInstant::Now(), deadline);
            Sleep(TDuration::MilliSeconds(10));
        }
        while (true) {
            auto graph = ReadPartitionGraph();
            if (graph && isDescribed(*graph)) {
                break;
            }
            if (TInstant::Now() > deadline) {
                ADD_FAILURE() << "The description of the topic was not merged in time";
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        return controller;
    }

    std::optional<TYdbPartitionGraph> ReadPartitionGraph()
    {
        Run([&] {
            StateManager->Sync();
            return true;
        });
        auto state = StateManager->TryGet("/ydb_partition_graph/v0");
        if (!state) {
            return std::nullopt;
        }
        auto snapshot = ReadYdbStateSnapshot(*ConvertTo<TYdbPartitionGraphStatePtr>(*state));
        auto* graph = snapshot.Clusters.FindPtr(Server.GetEndpoint());
        return graph ? std::optional(*graph) : std::nullopt;
    }

    ISourceControllerPtr RestartController(bool finite = false, TGraphPredicate isDescribed = IsTopologyKnown)
    {
        Run([&] {
            StateManager->Sync();
            return true;
        });
        auto storage = StateManager->GetStorage();
        StateManager = New<TStateManagerMock>();
        StateManager->SetStorage(std::move(storage));
        return StartController(finite, std::move(isDescribed));
    }

    void WriteLegacyPartitionCount(i64 partitionCount)
    {
        auto state = New<TYdbTopicInfoControllerState>();
        state->CachedPartitionCounts[Server.GetEndpoint()] = partitionCount;
        StateManager->Set("/ydb_info/v0", ConvertToYsonString(state));
    }

    TKey MakePartitionKey(i64 partitionId)
    {
        return GenerateYdbTopicKey(SourceIdentity, Server.GetEndpoint(), partitionId);
    }

    THashMap<i64, TYdbSourcePartitionSpecPtr> ListPartitions(
        const ISourceControllerPtr& controller,
        THashMap<i64, EPartitionState> states = {},
        const THashSet<i64>& inputExhausted = {})
    {
        return Run([&] {
            THashMap<TKey, TExtendedSourcePartitionStatusPtr> statuses;
            for (auto partitionId : inputExhausted) {
                states.emplace(partitionId, EPartitionState::Executing);
            }
            for (const auto& [partitionId, state] : states) {
                auto status = New<TExtendedSourcePartitionStatus>();
                status->PartitionState = state;
                auto sourceStatus = New<TYdbSourcePartitionStatus>();
                sourceStatus->InputExhausted = inputExhausted.contains(partitionId);
                status->PartitionStatus = ConvertTo<IMapNodePtr>(sourceStatus);
                statuses[MakePartitionKey(partitionId)] = std::move(status);
            }
            controller->ProcessPartitionStatuses(statuses);
            THashMap<i64, TYdbSourcePartitionSpecPtr> partitions;
            auto keys = controller->ListKeys();
            if (!keys) {
                ADD_FAILURE() << "The controller listed no keys";
                return partitions;
            }
            for (const auto& [key, spec] : *keys) {
                partitions[UnpackYdbTopicKey(key).second] = ConvertTo<TYdbSourcePartitionSpecPtr>(spec);
            }
            return partitions;
        });
    }

    bool IsExcludedFromReadAlignment(const ISourceControllerPtr& controller, i64 partitionId)
    {
        return Run([&] {
            return controller->IsExcludedFromReadAlignment(MakePartitionKey(partitionId));
        });
    }

    EStreamState GetFutureKeysState(const ISourceControllerPtr& controller)
    {
        return Run([&] {
            return (*controller->GetFutureKeysStreamTraverseData())->State;
        });
    }

    TSystemTimestamp GetFutureKeysEventWatermark(const ISourceControllerPtr& controller)
    {
        return Run([&] {
            return (*controller->GetFutureKeysStreamTraverseData())->EventWatermark;
        });
    }
};

TEST_F(TYdbSourceControllerLifecycleTest, LegacyPartitionsAreKeptWhenTheFirstDescriptionArrivesFirst)
{
    WriteLegacyPartitionCount(3);
    Server.SetPartitions({{.Id = 0}, {.Id = 1}});
    auto controller = StartController(/*finite*/ false, [] (const TYdbPartitionGraph& graph) {
        return graph.TopologyKnown && AllOf(graph.Partitions, [] (const auto& item) {
            return item.second.Present || item.second.Removed;
        });
    });
    ASSERT_TRUE(StateManager->Contains("/ydb_info/v0"));

    auto partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 3u);
    EXPECT_TRUE(partitions[2]->Retired);
    EXPECT_TRUE(partitions[0]->ReadBlocked);
    EXPECT_TRUE(partitions[1]->ReadBlocked);
    EXPECT_TRUE(IsExcludedFromReadAlignment(controller, 0));
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 2));

    partitions = ListPartitions(controller, {{2, EPartitionState::Executing}, {0, EPartitionState::Executing}});
    EXPECT_TRUE(partitions[0]->ReadBlocked);

    controller = RestartController();
    partitions = ListPartitions(controller, {{2, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_FALSE(partitions[0]->ReadBlocked);
    EXPECT_FALSE(partitions[1]->ReadBlocked);
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 0));
}

TEST_F(TYdbSourceControllerLifecycleTest, PartitionDescribedOnlyOnItsOwnIsReadBeforeItsChild)
{
    Server.SetPartitions(
        {{.Id = 20, .ParentIds = {10}}},
        {{.Id = 10, .Active = false, .ChildIds = {20}, .EndOffset = 5}});
    auto controller = StartController(/*finite*/ false, [] (const TYdbPartitionGraph& graph) {
        return graph.Partitions.contains(10) && graph.Partitions.at(10).Present;
    });

    auto partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[10]->ReadBlocked);
    EXPECT_FALSE(partitions[10]->Retired);
    EXPECT_EQ(partitions[10]->FinalOffsetExclusive, 5);

    Server.SetPartitionDescriptionsAvailable(false);
    controller = RestartController(/*finite*/ false, [] (const TYdbPartitionGraph& graph) {
        return !graph.Partitions.at(10).Present;
    });
    partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_TRUE(partitions[10]->ReadBlocked);
    EXPECT_FALSE(partitions[10]->Retired);
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 10));

    partitions = ListPartitions(controller, {{10, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[20]->ReadBlocked);
}

TEST_F(TYdbSourceControllerLifecycleTest, ChildrenStartOnlyAfterTheirParentCompletesAcrossRestarts)
{
    Server.SetPartitions({
        {.Id = 10, .Active = false, .ChildIds = {20, 30}, .EndOffset = 5},
        {.Id = 20, .Active = false, .ParentIds = {10}, .ChildIds = {40}, .EndOffset = 9},
        {.Id = 30, .ParentIds = {10}},
        {.Id = 40, .ParentIds = {20}},
    });
    auto controller = StartController(/*finite*/ true);

    auto partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[10]->ReadBlocked);
    EXPECT_EQ(GetFutureKeysState(controller), EStreamState::Drained);

    for (auto state : {EPartitionState::Executing, EPartitionState::Completing}) {
        partitions = ListPartitions(controller, {{10, state}});
        EXPECT_EQ(partitions.size(), 1u);
        EXPECT_EQ(GetFutureKeysState(controller), EStreamState::Drained);
    }

    controller = RestartController(/*finite*/ true);
    EXPECT_EQ(ListPartitions(controller).size(), 1u);
    partitions = ListPartitions(controller, {{10, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_FALSE(partitions[20]->ReadBlocked);
    EXPECT_EQ(partitions[20]->FinalOffsetExclusive, 9);
    EXPECT_FALSE(partitions[30]->ReadBlocked);
    EXPECT_EQ(GetFutureKeysState(controller), EStreamState::Drained);

    controller = RestartController(/*finite*/ true);
    partitions = ListPartitions(controller, {{20, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_FALSE(partitions[40]->ReadBlocked);
    EXPECT_EQ(GetFutureKeysState(controller), EStreamState::Completed);

    controller = RestartController(/*finite*/ false);
    EXPECT_EQ(GetFutureKeysState(controller), EStreamState::Drained);
}

TEST_F(TYdbSourceControllerLifecycleTest, OwnedChildKeepsItsKeyWhileItsParentDrains)
{
    WriteLegacyPartitionCount(2);
    Server.SetPartitions({
        {.Id = 0, .Active = false, .ChildIds = {1}, .EndOffset = 3},
        {.Id = 1, .ParentIds = {0}},
    });
    auto controller = StartController();

    auto partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_FALSE(partitions[0]->ReadBlocked);
    EXPECT_TRUE(partitions[1]->ReadBlocked);
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 0));
    EXPECT_TRUE(IsExcludedFromReadAlignment(controller, 1));

    controller = RestartController();
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 1));
    partitions = ListPartitions(controller, {{0, EPartitionState::Completing}});
    EXPECT_TRUE(partitions[1]->ReadBlocked);
    EXPECT_TRUE(IsExcludedFromReadAlignment(controller, 1));

    partitions = ListPartitions(controller, {{0, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[1]->ReadBlocked);
    EXPECT_FALSE(IsExcludedFromReadAlignment(controller, 1));
}

TEST_F(TYdbSourceControllerLifecycleTest, BlockedPartitionsAreUnavailableOnlyWhileTheTopicFailsToBeDescribed)
{
    WriteLegacyPartitionCount(2);
    Server.SetTopicDescriptionsAvailable(false);
    auto controller = StartController(/*finite*/ false, [] (const TYdbPartitionGraph& /*graph*/) {
        return true;
    });

    THashMap<i64, TYdbSourcePartitionSpecPtr> partitions;
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    while (true) {
        partitions = ListPartitions(controller);
        ASSERT_EQ(partitions.size(), 2u);
        if (partitions[0]->TopologyUnavailable || TInstant::Now() > deadline) {
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }
    for (const auto& [partitionId, spec] : partitions) {
        EXPECT_TRUE(spec->ReadBlocked);
        EXPECT_TRUE(spec->TopologyUnavailable);
    }

    Server.SetTopicDescriptionsAvailable(true);
    Server.SetPartitions({
        {.Id = 0, .Active = false, .ChildIds = {1}, .EndOffset = 3},
        {.Id = 1, .ParentIds = {0}},
    });
    controller = RestartController();
    partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_FALSE(partitions[0]->ReadBlocked);
    EXPECT_FALSE(partitions[0]->TopologyUnavailable);
    EXPECT_TRUE(partitions[1]->ReadBlocked);
    EXPECT_FALSE(partitions[1]->TopologyUnavailable);
}

TEST_F(TYdbSourceControllerLifecycleTest, ExhaustedParentCompletesOnlyAfterItsChildIsListedAcrossRestarts)
{
    Server.SetPartitions({
        {.Id = 10, .Active = false, .ChildIds = {20}, .EndOffset = 5},
        {.Id = 20, .ParentIds = {10}},
    });
    auto controller = StartController();
    EXPECT_EQ(GetFutureKeysEventWatermark(controller), ZeroSystemTimestamp);

    auto partitions = ListPartitions(controller);
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[10]->CompletionAllowed);
    EXPECT_GT(GetFutureKeysEventWatermark(controller), ZeroSystemTimestamp);

    partitions = ListPartitions(controller, {{10, EPartitionState::Executing}});
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[10]->CompletionAllowed);

    partitions = ListPartitions(controller, {}, {10});
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_TRUE(partitions[10]->CompletionAllowed);
    EXPECT_TRUE(partitions[20]->ReadBlocked);
    EXPECT_TRUE(IsExcludedFromReadAlignment(controller, 20));

    controller = RestartController();
    partitions = ListPartitions(controller, {{10, EPartitionState::Completing}});
    ASSERT_EQ(partitions.size(), 2u);
    EXPECT_TRUE(partitions[10]->CompletionAllowed);
    EXPECT_TRUE(partitions[20]->ReadBlocked);

    partitions = ListPartitions(controller, {{10, EPartitionState::Completed}});
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_FALSE(partitions[20]->ReadBlocked);
    EXPECT_FALSE(partitions[20]->CompletionAllowed);
}

TEST(TYdbSourceSchemaTest, FrameMetaIsAnOrdinaryAttributesColumnName)
{
    EXPECT_EQ(GetYdbTopicSourceSchema(std::nullopt)->GetColumnCount(), 4);
    auto schema = GetYdbTopicSourceSchema("frame_meta");
    EXPECT_EQ(schema->GetColumnCount(), 5);
    EXPECT_EQ(schema->GetColumnIndexOrThrow("frame_meta"), 4);
}

////////////////////////////////////////////////////////////////////////////////

const std::string LiveConsumer = "consumer";
constexpr auto LiveUpdateInfoPeriod = TDuration::MilliSeconds(200);

struct TBoundedPartitionSpec
    : public TYdbSourcePartitionSpec
{
    int Limit = 1;

    REGISTER_YSON_STRUCT(TBoundedPartitionSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("limit", &TThis::Limit)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TBoundedPartitionSpec);

class TBoundedPartitionYdbSource
    : public TYdbSource
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TBoundedPartitionSpec);

    using TYdbSource::TYdbSource;
    using TYdbSourceBase::ApplyPartitionInfoUpdate;
    using TYdbSourceBase::CaptureReadSettings;
    using TYdbSourceBase::DoFetchTrimmedOffset;
    using TYdbSourceBase::GetDynamicParameters;
};

YT_FLOW_DEFINE_SOURCE(TBoundedPartitionYdbSource);

IMapNodePtr MakePausedPartitionSpec(TStringBuf pause)
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item(pause).Value(true)
        .EndMap()
        ->AsMap();
    // clang-format on
}

IResourcePtr CreateRegisteredDriver()
{
    auto context = New<TResourceContext>();
    context->ResourceId = YdbDriverDefaultResourceId;
    context->ResourceSpec = New<TResourceSpec>();
    context->ResourceSpec->ResourceClassName = TypeName<TYdbTopicDriver>();
    context->Logger = TestLogger;
    context->StatusProfiler = CreateSyncStatusProfiler(TestLogger);
    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    auto resource = TRegistry::Get()->CreateResource(context, dynamicContext);
    WaitFor(resource->Load({})).ThrowOnError();
    return resource;
}

IMapNodePtr MakeSourceParameters(const std::string& endpoint, const std::string& database, const std::string& topic)
{
    return BuildYsonNodeFluently()
        .BeginMap()
        .Item("endpoint")
        .Value(endpoint)
        .Item("database")
        .Value(database)
        .Item("topic")
        .Value(topic)
        .Item("consumer")
        .Value(LiveConsumer)
        .EndMap()
        ->AsMap();
}

TSourceFamily MakeOfflineFamily()
{
    auto parameters = MakeSourceParameters("localhost:1", "/Root", "topic");
    return {
        .SourceClassName = TypeName<TBoundedPartitionYdbSource>(),
        .Parameters = parameters,
        .SourceKey = GenerateYdbTopicKey(MakeSourceIdentity({"ydb-topic/v1", "/Root", "topic"}), "localhost:1", 0),
        .Driver = CreateRegisteredDriver(),
    };
}

TEST(TYdbSourceShellTest, RepositionedPartitionUpdateSkipsWithoutReportingTrimmedData)
{
    CheckRepositionedPartitionUpdate(MakeOfflineFamily(), [] (const ISourcePtr& source, const TYdbPartitionInfoUpdate& update) {
        DynamicPointerCast<TBoundedPartitionYdbSource>(source)->ApplyPartitionInfoUpdate(update);
    });
}

TEST(TYdbSourceShellTest, RetiredPartitionDrainsToTheMaximumItHasSeen)
{
    auto family = MakeOfflineFamily();
    auto stateManager = New<TStateManagerMock>();
    {
        TSourceRun original(family, MakeDynamicSourceContext(), stateManager);
        original.RunInQueue([&] {
            DynamicPointerCast<TBoundedPartitionYdbSource>(original.GetSource())->ApplyPartitionInfoUpdate({
                .MaxOffsetExclusive = 100,
                .UpdateInstant = TInstant::Now(),
            });
            original.GetSource()->Sync();
            stateManager->Sync();
        });
    }

    // clang-format off
    auto retiredSpec = BuildYsonNodeFluently()
        .BeginMap()
            .Item("retired").Value(true)
            .Item("final_offset_exclusive").Value(7)
        .EndMap()
        ->AsMap();
    // clang-format on
    TSourceRun retired(family, MakeDynamicSourceContext({}, retiredSpec), stateManager);
    auto inflight = retired.RunInQueue([&] {
        retired.GetSource()->Sync();
        stateManager->Sync();
        return retired.GetSource()->BuildInflight();
    });
    EXPECT_TRUE(inflight->Empty);
    auto state = ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"));
    EXPECT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 100);
}

TEST(TYdbSourceShellTest, ExhaustedPartitionCompletesOnlyWhenAllowed)
{
    auto family = MakeOfflineFamily();
    auto stateManager = New<TStateManagerMock>();
    auto makeContext = [] (bool completionAllowed, bool draining) {
        // clang-format off
        auto spec = BuildYsonNodeFluently()
            .BeginMap()
                .Item("retired").Value(true)
                .Item("final_offset_exclusive").Value(7)
                .Item("completion_allowed").Value(completionAllowed)
            .EndMap()
            ->AsMap();
        // clang-format on
        auto context = MakeDynamicSourceContext({}, spec);
        context->DynamicSourceSpec->Draining = draining;
        return context;
    };
    TSourceRun run(family, makeContext(/*completionAllowed*/ false, /*draining*/ false), stateManager);
    auto buildInflight = [&] {
        return run.RunInQueue([&] {
            run.GetSource()->Sync();
            stateManager->Sync();
            return run.GetSource()->BuildInflight();
        });
    };
    auto isInputExhausted = [&] {
        return ConvertTo<TYdbSourcePartitionStatusPtr>(run.GetSource()->GetPartitionStatus())->InputExhausted;
    };
    auto reconfigure = [&] (bool completionAllowed, bool draining) {
        DynamicPointerCast<TSourceBase>(run.GetSource())->Reconfigure(makeContext(completionAllowed, draining));
    };

    auto held = buildInflight();
    EXPECT_TRUE(isInputExhausted());
    EXPECT_FALSE(held->Empty);
    EXPECT_FALSE(held->Suspended);
    EXPECT_EQ(held->InflightMetrics->Count, 1);
    EXPECT_EQ(held->InflightMetrics->ReadyCount, 0);
    EXPECT_FALSE(held->InflightMetrics->IdleDuration);
    EXPECT_FALSE(held->InflightMetrics->LastIdleTimestamp);

    reconfigure(/*completionAllowed*/ false, /*draining*/ true);
    auto heldWhileDraining = buildInflight();
    reconfigure(/*completionAllowed*/ true, /*draining*/ true);
    auto allowedWhileDraining = buildInflight();
    EXPECT_FALSE(heldWhileDraining->Empty);
    EXPECT_TRUE(heldWhileDraining->Suspended);
    EXPECT_TRUE(allowedWhileDraining->Empty);
    EXPECT_TRUE(allowedWhileDraining->Suspended);

    reconfigure(/*completionAllowed*/ true, /*draining*/ false);
    auto released = buildInflight();
    EXPECT_TRUE(isInputExhausted());
    EXPECT_TRUE(released->Empty);
    EXPECT_EQ(released->InflightMetrics->Count, 0);
}

TEST(TYdbSourceShellTest, ReadBlockedPartitionIsNeitherExhaustedNorIdle)
{
    // clang-format off
    auto spec = BuildYsonNodeFluently()
        .BeginMap()
            .Item("read_blocked").Value(true)
            .Item("final_offset_exclusive").Value(0)
        .EndMap()
        ->AsMap();
    // clang-format on
    TSourceRun run(MakeOfflineFamily(), MakeDynamicSourceContext({}, spec), New<TStateManagerMock>());
    auto inflight = run.RunInQueue([&] {
        return run.GetSource()->BuildInflight();
    });
    EXPECT_FALSE(inflight->Empty);
    EXPECT_EQ(inflight->InflightMetrics->Count, 1);
    EXPECT_EQ(inflight->InflightMetrics->ReadyCount, 0);
    EXPECT_FALSE(inflight->InflightMetrics->IdleDuration);
    EXPECT_FALSE(ConvertTo<TYdbSourcePartitionStatusPtr>(run.GetSource()->GetPartitionStatus())->InputExhausted);
}

TEST(TYdbSourceShellTest, BlockedPartitionIsUnavailableOnlyWhileItsTopologyIs)
{
    auto makeContext = [] (bool topologyUnavailable) {
        // clang-format off
        auto spec = BuildYsonNodeFluently()
            .BeginMap()
                .Item("read_blocked").Value(true)
                .Item("topology_unavailable").Value(topologyUnavailable)
            .EndMap()
            ->AsMap();
        // clang-format on
        return MakeDynamicSourceContext({{"unavailable_threshold", ConvertToNode(TDuration::MilliSeconds(1))}}, spec);
    };
    TSourceRun run(MakeOfflineFamily(), makeContext(/*topologyUnavailable*/ false), New<TStateManagerMock>());
    auto buildInflight = [&] {
        return run.RunInQueue([&] {
            return run.GetSource()->BuildInflight();
        });
    };
    auto reconfigure = [&] (bool topologyUnavailable) {
        DynamicPointerCast<TSourceBase>(run.GetSource())->Reconfigure(makeContext(topologyUnavailable));
    };

    auto pending = buildInflight();
    EXPECT_FALSE(pending->Empty);
    EXPECT_EQ(pending->InflightMetrics->Count, 1);
    EXPECT_FALSE(pending->InflightMetrics->UnavailableTimestamp);

    reconfigure(/*topologyUnavailable*/ true);
    buildInflight();
    Sleep(TDuration::MilliSeconds(10));
    auto unavailable = buildInflight();
    EXPECT_FALSE(unavailable->Empty);
    EXPECT_EQ(unavailable->InflightMetrics->Count, 1);
    EXPECT_TRUE(unavailable->InflightMetrics->UnavailableTimestamp);

    reconfigure(/*topologyUnavailable*/ false);
    auto recovered = buildInflight();
    EXPECT_FALSE(recovered->Empty);
    EXPECT_EQ(recovered->InflightMetrics->Count, 1);
    EXPECT_FALSE(recovered->InflightMetrics->UnavailableTimestamp);
}

TEST(TYdbSourceShellTest, RejectedPartitionSpecKeepsContextAndEffectiveOptions)
{
    CheckRejectedPartitionSpec(MakeOfflineFamily(), [] (const ISourcePtr& source) {
        auto concrete = DynamicPointerCast<TBoundedPartitionYdbSource>(source);
        return std::pair(concrete->GetDynamicPartitionSpec()->Limit, ToString(concrete->GetDynamicParameters()->MalformedMessagePolicy));
    });
}

TEST(TYdbSourceShellTest, ReadSettingsCaptureThePolicyTheEngineApplied)
{
    TSourceRun run(
        MakeOfflineFamily(),
        MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("fail")}}),
        New<TStateManagerMock>());
    auto source = DynamicPointerCast<TBoundedPartitionYdbSource>(run.GetSource());
    auto capturePolicy = [&] {
        return run.RunInQueue([&] {
            return ConvertToNode(source->CaptureReadSettings())->AsMap()->GetChildValueOrThrow<std::string>("malformed_message_policy");
        });
    };
    run.GetInvoker()->StartHolding();
    source->Reconfigure(MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("keep")}}));
    source->Reconfigure(MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("drop")}}));
    run.GetInvoker()->StopHolding();
    ASSERT_EQ(run.GetInvoker()->GetHeldCount(), 2);
    EXPECT_EQ(capturePolicy(), "fail");

    run.GetInvoker()->ReleaseNext();
    EXPECT_EQ(source->GetDynamicParameters()->MalformedMessagePolicy, EMalformedYdbMessagePolicy::Drop);
    EXPECT_EQ(capturePolicy(), "keep");

    run.GetInvoker()->ReleaseNext();
    EXPECT_EQ(capturePolicy(), "drop");
}

TEST(TYdbSourceShellTest, PausedPartitionDoesNotProbeTrimmedOffset)
{
    for (auto pause : {"read_blocked", "retired"}) {
        TSourceRun run(MakeOfflineFamily(), MakeDynamicSourceContext({}, MakePausedPartitionSpec(pause)), New<TStateManagerMock>());
        auto source = DynamicPointerCast<TBoundedPartitionYdbSource>(run.GetSource());
        auto trimmedOffset = run.RunInQueue([&] {
            return WaitFor(source->DoFetchTrimmedOffset()).ValueOrThrow();
        });
        EXPECT_FALSE(trimmedOffset) << pause;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYdbLiveSourceTest
    : public ::testing::Test
{
protected:
    std::string Endpoint;
    std::string Database;
    std::string Topic;
    IResourcePtr Driver;
    std::shared_ptr<NYdb::NTopic::TTopicClient> Client;

    void SetUp() override
    {
        Endpoint = GetEnv("YTFLOW_PUBLIC_YDB_ENDPOINT");
        Database = GetEnv("YTFLOW_PUBLIC_YDB_DATABASE");
        if (Endpoint.empty()) {
            GTEST_SKIP() << "Runs only under the public YDB fixture";
        }
        Topic = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        Driver = CreateRegisteredDriver();
        Client = Driver->As<IYdbClientFactory>()->CreateTopicClient(Endpoint, Database);
        auto settings = NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(1, 1);
        settings.BeginAddConsumer(LiveConsumer).EndAddConsumer();
        auto created = WaitFor(FromArcadiaFuture(Client->CreateTopic(Topic, settings))).ValueOrThrow();
        ASSERT_TRUE(created.IsSuccess()) << ToString(static_cast<const NYdb::TStatus&>(created));
    }

    void WriteMessages(std::vector<NYdb::NTopic::TWriteMessage> messages)
    {
        auto session = Client->CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
                .Path(Topic)
                .ProducerId("live-source-producer")
                .MessageGroupId("live-source-producer")
                .Codec(NYdb::NTopic::ECodec::RAW));
        for (auto& message : messages) {
            ASSERT_TRUE(session->Write(std::move(message)));
        }
        ASSERT_TRUE(session->Close(SourceRunDeadline));
    }

    void WriteRecords(const std::vector<std::string>& payloads)
    {
        std::vector<NYdb::NTopic::TWriteMessage> messages;
        for (const auto& payload : payloads) {
            messages.emplace_back(payload);
        }
        WriteMessages(std::move(messages));
    }

    static void CommitPersistedState(TSourceRun* run)
    {
        run->RunInQueue([&] {
            run->GetSource()->Commit();
        });
    }

    void WaitForBrokerCommittedOffset(i64 offset)
    {
        const auto deadline = TInstant::Now() + SourceRunDeadline;
        while (GetBrokerCommittedOffset() != offset) {
            ASSERT_LT(TInstant::Now(), deadline);
            Sleep(TDuration::MilliSeconds(100));
        }
    }

    i64 GetBrokerCommittedOffset()
    {
        auto description = WaitFor(FromArcadiaFuture(Client->DescribeConsumer(
            Topic,
            LiveConsumer,
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true))))
            .ValueOrThrow();
        THROW_ERROR_EXCEPTION_UNLESS(description.IsSuccess(), "Failed to describe consumer");
        for (const auto& partition : description.GetConsumerDescription().GetPartitions()) {
            if (partition.GetPartitionId() == 0) {
                return partition.GetPartitionConsumerStats()->GetCommittedOffset();
            }
        }
        THROW_ERROR_EXCEPTION("Consumer has no partition 0");
    }

    i64 SplitFirstPartitionByWrites()
    {
        auto alterSettings = NYdb::NTopic::TAlterTopicSettings();
        alterSettings.BeginAlterPartitioningSettings()
            .MinActivePartitions(1)
            .MaxActivePartitions(2)
            .BeginAlterAutoPartitioningSettings()
            .Strategy(NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp)
            .StabilizationWindow(TDuration::Seconds(1))
            .DownUtilizationPercent(1)
            .UpUtilizationPercent(2)
            .EndAlterAutoPartitioningSettings()
            .EndAlterTopicPartitioningSettings();
        auto altered = WaitFor(FromArcadiaFuture(Client->AlterTopic(Topic, alterSettings))).ValueOrThrow();
        THROW_ERROR_EXCEPTION_UNLESS(altered.IsSuccess(), "Failed to enable topic autopartitioning: %v", ToString(altered));

        std::deque<std::string> payloads;
        std::vector<std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession>> sessions;
        for (const std::string producerId : {"live-source-split-producer-a", "live-source-split-producer-b"}) {
            sessions.push_back(Client->CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
                    .Path(Topic)
                    .ProducerId(producerId)
                    .MessageGroupId(producerId)
                    .Codec(NYdb::NTopic::ECodec::RAW)));
        }
        auto closeSessions = [&] {
            bool closed = true;
            for (const auto& session : sessions) {
                closed &= session->Close(SourceRunDeadline);
            }
            sessions.clear();
            return closed;
        };
        auto closeSessionsOnError = Finally([&] {
            closeSessions();
        });

        const auto deadline = TInstant::Now() + SourceRunDeadline;
        int written = 0;
        while (true) {
            auto description = WaitFor(FromArcadiaFuture(Client->DescribeTopic(
                Topic,
                NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true))))
                .ValueOrThrow();
            THROW_ERROR_EXCEPTION_UNLESS(description.IsSuccess(), "Failed to describe topic");
            const auto& partitions = description.GetTopicDescription().GetPartitions();
            const auto* parent = FindIfPtr(partitions, [] (const auto& partition) {
                return partition.GetPartitionId() == 0;
            });
            THROW_ERROR_EXCEPTION_UNLESS(parent, "Topic has no partition 0");
            if (!parent->GetActive()) {
                const auto& childIds = parent->GetChildPartitionIds();
                THROW_ERROR_EXCEPTION_IF(childIds.empty(), "Inactive partition 0 has no children");
                for (auto childId : childIds) {
                    const auto* child = FindIfPtr(partitions, [&] (const auto& partition) {
                        return partition.GetPartitionId() == childId;
                    });
                    THROW_ERROR_EXCEPTION_UNLESS(child, "Child partition %v is not described", childId);
                    THROW_ERROR_EXCEPTION_UNLESS(IsIn(child->GetParentPartitionIds(), 0u), "Child partition %v does not name its parent", childId);
                }
                const auto parentEndOffset = static_cast<i64>(parent->GetPartitionStats()->GetEndOffset());
                THROW_ERROR_EXCEPTION_UNLESS(closeSessions(), "Failed to complete the writes that split partition 0");
                return parentEndOffset;
            }
            THROW_ERROR_EXCEPTION_IF(TInstant::Now() > deadline,
                "Partition 0 did not split after %v written records",
                written);
            for (int index = 0; index < 16; ++index) {
                const auto& payload = payloads.emplace_back(PackLenvalRecord({std::string(16_KB, static_cast<char>('a' + written % 26))}));
                THROW_ERROR_EXCEPTION_UNLESS(
                    sessions[written % sessions.size()]->Write(NYdb::NTopic::TWriteMessage(payload)),
                    "Failed to write a record that splits partition 0");
                ++written;
            }
        }
    }

    IMapNodePtr MakeFramingParameters()
    {
        auto parameters = MakeSourceParameters(Endpoint, Database, Topic);
        parameters->AddChild("framing_format", ConvertToNode("lenval"));
        parameters->AddChild("update_info_period", ConvertToNode(LiveUpdateInfoPeriod));
        return parameters;
    }

    TDynamicSourceContextPtr MakeShortCommitTimeoutContext(IMapNodePtr partitionSpec = nullptr)
    {
        return MakeDynamicSourceContext({{"commit_timeout", ConvertToNode(LiveUpdateInfoPeriod)}}, std::move(partitionSpec));
    }

    std::vector<ISource::TMessageBatch> ReadNothing(TSourceRun* run)
    {
        auto settings = New<TMessageBatcherSettings>();
        settings->BatchDuration = TDuration::MilliSeconds(200);
        std::vector<ISource::TMessageBatch> batches;
        for (int attempt = 0; attempt < 10; ++attempt) {
            auto next = run->RunInQueue([&] {
                return WaitFor(run->GetSource()->GetNextBatch(settings)).ValueOrThrow();
            });
            for (auto& batch : next) {
                batches.push_back(std::move(batch));
            }
        }
        return batches;
    }

    TKey MakeKey()
    {
        return GenerateYdbTopicKey(MakeSourceIdentity({"ydb-topic/v1", Database, Topic}), Endpoint, 0);
    }

    static std::vector<std::vector<ISource::TMessageBatch>> ReadGroups(TSourceRun* run, i64 maxRowsPerBatch, int messageCount)
    {
        auto settings = New<TMessageBatcherSettings>();
        settings->MaxRowsPerBatch = TSize(maxRowsPerBatch);
        settings->BatchDuration = TDuration::Seconds(1);
        std::vector<std::vector<ISource::TMessageBatch>> groups;
        int readCount = 0;
        const auto deadline = TInstant::Now() + SourceRunDeadline;
        while (readCount < messageCount) {
            THROW_ERROR_EXCEPTION_IF(TInstant::Now() > deadline,
                "Source did not deliver the expected messages");
            auto group = run->RunInQueue([&] {
                return WaitFor(run->GetSource()->GetNextBatch(settings)).ValueOrThrow();
            });
            if (group.empty()) {
                continue;
            }
            for (const auto& batch : group) {
                readCount += std::ssize(batch.Messages);
            }
            groups.push_back(std::move(group));
        }
        return groups;
    }

    std::vector<std::vector<std::string>> DescribeGroups(const std::vector<std::vector<ISource::TMessageBatch>>& groups)
    {
        const auto family = MakeLiveFamily();
        std::vector<std::vector<std::string>> descriptions;
        for (const auto& group : groups) {
            auto& description = descriptions.emplace_back();
            for (const auto& batch : group) {
                for (const auto& message : batch.Messages) {
                    const auto unparsed = GetColumnValue<std::optional<TYsonString>>(message, "unparsed");
                    description.push_back(Format("%v %v %v",
                        GetColumnValue<i64>(message, "offset"),
                        message->MessageId,
                        unparsed
                            ? family.ReadUnparsedData(*unparsed)
                            : GetColumnValue<std::string>(message, "data")));
                }
            }
        }
        return descriptions;
    }

    TSourceFamily MakeLiveFamily()
    {
        return {
            .SourceClassName = TypeName<TYdbFramingSource>(),
            .Parameters = MakeFramingParameters(),
            .SourceKey = MakeKey(),
            .Driver = Driver,
            .ReadUnparsedData = [] (const TYsonString& unparsed) {
                return *ConvertTo<TUnparsedYdbPayloadPtr>(unparsed)->Data;
            },
        };
    }
};

TEST_F(TYdbLiveSourceTest, RestoredOffsetReachesTheSessionBeforeItStarts)
{
    WriteRecords({
        PackLenvalRecord({"zero"}),
        PackLenvalRecord({"one"}),
        PackLenvalRecord({"two"}),
    });
    auto stateManager = New<TStateManagerMock>();
    {
        TSourceRun original(MakeLiveFamily(), MakeDynamicSourceContext(), stateManager);
        original.Persist(original.ReadBatches(2));
    }
    EXPECT_EQ(GetBrokerCommittedOffset(), 0);

    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    TSourceRun restored(MakeLiveFamily(), MakeDynamicSourceContext(), restoredStateManager);
    auto batches = restored.ReadBatches(1);
    EXPECT_EQ(GetRecordOffset(batches.front()), 2);

    WaitForBrokerCommittedOffset(2);
}

TEST_F(TYdbLiveSourceTest, ReadBlockedPartitionResumesFromItsPersistedOffset)
{
    WriteRecords({
        PackLenvalRecord({"zero"}),
        PackLenvalRecord({"one"}),
        PackLenvalRecord({"two"}),
    });
    auto stateManager = New<TStateManagerMock>();
    {
        TSourceRun original(MakeLiveFamily(), MakeShortCommitTimeoutContext(), stateManager);
        original.Persist(original.ReadBatches(1));
        CommitPersistedState(&original);
        WaitForBrokerCommittedOffset(1);
    }

    auto makePartitionSpec = [] (bool readBlocked) {
        // clang-format off
        return BuildYsonNodeFluently()
            .BeginMap()
                .Item("read_blocked").Value(readBlocked)
            .EndMap()
            ->AsMap();
        // clang-format on
    };
    TSourceRun blocked(MakeLiveFamily(), MakeShortCommitTimeoutContext(makePartitionSpec(true)), stateManager);
    EXPECT_TRUE(ReadNothing(&blocked).empty());
    EXPECT_EQ(GetBrokerCommittedOffset(), 1);

    DynamicPointerCast<TSourceBase>(blocked.GetSource())->Reconfigure(MakeShortCommitTimeoutContext(makePartitionSpec(false)));
    auto batches = blocked.ReadBatches(1);
    EXPECT_EQ(GetRecordOffset(batches.front()), 1);
    blocked.Persist(batches);
    CommitPersistedState(&blocked);
    WaitForBrokerCommittedOffset(2);
}

TEST_F(TYdbLiveSourceTest, InactivePartitionCompletesOnlyAfterItsReadMessagesPersist)
{
    WriteRecords({
        PackLenvalRecord({"zero"}),
        PackLenvalRecord({"one"}),
        PackLenvalRecord({"two"}),
    });
    // clang-format off
    auto inactiveSpec = BuildYsonNodeFluently()
        .BeginMap()
            .Item("final_offset_exclusive").Value(3)
        .EndMap()
        ->AsMap();
    // clang-format on
    auto stateManager = New<TStateManagerMock>();
    TSourceRun run(MakeLiveFamily(), MakeShortCommitTimeoutContext(inactiveSpec), stateManager);
    auto batches = run.ReadBatches(3);
    ASSERT_EQ(GetRecordOffset(batches.back()), 2);

    auto buildInflight = [&] {
        return run.RunInQueue([&] {
            return run.GetSource()->BuildInflight();
        });
    };
    EXPECT_FALSE(buildInflight()->Empty);
    run.RunInQueue([&] {
        run.GetSource()->Sync();
        stateManager->Sync();
    });
    EXPECT_EQ(OffsetToInt(ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"))->PersistedOffsetExclusive), 0);
    EXPECT_FALSE(buildInflight()->Empty);
    EXPECT_EQ(GetBrokerCommittedOffset(), 0);

    run.Persist(batches);
    CommitPersistedState(&run);
    EXPECT_EQ(OffsetToInt(ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"))->PersistedOffsetExclusive), 3);
    const auto deadline = TInstant::Now() + SourceRunDeadline;
    while (!buildInflight()->Empty) {
        ASSERT_LT(TInstant::Now(), deadline);
        Sleep(TDuration::MilliSeconds(100));
    }
    EXPECT_EQ(GetBrokerCommittedOffset(), 3);
}

TEST_F(TYdbLiveSourceTest, SplitPartitionCompletesOnlyAfterItsReadMessagesAreCommitted)
{
    const i64 parentEndOffset = SplitFirstPartitionByWrites();
    ASSERT_GT(parentEndOffset, 0);

    auto stateManager = New<TStateManagerMock>();
    TSourceRun run(MakeLiveFamily(), MakeShortCommitTimeoutContext(), stateManager);
    auto batches = run.ReadBatches(static_cast<int>(parentEndOffset));
    for (i64 offset = 0; offset < parentEndOffset; ++offset) {
        ASSERT_EQ(GetRecordOffset(batches[offset]), offset);
    }
    EXPECT_TRUE(ReadNothing(&run).empty());

    auto buildInflight = [&] {
        return run.RunInQueue([&] {
            run.GetSource()->Sync();
            stateManager->Sync();
            return run.GetSource()->BuildInflight();
        });
    };
    EXPECT_FALSE(buildInflight()->Empty);
    EXPECT_EQ(OffsetToInt(ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"))->PersistedOffsetExclusive), 0);
    EXPECT_EQ(GetBrokerCommittedOffset(), 0);

    run.Persist(batches);
    CommitPersistedState(&run);
    const auto deadline = TInstant::Now() + SourceRunDeadline;
    while (!buildInflight()->Empty) {
        ASSERT_LT(TInstant::Now(), deadline);
        Sleep(TDuration::MilliSeconds(100));
    }
    EXPECT_TRUE(ConvertTo<TYdbSourcePartitionStatusPtr>(run.GetSource()->GetPartitionStatus())->InputExhausted);
    EXPECT_EQ(GetBrokerCommittedOffset(), parentEndOffset);
    EXPECT_EQ(OffsetToInt(ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"))->PersistedOffsetExclusive), parentEndOffset);
}

TEST_F(TYdbLiveSourceTest, DeliveredRecordCarriesEverySourceField)
{
    constexpr auto CreateTime = TInstant::Seconds(1700000000);
    constexpr ui64 EventWatermark = 1700000005;
    const auto writtenNotBefore = TInstant::Now().Seconds();
    const auto payload = PackLenvalRecord({"frame-0", "frame-1", "frame-2"});
    NYdb::NTopic::TWriteMessage message(payload);
    message.CreateTimestamp(CreateTime);
    NYdb::NTopic::TWriteMessage::TMessageMeta meta;
    meta.emplace_back("_ytflow_queue_meta", Format("{event_watermark=%v;}", EventWatermark));
    meta.emplace_back("custom", "attribute");
    message.MessageMeta(meta);
    std::vector<NYdb::NTopic::TWriteMessage> messages;
    messages.push_back(std::move(message));
    WriteMessages(std::move(messages));
    const auto writtenNotAfter = TInstant::Now().Seconds();

    auto parameters = MakeFramingParameters();
    parameters->AddChild("try_parse_flow_queue_meta", ConvertToNode(true));
    parameters->AddChild("attributes_column", ConvertToNode("attributes"));
    auto stateManager = New<TStateManagerMock>();
    auto family = MakeLiveFamily();
    family.Parameters = parameters;
    TSourceRun run(family, MakeDynamicSourceContext(), stateManager);
    auto batches = run.ReadBatches(1);

    ASSERT_EQ(batches.front().Messages.size(), 3u);
    for (int frameIndex = 0; frameIndex < 3; ++frameIndex) {
        const auto& delivered = batches.front().Messages[frameIndex];
        EXPECT_EQ(GetColumnValue<i64>(delivered, "offset"), 0);
        EXPECT_EQ(GetColumnValue<i64>(delivered, "sub_offset"), frameIndex);
        EXPECT_EQ(GetColumnValue<std::string>(delivered, "data"), Format("frame-%v", frameIndex));
        EXPECT_EQ(
            ConvertToNode(GetColumnValue<TYsonString>(delivered, "attributes"))->AsMap()->GetChildValueOrThrow<std::string>("custom"),
            "attribute");
        EXPECT_GE(delivered->GetMeta().SystemTimestamp.Underlying(), writtenNotBefore);
        EXPECT_LE(delivered->GetMeta().SystemTimestamp.Underlying(), writtenNotAfter);
        EXPECT_EQ(delivered->GetMeta().EventTimestamp, TSystemTimestamp(CreateTime.Seconds()));
    }
    run.Persist(batches);
    auto state = ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"));
    EXPECT_EQ(state->PersistedEventWatermark, TSystemTimestamp(EventWatermark));
}

TEST_F(TYdbLiveSourceTest, KeptMalformedPayloadUsesTheUnparsedPayloadSchema)
{
    WriteRecords({std::string(MalformedLenvalPayload)});
    CheckKeptMalformedPayload(MakeLiveFamily());
}

TEST_F(TYdbLiveSourceTest, QueuedReconfigurationsApplyTheirOwnOptions)
{
    WriteRecords({
        PackLenvalRecord({"before"}),
        std::string(MalformedLenvalPayload),
        std::string(MalformedLenvalPayload),
        PackLenvalRecord({"after"}),
    });
    CheckQueuedSourceReconfigurations(MakeLiveFamily());
}

TEST_F(TYdbLiveSourceTest, StableReplayRepeatsReadGroupsUnderTheirRememberedPolicy)
{
    WriteRecords({
        PackLenvalRecord({"zero"}),
        std::string(MalformedLenvalPayload),
        PackLenvalRecord({"two"}),
    });
    auto stateManager = New<TStateManagerMock>();
    std::vector<std::vector<std::string>> firstSight;
    {
        TSourceRun original(
            MakeLiveFamily(),
            MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("keep")}}),
            stateManager,
            /*stableReadGroups*/ true);
        auto groups = ReadGroups(&original, /*maxRowsPerBatch*/ 100, /*messageCount*/ 3);
        firstSight = DescribeGroups(groups);
        original.RunInQueue([&] {
            for (const auto& group : groups) {
                for (const auto& batch : group) {
                    original.GetSource()->MarkPublished(batch.Cookie);
                }
            }
            original.GetSource()->Sync();
            stateManager->Sync();
        });
    }
    ASSERT_EQ(firstSight.size(), 1u);
    ASSERT_EQ(firstSight.front().size(), 3u);
    auto state = ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"));
    EXPECT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 0);
    ASSERT_TRUE(state->ReadSettingsMemory);
    EXPECT_FALSE(state->ReadSettingsMemory->empty());
    EXPECT_EQ(GetBrokerCommittedOffset(), 0);
    WriteRecords({PackLenvalRecord({"three"})});

    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    TSourceRun restored(
        MakeLiveFamily(),
        MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("drop")}}),
        restoredStateManager,
        /*stableReadGroups*/ true);
    auto replayed = DescribeGroups(ReadGroups(&restored, /*maxRowsPerBatch*/ 1, /*messageCount*/ 3));
    EXPECT_EQ(replayed, firstSight);
    std::vector<std::string> replayedMessages;
    for (const auto& group : replayed) {
        replayedMessages.insert(replayedMessages.end(), group.begin(), group.end());
    }
    ASSERT_EQ(std::ssize(replayedMessages), 3);
    EXPECT_TRUE(replayedMessages[1].ends_with(MalformedLenvalPayload));

    auto fresh = ReadGroups(&restored, /*maxRowsPerBatch*/ 100, /*messageCount*/ 1);
    EXPECT_EQ(GetRecordOffset(fresh.front().front()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(fresh.front().front().Messages.front(), "data"), "three");
}

TEST_F(TYdbLiveSourceTest, OnlyAChangedRestartTokenReplacesTheReadSession)
{
    auto actionQueue = New<TActionQueue>("ReadSessionRestart");
    auto invoker = CreateSerializedInvoker(actionQueue->GetInvoker());
    auto context = CreateTestSourceContext(invoker);
    context->SourceStreamId = SourceRunStreamId;
    context->StaticResources.emplace(YdbDriverDefaultResourceId, Driver);
    auto makeDynamicOptions = [] (TDuration unavailableThreshold) {
        auto parameters = ConvertTo<TDynamicYdbSourceParametersPtr>(BuildYsonNodeFluently()
                .BeginMap()
                .Item("unavailable_threshold")
                .Value(unavailableThreshold)
                .EndMap());
        return MakeYdbSourceDynamicOptions(*parameters);
    };
    auto options = MakeYdbSourceOptions(
        *ConvertTo<TYdbSourceParametersPtr>(MakeSourceParameters(Endpoint, Database, Topic)),
        CreateWholeYdbPayloadSplitter());
    auto session = New<TYdbSourceReadSession>(
        context,
        options,
        makeDynamicOptions(TDuration::Seconds(10)),
        Client,
        Topic,
        /*partitionIndex*/ 0,
        /*persistedOffsetExclusive*/ 0,
        TestLogger,
        context->StatusProfiler->ErrorState("read"));

    WaitFor(BIND(&TYdbSourceReadSession::Init, session).AsyncVia(invoker).Run()).ThrowOnError();
    auto terminateSession = Finally([&] {
        WaitFor(BIND(&TYdbSourceReadSession::Terminate, session).AsyncVia(invoker).Run()).ThrowOnError();
        WaitFor(BIND([&] {
            session.Reset();
        }).AsyncVia(invoker)
                .Run())
            .ThrowOnError();
    });
    auto waitForEstablishedSession = [&] {
        const auto deadline = TInstant::Now() + SourceRunDeadline;
        while (true) {
            auto sessionId = WaitFor(BIND(&TYdbSourceReadSession::GetReadSessionId, session).AsyncVia(invoker).Run()).ValueOrThrow();
            if (sessionId) {
                return *sessionId;
            }
            THROW_ERROR_EXCEPTION_IF(TInstant::Now() > deadline, "Read session was not established");
            Sleep(TDuration::MilliSeconds(50));
        }
    };

    const auto initialSessionId = waitForEstablishedSession();
    WaitFor(BIND(&TYdbSourceReadSession::Reconfigure, session, makeDynamicOptions(TDuration::Seconds(10))).AsyncVia(invoker).Run()).ThrowOnError();
    EXPECT_EQ(waitForEstablishedSession(), initialSessionId);

    WaitFor(BIND(&TYdbSourceReadSession::Reconfigure, session, makeDynamicOptions(TDuration::Seconds(20))).AsyncVia(invoker).Run()).ThrowOnError();
    EXPECT_NE(waitForEstablishedSession(), initialSessionId);
}

TEST_F(TYdbLiveSourceTest, SuccessfulDiscoveryKeepsLatchedReadOnlyInstant)
{
    constexpr auto ReadOnlyAt = TInstant::Seconds(1700000000);
    auto actionQueue = New<TActionQueue>("TopologyLive");
    auto invoker = CreateSerializedInvoker(actionQueue->GetInvoker());
    TYdbTopologySnapshot snapshot;
    snapshot.Clusters[Endpoint] = {.ReadOnlyAt = ReadOnlyAt};
    auto controller = New<TYdbTopicInfoController>(
        TYdbTopologyOptions{
            .DiscoveryEntries = {TYdbWriteTarget{.Endpoint = Endpoint, .Database = Database}},
            .Database = Database,
            .Topic = Topic,
            .ClientTimeout = TDuration::Seconds(30),
            .UpdatePartitionCountPeriod = TDuration::Hours(1),
        },
        Driver->As<IYdbClientFactory>(),
        invoker,
        TestLogger,
        CreateSyncStatusProfiler(TestLogger),
        TYdbStateAccess<TYdbTopologySnapshot>{
            .Init = BIND([] (IInitContextPtr /*initContext*/) {
            }),
            .Read = BIND([&snapshot] {
                return snapshot;
            }),
            .Write = BIND([&snapshot] (const TYdbTopologySnapshot& value) {
                snapshot = value;
            }),
        });
    WaitFor(BIND(&TYdbTopicInfoController::Init, controller, /*initContext*/ nullptr).AsyncVia(invoker).Run()).ThrowOnError();

    auto readCluster = BIND([&] {
        return GetOrCrash(snapshot.Clusters, Endpoint);
    }).AsyncVia(invoker);
    const auto deadline = TInstant::Now() + SourceRunDeadline;
    while (!WaitFor(readCluster.Run()).ValueOrThrow().PartitionCount) {
        ASSERT_LT(TInstant::Now(), deadline);
        Sleep(TDuration::MilliSeconds(50));
    }
    auto cluster = WaitFor(readCluster.Run()).ValueOrThrow();
    EXPECT_EQ(cluster.PartitionCount, 1);
    EXPECT_EQ(cluster.ReadOnlyAt, ReadOnlyAt);
    WaitFor(BIND([&] {
        controller.Reset();
    }).AsyncVia(invoker)
            .Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
