#include <yt/yt/flow/library/cpp/multiplexer/dynamic_table_multiplexer_process_function.h>
#include <yt/yt/flow/library/cpp/multiplexer/multiplexer_process_function.h>

#include <yt/yt/flow/library/cpp/process_function/testing/unittest.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <deque>

namespace NYT::NFlow {
namespace {

using namespace NTesting;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TTestUserState
    : public TYsonStruct
{
    int Generation = 0;

    REGISTER_YSON_STRUCT(TTestUserState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("generation", &TThis::Generation)
            .Default(0);
    }
};

class TTestMultiplexerProcessFunction
    : public TMultiplexerProcessFunction<TTestUserState>
{
public:
    using TMultiplexerProcessFunction::TMultiplexerProcessFunction;

    struct TFetchCall
    {
        TKey Key;
        std::optional<TKey> StartOffset;
        std::optional<TKey> EndOffset;
        i64 Limit;
        int Generation;
    };

    std::deque<std::optional<TKey>> Results;
    std::vector<TFetchCall> FetchCalls;
    TTableSchemaPtr OffsetSchema;

protected:
    std::optional<TKey> FetchBatch(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit,
        TStateAccessor<TTestUserState>& userState,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        FetchCalls.push_back({
            .Key = key,
            .StartOffset = startOffsetExclusive,
            .EndOffset = endOffsetInclusive,
            .Limit = limit,
            .Generation = userState->Generation,
        });
        YT_VERIFY(!Results.empty());
        auto result = std::move(Results.front());
        Results.pop_front();
        return result;
    }

    void OnInputMessage(
        const TKey& /*key*/,
        const TInputMessageConstPtr& /*message*/,
        TStateAccessor<TTestUserState>& userState,
        const IRuntimeContextPtr& /*context*/) override
    {
        ++userState->Generation;
    }

    TTableSchemaPtr GetCurrentOffsetSchema(const IRuntimeContextPtr& /*context*/) override
    {
        return OffsetSchema;
    }
};

class TNoopDynamicTableMultiplexerProcessFunction
    : public TDynamicTableMultiplexerProcessFunction<>
{
public:
    using TDynamicTableMultiplexerProcessFunction::TDynamicTableMultiplexerProcessFunction;

private:
    void BuildOutputForRow(
        const TKey& /*key*/,
        const TPayload& /*row*/,
        const TTableSchemaPtr& /*rowSchema*/,
        TStateAccessor<TEmptyMultiplexerUserState>& /*userState*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TTestMultiplexerProcessFunction,
    TEmptyProcessFunctionParameters,
    TDynamicMultiplexerParameters);
YT_FLOW_DEFINE_PROCESS_FUNCTION(
    TNoopDynamicTableMultiplexerProcessFunction,
    TDynamicTableMultiplexerParameters,
    TDynamicMultiplexerParameters);

class TMockClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TMockClientsCache(NApi::IClientPtr client)
        : Client_(std::move(client))
    { }

    NApi::IClientPtr GetClient(TStringBuf cluster) override
    {
        RequestedClusters.push_back(std::string(cluster));
        return Client_;
    }

    std::vector<std::string> RequestedClusters;

private:
    const NApi::IClientPtr Client_;
};

struct TFixture
{
    TTestStateEnvironment Environment;
    IRuntimeContextPtr Context;
    TProcessFunctionTestHarness Harness;
    TIntrusivePtr<TTestMultiplexerProcessFunction> Function;

    TFixture()
        : Context(BuildContext())
        , Harness(TProcessFunctionTestHarness::Create<TTestMultiplexerProcessFunction>(
            Environment,
            Context))
        , Function(Harness.GetFunction<TTestMultiplexerProcessFunction>())
    { }

    static IRuntimeContextPtr BuildContext()
    {
        auto parameters = New<TDynamicMultiplexerParameters>();
        parameters->TimerPeriod = TDuration::Seconds(10);
        parameters->BatchSize = 2;
        return TTestRuntimeContextBuilder()
            .SetProcessingFunction<TTestMultiplexerProcessFunction>()
            .SetCurrentTimestamp(TSystemTimestamp(100))
            .SetDynamicParameters(parameters)
            .Build();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiplexerProcessFunctionTest, ActivatesKeyAndPersistsStateNames)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(7);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());

    fixture.Harness.RunEpoch({message}, {}, {});

    auto internal = fixture.Environment.ReadRefCountedKeyState<TMultiplexerKeyState>(
        "/multiplexer/internal",
        key);
    auto user = fixture.Environment.ReadRefCountedKeyState<TTestUserState>("/multiplexer/user", key);
    EXPECT_TRUE(internal->IsActive);
    EXPECT_FALSE(internal->Offset);
    EXPECT_FALSE(internal->InitialStartOffset);
    EXPECT_FALSE(internal->InSecondPhase);
    EXPECT_EQ(user->Generation, 1);

    ASSERT_EQ(fixture.Harness.GetTimers().size(), 1u);
    EXPECT_EQ(
        fixture.Harness.GetTimers()[0].TriggerTimestamp,
        TSystemTimestamp(110 + THash<TKey>()(key) % 10));
    ASSERT_EQ(fixture.Harness.GetTimers()[0].ParentIds.size(), 1u);
    EXPECT_EQ(fixture.Harness.GetTimers()[0].ParentIds[0], message->MessageId);
}

TEST(TMultiplexerProcessFunctionTest, AdvancesAndCompletesIteration)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(1);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    fixture.Harness.RunEpoch({message}, {}, {});

    auto firstTimer = MakeTestTimer(key, fixture.Harness.GetTimers()[0].TriggerTimestamp);
    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    fixture.Harness.RunEpoch({}, {firstTimer}, {});

    ASSERT_EQ(fixture.Function->FetchCalls.size(), 1u);
    EXPECT_FALSE(fixture.Function->FetchCalls[0].StartOffset);
    EXPECT_FALSE(fixture.Function->FetchCalls[0].EndOffset);
    EXPECT_EQ(fixture.Function->FetchCalls[0].Limit, 2);
    EXPECT_EQ(fixture.Function->FetchCalls[0].Generation, 1);
    EXPECT_EQ(
        fixture.Environment.ReadRefCountedKeyState<TMultiplexerKeyState>(
            "/multiplexer/internal",
            key)
            ->Offset,
        MakeKey<ui64>(5));
    ASSERT_EQ(fixture.Harness.GetTimers().size(), 1u);
    EXPECT_EQ(fixture.Harness.GetTimers()[0].ParentIds[0], firstTimer->MessageId);

    auto secondTimer = MakeTestTimer(key, fixture.Harness.GetTimers()[0].TriggerTimestamp);
    fixture.Function->Results.push_back(std::nullopt);
    fixture.Harness.RunEpoch({}, {secondTimer}, {});

    EXPECT_FALSE(
        fixture.Environment.ReadRefCountedKeyState<TMultiplexerKeyState>(
            "/multiplexer/internal",
            key)
            ->IsActive);
    EXPECT_EQ(
        fixture.Environment.ReadRefCountedKeyState<TTestUserState>(
            "/multiplexer/user",
            key)
            ->Generation,
        0);
    EXPECT_TRUE(fixture.Harness.GetTimers().empty());
}

TEST(TMultiplexerProcessFunctionTest, CollapsePerformsCircularPass)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(2);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    fixture.Harness.RunEpoch({message}, {}, {});

    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(1))}, {});
    fixture.Harness.RunEpoch({message}, {}, {});

    fixture.Function->Results.push_back(MakeKey<ui64>(8));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(2))}, {});
    fixture.Function->Results.push_back(std::nullopt);
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(3))}, {});
    fixture.Function->Results.push_back(MakeKey<ui64>(3));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(4))}, {});
    fixture.Function->Results.push_back(std::nullopt);
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(5))}, {});

    ASSERT_EQ(fixture.Function->FetchCalls.size(), 5u);
    EXPECT_EQ(fixture.Function->FetchCalls[1].StartOffset, MakeKey<ui64>(5));
    EXPECT_FALSE(fixture.Function->FetchCalls[1].EndOffset);
    EXPECT_FALSE(fixture.Function->FetchCalls[2].EndOffset);
    EXPECT_FALSE(fixture.Function->FetchCalls[3].StartOffset);
    EXPECT_EQ(fixture.Function->FetchCalls[3].EndOffset, MakeKey<ui64>(5));
    EXPECT_EQ(fixture.Function->FetchCalls[4].StartOffset, MakeKey<ui64>(3));
    EXPECT_EQ(fixture.Function->FetchCalls[4].EndOffset, MakeKey<ui64>(5));
    EXPECT_EQ(fixture.Function->FetchCalls.back().Generation, 2);
    EXPECT_FALSE(
        fixture.Environment.ReadRefCountedKeyState<TMultiplexerKeyState>(
            "/multiplexer/internal",
            key)
            ->IsActive);
}

TEST(TMultiplexerProcessFunctionTest, RejectsInvalidReturnedOffsets)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(3);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    fixture.Harness.RunEpoch({message}, {}, {});

    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(1))}, {});
    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    EXPECT_THROW_WITH_SUBSTRING(
        fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(2))}, {}),
        "non-monotonic");
}

TEST(TMultiplexerProcessFunctionTest, RejectsOffsetBeyondCollapseBookmark)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(3);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    fixture.Harness.RunEpoch({message}, {}, {});

    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(1))}, {});
    fixture.Harness.RunEpoch({message}, {}, {});
    fixture.Function->Results.push_back(std::nullopt);
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(2))}, {});
    fixture.Function->Results.push_back(MakeKey<ui64>(6));

    EXPECT_THROW_WITH_SUBSTRING(
        fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(3))}, {}),
        "overshot endOffsetInclusive");
}

TEST(TMultiplexerProcessFunctionTest, SchemaChangeRestartsFromBeginning)
{
    TFixture fixture;
    auto key = MakeKey<ui64>(4);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());
    fixture.Function->OffsetSchema = DefaultTestKeySchema();
    fixture.Harness.RunEpoch({message}, {}, {});

    fixture.Function->Results.push_back(MakeKey<ui64>(5));
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(1))}, {});

    fixture.Function->OffsetSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=changed;type=uint64;sort_order=ascending}])")));
    fixture.Function->Results.push_back(std::nullopt);
    fixture.Harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(2))}, {});

    ASSERT_EQ(fixture.Function->FetchCalls.size(), 2u);
    EXPECT_FALSE(fixture.Function->FetchCalls[1].StartOffset);
}

TEST(TMultiplexerProcessFunctionTest, KeepsKeysIndependent)
{
    TFixture fixture;
    auto firstKey = MakeKey<ui64>(10);
    auto secondKey = MakeKey<ui64>(20);
    fixture.Harness.RunEpoch(
        {
            MakeTestMessage("input", firstKey, New<TTableSchema>()),
            MakeTestMessage("input", secondKey, New<TTableSchema>()),
        },
        {},
        {});

    auto firstState = fixture.Environment.ReadRefCountedKeyState<TTestUserState>(
        "/multiplexer/user",
        firstKey);
    auto secondState = fixture.Environment.ReadRefCountedKeyState<TTestUserState>(
        "/multiplexer/user",
        secondKey);
    EXPECT_EQ(firstState->Generation, 1);
    EXPECT_EQ(secondState->Generation, 1);
    ASSERT_EQ(fixture.Harness.GetTimers().size(), 2u);
}

TEST(TDynamicTableMultiplexerProcessFunctionTest, RequiresClusterInStaticParameters)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TDynamicTableMultiplexerParametersPtr>(TYsonString(TStringBuf(
            R"({table_path="//tmp/index"})"))),
        "must specify a cluster");

    auto parameters = ConvertTo<TDynamicTableMultiplexerParametersPtr>(
        TYsonString(TStringBuf(R"({table_path="<cluster=primary>//tmp/index"})")));
    EXPECT_EQ(parameters->TablePath.GetCluster(), "primary");
}

TEST(TDynamicTableMultiplexerProcessFunctionTest, ConstructorDefensivelyRequiresCluster)
{
    TTestStateEnvironment environment;
    auto parameters = New<TDynamicTableMultiplexerParameters>();
    parameters->TablePath = NYPath::TRichYPath("//tmp/index");
    environment.SetStaticParameters(parameters);
    auto context = environment.CreateProcessFunctionContext();

    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(New<TNoopDynamicTableMultiplexerProcessFunction>(context)),
        "must specify a cluster");
}

TEST(TDynamicTableMultiplexerProcessFunctionTest, UsesNamedClientAndLoadsSchemaOnce)
{
    auto client = New<NApi::TMockClient>();
    auto tableSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"([
        {name=key;type=uint64;sort_order=ascending};
        {name=secondary_key;type=uint64;sort_order=ascending}
    ])")));
    auto node = GetEphemeralNodeFactory()->CreateMap();
    node->MutableAttributes()->Set("schema", tableSchema);

    testing::InSequence sequence;
    EXPECT_CALL(*client, GetNode("//tmp/index", testing::_))
        .Times(1)
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(node))));
    auto emptyRowset = NApi::CreateRowset<TUnversionedRow>(New<TNameTable>(), {});
    EXPECT_CALL(*client, SelectRows(testing::HasSubstr("FROM [//tmp/index]"), testing::_))
        .Times(2)
        .WillRepeatedly(testing::Return(MakeFuture(NApi::TSelectRowsResult{
            .Rowset = emptyRowset,
        })));

    TTestStateEnvironment environment;
    auto clientsCache = New<TMockClientsCache>(client);
    auto staticParameters = New<TDynamicTableMultiplexerParameters>();
    staticParameters->TablePath = NYPath::TRichYPath("<cluster=secondary>//tmp/index");
    environment.SetStaticParameters(staticParameters);
    environment.SetClientsCache(clientsCache);

    auto dynamicParameters = New<TDynamicMultiplexerParameters>();
    dynamicParameters->TimerPeriod = TDuration::Seconds(10);
    dynamicParameters->BatchSize = 2;
    auto context = TTestRuntimeContextBuilder()
        .SetProcessingFunction<TNoopDynamicTableMultiplexerProcessFunction>()
        .SetCurrentTimestamp(TSystemTimestamp(100))
        .SetDynamicParameters(dynamicParameters)
        .Build();
    auto harness =
        TProcessFunctionTestHarness::Create<TNoopDynamicTableMultiplexerProcessFunction>(
        environment,
        context);
    auto key = MakeKey<ui64>(1);
    auto message = MakeTestMessage("input", key, New<TTableSchema>());

    EXPECT_EQ(clientsCache->RequestedClusters, std::vector<std::string>{"secondary"});
    harness.RunEpoch({message}, {}, {});
    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(1))}, {});
    harness.RunEpoch({message}, {}, {});
    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(2))}, {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
