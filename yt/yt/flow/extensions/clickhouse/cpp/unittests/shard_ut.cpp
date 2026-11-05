#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/shard.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/xrange.h>

namespace NYT::NFlow {

class TClickHouseShardRouterTestPeer
{
public:
    static size_t GetSchemaCacheSize(const TClickHouseShardRouter& router)
    {
        auto guard = Guard(router.CachedSchemaIndexesLock_);
        return router.CachedSchemaIndexes_.size();
    }
};

namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TCommonClickHouseSinkParametersPtr ParseParameters(TStringBuf yson)
{
    return NYTree::ConvertTo<TCommonClickHouseSinkParametersPtr>(NYson::TYsonString(yson));
}

std::vector<TClickHouseShard> ResolveShardsFromYson(TStringBuf yson)
{
    return ResolveShards(*ParseParameters(yson));
}

std::vector<std::string> GetShardNames(const std::vector<TClickHouseShard>& shards)
{
    std::vector<std::string> names;
    for (const auto& shard : shards) {
        names.push_back(shard.Name);
    }
    return names;
}

////////////////////////////////////////////////////////////////////////////////

class TMessageFactory
{
public:
    TMessageFactory()
        : TMessageFactory(New<TTableSchema>(std::vector{
              TColumnSchema("user_id", EValueType::Int64),
              TColumnSchema("data", EValueType::Int64),
          }))
    { }

    explicit TMessageFactory(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = Schema_;
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
        specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
        SpecStorage_ = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(specs),
            New<TTableSchema>(),
            /*evaluatorCache*/ nullptr);
    }

    TOutputMessageConstPtr Make(i64 id, i64 userId) const
    {
        TMessageBuilder builder("test", Schema_);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<i64>(userId, "user_id");
        builder.Payload().Set<i64>(id, "data");
        return New<TOutputMessage>(builder.Finish(), SpecStorage_);
    }

    const TTableSchemaPtr& GetSchema() const
    {
        return Schema_;
    }

private:
    const TTableSchemaPtr Schema_;
    TComputationStreamSpecStoragePtr SpecStorage_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseShardTest, ResolveShardsSingleHost)
{
    auto shards = ResolveShardsFromYson("{host=h;port=9001;table=t}");

    ASSERT_EQ(std::ssize(shards), 1);
    const auto& shard = shards.front();
    EXPECT_TRUE(shard.Name.empty());
    EXPECT_EQ(shard.Hosts, (std::vector<std::string>{"h"}));
    EXPECT_EQ(shard.Port, 9001);
    EXPECT_EQ(shard.Database, "default");
    EXPECT_EQ(shard.Table, "t");
    EXPECT_FALSE(shard.DedupTokenSuffix.has_value());
}

TEST(TClickHouseShardTest, ResolveShardsUnshardedHostList)
{
    auto shards = ResolveShardsFromYson(R"({hosts=["h";"r1";"r2"];port=9001;table=t})");

    ASSERT_EQ(std::ssize(shards), 1);
    const auto& shard = shards.front();
    EXPECT_TRUE(shard.Name.empty());
    EXPECT_EQ(shard.Hosts, (std::vector<std::string>{"h", "r1", "r2"}));
    EXPECT_EQ(shard.Port, 9001);
    EXPECT_FALSE(shard.DedupTokenSuffix.has_value());
}

TEST(TClickHouseShardTest, ResolveShardsFromShardHosts)
{
    auto shards = ResolveShardsFromYson(
        R"({shard_hosts={c=["ch-c-1"];a=["ch-a-1";"ch-a-2"];b=["ch-b-1"]};port=9002;)"
        R"(database=db;table=t})");

    ASSERT_EQ(std::ssize(shards), 3);
    EXPECT_EQ(GetShardNames(shards), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(shards[0].Hosts, (std::vector<std::string>{"ch-a-1", "ch-a-2"}));
    EXPECT_EQ(shards[0].Port, 9002);
    EXPECT_EQ(shards[2].Port, 9002);
    for (const auto& shard : shards) {
        EXPECT_EQ(shard.Database, "db");
        EXPECT_EQ(shard.Table, "t");
    }
    EXPECT_EQ(shards[0].DedupTokenSuffix, std::optional<std::string>(":a"));
    EXPECT_EQ(shards[1].DedupTokenSuffix, std::optional<std::string>(":b"));
    EXPECT_EQ(shards[2].DedupTokenSuffix, std::optional<std::string>(":c"));
}

TEST(TClickHouseShardTest, HostFormsAreMutuallyExclusive)
{
    EXPECT_THROW(
        ParseParameters(R"({shard_hosts={a=["ch-a-1"]};host=h;table=t})"),
        std::exception);
    EXPECT_THROW(
        ParseParameters(R"({shard_hosts={a=["ch-a-1"]};hosts=["r1";"r2"];table=t})"),
        std::exception);
    EXPECT_THROW(
        ParseParameters(R"({host=h;hosts=["r1";"r2"];table=t})"),
        std::exception);
    // A one-entry list is exactly "host", so it is rejected to keep the three forms disjoint.
    EXPECT_THROW(ParseParameters(R"({hosts=["h"];table=t})"), std::exception);
    EXPECT_THROW(ParseParameters("{table=t}"), std::exception);
}

TEST(TClickHouseShardTest, ShardHostsRejectInvalidName)
{
    EXPECT_THROW(ParseParameters(R"({shard_hosts={"a:b"=["h"]};table=t})"), std::exception);
    EXPECT_THROW(ParseParameters(R"({shard_hosts={""=["h"]};table=t})"), std::exception);
    EXPECT_THROW(ParseParameters(R"({shard_hosts={"шард"=["h"]};table=t})"), std::exception);
}

TEST(TClickHouseShardTest, ShardHostsRejectEmptyHostList)
{
    EXPECT_THROW(ParseParameters("{shard_hosts={a=[]};table=t}"), std::exception);
}

TEST(TClickHouseShardTest, ShardHostsRejectEmptyHost)
{
    EXPECT_THROW(ParseParameters(R"({shard_hosts={a=[""]};table=t})"), std::exception);
}

TEST(TClickHouseShardTest, TokenIsBareForUnshardedShardAndSuffixedOtherwise)
{
    auto unshardedShards = ResolveShardsFromYson("{host=h;table=t}");
    auto shards = ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"]};table=t})");

    EXPECT_EQ(BuildShardDedupToken("m1", unshardedShards.front()), "m1");
    EXPECT_EQ(BuildShardDedupToken("m1", shards[0]), "m1:a");
    EXPECT_EQ(BuildShardDedupToken("m1", shards[1]), "m1:b");
    EXPECT_NE(
        BuildShardDedupToken("m1", shards[0]),
        BuildShardDedupToken("m1", shards[1]));
}

TEST(TClickHouseShardTest, RoutingIsDeterministic)
{
    TMessageFactory factory;
    constexpr TStringBuf Spec = R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"]};table=t})";

    TClickHouseShardRouter first(ResolveShardsFromYson(Spec), {"user_id"}, {factory.GetSchema()});
    TClickHouseShardRouter second(ResolveShardsFromYson(Spec), {"user_id"}, {factory.GetSchema()});
    for (i64 id : xrange(1000)) {
        auto message = factory.Make(id, id % 97);
        auto index = first.SelectShard(message);
        EXPECT_EQ(index, second.SelectShard(message));
        EXPECT_EQ(index, first.SelectShard(message));
    }
}

TEST(TClickHouseShardTest, PreResolvedIndexesPreserveRouting)
{
    auto ordinary = New<TTableSchema>(std::vector{
        TColumnSchema("user_id", EValueType::Int64),
        TColumnSchema("data", EValueType::Int64),
    });
    auto reversed = New<TTableSchema>(std::vector{
        TColumnSchema("data", EValueType::Int64),
        TColumnSchema("user_id", EValueType::Int64),
    });
    auto equalCopy = New<TTableSchema>(*ordinary);
    TMessageFactory first(ordinary);
    TMessageFactory second(reversed);
    TMessageFactory copied(equalCopy);
    auto shards = ResolveShardsFromYson(
        "{shard_hosts={a=[h1];b=[h2];c=[h3]};table=t}");
    TClickHouseShardRouter router(shards, {"user_id"}, {ordinary, reversed});
    for (i64 id = 0; id < 100; ++id) {
        EXPECT_EQ(router.SelectShard(first.Make(id, id % 7)),
            router.SelectShard(second.Make(id, id % 7)));
        EXPECT_EQ(router.SelectShard(first.Make(id, id % 7)),
            router.SelectShard(copied.Make(id, id % 7)));
    }
}

TEST(TClickHouseShardTest, EqualSchemaPointersAreMemoizedIndependently)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("user_id", EValueType::Int64),
        TColumnSchema("data", EValueType::Int64),
    });
    auto firstCopy = New<TTableSchema>(*schema);
    auto secondCopy = New<TTableSchema>(*schema);
    TMessageFactory firstFactory(firstCopy);
    TMessageFactory secondFactory(secondCopy);
    TClickHouseShardRouter router(
        ResolveShardsFromYson("{shard_hosts={a=[h1];b=[h2]};table=t}"),
        {"user_id"},
        {schema});

    EXPECT_EQ(TClickHouseShardRouterTestPeer::GetSchemaCacheSize(router), 0);
    router.SelectShard(firstFactory.Make(1, 1));
    EXPECT_EQ(TClickHouseShardRouterTestPeer::GetSchemaCacheSize(router), 1);
    for (i64 id : xrange(100)) {
        router.SelectShard(firstFactory.Make(id, id));
    }
    EXPECT_EQ(TClickHouseShardRouterTestPeer::GetSchemaCacheSize(router), 1);
    router.SelectShard(secondFactory.Make(1, 1));
    EXPECT_EQ(TClickHouseShardRouterTestPeer::GetSchemaCacheSize(router), 2);
}

TEST(TClickHouseShardTest, RoutingByKeyMatchesGolden)
{
    TMessageFactory factory;
    auto shards = ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"];d=["h4"]};table=t})");
    TClickHouseShardRouter routerK(shards, {"user_id"}, {factory.GetSchema()});
    EXPECT_EQ(routerK.SelectShard(factory.Make(0LL, 0LL)), 2);
    EXPECT_EQ(routerK.SelectShard(factory.Make(1LL, -1LL)), 0);
    EXPECT_EQ(routerK.SelectShard(factory.Make(2LL, 2LL)), 2);
    EXPECT_EQ(routerK.SelectShard(factory.Make(3LL, 0LL)), 2);
    EXPECT_EQ(routerK.SelectShard(factory.Make(17LL, 97LL)), 1);
    EXPECT_EQ(routerK.SelectShard(factory.Make(97LL, 17LL)), 2);
    EXPECT_EQ(routerK.SelectShard(factory.Make(1024LL, -1024LL)), 3);
    EXPECT_EQ(routerK.SelectShard(factory.Make(8191LL, 8191LL)), 1);
    EXPECT_EQ(routerK.SelectShard(factory.Make(9223372036854775806LL, -9223372036854775807LL)), 2);
    TClickHouseShardRouter routerC(shards, {"user_id", "data"}, {factory.GetSchema()});
    EXPECT_EQ(routerC.SelectShard(factory.Make(0LL, 0LL)), 0);
    EXPECT_EQ(routerC.SelectShard(factory.Make(1LL, -1LL)), 2);
    EXPECT_EQ(routerC.SelectShard(factory.Make(2LL, 2LL)), 3);
    EXPECT_EQ(routerC.SelectShard(factory.Make(3LL, 0LL)), 0);
    EXPECT_EQ(routerC.SelectShard(factory.Make(17LL, 97LL)), 1);
    EXPECT_EQ(routerC.SelectShard(factory.Make(97LL, 17LL)), 1);
    EXPECT_EQ(routerC.SelectShard(factory.Make(1024LL, -1024LL)), 1);
    EXPECT_EQ(routerC.SelectShard(factory.Make(8191LL, 8191LL)), 0);
    EXPECT_EQ(routerC.SelectShard(factory.Make(9223372036854775806LL, -9223372036854775807LL)), 2);
}

TEST(TClickHouseShardTest, RoutingByMessageIdMatchesGolden)
{
    TMessageFactory factory;
    auto shards = ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"];d=["h4"]};table=t})");
    TClickHouseShardRouter routerM(shards, {}, {factory.GetSchema()});
    EXPECT_EQ(routerM.SelectShard(factory.Make(0LL, 0LL)), 1);
    EXPECT_EQ(routerM.SelectShard(factory.Make(1LL, -1LL)), 0);
    EXPECT_EQ(routerM.SelectShard(factory.Make(2LL, 2LL)), 0);
    EXPECT_EQ(routerM.SelectShard(factory.Make(3LL, 0LL)), 3);
    EXPECT_EQ(routerM.SelectShard(factory.Make(17LL, 97LL)), 1);
    EXPECT_EQ(routerM.SelectShard(factory.Make(97LL, 17LL)), 1);
    EXPECT_EQ(routerM.SelectShard(factory.Make(1024LL, -1024LL)), 1);
    EXPECT_EQ(routerM.SelectShard(factory.Make(8191LL, 8191LL)), 3);
    EXPECT_EQ(routerM.SelectShard(factory.Make(9223372036854775806LL, -9223372036854775807LL)), 0);
}

TEST(TClickHouseShardTest, TopologyFingerprintMatchesGolden)
{
    auto shards = ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"];d=["h4"]};table=t})");
    EXPECT_EQ(BuildShardTopologyFingerprint(shards, {}), "4ff129f273d570b8");
    EXPECT_EQ(BuildShardTopologyFingerprint(shards, {"user_id"}), "95669ff6a63262cc");
    EXPECT_EQ(BuildShardTopologyFingerprint(shards, {"user_id", "data"}), "cc5bdfa15e7f8a81");
    EXPECT_EQ(BuildShardTopologyFingerprint(shards, {"data", "user_id"}), "eeff21bf849d133e");
}

TEST(TClickHouseShardTest, TargetIdentityAllowsOnlyProvenReplicaReplacement)
{
    auto original = ResolveShardsFromYson(R"({shard_hosts={a=["h1"]};table=t})").front();
    auto replacement = ResolveShardsFromYson(R"({shard_hosts={a=["h2"]};table=t})").front();
    const TClickHouseReplicationIdentity identity{"default", "/tables/a"};
    const TClickHouseReplicationIdentity anotherIdentity{"default", "/tables/another"};

    EXPECT_EQ(
        BuildShardTargetIdentityFingerprint(original, identity),
        BuildShardTargetIdentityFingerprint(replacement, identity));
    EXPECT_NE(
        BuildShardTargetIdentityFingerprint(original, identity),
        BuildShardTargetIdentityFingerprint(replacement, anotherIdentity));
    EXPECT_NE(
        BuildShardTargetIdentityFingerprint(original, std::nullopt),
        BuildShardTargetIdentityFingerprint(replacement, std::nullopt));
}

TEST(TClickHouseShardTest, RoutingIgnoresSpecOrder)
{
    TMessageFactory factory;
    TClickHouseShardRouter first(
        ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"]};table=t})"),
        {"user_id"},
        {factory.GetSchema()});
    TClickHouseShardRouter second(
        ResolveShardsFromYson(R"({shard_hosts={c=["h3"];b=["h2"];a=["h1"]};table=t})"),
        {"user_id"},
        {factory.GetSchema()});

    for (i64 id : xrange(1000)) {
        auto message = factory.Make(id, id % 101);
        EXPECT_EQ(
            first.GetShards()[first.SelectShard(message)].Name,
            second.GetShards()[second.SelectShard(message)].Name);
    }
}

TEST(TClickHouseShardTest, RoutingColocatesEqualKeys)
{
    TMessageFactory factory;
    TClickHouseShardRouter router(
        ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"]};table=t})"),
        {"user_id"},
        {factory.GetSchema()});

    THashMap<i64, int> shardByUserId;
    THashSet<int> usedShards;
    for (i64 id : xrange(3000)) {
        const i64 userId = id % 37;
        auto index = router.SelectShard(factory.Make(id, userId));
        usedShards.insert(index);
        auto [it, inserted] = shardByUserId.emplace(userId, index);
        EXPECT_EQ(it->second, index);
    }
    EXPECT_GT(std::ssize(usedShards), 1);
}

TEST(TClickHouseShardTest, RoutingWithoutKeyUsesMessageId)
{
    TMessageFactory factory;
    TClickHouseShardRouter router(
        ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"];d=["h4"]};table=t})"),
        /*shardingKeyColumns*/ {},
        {factory.GetSchema()});

    std::vector<int> counts(4, 0);
    for (i64 id : xrange(10000)) {
        auto index = router.SelectShard(factory.Make(id, /*userId*/ 0));
        EXPECT_EQ(index, router.SelectShard(factory.Make(id, /*userId*/ 1)));
        ++counts[index];
    }
    for (int count : counts) {
        EXPECT_GT(count, 1500);
    }
}

TEST(TClickHouseShardTest, RoutingIsStableWhenAShardIsRemoved)
{
    TMessageFactory factory;
    TClickHouseShardRouter before(
        ResolveShardsFromYson(
            R"({shard_hosts={a=["h1"];b=["h2"];c=["h3"];d=["h4"]};table=t})"),
        {"user_id"},
        {factory.GetSchema()});
    TClickHouseShardRouter after(
        ResolveShardsFromYson(R"({shard_hosts={a=["h1"];b=["h2"];d=["h4"]};table=t})"),
        {"user_id"},
        {factory.GetSchema()});

    int movedFromRemovedShard = 0;
    for (i64 id : xrange(10000)) {
        auto message = factory.Make(id, id);
        const auto& beforeName = before.GetShards()[before.SelectShard(message)].Name;
        const auto& afterName = after.GetShards()[after.SelectShard(message)].Name;
        if (beforeName == "c") {
            ++movedFromRemovedShard;
            EXPECT_NE(afterName, "c");
        } else {
            EXPECT_EQ(beforeName, afterName);
        }
    }
    EXPECT_GT(movedFromRemovedShard, 0);
}

TEST(TClickHouseShardTest, TopologyFingerprintIgnoresOrderAndHostsAndTracksShardNames)
{
    auto fingerprint = [] (TStringBuf yson, std::vector<std::string> shardingKeyColumns = {}) {
        return BuildShardTopologyFingerprint(
            ResolveShardsFromYson(yson),
            shardingKeyColumns);
    };

    auto base = fingerprint(R"({shard_hosts={a=["h1";"h2"];b=["h3"]};table=t})");
    EXPECT_EQ(base, fingerprint(R"({shard_hosts={b=["h3"];a=["h2";"h1"]};table=t})"));

    EXPECT_EQ(base, fingerprint(R"({shard_hosts={a=["h1";"h2";"h5"];b=["h3"]};table=t})"));
    EXPECT_EQ(base, fingerprint(R"({shard_hosts={a=["h-new"];b=["h3"]};table=t})"));

    EXPECT_NE(base, fingerprint(R"({shard_hosts={a=["h1";"h2"];b=["h3"];c=["h4"]};table=t})"));
    EXPECT_NE(base, fingerprint(R"({shard_hosts={z=["h1";"h2"];b=["h3"]};table=t})"));

    EXPECT_EQ(fingerprint("{host=h;table=t}"), "unsharded");
    EXPECT_EQ(fingerprint(R"({hosts=["h";"r1"];table=t})"), "unsharded");
}

TEST(TClickHouseShardTest, TopologyFingerprintTracksShardingKeyColumns)
{
    constexpr TStringBuf Spec = R"({shard_hosts={a=["h1"];b=["h2"]};table=t})";
    auto fingerprint = [&] (std::vector<std::string> shardingKeyColumns) {
        return BuildShardTopologyFingerprint(
            ResolveShardsFromYson(Spec),
            shardingKeyColumns);
    };

    // Routing reads the key columns in spec order, so order is as load-bearing as membership.
    EXPECT_NE(fingerprint({}), fingerprint({"a"}));
    EXPECT_NE(fingerprint({"a"}), fingerprint({"b"}));
    EXPECT_NE(fingerprint({"a", "b"}), fingerprint({"b", "a"}));
    EXPECT_NE(fingerprint({"a", "b"}), fingerprint({"ab"}));
    EXPECT_EQ(fingerprint({"a", "b"}), fingerprint({"a", "b"}));
}

TEST(TClickHouseShardTest, TopologyFingerprintIsUnambiguousAcrossSeparators)
{
    constexpr TStringBuf Spec = R"({shard_hosts={a=["h1"];b=["h2"]};table=t})";
    auto fingerprint = [&] (std::vector<std::string> shardingKeyColumns) {
        return BuildShardTopologyFingerprint(
            ResolveShardsFromYson(Spec),
            shardingKeyColumns);
    };

    // A column name carrying a separator must not serialize to the same bytes as two columns.
    EXPECT_NE(fingerprint({"a,b"}), fingerprint({"a", "b"}));
    EXPECT_NE(fingerprint({"a|"}), fingerprint({"a"}));
}

TEST(TClickHouseShardTest, ValidateShardingKeyColumnsRejectsMissingColumn)
{
    TMessageFactory factory;
    std::vector<TTableSchemaPtr> schemas{factory.GetSchema()};

    EXPECT_NO_THROW(ValidateShardingKeyColumns({}, schemas));
    EXPECT_NO_THROW(ValidateShardingKeyColumns({"user_id"}, schemas));
    EXPECT_THROW(ValidateShardingKeyColumns({"missing"}, schemas), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
