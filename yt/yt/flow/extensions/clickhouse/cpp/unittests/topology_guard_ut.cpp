#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf TwoShards = R"(shard_hosts={a=["ch-a"];b=["ch-b"]};)";
constexpr TStringBuf TwoShardsReordered = R"(shard_hosts={b=["ch-b"];a=["ch-a"]};)";
constexpr TStringBuf ThreeShards = R"(shard_hosts={a=["ch-a"];b=["ch-b"];c=["ch-c"]};)";
constexpr TStringBuf TwoShardsWithReplacedHost = R"(shard_hosts={a=["ch-a-new"];b=["ch-b"]};)";
constexpr TStringBuf SingleHost = R"(host="ch-a";)";
constexpr TStringBuf UnshardedHostList = R"(hosts=["ch-a";"ch-a-2"];)";
constexpr TStringBuf TwoShardsKeyedById =
    R"(shard_hosts={a=["ch-a"];b=["ch-b"]};sharding_key_columns=["id"];)";
constexpr TStringBuf TwoShardsKeyedByData =
    R"(shard_hosts={a=["ch-a"];b=["ch-b"]};sharding_key_columns=["data"];)";

////////////////////////////////////////////////////////////////////////////////

class TTopologyGuardFixture
{
public:
    TTopologyGuardFixture()
        : Schema_(New<TTableSchema>(std::vector{
              TColumnSchema("id", EValueType::Int64),
              TColumnSchema("data", EValueType::Int64),
          }))
        , StateManager_(New<TStateManagerMock>())
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = Schema_;
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
        streamSpecs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
        SpecStorage_ = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(std::move(streamSpecs)),
            New<TTableSchema>(),
            /*evaluatorCache*/ nullptr);
    }

    ISinkPtr CreateSink(TStringBuf hostParameters) const
    {
        auto context = New<TSinkContext>();
        context->Logger = NLogging::TLogger("ClickHouseTopologyGuardTest");
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->StreamSpecStorage = SpecStorage_;
        const bool sharded = hostParameters.find("shard_hosts") != TStringBuf::npos;
        const auto typeName = sharded
            ? TypeName<TShardedClickHouseBatchingSink>()
            : TypeName<TClickHouseBatchingSink>();
        context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(Format("{sink_class_name=%Qv;input_stream_ids=[test];parameters={%vport=9000;table=events};}",
            typeName,
            hostParameters)));

        auto dynamicSpec = New<TDynamicSinkSpec>();
        dynamicSpec->Parameters->AddChild("max_rows_per_batch", ConvertToNode(1));
        auto dynamicContext = New<TDynamicSinkContext>();
        dynamicContext->DynamicSinkSpec = std::move(dynamicSpec);

        if (sharded) {
            return New<TShardedClickHouseBatchingSink>(context, dynamicContext);
        }
        return New<TClickHouseBatchingSink>(context, dynamicContext);
    }

    TOutputMessageConstPtr MakeMessage(i64 id) const
    {
        TMessageBuilder builder("test", Schema_);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<i64>(id, "id");
        builder.Payload().Set<i64>(id, "data");
        return New<TOutputMessage>(builder.Finish(), SpecStorage_);
    }

    void LeaveUndeliveredBatch(TStringBuf hostParameters) const
    {
        auto sink = CreateSink(hostParameters);
        sink->Init(StateManager_->CreateContext());
        auto tracker = TDistributingTracker([] {
        });
        DynamicPointerCast<TClickHouseBatchingSinkBase>(sink)
            ->TOrderedBatchingAsyncSinkBase::Distribute(MakeMessage(1), tracker.AddDestination());
        tracker.Activate();
        sink->Sync(nullptr);
        StateManager_->Sync();
    }

    void ForgetTopologyState() const
    {
        auto storage = StateManager_->GetStorage();
        THashMap<std::string, NYson::TYsonString> kept;
        for (auto& [name, value] : storage) {
            if (name.find("shard_topology") == std::string::npos) {
                kept.emplace(name, value);
            }
        }
        StateManager_->SetStorage(std::move(kept));
    }

    void InitAndPersist(TStringBuf hostParameters) const
    {
        auto sink = CreateSink(hostParameters);
        sink->Init(StateManager_->CreateContext());
        StateManager_->Sync();
    }

private:
    const TTableSchemaPtr Schema_;
    const TStateManagerMockPtr StateManager_;
    TComputationStreamSpecStoragePtr SpecStorage_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseTopologyGuardTest, FirstInitStampsAndRepeatedInitAccepts)
{
    TTopologyGuardFixture fixture;
    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShards));
    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShards));
}

TEST(TClickHouseTopologyGuardTest, KeyOrderAloneNeverTrips)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(TwoShards);
    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShardsReordered));
}

TEST(TClickHouseTopologyGuardTest, ShardHostReplacementDoesNotChangeRoutingTopology)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(TwoShards);

    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShardsWithReplacedHost));
}

TEST(TClickHouseTopologyGuardTest, ChangedTopologyWithUndeliveredBatchesIsRefused)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(TwoShards);

    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShards));
    try {
        fixture.InitAndPersist(ThreeShards);
        ADD_FAILURE() << "Expected the topology guard to refuse the changed spec";
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), ::testing::HasSubstr("Refusing to start the ClickHouse sink"));
        EXPECT_THAT(ex.what(), ::testing::HasSubstr("oldest_undelivered_batch_bound"));
    }
}

TEST(TClickHouseTopologyGuardTest, DrainedStateAcceptsAndRestampsChangedTopology)
{
    TTopologyGuardFixture fixture;
    fixture.InitAndPersist(TwoShards);
    EXPECT_NO_THROW(fixture.InitAndPersist(ThreeShards));

    fixture.LeaveUndeliveredBatch(ThreeShards);
    EXPECT_NO_THROW(fixture.InitAndPersist(ThreeShards));
    EXPECT_THROW(fixture.InitAndPersist(TwoShards), TErrorException);
}

TEST(TClickHouseTopologyGuardTest, UnshardedReplicaAdditionNeverTrips)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(SingleHost);
    EXPECT_NO_THROW(fixture.InitAndPersist(UnshardedHostList));
}

TEST(TClickHouseTopologyGuardTest, UnshardedToShardedMigrationIsRefusedWhileUndrained)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(SingleHost);
    EXPECT_THROW(fixture.InitAndPersist(TwoShards), TErrorException);
}

TEST(TClickHouseTopologyGuardTest, UnstampedStateWithUndeliveredBatchesRefusesSharding)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(SingleHost);
    fixture.ForgetTopologyState();

    try {
        fixture.InitAndPersist(TwoShards);
        ADD_FAILURE() << "Expected the topology guard to refuse sharding on a never-stamped state";
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), ::testing::HasSubstr("Refusing to start the ClickHouse sink"));
    }
}

TEST(TClickHouseTopologyGuardTest, UnstampedStateWithUndeliveredBatchesAllowsUnsharded)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(SingleHost);
    fixture.ForgetTopologyState();

    // The upgrade of an existing single-shard pipeline must not be blocked by its own
    // in-flight batches: nothing about their routing or tokens changes.
    EXPECT_NO_THROW(fixture.InitAndPersist(UnshardedHostList));
}

TEST(TClickHouseTopologyGuardTest, ShardingKeyColumnChangeIsRefusedWhileUndrained)
{
    TTopologyGuardFixture fixture;
    fixture.LeaveUndeliveredBatch(TwoShardsKeyedById);
    EXPECT_NO_THROW(fixture.InitAndPersist(TwoShardsKeyedById));
    EXPECT_THROW(fixture.InitAndPersist(TwoShardsKeyedByData), TErrorException);
}

TEST(TClickHouseTopologyGuardTest, MatchingTargetIdentitiesAreAcceptedWhileUndrained)
{
    auto state = New<TClickHouseShardTopologyState>();
    state->TargetIdentityFingerprints = std::vector<std::string>{"target-a", "target-b"};
    const std::deque<TMessageId> pending{TMessageId(LexicographicallySerialize(i64{1}))};

    EXPECT_NO_THROW(ValidateAndUpdateShardTargetIdentityFingerprints(
        state.Get(),
        {"target-a", "target-b"},
        pending,
        true));
}

TEST(TClickHouseTopologyGuardTest, ChangedTargetIdentityIsRefusedWhileUndrained)
{
    auto state = New<TClickHouseShardTopologyState>();
    state->TargetIdentityFingerprints = std::vector<std::string>{"target-a", "target-b"};
    const std::deque<TMessageId> pending{TMessageId(LexicographicallySerialize(i64{1}))};

    try {
        ValidateAndUpdateShardTargetIdentityFingerprints(
            state.Get(),
            {"other-a", "target-b"},
            pending,
            true);
        ADD_FAILURE() << "Expected the target identity guard to refuse the changed target";
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), ::testing::HasSubstr("another deduplication log"));
    }
}

TEST(TClickHouseTopologyGuardTest, UnstampedShardedTargetIsRefusedWhileUndrained)
{
    auto state = New<TClickHouseShardTopologyState>();
    const std::deque<TMessageId> pending{TMessageId(LexicographicallySerialize(i64{1}))};

    EXPECT_THROW(ValidateAndUpdateShardTargetIdentityFingerprints(
        state.Get(),
        {"target-a", "target-b"},
        pending,
        true),
        TErrorException);
}

TEST(TClickHouseTopologyGuardTest, DrainedTargetIdentityIsRestamped)
{
    auto state = New<TClickHouseShardTopologyState>();
    state->TargetIdentityFingerprints = std::vector<std::string>{"target-a", "target-b"};

    ValidateAndUpdateShardTargetIdentityFingerprints(
        state.Get(),
        {"other-a", "target-b"},
        {},
        true);

    EXPECT_EQ(
        *state->TargetIdentityFingerprints,
        (std::vector<std::string>{"other-a", "target-b"}));
}

TEST(TClickHouseTopologyGuardTest, UnstampedUnshardedTargetIsAcceptedWhileUndrained)
{
    auto state = New<TClickHouseShardTopologyState>();
    const std::deque<TMessageId> pending{TMessageId(LexicographicallySerialize(i64{1}))};

    EXPECT_NO_THROW(ValidateAndUpdateShardTargetIdentityFingerprints(
        state.Get(),
        {"target"},
        pending,
        false));
    EXPECT_EQ(*state->TargetIdentityFingerprints, (std::vector<std::string>{"target"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
