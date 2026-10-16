#include <yt/yt/flow/library/cpp/connectors/queue/spec.h>
#include <yt/yt/flow/library/cpp/connectors/queue/tablet_index_evaluator.h>
#include <yt/yt/flow/library/cpp/connectors/queue/tablet_router.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <deque>
#include <limits>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// farm_hash(456u) computed by YT's column evaluator (see schema_ut.cpp).
constexpr ui64 KnownHash = 2044001940267648219ull;
constexpr auto MaxHash = std::numeric_limits<ui64>::max();

TTableSchemaPtr MakeSchema()
{
    return New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("order_id", EValueType::Uint64),
        TColumnSchema("data", EValueType::String),
        // A non-comparable column must not break evaluator construction.
        TColumnSchema("blob", EValueType::Any),
    });
}

TPayload MakePayload(const TTableSchemaPtr& schema, ui64 orderId)
{
    TPayloadBuilder builder(schema);
    builder.SetValue(MakeUnversionedUint64Value(orderId, 0));
    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TReduceHashToTabletIndexTest, Reduce)
{
    constexpr auto Modulo = EQueueTabletIndexRoutingHashPolicy::Modulo;
    constexpr auto Range = EQueueTabletIndexRoutingHashPolicy::Range;
    // Range: rangeSize = MAX / N + 1, shard = hash / rangeSize.
    constexpr ui64 RangeSize4 = MaxHash / 4 + 1;

    struct TCase
    {
        ui64 Hash;
        i64 TabletCount;
        EQueueTabletIndexRoutingHashPolicy Policy;
        i64 Expected;
    };

    for (const auto& c : std::vector<TCase>{
            {10, 3, Modulo, 1},
            {0, 5, Modulo, 0},
            {9, 3, Modulo, 0},
            {MaxHash, 1, Modulo, 0},
            {0, 1, Range, 0},
            {MaxHash, 1, Range, 0},
            {0, 4, Range, 0},
            {MaxHash, 4, Range, 3},
            {RangeSize4, 4, Range, 1},
            {RangeSize4 - 1, 4, Range, 0},
            {2 * RangeSize4, 4, Range, 2},
            {3 * RangeSize4, 4, Range, 3},
         }) {
        SCOPED_TRACE(Format("hash=%v count=%v policy=%v", c.Hash, c.TabletCount, c.Policy));
        EXPECT_EQ(ReduceHashToTabletIndex(c.Hash, c.TabletCount, c.Policy), c.Expected);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTabletIndexEvaluatorTest, Evaluate)
{
    constexpr auto Modulo = EQueueTabletIndexRoutingHashPolicy::Modulo;
    constexpr auto Range = EQueueTabletIndexRoutingHashPolicy::Range;

    struct TCase
    {
        const char* Expression;
        std::optional<EQueueTabletIndexRoutingHashPolicy> Policy;
        i64 TabletCount;
        i64 Expected;
    };

    for (const auto& c : std::vector<TCase>{
            {"farm_hash(order_id) % 100", std::nullopt, 100, static_cast<i64>(KnownHash % 100)},
            {"farm_hash(order_id)", Modulo, 100, ReduceHashToTabletIndex(KnownHash, 100, Modulo)},
            {"farm_hash(order_id)", Range, 100, ReduceHashToTabletIndex(KnownHash, 100, Range)},
         }) {
        SCOPED_TRACE(c.Expression);
        auto schema = MakeSchema();
        auto evaluator = New<TTabletIndexEvaluator>(schema, c.Expression, c.Policy);
        EXPECT_EQ(evaluator->GetTabletIndex(MakePayload(schema, 456), c.TabletCount), c.Expected);
    }
}

// Evaluating different payloads on one instance must not leak state through the reused row.
TEST(TTabletIndexEvaluatorTest, RowReuse)
{
    auto schema = MakeSchema();
    auto evaluator = New<TTabletIndexEvaluator>(schema, "farm_hash(order_id)", EQueueTabletIndexRoutingHashPolicy::Modulo);
    auto a1 = evaluator->GetTabletIndex(MakePayload(schema, 12345), 7);
    auto b = evaluator->GetTabletIndex(MakePayload(schema, 67890), 7);
    auto a2 = evaluator->GetTabletIndex(MakePayload(schema, 12345), 7);
    EXPECT_EQ(a1, a2);
    for (auto index : {a1, b, a2}) {
        EXPECT_GE(index, 0);
        EXPECT_LT(index, 7);
    }
}

TEST(TTabletIndexEvaluatorTest, VerbatimOverflowThrows)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("big", EValueType::Uint64),
    });
    auto evaluator = New<TTabletIndexEvaluator>(schema, "big", /*policy*/ std::nullopt);

    TPayloadBuilder ok(schema);
    ok.SetValue(MakeUnversionedUint64Value(5, 0));
    EXPECT_EQ(evaluator->GetTabletIndex(ok.Finish(), /*tabletCount*/ 10), 5);

    // 2^63 does not fit into a non-negative int64 $tablet_index.
    TPayloadBuilder overflow(schema);
    overflow.SetValue(MakeUnversionedUint64Value(1ull << 63, 0));
    EXPECT_THROW(evaluator->GetTabletIndex(overflow.Finish(), /*tabletCount*/ 10), std::exception);
}

TEST(TTabletIndexEvaluatorTest, OutOfRangeThrows)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("big", EValueType::Uint64),
    });
    auto evaluator = New<TTabletIndexEvaluator>(schema, "big", /*policy*/ std::nullopt);

    TPayloadBuilder builder(schema);
    builder.SetValue(MakeUnversionedUint64Value(5, 0));
    // 5 is not a valid tablet index for a 3-tablet queue.
    EXPECT_THROW(evaluator->GetTabletIndex(builder.Finish(), /*tabletCount*/ 3), std::exception);
}

TEST(TTabletIndexEvaluatorTest, StringResultTypeThrows)
{
    auto schema = MakeSchema();
    // A bare string column produces a string, not a uint64 tablet index.
    EXPECT_THROW(
        New<TTabletIndexEvaluator>(schema, "data", /*policy*/ std::nullopt),
        std::exception);
}

TEST(TTabletIndexEvaluatorTest, UnknownColumnThrows)
{
    auto schema = MakeSchema();
    EXPECT_THROW(
        New<TTabletIndexEvaluator>(schema, "farm_hash(no_such_column)", /*policy*/ std::nullopt),
        std::exception);
}

TEST(TTabletIndexEvaluatorTest, MistypedColumnThrows)
{
    auto schema = MakeSchema();
    // "data" exists but is a string; modulo on it is a type error surfaced at build time.
    EXPECT_THROW(
        New<TTabletIndexEvaluator>(schema, "data % 5", /*policy*/ std::nullopt),
        std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TGroupMessagesByTabletIndexTest, OrdersTabletsAndPreservesPerTabletOrder)
{
    auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("tablet", EValueType::Uint64),
        TColumnSchema("id", EValueType::Uint64),
    });

    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("s")][TStreamSpecId(1)] = streamSpec;
    auto specStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        /*groupBySchema*/ New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    auto makeMessage = [&] (ui64 tablet, ui64 id) {
        TMessageBuilder builder("s", schema);
        builder.SetMessageId(TMessageId("msg" + ToString(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<ui64>(tablet, "tablet");
        builder.Payload().Set<ui64>(id, "id");
        return New<TOutputMessage>(builder.Finish(), specStorage);
    };

    // Verbatim: $tablet_index is the "tablet" column value; explicit count => no queue read.
    auto router = New<TTabletRouter>(
        New<TTabletIndexEvaluator>(schema, "tablet", /*policy*/ std::nullopt),
        /*explicitTabletCount*/ 4,
        /*context*/ nullptr,
        NYPath::TRichYPath("//queue"),
        TDuration::Zero(),
        NLogging::TLogger());

    std::deque<TOutputMessageConstPtr> messages;
    for (auto [tablet, id] : std::vector<std::pair<ui64, ui64>>{{2, 0}, {0, 1}, {2, 2}, {1, 3}, {0, 4}}) {
        messages.push_back(makeMessage(tablet, id));
    }

    auto groups = GroupMessagesByTabletIndex(*router, messages);

    // Groups are ordered by tablet index.
    std::vector<i64> tablets;
    for (const auto& [tablet, group] : groups) {
        tablets.push_back(tablet);
    }
    EXPECT_EQ(tablets, (std::vector<i64>{0, 1, 2}));

    // Per-tablet arrival order is preserved.
    auto idsOf = [] (const std::deque<TOutputMessageConstPtr>& group) {
        std::vector<ui64> ids;
        for (const auto& message : group) {
            ids.push_back(GetColumnValue<ui64>(*message, "id"));
        }
        return ids;
    };
    EXPECT_EQ(idsOf(groups[0]), (std::vector<ui64>{1, 4}));
    EXPECT_EQ(idsOf(groups[1]), (std::vector<ui64>{3}));
    EXPECT_EQ(idsOf(groups[2]), (std::vector<ui64>{0, 2}));
}

////////////////////////////////////////////////////////////////////////////////

// Async queue sinks recognize the routing params (so they are not silently swallowed) but reject
// them (routing is sync-only for now, YTFLOW-766).
TEST(TAsyncSinkRoutingRejectionTest, RejectsRoutingParameters)
{
    auto parameters = New<TQueueSinkTabletRoutingParameters>();
    EXPECT_NO_THROW(ValidateAsyncSinkTabletRoutingUnsupported(*parameters));

    parameters->TabletIndexExpression = "farm_hash(data) % 5";
    EXPECT_THROW(ValidateAsyncSinkTabletRoutingUnsupported(*parameters), std::exception);

    parameters->TabletIndexExpression.reset();
    parameters->TabletIndexRoutingHashExpression = "farm_hash(data)";
    EXPECT_THROW(ValidateAsyncSinkTabletRoutingUnsupported(*parameters), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
