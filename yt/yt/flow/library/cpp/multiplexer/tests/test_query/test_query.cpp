#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/multiplexer/dynamic_table_multiplexer_computation.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCppTests;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// Lookup table with one group_by column (string) and four secondary-key columns
// covering all main key types: i64, ui64, string, and any. Plus an Any value column.
class TBuildSelectQueryYtTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();
        CreateTable(
            /*tablePath*/ "//tmp/multiplexer_lookup",
            /*schema*/ NYson::TYsonString(R"([
                {name = key; type = string; required = %true; sort_order = ascending};
                {name = sk_i64; type = int64; required = %true; sort_order = ascending};
                {name = sk_ui64; type = uint64; required = %true; sort_order = ascending};
                {name = sk_string; type = string; required = %true; sort_order = ascending};
                {name = sk_any; type = any; sort_order = ascending};
                {name = payload; type = any}
            ])"_sb));

        SeedRows();
    }

protected:
    static TTableSchemaPtr GetGroupBySchema()
    {
        return New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
        });
    }

    static TTableSchemaPtr GetRowSchema()
    {
        return New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("sk_i64", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("sk_ui64", EValueType::Uint64, ESortOrder::Ascending),
            TColumnSchema("sk_string", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("sk_any", EValueType::Any, ESortOrder::Ascending),
            TColumnSchema("payload", EValueType::Any),
        });
    }

    static const std::vector<std::string>& SecondaryKeyColumns()
    {
        static const std::vector<std::string> result = {"sk_i64", "sk_ui64", "sk_string", "sk_any"};
        return result;
    }

    static std::string BuildQuery(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit = 100)
    {
        return NDynamicTableMultiplexer::BuildSelectQuery(
            *GetGroupBySchema(),
            key,
            SecondaryKeyColumns(),
            startOffsetExclusive,
            endOffsetInclusive,
            *GetRowSchema(),
            Table_,
            limit)
            .Query;
    }

    // Run BuildSelectQuery and execute it via Client_, returning the resulting rowset.
    static IUnversionedRowsetPtr RunQuery(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit = 100)
    {
        auto queryString = BuildQuery(key, startOffsetExclusive, endOffsetInclusive, limit);
        auto result = WaitFor(Client_->SelectRows(queryString)).ValueOrThrow();
        return result.Rowset;
    }

    static TKey MakeStringKey(TStringBuf value)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(value));
        return TKey(TKey::TUnderlying(builder.FinishRow()));
    }

    static TKey MakeSecondaryKey(i64 i, ui64 u, TStringBuf s, TStringBuf anyYson)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(i));
        builder.AddValue(MakeUnversionedUint64Value(u));
        builder.AddValue(MakeUnversionedStringValue(s));
        builder.AddValue(MakeUnversionedAnyValue(anyYson));
        return TKey(TKey::TUnderlying(builder.FinishRow()));
    }

private:
    static void SeedRows()
    {
        // Three rows for "alpha" + one row for "beta".
        // sk_any varies — list/map/scalar — to confirm Any works as a sort key.
        // payload is also Any, varied independently.
        auto nameTable = New<TNameTable>();
        int keyId = nameTable->GetIdOrRegisterName("key");
        int skI64Id = nameTable->GetIdOrRegisterName("sk_i64");
        int skUi64Id = nameTable->GetIdOrRegisterName("sk_ui64");
        int skStringId = nameTable->GetIdOrRegisterName("sk_string");
        int skAnyId = nameTable->GetIdOrRegisterName("sk_any");
        int payloadId = nameTable->GetIdOrRegisterName("payload");

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;
        auto addRow = [&] (
            TStringBuf key,
            i64 i,
            ui64 u,
            TStringBuf s,
            TStringBuf skAnyYson,
            TStringBuf payloadYson) {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedStringValue(key, keyId));
            builder.AddValue(MakeUnversionedInt64Value(i, skI64Id));
            builder.AddValue(MakeUnversionedUint64Value(u, skUi64Id));
            builder.AddValue(MakeUnversionedStringValue(s, skStringId));
            builder.AddValue(MakeUnversionedAnyValue(skAnyYson, skAnyId));
            builder.AddValue(MakeUnversionedAnyValue(payloadYson, payloadId));
            auto owning = builder.FinishRow();
            rows.push_back(rowBuffer->CaptureRow(owning.Get()));
        };
        // sk_any is treated as a tuple (a fixed-shape YSON list) — that's the only
        // form we promise to support for Any-typed sort columns.
        addRow("alpha", 1, 10u, "a", R"([1])", R"({foo=bar})");
        addRow("alpha", 2, 20u, "b", R"([2])", R"([1; 2; 3])");
        addRow("alpha", 3, 30u, "c", R"([3])", R"(42)");
        addRow("beta", 100, 1000u, "x", R"([100])", R"("hello")");

        // Rows under the "any_filter" key share the leading secondary-key
        // components and differ only by the Any-tuple sk_any. They exist to
        // verify that range conditions on Any-typed sort columns properly
        // filter when the comparison falls through to the Any component.
        addRow("any_filter", 1, 1u, "x", R"([1; 1])", R"(#)");
        addRow("any_filter", 1, 1u, "x", R"([1; 2])", R"(#)");
        addRow("any_filter", 1, 1u, "x", R"([1; 3])", R"(#)");

        WriteRows(nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBuildSelectQueryYtTest, ExactQueryString)
{
    // No offset: just the projection, the group_by equality, ORDER BY full key, LIMIT.
    EXPECT_EQ(
        BuildQuery(MakeStringKey("alpha"), std::nullopt, std::nullopt),
        R"(sk_i64,sk_ui64,sk_string,sk_any,payload )"
        R"(FROM [//tmp/multiplexer_lookup] )"
        R"(WHERE (key) = ("alpha") )"
        R"(ORDER BY key,sk_i64,sk_ui64,sk_string,sk_any )"
        R"(LIMIT 100)");

    // Start offset only.
    EXPECT_EQ(
        BuildQuery(MakeStringKey("alpha"), MakeSecondaryKey(1, 10u, "a", R"([1])"), std::nullopt, /*limit*/ 50),
        R"(sk_i64,sk_ui64,sk_string,sk_any,payload )"
        R"(FROM [//tmp/multiplexer_lookup] )"
        R"(WHERE (key) = ("alpha") )"
        R"(AND (sk_i64,sk_ui64,sk_string,sk_any) > (1,10u,"a",yson_string_to_any("[1]")) )"
        R"(ORDER BY key,sk_i64,sk_ui64,sk_string,sk_any )"
        R"(LIMIT 50)");

    // Both offsets — start exclusive, end inclusive.
    EXPECT_EQ(
        BuildQuery(
            MakeStringKey("any_filter"),
            MakeSecondaryKey(1, 1u, "x", R"([1; 2])"),
            MakeSecondaryKey(1, 1u, "x", R"([1; 3])"),
            /*limit*/ 100),
        R"(sk_i64,sk_ui64,sk_string,sk_any,payload )"
        R"(FROM [//tmp/multiplexer_lookup] )"
        R"(WHERE (key) = ("any_filter") )"
        R"(AND (sk_i64,sk_ui64,sk_string,sk_any) > (1,1u,"x",yson_string_to_any("[1; 2]")) )"
        R"(AND (sk_i64,sk_ui64,sk_string,sk_any) <= (1,1u,"x",yson_string_to_any("[1; 3]")) )"
        R"(ORDER BY key,sk_i64,sk_ui64,sk_string,sk_any )"
        R"(LIMIT 100)");
}

TEST_F(TBuildSelectQueryYtTest, NoOffsetReturnsAllRowsForKey)
{
    auto rowset = RunQuery(MakeStringKey("alpha"), std::nullopt, std::nullopt);
    auto rows = rowset->GetRows();
    auto schema = rowset->GetSchema();

    ASSERT_EQ(rows.Size(), 3u);

    // Order matches secondary_key_columns ordering (ASC by sk_i64, sk_ui64, sk_string, sk_any).
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[0])), schema, "sk_i64"), 1);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[1])), schema, "sk_i64"), 2);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[2])), schema, "sk_i64"), 3);

    EXPECT_EQ(GetColumnValue<ui64>(TPayload(TPayload::TUnderlying(rows[0])), schema, "sk_ui64"), 10u);
    EXPECT_EQ(GetColumnValue<TString>(TPayload(TPayload::TUnderlying(rows[0])), schema, "sk_string"), "a");

    // sk_any (key column of type Any) round-trips through TYsonString as a tuple.
    auto extractSkAny = [&] (TUnversionedRow row) {
        auto yson = GetColumnValue<NYson::TYsonString>(TPayload(TPayload::TUnderlying(row)), schema, "sk_any");
        auto list = ConvertToNode(yson)->AsList()->GetChildren();
        EXPECT_EQ(std::ssize(list), 1);
        return list[0]->GetValue<i64>();
    };
    EXPECT_EQ(extractSkAny(rows[0]), 1);
    EXPECT_EQ(extractSkAny(rows[1]), 2);
    EXPECT_EQ(extractSkAny(rows[2]), 3);

    // payload (value column of type Any) also round-trips through TYsonString.
    auto payload0 = GetColumnValue<NYson::TYsonString>(TPayload(TPayload::TUnderlying(rows[0])), schema, "payload");
    EXPECT_EQ(ConvertToNode(payload0)->AsMap()->GetChildValueOrThrow<TString>("foo"), "bar");
}

TEST_F(TBuildSelectQueryYtTest, StartOffsetSkipsRows)
{
    // Skip past row #1 — only rows #2 and #3 remain.
    auto rowset = RunQuery(
        MakeStringKey("alpha"),
        MakeSecondaryKey(1, 10u, "a", R"([1])"),
        std::nullopt);
    auto rows = rowset->GetRows();
    auto schema = rowset->GetSchema();

    ASSERT_EQ(rows.Size(), 2u);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[0])), schema, "sk_i64"), 2);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[1])), schema, "sk_i64"), 3);
}

TEST_F(TBuildSelectQueryYtTest, BothOffsetsRestrictRange)
{
    // (1, 10, "a", [1]) < x ≤ (3, 30, "c", [3]) — rows #2 and #3.
    // The upper bound's last component (Any tuple [3]) must compare equal to
    // row #3's sk_any value to include it — verifies that range conditions on
    // Any-typed sort columns work correctly when values share a fixed shape.
    auto rowset = RunQuery(
        MakeStringKey("alpha"),
        MakeSecondaryKey(1, 10u, "a", R"([1])"),
        MakeSecondaryKey(3, 30u, "c", R"([3])"));
    auto rows = rowset->GetRows();
    auto schema = rowset->GetSchema();

    ASSERT_EQ(rows.Size(), 2u);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[0])), schema, "sk_i64"), 2);
    EXPECT_EQ(GetColumnValue<i64>(TPayload(TPayload::TUnderlying(rows[1])), schema, "sk_i64"), 3);
}

TEST_F(TBuildSelectQueryYtTest, KeyIsolation)
{
    // Rows for "beta" are not visible when iterating "alpha" and vice versa.
    auto alphaRows = RunQuery(MakeStringKey("alpha"), std::nullopt, std::nullopt)->GetRows();
    auto betaRows = RunQuery(MakeStringKey("beta"), std::nullopt, std::nullopt)->GetRows();

    EXPECT_EQ(alphaRows.Size(), 3u);
    EXPECT_EQ(betaRows.Size(), 1u);
}

TEST_F(TBuildSelectQueryYtTest, LimitClipsResults)
{
    auto rowset = RunQuery(MakeStringKey("alpha"), std::nullopt, std::nullopt, /*limit*/ 2);
    EXPECT_EQ(rowset->GetRows().Size(), 2u);
}

TEST_F(TBuildSelectQueryYtTest, AnyConditionFiltersByTuple)
{
    // Three rows under "any_filter" share leading components (1, 1, "x") and
    // differ only by sk_any: [1;1], [1;2], [1;3].
    //
    // Condition (sk_i64, sk_ui64, sk_string, sk_any) > (1, 1, "x", [1;2])
    // must short-circuit to the Any comparison and return the [1;3] row only.
    auto rowset = RunQuery(
        MakeStringKey("any_filter"),
        MakeSecondaryKey(1, 1u, "x", R"([1; 2])"),
        std::nullopt);
    auto rows = rowset->GetRows();
    auto schema = rowset->GetSchema();

    ASSERT_EQ(rows.Size(), 1u);
    auto skAnyYson = GetColumnValue<NYson::TYsonString>(
        TPayload(TPayload::TUnderlying(rows[0])),
        schema,
        "sk_any");
    auto list = ConvertToNode(skAnyYson)->AsList()->GetChildren();
    ASSERT_EQ(std::ssize(list), 2);
    EXPECT_EQ(list[0]->GetValue<i64>(), 1);
    EXPECT_EQ(list[1]->GetValue<i64>(), 3);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
