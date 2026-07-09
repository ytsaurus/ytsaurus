#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/connectors/servicelog/fetcher.h>
#include <yt/yt/flow/library/cpp/connectors/servicelog/range.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr MakeSimpleSchema()
{
    NTableClient::TColumnSchema keyColumn1("hash", NTableClient::EValueType::Uint64, NTableClient::ESortOrder::Ascending);
    NTableClient::TColumnSchema keyColumn2("name", NTableClient::EValueType::String, NTableClient::ESortOrder::Ascending);
    NTableClient::TColumnSchema valueColumn("value", NTableClient::EValueType::String);
    return New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{keyColumn1, keyColumn2, valueColumn});
}

NTableClient::TTableSchemaPtr MakeSingleKeySchema()
{
    NTableClient::TColumnSchema keyColumn("id", NTableClient::EValueType::Uint64, NTableClient::ESortOrder::Ascending);
    NTableClient::TColumnSchema valueColumn("data", NTableClient::EValueType::String);
    return New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{keyColumn, valueColumn});
}

////////////////////////////////////////////////////////////////////////////////

NYTree::IMapNodePtr ParsePlaceholderValues(const NYson::TYsonString& yson)
{
    return ConvertTo<NYTree::IMapNodePtr>(yson);
}

TEST(TBuildParameterizedSelectRowsQueryTest, NoRange)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 100);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table]  ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 100);
    EXPECT_EQ(map->GetChildCount(), 1);
}

TEST(TBuildParameterizedSelectRowsQueryTest, LowerBoundInclusive)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    range->Lower = TServiceLogEndpoint();
    range->Lower->Key = MakeKey(static_cast<ui64>(10));
    range->Lower->Exclusive = false;
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 50);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table] WHERE (id) >= ({lower_0}) ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 10u);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 50);
}

TEST(TBuildParameterizedSelectRowsQueryTest, LowerBoundExclusive)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    range->Lower = TServiceLogEndpoint();
    range->Lower->Key = MakeKey(static_cast<ui64>(10));
    range->Lower->Exclusive = true;
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 50);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table] WHERE (id) > ({lower_0}) ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 10u);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 50);
}

TEST(TBuildParameterizedSelectRowsQueryTest, UpperBoundExclusive)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    range->Upper = TServiceLogEndpoint();
    range->Upper->Key = MakeKey(static_cast<ui64>(100));
    range->Upper->Exclusive = true;
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 50);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table] WHERE (id) < ({upper_0}) ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("upper_0"), 100u);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 50);
}

TEST(TBuildParameterizedSelectRowsQueryTest, UpperBoundInclusive)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    range->Upper = TServiceLogEndpoint();
    range->Upper->Key = MakeKey(static_cast<ui64>(100));
    range->Upper->Exclusive = false;
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 50);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table] WHERE (id) <= ({upper_0}) ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("upper_0"), 100u);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 50);
}

TEST(TBuildParameterizedSelectRowsQueryTest, BothBounds)
{
    auto schema = MakeSingleKeySchema();
    auto range = New<TServiceLogRange>();
    range->Lower = TServiceLogEndpoint();
    range->Lower->Key = MakeKey(static_cast<ui64>(10));
    range->Lower->Exclusive = false;
    range->Upper = TServiceLogEndpoint();
    range->Upper->Key = MakeKey(static_cast<ui64>(100));
    range->Upper->Exclusive = true;
    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 200);
    EXPECT_EQ(result.Query, "SELECT id,data FROM [//tmp/table] WHERE (id) >= ({lower_0}) AND (id) < ({upper_0}) ORDER BY (id) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 10u);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("upper_0"), 100u);
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 200);
}

TEST(TBuildParameterizedSelectRowsQueryTest, CompositeKeyWithString)
{
    auto schema = MakeSimpleSchema();
    auto range = New<TServiceLogRange>();

    NTableClient::TUnversionedOwningRowBuilder lowerBuilder;
    lowerBuilder.AddValue(NTableClient::MakeUnversionedUint64Value(5));
    lowerBuilder.AddValue(NTableClient::MakeUnversionedStringValue("abc\\9"));
    range->Lower = TServiceLogEndpoint();
    range->Lower->Key = TKey(TKey::TUnderlying(lowerBuilder.FinishRow()));
    range->Lower->Exclusive = true;

    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 10);
    EXPECT_EQ(result.Query, "SELECT hash,name,value FROM [//tmp/table] WHERE (hash,name) > ({lower_0},{lower_1}) ORDER BY (hash,name) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 5u);
    // String values with backslashes are passed as-is (no escaping needed for placeholders).
    EXPECT_EQ(map->GetChildValueOrThrow<TString>("lower_1"), "abc\\9");
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 10);
}

TEST(TBuildParameterizedSelectRowsQueryTest, CompositeKeyBothBounds)
{
    auto schema = MakeSimpleSchema();
    auto range = New<TServiceLogRange>();

    range->Lower = TServiceLogEndpoint();
    range->Lower->Key = MakeKey(ui64(1), "start");
    range->Lower->Exclusive = false;

    range->Upper = TServiceLogEndpoint();
    range->Upper->Key = MakeKey(ui64(99), "end");
    range->Upper->Exclusive = true;

    auto result = BuildParameterizedSelectRowsQuery("//tmp/table", schema, range, 500);
    EXPECT_EQ(result.Query, "SELECT hash,name,value FROM [//tmp/table] WHERE (hash,name) >= ({lower_0},{lower_1}) AND (hash,name) < ({upper_0},{upper_1}) ORDER BY (hash,name) LIMIT {row_limit}");
    auto map = ParsePlaceholderValues(result.PlaceholderValues);
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("lower_0"), 1u);
    EXPECT_EQ(map->GetChildValueOrThrow<TString>("lower_1"), "start");
    EXPECT_EQ(map->GetChildValueOrThrow<ui64>("upper_0"), 99u);
    EXPECT_EQ(map->GetChildValueOrThrow<TString>("upper_1"), "end");
    EXPECT_EQ(map->GetChildValueOrThrow<i64>("row_limit"), 500);
}

TTableFetcherSpecPtr LoadSpec(TStringBuf yson)
{
    return ConvertTo<TTableFetcherSpecPtr>(NYson::TYsonString(TString(yson)));
}

TEST(TTableFetcherSpecTest, ValidWithCluster)
{
    auto spec = LoadSpec(R"({table_path = "<cluster=primary>//tmp/table"})");
    EXPECT_EQ(spec->TablePath.GetPath(), "//tmp/table");
    ASSERT_TRUE(spec->TablePath.GetCluster().has_value());
    EXPECT_EQ(*spec->TablePath.GetCluster(), "primary");
}

TEST(TTableFetcherSpecTest, ValidWithClusters)
{
    auto spec = LoadSpec(R"({table_path = "<clusters=[primary;secondary]>//tmp/table"})");
    EXPECT_EQ(spec->TablePath.GetPath(), "//tmp/table");
    ASSERT_TRUE(spec->TablePath.GetClusters().has_value());
    EXPECT_EQ(*spec->TablePath.GetClusters(), (std::vector<std::string>{"primary", "secondary"}));
}

TEST(TTableFetcherSpecTest, ValidWithClusterShortcutSyntax)
{
    auto spec = LoadSpec(R"({table_path = "primary://tmp/table"})");
    EXPECT_EQ(spec->TablePath.GetPath(), "//tmp/table");
    ASSERT_TRUE(spec->TablePath.GetCluster().has_value());
    EXPECT_EQ(*spec->TablePath.GetCluster(), "primary");
}

TEST(TTableFetcherSpecTest, RejectsMissingTablePath)
{
    EXPECT_THROW_WITH_SUBSTRING(LoadSpec(R"({})"), "table_path");
}

TEST(TTableFetcherSpecTest, RejectsEmptyTablePath)
{
    EXPECT_THROW_WITH_SUBSTRING(LoadSpec(R"({table_path = ""})"), "non-empty path");
}

TEST(TTableFetcherSpecTest, RejectsTablePathWithoutCluster)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadSpec(R"({table_path = "//tmp/table"})"),
        "must specify a cluster");
}

TEST(TTableFetcherSpecTest, RejectsTablePathWithUnrelatedAttributes)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadSpec(R"({table_path = "<append=%true>//tmp/table"})"),
        "must specify a cluster");
}

TEST(TTableFetcherSpecTest, RejectsTablePathWithEmptyClustersList)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadSpec(R"({table_path = "<clusters=[]>//tmp/table"})"),
        "empty <clusters=[]> attribute");
}

TEST(TTableFetcherSpecTest, AcceptsOptionalFields)
{
    auto spec = LoadSpec(R"({
        table_path = "<cluster=primary>//tmp/table";
        value_columns = ["a"; "b"];
        attempts = 5;
        retry_timeout = 1000;
        fetch_type = "select_rows";
    })");
    ASSERT_TRUE(spec->ValueColumns.has_value());
    EXPECT_EQ(*spec->ValueColumns, (THashSet<std::string>{"a", "b"}));
    EXPECT_EQ(spec->Attempts, 5);
    EXPECT_EQ(spec->RetryTimeout, TDuration::MilliSeconds(1000));
    EXPECT_EQ(spec->FetchType, EFetchType::SelectRows);
}

TEST(TTableFetcherSpecTest, DefaultsForOptionalFields)
{
    auto spec = LoadSpec(R"({table_path = "<cluster=primary>//tmp/table"})");
    EXPECT_FALSE(spec->ValueColumns.has_value());
    EXPECT_EQ(spec->Attempts, 1);
    EXPECT_EQ(spec->RetryTimeout, TDuration::Seconds(5));
    EXPECT_EQ(spec->FetchType, EFetchType::TableReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
