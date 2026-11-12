#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/ytlib/table_client/granule_filter.h>
#include <yt/yt/ytlib/table_client/granule_min_max_filter.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "GranuleMinMaxFilterTest");

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilterTestBase
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

struct TTestRow
{
    i64 K0 = 0;
    i64 K1 = 0;
    i64 V0 = 0;
    i64 V1 = 0;
    std::string S;
};

class TGranuleMinMaxFilterSimpleTest
    : public TGranuleMinMaxFilterTestBase
    , public ::testing::WithParamInterface<std::tuple<
        const char*, // queryString
        std::vector<TTestRow>, // data
        bool>> // result
{
protected:
    TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{
        TColumnSchema("k0", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("k1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("v0", EValueType::Int64),
        TColumnSchema("v1", EValueType::Int64),
        TColumnSchema("s", EValueType::String),
    });

    TUnversionedOwningRow MakeRow(const TTestRow& row)
    {
        auto builder = NTableClient::TUnversionedOwningRowBuilder();
        builder.AddValue(MakeUnversionedInt64Value(row.K0, 0));
        builder.AddValue(MakeUnversionedInt64Value(row.K1, 1));
        builder.AddValue(MakeUnversionedInt64Value(row.V0, 2));
        builder.AddValue(MakeUnversionedInt64Value(row.V1, 3));
        builder.AddValue(MakeUnversionedStringValue(row.S, 4));
        return builder.FinishRow();
    };

    std::pair<std::vector<TUnversionedOwningRow>, std::vector<TUnversionedRow>> MakeRowset(const std::vector<TTestRow>& rows)
    {
        auto owning = std::vector<TUnversionedOwningRow>();
        auto notOwning = std::vector<TUnversionedRow>();

        for (auto& row : rows) {
            owning.push_back(MakeRow(row));
            notOwning.push_back(owning.back());
        }

        return std::pair(owning, notOwning);
    }

    TColumnarStatistics MakeStatistics(const std::vector<TTestRow>& rows)
    {
        auto statistics = TColumnarStatistics::MakeEmpty(5);
        auto rowset = MakeRowset(rows);
        statistics.Update(rowset.second);
        return statistics;
    }
};

INSTANTIATE_TEST_SUITE_P(
    GranuleMinMaxFilterSimpleTest,
    TGranuleMinMaxFilterSimpleTest,
    ::testing::Values(
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<TTestRow>{{.V0 = 5, .V1 = 3}, {.K1 = 1, .V0 = 6, .V1 = 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<TTestRow>{{.V1 = 3}, {.K1 = 1, .V0 = 7, .V1 = 4}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<TTestRow>{{}, {.K1 = 1, .V1 = 6}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<TTestRow>{{}, {.K1 = 1, .V0 = 7, .V1 = 6}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<TTestRow>{{.V0 = 5, .V1 = 3}, {.K1 = 1, .V0 = 6, .V1 = 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<TTestRow>{{.V1 = 3}, {.K1 = 1, .V0 = 7, .V1 = 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<TTestRow>{{}, {.K1 = 1, .V1 = 6}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<TTestRow>{{}, {.K1 = 1, .V0 = 7, .V1 = 6}},
            false),
        std::tuple(
            "* where s in ('bar', 'foo')",
            std::vector<TTestRow>{{.S = "baz"}},
            true),
        std::tuple(
            "* where s in ('bar', 'foo')",
            std::vector<TTestRow>{{.S = "bar"}},
            false)));

TEST_P(TGranuleMinMaxFilterSimpleTest, All)
{
    auto granuleNameTable = TNameTable::FromSchema(*Schema_);

    auto& param = GetParam();
    auto* queryString = std::get<0>(param);
    auto& data = std::get<1>(param);
    bool expected = std::get<2>(param);

    auto filter = CreateGranuleMinMaxFilter(
        PrepareJobQuery(queryString, Schema_, DefaultFetchFunctions),
        Logger());
    auto statistics = MakeStatistics(data);

    EXPECT_EQ(expected, filter->CanSkip(statistics, granuleNameTable));
}

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilterHugeTest
    : public TGranuleMinMaxFilterTestBase
{ };

TEST_F(TGranuleMinMaxFilterHugeTest, First)
{
    int columnCount = 50;

    auto columnSchemas = std::vector<TColumnSchema>();
    for (int i = 0; i < columnCount; ++i) {
        auto type = i % 2 == 0 ? EValueType::Int64 : EValueType::String;
        columnSchemas.emplace_back(Format("v%v", i), type);
    }

    auto schema = New<TTableSchema>(columnSchemas);

    auto statistics = TColumnarStatistics {
        .ColumnDataWeights = std::vector<i64>(columnCount, 5'000'000),
        .TimestampTotalWeight = std::nullopt,
        .LegacyChunkDataWeight = 0,
        .ColumnMinValues = std::vector<TUnversionedOwningValue>(
            columnCount,
            TUnversionedOwningValue(TUnversionedValue{.Type = EValueType::Min})),
        .ColumnMaxValues = std::vector<TUnversionedOwningValue>(
            columnCount,
            TUnversionedOwningValue(TUnversionedValue{.Type = EValueType::Max})),
        .ColumnNonNullValueCounts = std::vector<i64>(columnCount, 1'000'000),
        .ChunkRowCount = 5'000'000,
        .LegacyChunkRowCount = 0,
        .LargeStatistics = {},
    };

    auto filter = CreateGranuleMinMaxFilter(
        PrepareJobQuery("* where v0 in (1, 2, 3, 4) or v1 in ('bar', 'foo')", schema, DefaultFetchFunctions),
        Logger());

    auto granuleNameTable = TNameTable::FromSchema(*schema);

    EXPECT_FALSE(filter->CanSkip(statistics, granuleNameTable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
