#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/ytlib/table_client/granule_filter.h>
#include <yt/yt/ytlib/table_client/granule_min_max_filter.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "GranuleMinMaxFilterTest");

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilterTestBase
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

class TGranuleMinMaxFilterSimpleTest
    : public TGranuleMinMaxFilterTestBase
    , public ::testing::WithParamInterface<std::tuple<
        const char*, // queryString
        std::vector<std::vector<i64>>, // data
        bool>> // result
{
protected:
    TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{
        TColumnSchema("k0", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("k1", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("v0", EValueType::Int64),
        TColumnSchema("v1", EValueType::Int64),
    });

    TUnversionedOwningRow MakeRow(i64 k0, i64 k1, i64 v0, i64 v1)
    {
        auto builder = NTableClient::TUnversionedOwningRowBuilder();
        builder.AddValue(MakeUnversionedInt64Value(k0, 0));
        builder.AddValue(MakeUnversionedInt64Value(k1, 1));
        builder.AddValue(MakeUnversionedInt64Value(v0, 2));
        builder.AddValue(MakeUnversionedInt64Value(v1, 3));
        return builder.FinishRow();
    };

    std::pair<std::vector<TUnversionedOwningRow>, std::vector<TUnversionedRow>> MakeRowset(std::vector<std::vector<i64>> rows)
    {
        auto owning = std::vector<TUnversionedOwningRow>();
        auto notOwning = std::vector<TUnversionedRow>();

        for (auto& row : rows) {
            owning.push_back(MakeRow(row[0], row[1], row[2], row[3]));
            notOwning.push_back(owning.back());
        }

        return std::pair(owning, notOwning);
    }

    TColumnarStatistics MakeStatistics(std::vector<std::vector<i64>> rows)
    {
        auto statistics = TColumnarStatistics::MakeEmpty(4);
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
            std::vector<std::vector<i64>>{{0, 0, 5, 3}, {0, 1, 6, 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 3}, {0, 1, 7, 4}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 0}, {0, 1, 0, 6}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) or v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 0}, {0, 1, 7, 6}},
            false),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 5, 3}, {0, 1, 6, 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 3}, {0, 1, 7, 4}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 0}, {0, 1, 0, 6}},
            true),
        std::tuple(
            "* where v0 in (1, 2, 3, 4) and v1 in (5)",
            std::vector<std::vector<i64>>{{0, 0, 0, 0}, {0, 1, 7, 6}},
            false)));

TEST_P(TGranuleMinMaxFilterSimpleTest, All)
{
    auto granuleNameTable = TNameTable::FromSchema(*Schema_);

    auto& param = GetParam();
    auto* queryString = std::get<0>(param);
    auto& data = std::get<1>(param);
    bool expected = std::get<2>(param);

    auto query = PrepareJobQuery(queryString, Schema_, DefaultFetchFunctions);
    auto filter = CreateGranuleMinMaxFilter(query, Logger());
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
        columnSchemas.emplace_back(Format("v%v", i), EValueType::Int64);
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

    auto query = PrepareJobQuery("* where v0 in (1, 2, 3, 4) or v1 in (5)", schema, DefaultFetchFunctions);
    auto filter = CreateGranuleMinMaxFilter(query, Logger());

    auto granuleNameTable = TNameTable::FromSchema(*schema);

    EXPECT_FALSE(filter->CanSkip(statistics, granuleNameTable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
