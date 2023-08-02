#include "ql_helpers.h"

#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/coordinator.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

// Tests:
// TComputedColumnTest

namespace NYT::NQueryClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TComputedColumnTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::vector<const char*>>
{
protected:
    void SetUp() override
    {
        SetUpSchema();

        EXPECT_CALL(PrepareMock_, GetInitialSplit(_))
            .WillRepeatedly(Invoke(this, &TComputedColumnTest::MakeSimpleSplit));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = CreateColumnEvaluatorCache(config);
    }

    std::vector<TKeyRange> Coordinate(const TString& source, ui64 rangeExpansionLimit = 1000)
    {
        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            source);
        const auto& query = fragment->Query;
        const auto& dataSource = fragment->DataSource;

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = rangeExpansionLimit;
        options.VerboseLogging = true;

        auto prunedSplits = GetPrunedRanges(
            query,
            dataSource.ObjectId,
            dataSource.Ranges,
            rowBuffer,
            ColumnEvaluatorCache_,
            GetBuiltinRangeExtractors(),
            options);

        return GetRangesFromSources(prunedSplits);
    }

    std::vector<TKeyRange> CoordinateForeign(const TString& source)
    {
        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            source);
        const auto& query = fragment->Query;

        auto buffer = New<TRowBuffer>();
        TRowRanges foreignSplits{{
                buffer->CaptureRow(MinKey().Get()),
                buffer->CaptureRow(MaxKey().Get())
            }};

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = 1000;
        options.VerboseLogging = true;

        auto prunedSplits = GetPrunedRanges(
            query->WhereClause,
            query->JoinClauses[0]->Schema.Original,
            query->JoinClauses[0]->GetKeyColumns(),
            query->JoinClauses[0]->ForeignObjectId,
            MakeSharedRange(foreignSplits),
            rowBuffer,
            ColumnEvaluatorCache_,
            GetBuiltinRangeExtractors(),
            options);

        return GetRangesFromSources(prunedSplits);
    }

    void SetSchema(const TTableSchema& schema)
    {
        Schema_ = schema;
    }

    void SetSecondarySchema(const TTableSchema& schema)
    {
        SecondarySchema_ = schema;
    }

private:
    void SetUpSchema()
    {
        TTableSchema tableSchema({
            TColumnSchema("k", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("l * 2")),
            TColumnSchema("l", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("m", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64)
        });

        SetSchema(tableSchema);
    }

    TFuture<TDataSplit> MakeSimpleSplit(const NYPath::TYPath& path)
    {
        TDataSplit dataSplit;

        if (path == "//t") {
            dataSplit.TableSchema = New<TTableSchema>(Schema_);
        } else {
            dataSplit.TableSchema = New<TTableSchema>(SecondarySchema_);
        }

        return MakeFuture(dataSplit);
    }

    std::vector<TKeyRange> GetRangesFromSources(const TRowRanges& rowRanges)
    {
        std::vector<TKeyRange> ranges;

        for (const auto& range : rowRanges) {
            ranges.push_back(TKeyRange(TLegacyOwningKey(range.first), TLegacyOwningKey(range.second)));
        }

        std::sort(ranges.begin(), ranges.end());
        return ranges;
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    TTableSchema Schema_;
    TTableSchema SecondarySchema_;
};

TEST_F(TComputedColumnTest, NoKeyColumnsInPredicate)
{
    auto query = TString("k from [//t] where a = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Simple)
{
    auto query = TString("a from [//t] where l = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("20;10;"), result[0].first);
    EXPECT_EQ(YsonToKey("20;10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Inequality)
{
    auto query = TString("a from [//t] where l < 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Composite)
{
    auto query = TString("a from [//t] where l = 10 and m > 0 and m < 50");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("20;10;0;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("20;10;50;"), result[0].second);
}

TEST_F(TComputedColumnTest, Vector)
{
    auto query = TString("a from [//t] where l in (1,2,3)");
    auto result = Coordinate(query);

    EXPECT_EQ(3u, result.size());

    EXPECT_EQ(YsonToKey("2;1;"), result[0].first);
    EXPECT_EQ(YsonToKey("2;1;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("4;2;"), result[1].first);
    EXPECT_EQ(YsonToKey("4;2;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("6;3;"), result[2].first);
    EXPECT_EQ(YsonToKey("6;3;" _MAX_), result[2].second);
}

TEST_F(TComputedColumnTest, ComputedKeyInPredicate)
{
    auto query = TString("a from [//t] where (k,l) >= (10,20) ");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("10;20;"), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ConstantBeforeReferenceInExpression)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("2 * l")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64),
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("20;10;"), result[0].first);
    EXPECT_EQ(YsonToKey("20;10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ComputedColumnLast)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("k + 3")),
        TColumnSchema("a", EValueType::Int64),
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("10;13;"), result[0].first);
    EXPECT_EQ(YsonToKey("10;13;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Complex1)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n + 1")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("o + 2")),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("o", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k = 10 and n = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("10;21;"), result[0].first);
    EXPECT_EQ(YsonToKey("10;21;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Complex2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n + 1")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("o + 2")),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("o", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where (k,n) in ((10,20),(50,60))");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("10;21;"), result[0].first);
    EXPECT_EQ(YsonToKey("10;21;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("50;61;"), result[1].first);
    EXPECT_EQ(YsonToKey("50;61;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, Complex3)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("o + 1")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("o + 2")),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("o", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k = 10 and n = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("10;"), result[0].first);
    EXPECT_EQ(YsonToKey("10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far0)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l + 1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far1)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m + 1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("11;"), result[0].first);
    EXPECT_EQ(YsonToKey("11;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n + 1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where n = 10 and l = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("11;20;"), result[0].first);
    EXPECT_EQ(YsonToKey("11;20;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far3)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n + 1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where (n,l) in ((10,20), (30,40))");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("11;20;"), result[0].first);
    EXPECT_EQ(YsonToKey("11;20;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("31;40;"), result[1].first);
    EXPECT_EQ(YsonToKey("31;40;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, Far4)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n + 1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where n in (10,30) and l in (20,40)");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey("11;20;"), result[0].first);
    EXPECT_EQ(YsonToKey("11;20;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("11;40;"), result[1].first);
    EXPECT_EQ(YsonToKey("11;40;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("31;20;"), result[2].first);
    EXPECT_EQ(YsonToKey("31;20;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("31;40;"), result[3].first);
    EXPECT_EQ(YsonToKey("31;40;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, NoComputedColumns)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where a = 0");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Modulo0)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l % 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where a = 0");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Modulo1)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l % 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l > 0 and l <= 2000");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";0;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";2000;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("-1;0;" _MAX_), result[1].first);
    EXPECT_EQ(YsonToKey("-1;2000;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("0;0;" _MAX_), result[2].first);
    EXPECT_EQ(YsonToKey("0;2000;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("1;0;" _MAX_), result[3].first);
    EXPECT_EQ(YsonToKey("1;2000;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Modulo2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n % 1u")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n % 1u")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m = 1");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";" _NULL_ ";1;"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";" _NULL_ ";1;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey(_NULL_ ";0u;1;"), result[1].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";0u;1;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";1;"), result[2].first);
    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";1;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("0u;0u;1;"), result[3].first);
    EXPECT_EQ(YsonToKey("0u;0u;1;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Modulo3)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m % 1u")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m % 1u")),
        TColumnSchema("m", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t]");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Modulo4)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m % 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l in (0,1,2)");
    auto result = Coordinate(query, 10);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";0;"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";2;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("-1;0;"), result[1].first);
    EXPECT_EQ(YsonToKey("-1;2;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("0;0;"), result[2].first);
    EXPECT_EQ(YsonToKey("0;2;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("1;0;"), result[3].first);
    EXPECT_EQ(YsonToKey("1;2;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Modulo5)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n % 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m + 1")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m = 1");
    auto result = Coordinate(query);

    EXPECT_EQ(3u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";2;1;"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";2;1;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("0u;2;1;"), result[1].first);
    EXPECT_EQ(YsonToKey("0u;2;1;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("1u;2;1;"), result[2].first);
    EXPECT_EQ(YsonToKey("1u;2;1;" _MAX_), result[2].second);
}

TEST_F(TComputedColumnTest, Divide0)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= 3 and l < 6");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("1;3"), result[0].first);
    EXPECT_EQ(YsonToKey("1;6"), result[0].second);
    EXPECT_EQ(YsonToKey("2;3"), result[1].first);
    EXPECT_EQ(YsonToKey("2;6"), result[1].second);
}

TEST_F(TComputedColumnTest, Divide1)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l > 3 and l < 6");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("2;3;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("2;6"), result[0].second);
}

TEST_F(TComputedColumnTest, Divide2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m / 3")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m / 4")),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m > 0 and m <= 6");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey("0;0;0;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("0;0;6;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("1;0;0;" _MAX_), result[1].first);
    EXPECT_EQ(YsonToKey("1;0;6;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("1;1;0;" _MAX_), result[2].first);
    EXPECT_EQ(YsonToKey("1;1;6;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("2;1;0;" _MAX_), result[3].first);
    EXPECT_EQ(YsonToKey("2;1;6;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Divide3)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m / 2u")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("n % 1u")),
        TColumnSchema("m", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Uint64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m >= 0u and m < 3u");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";0u"), result[0].first);
    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";3u"), result[0].second);
    EXPECT_EQ(YsonToKey("0u;0u;0u"), result[1].first);
    EXPECT_EQ(YsonToKey("0u;0u;3u"), result[1].second);
    EXPECT_EQ(YsonToKey("1u;" _NULL_ ";0u"), result[2].first);
    EXPECT_EQ(YsonToKey("1u;" _NULL_ ";3u"), result[2].second);
    EXPECT_EQ(YsonToKey("1u;0u;0u"), result[3].first);
    EXPECT_EQ(YsonToKey("1u;0u;3u"), result[3].second);
}

TEST_F(TComputedColumnTest, Divide4)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / -9223372036854775808")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= -9223372036854775808 and l <= 9223372036854775807");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("0;-9223372036854775808;"), result[0].first);
    EXPECT_EQ(YsonToKey("0;9223372036854775807;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("1;-9223372036854775808"), result[1].first);
    EXPECT_EQ(YsonToKey("1;9223372036854775807;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, Divide5)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / -1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= -9223372036854775808 and l < -9223372036854775805");
    EXPECT_THROW_THAT(
        Coordinate(query),
        HasSubstr("Division of INT_MIN by -1"));
}

TEST_F(TComputedColumnTest, Divide6)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / -1")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l > -9223372036854775808 and l < -9223372036854775805");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("9223372036854775806;-9223372036854775808;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("9223372036854775806;-9223372036854775805;"), result[0].second);

    EXPECT_EQ(YsonToKey("9223372036854775807;-9223372036854775808;" _MAX_), result[1].first);
    EXPECT_EQ(YsonToKey("9223372036854775807;-9223372036854775805;"), result[1].second);
}

TEST_F(TComputedColumnTest, DivideEmptyRange)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l > 1 and l < 2");
    auto result = Coordinate(query);

    EXPECT_EQ(0u, result.size());
}

TEST_F(TComputedColumnTest, DivideSingleRange)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l > 1 and l < 3");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("2;1;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("2;3;"), result[0].second);
}

TEST_F(TComputedColumnTest, DivideComplex)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 3 + l / 5")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= -5 and l < 7");
    auto result = Coordinate(query);

    EXPECT_EQ(6u, result.size());

    EXPECT_EQ(YsonToKey("-2;-5"), result[0].first);
    EXPECT_EQ(YsonToKey("-2;7"), result[0].second);

    EXPECT_EQ(YsonToKey("-1;-5"), result[1].first);
    EXPECT_EQ(YsonToKey("-1;7"), result[1].second);

    EXPECT_EQ(YsonToKey("0;-5"), result[2].first);
    EXPECT_EQ(YsonToKey("0;7"), result[2].second);

    EXPECT_EQ(YsonToKey("1;-5"), result[3].first);
    EXPECT_EQ(YsonToKey("1;7"), result[3].second);

    EXPECT_EQ(YsonToKey("2;-5"), result[4].first);
    EXPECT_EQ(YsonToKey("2;7"), result[4].second);

    EXPECT_EQ(YsonToKey("3;-5"), result[5].first);
    EXPECT_EQ(YsonToKey("3;7"), result[5].second);
}

TEST_F(TComputedColumnTest, DivideComplex2)
{
    // 6148914691236517205u == std::numeric_limits<ui64>::max() / 2

    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 6148914691236517205u + l / 9223372036854775808u")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= 0");
    auto result = Coordinate(query);

    EXPECT_EQ(5u, result.size());

    // l = 0
    EXPECT_EQ(YsonToKey("0u;0u"), result[0].first);
    EXPECT_EQ(YsonToKey("0u;" _MAX_), result[0].second);

    // l = 6148914691236517205u
    EXPECT_EQ(YsonToKey("1u;0u"), result[1].first);
    EXPECT_EQ(YsonToKey("1u;" _MAX_), result[1].second);

    // l = 9223372036854775808u
    EXPECT_EQ(YsonToKey("2u;0u"), result[2].first);
    EXPECT_EQ(YsonToKey("2u;" _MAX_), result[2].second);

    // l = 2 * 6148914691236517205u = 12297829382473034410
    EXPECT_EQ(YsonToKey("3u;0u"), result[3].first);
    EXPECT_EQ(YsonToKey("3u;" _MAX_), result[3].second);

    // l = 3 * 6148914691236517205u = 18446744073709551615
    EXPECT_EQ(YsonToKey("4u;0u"), result[4].first);
    EXPECT_EQ(YsonToKey("4u;" _MAX_), result[4].second);
}

TEST_F(TComputedColumnTest, DivideNull)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 7")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= null and l < 16");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";" _NULL_), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";16"), result[0].second);

    EXPECT_EQ(YsonToKey("0;" _NULL_), result[1].first);
    EXPECT_EQ(YsonToKey("0;16"), result[1].second);

    EXPECT_EQ(YsonToKey("1;" _NULL_), result[2].first);
    EXPECT_EQ(YsonToKey("1;16"), result[2].second);

    EXPECT_EQ(YsonToKey("2;" _NULL_), result[3].first);
    EXPECT_EQ(YsonToKey("2;16"), result[3].second);
}

TEST_F(TComputedColumnTest, DivideOneBound)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / 7")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l < 16");
    auto result = Coordinate(query);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";16"), result[0].second);

    EXPECT_EQ(YsonToKey("0"), result[1].first);
    EXPECT_EQ(YsonToKey("0;16"), result[1].second);

    EXPECT_EQ(YsonToKey("1"), result[2].first);
    EXPECT_EQ(YsonToKey("1;16"), result[2].second);

    EXPECT_EQ(YsonToKey("2"), result[3].first);
    EXPECT_EQ(YsonToKey("2;16"), result[3].second);
}

TEST_F(TComputedColumnTest, DivideTypeCast)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l / -1")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l < 16");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";16u"), result[0].second);

    EXPECT_EQ(YsonToKey("0u"), result[1].first);
    EXPECT_EQ(YsonToKey("0u;16u"), result[1].second);
}

TEST_F(TComputedColumnTest, EstimationOverflow)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("farm_hash(l / 1, m)")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= -9223372036854775808 and l <= 9223372036854775807 and m >= -9223372036854775808 and m <= 9223372036854775807");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, FarDivide1)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m / 2")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m >= 3 and m < 5");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("1"), result[0].first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("2"), result[1].first);
    EXPECT_EQ(YsonToKey("2;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, ModuloDivide)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m % 2")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("m / 3")),
        TColumnSchema("m", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where m >= 4 and m < 7");
    auto result = Coordinate(query);

    EXPECT_EQ(6u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";1u;4u"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";1u;7u"), result[0].second);
    EXPECT_EQ(YsonToKey(_NULL_ ";2u;4u"), result[1].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";2u;7u"), result[1].second);

    EXPECT_EQ(YsonToKey("0u;1u;4u"), result[2].first);
    EXPECT_EQ(YsonToKey("0u;1u;7u"), result[2].second);
    EXPECT_EQ(YsonToKey("0u;2u;4u"), result[3].first);
    EXPECT_EQ(YsonToKey("0u;2u;7u"), result[3].second);

    EXPECT_EQ(YsonToKey("1u;1u;4u"), result[4].first);
    EXPECT_EQ(YsonToKey("1u;1u;7u"), result[4].second);
    EXPECT_EQ(YsonToKey("1u;2u;4u"), result[5].first);
    EXPECT_EQ(YsonToKey("1u;2u;7u"), result[5].second);
}

TEST_F(TComputedColumnTest, ModuloDivide2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("(l / 3) % 2")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= 0 and l < 5");
    auto result = Coordinate(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("0u;0u;"), result[0].first);
    EXPECT_EQ(YsonToKey("0u;5u;"), result[0].second);

    EXPECT_EQ(YsonToKey("1u;0u;"), result[1].first);
    EXPECT_EQ(YsonToKey("1u;5u;"), result[1].second);
}

TEST_F(TComputedColumnTest, ModuloDivide3)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("(l / 3) % 2")),
        TColumnSchema("l", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where l >= 0 and l < 10");
    auto result = Coordinate(query);

    EXPECT_EQ(3u, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";0u"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";10u"), result[0].second);
    EXPECT_EQ(YsonToKey("0u;0u"), result[1].first);
    EXPECT_EQ(YsonToKey("0u;10u"), result[1].second);
    EXPECT_EQ(YsonToKey("1u;0u"), result[2].first);
    EXPECT_EQ(YsonToKey("1u;10u"), result[2].second);
}

TEST_F(TComputedColumnTest, ContianuationKeyToken)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("l")),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where (k, l, m) > (1, 1, 2) and l = 1");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("1;1;2;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("1;1;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ContianuationKeyToken2)
{
    TTableSchema tableSchema({
        TColumnSchema("h", EValueType::Uint64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("farm_hash(k)")),
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where (h, k, l) > (147449605462316706u, 225350873616724758, 155984532810876166)");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("147449605462316706u;225350873616724758;155984532810876166;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ContianuationKeyTokenRepeatedField)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k = 3 and (l, k, l, m) > (4, 3, 4, 5)");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("3;4;5;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("3;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ContianuationKeyTokenRepeatedField2)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k = 3 and (m, k, l, m) > (5, 3, 4, 5)");
    auto result = Coordinate(query);

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("3;"), result[0].first);
    EXPECT_EQ(YsonToKey("3;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, RangeExpansionLimit)
{
    TTableSchema tableSchema({
        TColumnSchema("h", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("k + 1")),
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k in (10, 20, 30, 40, 50) and l in (1, 3, 5, 7)");
    auto result = Coordinate(query, 6);


    EXPECT_EQ(5u, result.size());

    EXPECT_EQ(YsonToKey("11;10;1"), result[0].first);
    EXPECT_EQ(YsonToKey("11;10;7;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("21;20;1"), result[1].first);
    EXPECT_EQ(YsonToKey("21;20;7;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("31;30;1"), result[2].first);
    EXPECT_EQ(YsonToKey("31;30;7;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("41;40;1"), result[3].first);
    EXPECT_EQ(YsonToKey("41;40;7;" _MAX_), result[3].second);

    EXPECT_EQ(YsonToKey("51;50;1"), result[4].first);
    EXPECT_EQ(YsonToKey("51;50;7;" _MAX_), result[4].second);
}

TEST_F(TComputedColumnTest, RangeExpansionLimitSimple)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("a", EValueType::Int64)
    });

    SetSchema(tableSchema);

    auto query = TString("a from [//t] where k in (10, 20, 30, 40, 50) and l in (1, 3, 5, 7)");
    auto result = Coordinate(query, 6);

    EXPECT_EQ(5u, result.size());

    EXPECT_EQ(YsonToKey("10;1"), result[0].first);
    EXPECT_EQ(YsonToKey("10;7;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("20;1"), result[1].first);
    EXPECT_EQ(YsonToKey("20;7;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("30;1"), result[2].first);
    EXPECT_EQ(YsonToKey("30;7;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("40;1"), result[3].first);
    EXPECT_EQ(YsonToKey("40;7;" _MAX_), result[3].second);

    EXPECT_EQ(YsonToKey("50;1"), result[4].first);
    EXPECT_EQ(YsonToKey("50;7;" _MAX_), result[4].second);
}

TEST_P(TComputedColumnTest, Join)
{
    const auto& args = GetParam();
    const auto& schemaString1 = args[0];
    const auto& schemaString2 = args[1];

    TTableSchema tableSchema1;
    TTableSchema tableSchema2;
    Deserialize(tableSchema1, ConvertToNode(TYsonString(TString(schemaString1))));
    Deserialize(tableSchema2, ConvertToNode(TYsonString(TString(schemaString2))));

    SetSchema(tableSchema1);
    SetSecondarySchema(tableSchema2);

    auto query = TString("l from [//t] join [//t1] using l where l in (0, 1)");
    auto result = CoordinateForeign(query);

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey(args[2]), result[0].first);
    EXPECT_EQ(YsonToKey(args[3]), result[0].second);
    EXPECT_EQ(YsonToKey(args[4]), result[1].first);
    EXPECT_EQ(YsonToKey(args[5]), result[1].second);
}

INSTANTIATE_TEST_SUITE_P(
    TComputedColumnTest,
    TComputedColumnTest,
    ::testing::Values(
        std::vector<const char*>{
            "[{name=k;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; "
                "{name=a;type=int64}]",
            "[{name=n;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=b;type=int64}]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[{name=l;type=int64;sort_order=ascending}; {name=b;type=int64}]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;sort_order=ascending;expression=k}; {name=k;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[{name=l;type=int64;sort_order=ascending}; {name=b;type=int64}]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[{name=n;type=int64;sort_order=ascending;expression=l}; {name=l;type=int64;sort_order=ascending}; {name=b;type=int64}]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[{name=l;type=int64;sort_order=ascending;expression=n}; {name=n;type=int64;sort_order=ascending}; {name=b;type=int64}]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;sort_order=ascending}; {name=a;type=int64}]",
            "[{name=l;type=int64;sort_order=ascending}; {name=n;type=int64;sort_order=ascending;expression=l}; {name=b;type=int64}]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_}
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
