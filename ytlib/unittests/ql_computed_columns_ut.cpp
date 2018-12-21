#include <yt/core/test_framework/framework.h>
#include "ql_helpers.h"

#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/config.h>
#include <yt/ytlib/query_client/coordinator.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

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
    virtual void SetUp() override
    {
        SetUpSchema();

        EXPECT_CALL(PrepareMock_, GetInitialSplit(_, _))
            .WillRepeatedly(Invoke(this, &TComputedColumnTest::MakeSimpleSplit));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(config);
    }

    std::vector<TKeyRange> Coordinate(const TString& source, ui64 rangeExpansionLimit = 1000)
    {
        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            source);
        const auto& query = fragment->Query;
        const auto& dataSource = fragment->Ranges;

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = rangeExpansionLimit;
        options.VerboseLogging = true;

        auto prunedSplits = GetPrunedRanges(
            query,
            dataSource.Id,
            dataSource.Ranges,
            rowBuffer,
            ColumnEvaluatorCache_,
            BuiltinRangeExtractorMap,
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
                buffer->Capture(MinKey().Get()),
                buffer->Capture(MaxKey().Get())
            }};

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = 1000;
        options.VerboseLogging = true;

        auto prunedSplits = GetPrunedRanges(
            query->WhereClause,
            query->JoinClauses[0]->OriginalSchema,
            query->JoinClauses[0]->GetKeyColumns(),
            query->JoinClauses[0]->ForeignDataId,
            MakeSharedRange(foreignSplits),
            rowBuffer,
            ColumnEvaluatorCache_,
            BuiltinRangeExtractorMap,
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

    TFuture<TDataSplit> MakeSimpleSplit(const NYPath::TYPath& path, ui64 counter = 0)
    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

        if (path == "//t") {
            SetTableSchema(&dataSplit, Schema_);
        } else {
            SetTableSchema(&dataSplit, SecondarySchema_);
        }

        return MakeFuture(dataSplit);
    }

    std::vector<TKeyRange> GetRangesFromSources(const TRowRanges& rowRanges)
    {
        std::vector<TKeyRange> ranges;

        for (const auto& range : rowRanges) {
            ranges.push_back(TKeyRange(TOwningKey(range.first), TOwningKey(range.second)));
        }

        std::sort(ranges.begin(), ranges.end());
        return ranges;
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    TTableSchema Schema_;
    TTableSchema SecondarySchema_;
};

TEST_F(TComputedColumnTest, NoKeyColumnsInPredicate)
{
    auto query = TString("k from [//t] where a = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Simple)
{
    auto query = TString("a from [//t] where l = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey("20;10;"), result[0].first);
    EXPECT_EQ(YsonToKey("20;10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Inequality)
{
    auto query = TString("a from [//t] where l < 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey(_MIN_), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Composite)
{
    auto query = TString("a from [//t] where l = 10 and m > 0 and m < 50");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey("20;10;0;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("20;10;50;"), result[0].second);
}

TEST_F(TComputedColumnTest, Vector)
{
    auto query = TString("a from [//t] where l in (1,2,3)");
    auto result = Coordinate(query);

    EXPECT_EQ(3, result.size());

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

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey("10;20;"), result[0].first);
    EXPECT_EQ(YsonToKey(_MAX_), result[0].second);
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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(2, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(2, result.size());

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

    EXPECT_EQ(4, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(4, result.size());

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

    EXPECT_EQ(4, result.size());

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

    EXPECT_EQ(1, result.size());

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

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(YsonToKey(_NULL_ ";0;"), result[0].first);
    EXPECT_EQ(YsonToKey(_NULL_ ";2;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("-1;0;"), result[1].first);
    EXPECT_EQ(YsonToKey("-1;2;" _MAX_), result[1].second);
    EXPECT_EQ(YsonToKey("0;0;"), result[2].first);
    EXPECT_EQ(YsonToKey("0;2;" _MAX_), result[2].second);
    EXPECT_EQ(YsonToKey("1;0;"), result[3].first);
    EXPECT_EQ(YsonToKey("1;2;" _MAX_), result[3].second);
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

    auto query = TString("a from [//t] where l >= 3 and l < 6");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("1;3"), result[0].first);
    EXPECT_EQ(YsonToKey("1;4"), result[0].second);
    EXPECT_EQ(YsonToKey("2;4"), result[1].first);
    EXPECT_EQ(YsonToKey("2;6"), result[1].second);
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

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(YsonToKey("0;0;0;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("0;0;3"), result[0].second);
    EXPECT_EQ(YsonToKey("1;0;3"), result[1].first);
    EXPECT_EQ(YsonToKey("1;0;4"), result[1].second);
    EXPECT_EQ(YsonToKey("1;1;4"), result[2].first);
    EXPECT_EQ(YsonToKey("1;1;6"), result[2].second);
    EXPECT_EQ(YsonToKey("2;1;6"), result[3].first);
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

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";0u"), result[0].first);
    EXPECT_EQ(YsonToKey("0u;" _NULL_ ";2u"), result[0].second);
    EXPECT_EQ(YsonToKey("0u;0u;0u"), result[1].first);
    EXPECT_EQ(YsonToKey("0u;0u;2u"), result[1].second);
    EXPECT_EQ(YsonToKey("1u;" _NULL_ ";2u"), result[2].first);
    EXPECT_EQ(YsonToKey("1u;" _NULL_ ";3u"), result[2].second);
    EXPECT_EQ(YsonToKey("1u;0u;2u"), result[3].first);
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

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("0;0;"), result[0].first);
    EXPECT_EQ(YsonToKey("0;9223372036854775807;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("1;-9223372036854775808"), result[1].first);
    EXPECT_EQ(YsonToKey("1;0;"), result[1].second);
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

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("1"), result[0].first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result[0].second);
    EXPECT_EQ(YsonToKey("2"), result[1].first);
    EXPECT_EQ(YsonToKey("2;" _MAX_), result[1].second);
}

TEST_P(TComputedColumnTest, Join)
{
    const auto& args = GetParam();
    const auto& schemaString1 = args[0];
    const auto& schemaString2 = args[1];

    TTableSchema tableSchema1;
    TTableSchema tableSchema2;
    Deserialize(tableSchema1, ConvertToNode(TYsonString(schemaString1)));
    Deserialize(tableSchema2, ConvertToNode(TYsonString(schemaString2)));

    SetSchema(tableSchema1);
    SetSecondarySchema(tableSchema2);

    auto query = TString("l from [//t] join [//t1] using l where l in (0, 1)");
    auto result = CoordinateForeign(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey(args[2]), result[0].first);
    EXPECT_EQ(YsonToKey(args[3]), result[0].second);
    EXPECT_EQ(YsonToKey(args[4]), result[1].first);
    EXPECT_EQ(YsonToKey(args[5]), result[1].second);
}

INSTANTIATE_TEST_CASE_P(
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
