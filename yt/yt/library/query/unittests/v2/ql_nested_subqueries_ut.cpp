#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryEvaluateTest, NestedSubquery)
{
    TSplitMap splits;
    std::vector<std::vector<std::string>> sources;

    auto schema = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        {"b", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"k", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        {"s", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
    });

    auto data = std::vector<std::string> {
        "a=1; b=x; k=1; s=1",
        "a=2; b=y; k=1; s=1",
        "a=3; b=z; k=1; s=1",
        "a=2; b=k; k=2; s=2",
        "a=3; b=l; k=2; s=2",
        "     b=m; k=2; s=2",
        "a=3; b=x; k=3; s=1",
        "a=4; b=y; k=3; s=1",
        "     b=z; k=3; s=1",
        "a=1; b=x; k=4; s=1",
    };

    splits["//t"] = schema;
    sources.push_back(data);

    auto resultSplit = MakeSplit({
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"nested", ListLogicalType(StructLogicalType({
            {"x", "x", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"y", "y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"z", "z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))}
        }, /*removedFieldStableNames*/ {}))}
    });

    auto result = YsonToRows({
        "a=2;nested=[[3;3;x];[6;4;y];[9;5;z]]",
        "a=3;nested=[[12;5;k];[18;6;l];[#;#;m]]",
        "a=4;nested=[[9;7;x];[#;#;z]]",
        "a=5;nested=[[1;6;x]]"
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend("select t.k + 1 as a, (select li * sum(t.s) as x, li + a as y, ls as z "
        "from (array_agg(t.a, true) as li, array_agg(t.b, true) as ls) where li < 4) as nested from `//t` as t group by a",
        splits,
        sources,
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, OutOfLineBoundValues)
{
    std::string query = R"(
        (sum(SumCost)) AS SumCost_,
        (
            SELECT
                GMID_ * SumCost_ as X,
                sum(if(if_null(double(SumCost_) / SumNum_, 0) = 0, null, double(SumCost_) / SumNum_)) AS Y
            FROM (
                array_agg(GMID, false) AS GMID_,
                array_agg(SumNum, false) as SumNum_
            )
            GROUP BY GMID_
        ) AS SumArray
        FROM (
            SELECT
                GMID,
                sum(Num) AS SumNum,
                sum(Cost) AS SumCost
            FROM `//t`
            GROUP BY (GMID)
            LIMIT 4294967295
        )
        GROUP BY (1) AS FakeGroupBy
    )";

    TSplitMap splits;
    std::vector<std::vector<std::string>> sources;

    auto schema = MakeSplit({
        {"GMID", EValueType::Int64},
        {"Num", EValueType::Int64},
        {"Cost", EValueType::Int64},
    });

    auto data = std::vector<std::string>{};

    splits["//t"] = schema;
    sources.push_back(data);

    auto resultSplit = MakeSplit({
        {"SumCost_", EValueType::Int64},
        {"SumArray", ListLogicalType(StructLogicalType({
            {"X", "X", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"Y", "Y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double))}
        }, /*removedFieldStableNames*/ {}))}
    });

    auto result = YsonToRows({}, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        query,
        splits,
        sources,
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, NestedSubqueryGroupBy)
{
    TSplitMap splits;
    std::vector<std::vector<std::string>> sources;

    auto schema = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        {"b", SimpleLogicalType(ESimpleLogicalValueType::String)},
        {"k", SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        {"s", SimpleLogicalType(ESimpleLogicalValueType::Int32)}
        });

    auto data = std::vector<std::string> {
        "a=1; b=x; k=1; s=1",
        "a=2; b=y; k=1; s=1",
        "a=3; b=z; k=1; s=1",
        "a=2; b=k; k=2; s=2",
        "a=3; b=l; k=2; s=2",
        "     b=m; k=2; s=2",
        "a=3; b=x; k=3; s=1",
        "a=4; b=y; k=3; s=1",
        "     b=z; k=3; s=1",
        "a=1; b=x; k=4; s=1",
    };

    splits["//t"] = schema;
    sources.push_back(data);

    // Test inner aggregation.
    {
        // Test checks that sum(t.s) aggregation level is properly determined as outer by its substitution level.
        auto primaryQuery = Prepare(
            "select t.k % 2 as a, (select ls, sum(t.s) as x, sum(li) as y, sum(1) as z from (array_agg(t.a, true) as li, array_agg(t.b, true) as ls) group by ls) as nested from `//t` as t group by a",
            splits,
            {},
            2);

        const auto& aggregateItems = primaryQuery->GroupClause->AggregateItems;

        ASSERT_EQ(std::ssize(aggregateItems), 3);
        EXPECT_EQ(aggregateItems[2].Name, "sum(`t.s`)");
    }

    {
        // Test inner group key `ls + sum(t.s) as x` contains outer aggregate expression.
        auto primaryQuery = Prepare(
            "select t.k % 2 as a, (select li + sum(t.s) as x, sum(1) as z from (array_agg(t.a, true) as li) group by x) as nested from `//t` as t group by a",
            splits,
            {},
            2);

        const auto& aggregateItems = primaryQuery->GroupClause->AggregateItems;

        ASSERT_EQ(std::ssize(aggregateItems), 2);
        EXPECT_EQ(aggregateItems[1].Name, "sum(`t.s`)");
    }

    {
        // Test checks that sum(b) aggregation level is properly determined as outer while it has no substitution level.

        // FIXME: Expressiones `sum(1)` and `sum(b)` are indistinguishable if `2 as b` is replaced by `1 as b`. Enrich aggregate references with its level.
        auto primaryQuery = Prepare(
            "select t.k % 2 as a, sum(2) as b, (select b, sum(1) as z from (array_agg(t.a, true) as li) group by li) as nested from `//t` as t group by a",
            splits,
            {},
            2);

        const auto& aggregateItems = primaryQuery->GroupClause->AggregateItems;

        ASSERT_EQ(std::ssize(aggregateItems), 2);
        EXPECT_EQ(aggregateItems[0].Name, "sum(0#2)");
    }

    {
        // Test checks that sum(b) aggregation level is properly determined as outer while it has no substitution level.

        // FIXME: Expressiones `sum(1)` and `sum(b)` are indistinguishable if `2 as b` is replaced by `1 as b`. Enrich aggregate references with its level.
        auto primaryQuery = Prepare(
            "select t.k % 2 as a, sum(2) as b, (select li + b as x, sum(1) as z from (array_agg(t.a, true) as li) group by x) as nested from `//t` as t group by a",
            splits,
            {},
            2);

        const auto& aggregateItems = primaryQuery->GroupClause->AggregateItems;

        ASSERT_EQ(std::ssize(aggregateItems), 2);
        EXPECT_EQ(aggregateItems[0].Name, "sum(0#2)");
    }

    {
        auto resultSplit = MakeSplit({
            {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"nested", ListLogicalType(StructLogicalType({
                {"ls", "ls", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
                {"x", "x", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"y", "y", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"z", "z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
            }, /*removedFieldStableNames*/ {}))}
        });

        auto result = YsonToRows({
            "a=1;nested=[[\"x\";6;4;2];[\"z\";6;3;2];[\"y\";6;6;2]]",
            "a=0;nested=[[\"x\";7;#;1];[\"k\";7;2;1];[\"l\";7;3;1];[\"m\";7;1;1]]",
        }, resultSplit);

        // Test checks that sum(t.s) aggregation level is properly determined as outer by its substitution level.
        EvaluateOnlyViaNativeExecutionBackend(
            "select t.k % 2 as a, "
            "(select ls, sum(t.s) as x, sum(li) as y, sum(1) as z from (array_agg(t.a, true) as li, array_agg(t.b, true) as ls) group by ls) as nested "
            "from `//t` as t group by a",
            splits,
            sources,
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"nested", ListLogicalType(StructLogicalType({
                {"x", "x", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"z", "z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
            }, /*removedFieldStableNames*/ {}))}
        });

        auto result = YsonToRows({
            "a=1;nested=[[8;1];[9;2];[7;1];[10;1]]",
            "a=0;nested=[[8;1];[9;1];[10;1]]",
        }, resultSplit);

        // Test inner group key `ls + sum(t.s) as x` contains outer aggregate expression.
        EvaluateOnlyViaNativeExecutionBackend("select t.k % 2 as a, (select li + sum(t.s) as x, sum(1) as z from (array_agg(t.a, true) as li) group by x) as nested from `//t` as t group by a",
            splits,
            sources,
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"b", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"nested", ListLogicalType(StructLogicalType({
                {"b", "b", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"z", "z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
            }, /*removedFieldStableNames*/ {}))}
        });

        auto result = YsonToRows({
            "a=1;b=12;nested=[[12;1];[12;1];[12;2];[12;1]]",
            "a=0;b=8;nested=[[8;1];[8;1];[8;1]]",
        }, resultSplit);

        // Test checks that sum(b) aggregation level is properly determined as outer while it has no substitution level.
        EvaluateOnlyViaNativeExecutionBackend("select t.k % 2 as a, sum(2) as b, (select b, sum(1) as z from (array_agg(t.a, true) as li) group by li) as nested from `//t` as t group by a",
            splits,
            sources,
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"b", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"nested", ListLogicalType(StructLogicalType({
                {"x", "x", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"z", "z", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}
            }, /*removedFieldStableNames*/ {}))}
        });

        auto result = YsonToRows({
            "a=1;b=12;nested=[[16;1];[13;1];[14;1];[15;2]]",
            "a=0;b=8;nested=[[11;1];[9;1];[10;1]]",
        }, resultSplit);

         // Test checks that sum(b) aggregation level is properly determined as outer while it has no substitution level.
        EvaluateOnlyViaNativeExecutionBackend("select t.k % 2 as a, sum(2) as b, (select li + b as x, sum(1) as z from (array_agg(t.a, true) as li) group by x) as nested from `//t` as t group by a",
            splits,
            sources,
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
