#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

#include <yt/yt/library/query/base/query_visitors.h>

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryEvaluateTest, NestedSubquery)
{
    TSplitMap splits;
    std::vector<std::vector<std::string>> sources;

    auto schema = MakeSplit({
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))},
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
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))},
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
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

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
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

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
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

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
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

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

TEST_F(TQueryEvaluateTest, NestedSubquerySyntaxVersion3IsNotImplemented)
{
    auto split = MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    for (const auto& clauses : {"", "GROUP BY 1", "ORDER BY 1 DESC, item ASC LIMIT 1"}) {
        auto query = Format(
            "SELECT (SELECT item FROM (t.numbers AS item) %v) AS nested FROM `//t` AS t",
            clauses);
        SCOPED_TRACE(query);
        EXPECT_THROW_THAT(
            Prepare(
                query,
                {{"//t", split}},
                /*placeholderValues*/ {},
                {
                    .SyntaxVersion = 3,
                    .BuilderVersion = DefaultExpressionBuilderVersion,
                    // COMPAT(dtorilov): Remove after 26.2.
                    .EnableScalarSubqueryOrderByAndLimit = true,
                }),
            HasSubstr("Scalar subqueries are not implemented for syntax version 3"));
    }
}

TEST_F(TQueryEvaluateTest, HavingValidation)
{
    auto split = MakeSplit({
        {"item", EValueType::Int64},
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    for (const auto& [clauses, error] : std::vector<std::pair<std::string, std::string>>{
        {"LIMIT 0", "HAVING with LIMIT is not allowed"},
        {"LIMIT 1", "HAVING with LIMIT is not allowed"},
        {"OFFSET 0 LIMIT 1", "HAVING with LIMIT is not allowed"},
        {"OFFSET 1 LIMIT 1", "HAVING with LIMIT is not allowed"},
        {"OFFSET 1", "OFFSET used without LIMIT"},
        {"ORDER BY item", "ORDER BY used without LIMIT"},
    }) {
        for (const auto& query : {
            Format("SELECT item FROM `//t` GROUP BY item HAVING item > 0 %v", clauses),
            Format(
                "SELECT (SELECT item FROM (t.numbers AS item) GROUP BY item HAVING item > 0 %v) AS nested "
                "FROM `//t` AS t",
                clauses),
        }) {
            SCOPED_TRACE(query);
            EXPECT_THROW_THAT(
                Prepare(
                    query,
                    {{"//t", split}},
                    /*placeholderValues*/ {},
                    {
                        .SyntaxVersion = 2,
                        .BuilderVersion = DefaultExpressionBuilderVersion,
                        // COMPAT(dtorilov): Remove after 26.2.
                        .EnableScalarSubqueryOrderByAndLimit = true,
                    }),
                HasSubstr(error));
        }
    }

    for (const auto& clauses : {"", "ORDER BY item LIMIT 1"}) {
        auto query = Format(
            "SELECT (SELECT item FROM (t.numbers AS item) GROUP BY item HAVING item > 0 %v) AS nested "
            "FROM `//t` AS t",
            clauses);
        SCOPED_TRACE(query);
        EXPECT_THROW_THAT(
            Prepare(
                query,
                {{"//t", split}},
                /*placeholderValues*/ {},
                {
                    .SyntaxVersion = 2,
                    .BuilderVersion = DefaultExpressionBuilderVersion,
                    // COMPAT(dtorilov): Remove after 26.2.
                    .EnableScalarSubqueryOrderByAndLimit = true,
                }),
            HasSubstr("HAVING clause is not supported in subqueries"));
    }

    auto orderedQuery = "SELECT item FROM `//t` GROUP BY item HAVING item > 0 ORDER BY item LIMIT 1";
    EXPECT_NO_THROW(
        Prepare(
            orderedQuery,
            {{"//t", split}},
            /*placeholderValues*/ {},
            {.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}));

    auto sortedSplit = MakeSplit({
        {"item", EValueType::Int64, ESortOrder::Ascending},
    });
    EXPECT_THROW_THAT(
        Prepare(
            orderedQuery,
            {{"//t", sortedSplit}},
            /*placeholderValues*/ {},
            {.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        HasSubstr("HAVING with LIMIT is not allowed"));
}

TEST_F(TQueryEvaluateTest, OrderByOffsetAndLimit)
{
    auto split = MakeSplit({
        {"item", EValueType::Int64},
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"amounts", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"value", "value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });
    auto prepareOptions = TPreparePlanFragmentOptions{
        .SyntaxVersion = 2,
        .BuilderVersion = DefaultExpressionBuilderVersion,
        // COMPAT(dtorilov): Remove after 26.2.
        .EnableScalarSubqueryOrderByAndLimit = true,
    };

    for (const auto& [projection, clauses, expected] : std::vector<std::tuple<std::string, std::string, std::string>>{
        {"sum(1)", "GROUP BY item", "[[2];[2];[2];[2]]"},
        {"sum(1)", "GROUP BY item LIMIT 0", "[]"},
        {"sum(1)", "GROUP BY item LIMIT 1", "[[2]]"},
        {"sum(1)", "GROUP BY item OFFSET 1 LIMIT 2", "[[2];[2]]"},
        {"sum(1)", "GROUP BY item OFFSET 3 LIMIT 2", "[[2]]"},
        {"sum(1)", "GROUP BY item OFFSET 4 LIMIT 1", "[]"},
        {"sum(1)", "GROUP BY item LIMIT 10", "[[2];[2];[2];[2]]"},
        {"item", "GROUP BY item ORDER BY sum(amount) DESC, value ASC OFFSET 0 LIMIT 2", "[[4];[1]]"},
        {"item", "GROUP BY item ORDER BY sum(amount) DESC, value ASC OFFSET 1 LIMIT 2", "[[1];[2]]"},
        {"item", "GROUP BY item ORDER BY sum(amount) DESC, value ASC OFFSET 10 LIMIT 2", "[]"},
    }) {
        auto query = Format(
            "SELECT (SELECT %v AS value FROM (t.numbers AS item, t.amounts AS amount) %v) AS nested FROM `//t` AS t",
            projection,
            clauses);
        SCOPED_TRACE(query);
        auto result = YsonToRows({Format("nested=%v", expected), "nested=[]"}, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            query,
            split,
            {"numbers=[1;2;3;4;1;2;3;4];amounts=[2.25;3.0;0.5;4.0;5.75;5.0;0.5;5.0]", "numbers=[];amounts=[]"},
            ResultMatcher(result, resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }

    for (const auto& [clauses, error] : std::vector<std::pair<std::string, std::string>>{
        {"ORDER BY item", "ORDER BY used without LIMIT"},
        {"OFFSET 0", "OFFSET used without LIMIT"},
        {"OFFSET 1", "OFFSET used without LIMIT"},
        {"OFFSET 9223372036854775808 LIMIT 2", "Negative OFFSET is forbidden"},
        {"OFFSET 18446744073709551615 LIMIT 2", "Negative OFFSET is forbidden"},
        {"LIMIT 9223372036854775808", "Negative LIMIT is forbidden"},
        {"LIMIT 18446744073709551615", "Negative LIMIT is forbidden"},
        {Format("LIMIT %v", UnorderedReadHint), "Maximum LIMIT exceeded"},
        {Format("LIMIT %v", OrderedReadWithPrefetchHint), "Maximum LIMIT exceeded"},
        {Format("OFFSET 3 LIMIT %v", MaxQueryLimit), "overflows i64"},
        {"OFFSET 9223372036854775807 LIMIT 1", "overflows i64"},
        {Format("OFFSET 9223372036854775807 LIMIT %v", MaxQueryLimit), "overflows i64"},
    }) {
        for (const auto& query : {
            Format("SELECT item FROM `//t` %v", clauses),
            Format("SELECT (SELECT item FROM (t.numbers AS item) %v) AS nested FROM `//t` AS t", clauses),
        }) {
            SCOPED_TRACE(query);
            EXPECT_THROW_THAT(
                Prepare(
                    query,
                    {{"//t", split}},
                    /*placeholderValues*/ {},
                    prepareOptions),
                HasSubstr(error));
        }
    }

    auto query = Prepare(
        Format("SELECT (SELECT item FROM (t.numbers AS item)) AS nested FROM `//t` AS t LIMIT %v", MaxQueryLimit),
        {{"//t", split}},
        /*placeholderValues*/ {},
        prepareOptions);
    EXPECT_EQ(query->Limit, MaxQueryLimit);
    EXPECT_EQ(query->GetScanOrder(/*allowUnorderedGroupByWithLimit*/ true), EScanOrder::Ordered);
    EXPECT_FALSE(query->IsPrefetching());
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByAndLimitReusedAlias)
{
    auto split = MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"item", "item", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
        {"count", EValueType::Int64},
    });

    for (bool limited : {false, true}) {
        SCOPED_TRACE(Format("Limited: %v", limited));
        auto query = Format(
            "SELECT (SELECT item FROM (t.numbers AS item) %v) AS nested, yson_length(nested) AS count FROM `//t` AS t WHERE yson_length(nested) > 1",
            limited ? "LIMIT 2" : "");
        auto options = TEvaluateOptions{.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true};
        auto result = YsonToRows({limited ? "nested=[[3];[1]];count=2" : "nested=[[3];[1];[2]];count=3"}, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            query,
            split,
            {"numbers=[3;1;2]"},
            ResultMatcher(result, resultSplit.TableSchema),
            options);
    }
}

// COMPAT(dtorilov): Remove after 26.2.
TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByAndLimitDisabledInNestedExpressions)
{
    auto splits = TSplitMap{{"//t", MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    })}};

    for (const auto& query : {
        R"(SELECT t.numbers[int64(yson_length((SELECT item FROM (t.numbers AS item) LIMIT 1)))] AS value FROM `//t` AS t)",
        R"(SELECT (SELECT row.item FROM ((SELECT item FROM (t.numbers AS item) LIMIT 1) AS row)) AS nested FROM `//t` AS t)",
        R"(SELECT row.item FROM `//t` AS t ARRAY JOIN (SELECT item FROM (t.numbers AS item) LIMIT 1) AS row)",
        R"(SELECT nested FROM ( SELECT ( SELECT item FROM (t.numbers AS item) LIMIT 1) AS nested FROM `//t` AS t))",
    }) {
        EXPECT_THROW_THAT(
            EvaluateOnlyViaNativeExecutionBackend(
                query,
                splits,
                {{"numbers=[3;1;2]"}},
                AnyMatcher,
                {.SyntaxVersion = 2}),
            testing::HasSubstr("ORDER BY, OFFSET and LIMIT are disabled in scalar subqueries"));
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByOffsetAndLimit)
{
    auto split = MakeSplit({
        {"id", EValueType::Int64},
        {"direction", EValueType::Int64},
        {"numbers", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
        {"labels", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });
    auto rows = std::vector<std::string>{
        "id=1;direction=-1;numbers=[3;1;3;2;#];labels=[d;a;c;b;null_value]",
        "id=2;direction=1;numbers=[8;8;4];labels=[z;y;x]",
        "id=3;direction=1;numbers=[];labels=[]",
        "id=4;direction=1;numbers=[0;-1];labels=[zero;negative]",
    };
    auto resultSplit = MakeSplit({
        {"id", EValueType::Int64},
        {"nested", ListLogicalType(StructLogicalType({
            {"text", "text", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto cases = std::vector<std::tuple<std::string, std::string, std::string>>{
        {"ORDER BY item DESC, label ASC LIMIT 2", "[[c];[d]]", "[[y];[z]]"},
        {"ORDER BY item ASC, label DESC LIMIT 2", "[[a];[b]]", "[[x];[z]]"},
        {"LIMIT 2", "[[d];[a]]", "[[z];[y]]"},
        {"ORDER BY item DESC LIMIT 0", "[]", "[]"},
        {"LIMIT 100", "[[d];[a];[c];[b]]", "[[z];[y];[x]]"},
        {"ORDER BY item DESC, label ASC LIMIT 100", "[[c];[d];[b];[a]]", "[[y];[z];[x]]"},
        {"ORDER BY item * t.direction, label ASC LIMIT 2", "[[c];[d]]", "[[x];[y]]"},
        {"ORDER BY item DESC, label ASC OFFSET 1 LIMIT 2", "[[d];[b]]", "[[z];[x]]"},
        {"ORDER BY item DESC, label ASC OFFSET 0 LIMIT 2", "[[c];[d]]", "[[y];[z]]"},
        {"ORDER BY item DESC OFFSET 100 LIMIT 2", "[]", "[]"},
        {"ORDER BY item DESC OFFSET 1 LIMIT 0", "[]", "[]"},
        {"OFFSET 1 LIMIT 2", "[[a];[c]]", "[[y];[x]]"},
        {"OFFSET 100 LIMIT 2", "[]", "[]"},
        {Format("OFFSET 1 LIMIT %v", MaxQueryLimit), "[[a];[c];[b]]", "[[y];[x]]"},
    };
    for (const auto& [clauses, firstResult, secondResult] : cases) {
        auto result = YsonToRows({
            Format("id=1;nested=%v", firstResult),
            Format("id=2;nested=%v", secondResult),
            "id=3;nested=[]",
            "id=4;nested=[]",
        }, resultSplit);

        EvaluateOnlyViaNativeExecutionBackend(
            Format(R"(
                SELECT
                    t.id AS id,
                    (
                        SELECT label AS text
                        FROM (t.numbers AS item, t.labels AS label)
                        WHERE item IS NOT NULL AND item > 0
                        %v
                    ) AS nested
                FROM `//t` AS t
            )", clauses),
            split,
            rows,
            ResultMatcher(result, resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByOffsetSkipsProjection)
{
    constexpr auto QueryTemplate = TStringBuf(R"(
        SELECT (
            SELECT 10 / item AS value
            FROM (t.numbers AS item)
            ORDER BY item
            OFFSET %v
            LIMIT %v
        ) AS nested
        FROM `//t` AS t
    )");
    auto split = MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"value", "value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    for (const auto& [offset, limit, expected] : std::vector<std::tuple<i64, i64, std::string>>{
        {1, 2, "[[10];[5]]"},
        {3, 2, "[]"},
        {0, 0, "[]"},
    }) {
        SCOPED_TRACE(Format("Offset: %v, Limit: %v", offset, limit));
        auto result = YsonToRows({Format("nested=%v", expected), "nested=[]"}, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            Format(QueryTemplate, offset, limit),
            split,
            {"numbers=[0;1;2]", "numbers=[]"},
            ResultMatcher(result, resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByTopRows)
{
    auto split = MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"labels", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"text", "text", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        }, /*removedFieldStableNames*/ {}))},
    });
    auto source = TSource();
    auto expectedRows = TSource();
    for (int rowCount : {0, 1, 4, 5, 6, 9, 10, 11, 16, 37}) {
        auto numbers = std::string("[");
        auto labels = std::string("[");
        auto rows = std::vector<std::pair<i64, std::string>>();
        for (int index = 0; index < rowCount; ++index) {
            i64 score = index * 17 % 11;
            auto label = Format("item%v%v", std::string(80, 'a' + index % 26), index);
            numbers += Format("%v;", score);
            labels += Format("%v;", label);
            rows.emplace_back(score, label);
        }

        source.push_back(Format("numbers=%v];labels=%v]", numbers, labels));
        std::sort(rows.begin(), rows.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.first != rhs.first ? lhs.first > rhs.first : lhs.second < rhs.second;
        });
        auto expected = std::string("nested=[");
        for (int index = 2; index < std::min(rowCount, 5); ++index) {
            expected += Format("[%v_selected];", rows[index].second);
        }

        expectedRows.push_back(expected + "]");
    }

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            SELECT (
                SELECT concat(label, '_selected') AS text
                FROM (t.numbers AS item, t.labels AS label)
                ORDER BY item DESC, lower(label) ASC
                OFFSET 2
                LIMIT 3
            ) AS nested
            FROM `//t` AS t
        )",
        split,
        source,
        ResultMatcher(YsonToRows(expectedRows, resultSplit), resultSplit.TableSchema),
        // COMPAT(dtorilov): Remove after 26.2.
        {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByTopRowsCornerCases)
{
    constexpr auto QueryTemplate = TStringBuf(R"(
        SELECT (
            SELECT item
            FROM (t.numbers AS item)
            ORDER BY item %v
        ) AS nested
        FROM `//t` AS t
    )");
    auto split = MakeSplit({
        {"numbers", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"item", "item", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });
    auto cases = std::vector<std::tuple<std::string, std::string, std::string>>{
        {"[]", "ASC LIMIT 1", "[]"},
        {"[7]", "DESC LIMIT 1", "[[7]]"},
        {"[7]", "ASC OFFSET 1 LIMIT 1", "[]"},
        {"[5;4;3;2;1]", "ASC LIMIT 1", "[[1]]"},
        {"[1;2;3;4;5]", "DESC LIMIT 1", "[[5]]"},
        {"[2;2;2;2;2;2;2]", "ASC OFFSET 1 LIMIT 2", "[[2];[2]]"},
        {"[#;2;#;1;2]", "ASC LIMIT 2", "[[#];[#]]"},
        {"[#;2;#;1;2]", "DESC LIMIT 4", "[[2];[2];[1];[#]]"},
        {"[#;#;#;#;#]", "DESC LIMIT 2", "[[#];[#]]"},
        {"[1;2;3]", "ASC LIMIT 0", "[]"},
        {"[1;2;3]", "DESC OFFSET 2 LIMIT 0", "[]"},
        {"[3;1;2]", Format("ASC LIMIT %v", MaxQueryLimit), "[[1];[2];[3]]"},
        {"[3;1;2]", Format("ASC OFFSET 1 LIMIT %v", MaxQueryLimit), "[[2];[3]]"},
        {"[3;1;2]", "DESC OFFSET 9223372036854775807 LIMIT 0", "[]"},
    };
    for (const auto& [numbers, clauses, expected] : cases) {
        SCOPED_TRACE(Format("Numbers: %v, Clauses: %v", numbers, clauses));
        EvaluateOnlyViaNativeExecutionBackend(
            Format(QueryTemplate, clauses),
            split,
            {Format("numbers=%v", numbers)},
            ResultMatcher(YsonToRows({Format("nested=%v", expected)}, resultSplit), resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByTopRowsStress)
{
    constexpr auto QueryTemplate = TStringBuf(R"(
        SELECT (
            SELECT
                item AS score,
                label AS text
            FROM (t.numbers AS item, t.labels AS label)
            ORDER BY item %v, label ASC
            OFFSET %v
            LIMIT %v
        ) AS nested
        FROM `//t` AS t
    )");
    auto split = MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"labels", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"score", "score", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"text", "text", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
        }, /*removedFieldStableNames*/ {}))},
    });
    auto random = TFastRng64(42);
    for (int iteration = 0; iteration < 32; ++iteration) {
        int rowCount = random.GenRand() % 256;
        int offset = random.GenRand() % 25;
        int limit = random.GenRand() % 17;
        bool descending = iteration % 2 == 0;
        SCOPED_TRACE(Format("Iteration: %v, Rows: %v, Offset: %v, Limit: %v", iteration, rowCount, offset, limit));

        auto numbers = std::string("[");
        auto labels = std::string("[");
        auto rows = std::vector<std::pair<i64, std::string>>();
        for (int index = 0; index < rowCount; ++index) {
            i64 score = static_cast<i64>(random.GenRand() % 23) - 11;
            auto label = Format("value%v", random.GenRand() % 13);
            numbers += Format("%v;", score);
            labels += Format("%v;", label);
            rows.emplace_back(score, label);
        }

        std::sort(rows.begin(), rows.end(), [descending] (const auto& lhs, const auto& rhs) {
            if (lhs.first == rhs.first) {
                return lhs.second < rhs.second;
            }

            return descending ? lhs.first > rhs.first : lhs.first < rhs.first;
        });
        auto expected = std::string("nested=[");
        for (int index = offset; index < std::min(rowCount, offset + limit); ++index) {
            expected += Format("[%v;%v];", rows[index].first, rows[index].second);
        }

        expected += "]";

        EvaluateOnlyViaNativeExecutionBackend(
            Format(QueryTemplate, descending ? "DESC" : "ASC", offset, limit),
            split,
            {Format("numbers=%v];labels=%v]", numbers, labels)},
            ResultMatcher(YsonToRows({expected}, resultSplit), resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryLegacyLimit)
{
    auto splits = TSplitMap{{"//t", MakeSplit({
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    })}};
    auto query = Prepare(
        R"(
            SELECT (
                SELECT item
                FROM (t.numbers AS item)
                WHERE item > 0
            ) AS nested
            FROM `//t` AS t
        )",
        splits,
        /*placeholderValues*/ {},
        {.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});
    auto resultSplit = MakeSplit({
        {"nested", ListLogicalType(StructLogicalType({
            {"item", "item", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    for (const auto& [rootLimit, limit, expected] : std::vector<std::tuple<i64, std::optional<i64>, std::string>>{
        {UnorderedReadHint, std::nullopt, "[[3];[1];[2]]"},
        {UnorderedReadHint, 0, "[]"},
        {UnorderedReadHint, 2, "[[3];[1]]"},
        {UnorderedReadHint, MaxQueryLimit, "[[3];[1];[2]]"},
        {UnorderedReadHint, UnorderedReadHint, "[[3];[1];[2]]"},
        {OrderedReadWithPrefetchHint, std::nullopt, "[[3];[1];[2]]"},
        {OrderedReadWithPrefetchHint, 0, "[]"},
        {OrderedReadWithPrefetchHint, 2, "[[3];[1]]"},
        {OrderedReadWithPrefetchHint, MaxQueryLimit, "[[3];[1];[2]]"},
        {OrderedReadWithPrefetchHint, UnorderedReadHint, "[[3];[1];[2]]"},
    }) {
        SCOPED_TRACE(Format("Root limit: %v, Subquery limit: %v", rootLimit, limit));
        auto serialized = NProto::TQuery();
        ToProto(&serialized, query);
        serialized.set_limit(rootLimit);
        serialized.set_input_row_limit(std::numeric_limits<i64>::max());
        serialized.set_output_row_limit(std::numeric_limits<i64>::max());
        auto* subquery = serialized.mutable_project_clause()->mutable_projections(0)->mutable_expression()
            ->MutableExtension(NProto::TSubqueryExpression::subquery_expression);
        subquery->clear_order_clause();
        subquery->clear_offset();
        subquery->clear_limit();
        if (limit) {
            subquery->set_limit(*limit);
        }

        EXPECT_TRUE(serialized.IsInitialized());
        auto bytes = SerializeProtoToRef(serialized, /*partial*/ true);
        auto parsed = NProto::TQuery();
        DeserializeProto(&parsed, bytes);
        auto restored = TConstQueryPtr();
        FromProto(&restored, parsed);

        EXPECT_EQ(restored->Limit, rootLimit);
        EXPECT_EQ(restored->IsPrefetching(), rootLimit == OrderedReadWithPrefetchHint);
        EXPECT_EQ(
            restored->GetScanOrder(/*allowUnorderedGroupByWithLimit*/ true),
            rootLimit == UnorderedReadHint ? EScanOrder::Unordered : EScanOrder::Ordered);
        const auto* restoredSubquery = restored->ProjectClause->Projections[0].Expression->As<TSubqueryExpression>();
        ASSERT_NE(restoredSubquery, nullptr);
        EXPECT_EQ(restoredSubquery->Limit, limit.value_or(UnorderedReadHint));

        auto pipe = RunOnNodeThread(
            restored,
            {"numbers=[3;0;1;-1;2]", "numbers=[]"},
            EExecutionBackend::Native);
        auto reader = pipe->GetReader();
        auto buffer = New<TRowBuffer>();
        auto rows = std::vector<TRow>();
        while (auto batch = reader->Read()) {
            ASSERT_FALSE(batch->IsEmpty());
            for (auto row : batch->MaterializeRows()) {
                rows.push_back(buffer->CaptureRow(row));
            }
        }

        auto result = YsonToRows({
            Format("nested=%v", expected),
            "nested=[]",
        }, resultSplit);
        ResultMatcher(result, resultSplit.TableSchema)(rows, *restored->GetTableSchema());
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByJoinGroups)
{
    auto splits = TSplitMap();
    splits["//t"] = MakeSplit({
        {"id", EValueType::Int64},
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    splits["//first"] = MakeSplit({
        {"id", EValueType::Int64, ESortOrder::Ascending},
        {"direction", EValueType::Int64},
    });
    splits["//second"] = MakeSplit({
        {"length", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });
    auto resultSplit = MakeSplit({
        {"id", EValueType::Int64},
        {"value", EValueType::Int64},
    });
    auto result = YsonToRows({
        "id=1;value=200", "id=2;value=100", "id=3;value=200", "id=4;value=200", "id=5;value=200",
    }, resultSplit);

    for (const auto& [orderClause, expectedGroups, expectedReferences] : std::vector<std::tuple<std::string, std::vector<size_t>, TColumnSet>>{
        {"", {2}, {"t.numbers"}},
        {"ORDER BY value DESC", {2}, {"t.numbers"}},
        {"ORDER BY t.id DESC", {2}, {"t.numbers", "t.id"}},
        {"ORDER BY item DESC", {1, 1}, {"t.numbers", "item"}},
        {"ORDER BY t.id DESC, item ASC", {1, 1}, {"t.numbers", "t.id", "item"}},
        {"ORDER BY f.direction DESC", {1, 1}, {"t.numbers", "f.direction"}},
        {"ORDER BY item * f.direction DESC, item ASC", {1, 1}, {"t.numbers", "f.direction", "item"}},
        {"ORDER BY if_null(f.direction, 0) * t.id, item DESC", {1, 1}, {"t.numbers", "f.direction", "t.id", "item"}},
    }) {
        SCOPED_TRACE(orderClause);
        auto query = EvaluateOnlyViaNativeExecutionBackend(
            Format(
                "SELECT t.id AS id, s.value AS value FROM `//t` AS t "
                "JOIN `//first` AS f ON t.id = f.id "
                "JOIN `//second` AS s ON yson_length((SELECT 1 AS value FROM (t.numbers AS item) %v LIMIT 1)) = s.length",
                orderClause),
            splits,
            {
                {"id=1;numbers=[3;1;2]", "id=2;numbers=[]", "id=3;numbers=[7]",
                 "id=4;numbers=[2;2;1]", "id=5;numbers=[-2;0;2]", "id=6;numbers=[9]"},
                {"id=1;direction=1", "id=2;direction=-1", "id=3;direction=0", "id=4;direction=#", "id=5;direction=-2"},
                {"length=0;value=100", "length=1;value=200"},
            },
            ResultMatcher(result, resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
        EXPECT_EQ(GetJoinGroups(query->JoinClauses, query->Schema.GetRenamedSchema()), expectedGroups);

        auto references = TColumnSet();
        TReferenceHarvester(&references).Visit(query->JoinClauses[1]->SelfEquations[0]);
        EXPECT_EQ(references, expectedReferences);
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByGroupByPushDown)
{
    auto splits = TSplitMap();
    splits["//t"] = MakeSplit({
        {"id", EValueType::Int64},
        {"direction", EValueType::Int64},
    });
    splits["//foreign"] = MakeSplit({
        {"id", EValueType::Int64, ESortOrder::Ascending},
        {"detail", EValueType::Int64, ESortOrder::Ascending},
        {"rank", EValueType::Int64},
        {"numbers", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    auto resultSplit = MakeSplit({
        {"id", EValueType::Int64},
        {"length", EValueType::Int64},
    });
    for (const auto& [clauses, lastLength] : std::vector<std::pair<std::string, int>>{
        {"", 3}, {"LIMIT 2", 2}, {"ORDER BY f.rank * t.direction DESC LIMIT 2", 2},
    }) {
        SCOPED_TRACE(clauses);
        auto result = YsonToRows({"id=1;length=1", "id=2;length=0", Format("id=3;length=%v", lastLength)}, resultSplit);
        auto query = EvaluateOnlyViaNativeExecutionBackend(
            Format(
                "SELECT id, min(yson_length((SELECT 1 AS value FROM (f.numbers AS item) %v))) AS length "
                "FROM `//t` AS t JOIN `//foreign` AS f WITH HINT \"{push_down_group_by=%%true}\" ON t.id = f.id "
                "GROUP BY t.id AS id",
                clauses),
            splits,
            {
                {"id=1;direction=1", "id=2;direction=-1", "id=3;direction=0", "id=4;direction=1"},
                {"id=1;detail=1;rank=7;numbers=[3;1;2]", "id=1;detail=2;rank=9;numbers=[8]",
                 "id=2;detail=1;rank=8;numbers=[]", "id=2;detail=2;rank=-1;numbers=[2;4]",
                 "id=3;detail=1;rank=0;numbers=[7;6;5]", "id=3;detail=2;rank=#;numbers=[2;2;1]"},
            },
            ResultMatcher(result, resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
        const auto& join = query->JoinClauses[0];
        EXPECT_FALSE(join->GroupClause);
        EXPECT_EQ(join->ForeignJoinedColumns.contains("f.rank"), clauses.starts_with("ORDER BY"));
        EXPECT_TRUE(join->ForeignJoinedColumns.contains("f.numbers"));
    }
}

TEST_F(TQueryEvaluateTest, NestedSubqueryOrderByOuterAggregate)
{
    auto split = MakeSplit({
        {"id", EValueType::Int64},
        {"value", EValueType::Int64},
    });
    auto resultSplit = MakeSplit({
        {"id", EValueType::Int64},
        {"total", EValueType::Int64},
    });
    auto resultRows = TSource{
        "id=1;total=6", "id=2;total=-1", "id=3;total=0", "id=4;total=6",
        "id=5;total=-6", "id=6;total=1", "id=7;total=#",
    };

    for (const auto& [clauses, useLength, expectedIds] : std::vector<std::tuple<std::string, bool, std::vector<int>>>{
        {"LIMIT 1", false, {1, 2, 3, 4, 5, 6, 7}},
        {"ORDER BY item * sum(t.value) DESC, item ASC LIMIT 1", false, {2, 3, 5, 7, 1, 4, 6}},
        {"ORDER BY item * sum(t.value) ASC, item ASC LIMIT 1", false, {1, 3, 4, 6, 7, 2, 5}},
        {"ORDER BY item * total DESC, item ASC LIMIT 1", false, {2, 3, 5, 7, 1, 4, 6}},
        {"ORDER BY item * min(t.value) DESC, item ASC LIMIT 1", false, {2, 3, 4, 5, 7, 1, 6}},
        {"ORDER BY item * max(t.value) DESC, item ASC LIMIT 1", false, {3, 5, 7, 1, 2, 4, 6}},
        {"ORDER BY item * sum(t.value) DESC, item ASC OFFSET 1 LIMIT 1", false, {1, 2, 3, 4, 5, 6, 7}},
        {"WHERE item * sum(t.value) > 0 LIMIT 3", true, {2, 3, 5, 7, 1, 4, 6}},
        {"WHERE item > sum(t.value) ORDER BY item OFFSET 1 LIMIT 2", true, {1, 4, 6, 2, 3, 5, 7}},
        {"ORDER BY item * sum(t.value) DESC LIMIT 0", true, {1, 2, 3, 4, 5, 6, 7}},
    }) {
        SCOPED_TRACE(clauses);
        auto subquery = Format(
            "(SELECT item FROM (CAST(make_list(3, 1, 2) AS `List<Int64>`) AS item) %v)", clauses);
        auto orderExpression = useLength
            ? Format("yson_length(%v)", subquery)
            : Format("get_int64(%v, '/0/0')", subquery);
        auto expectedRows = TSource();
        for (int id : expectedIds) {
            expectedRows.push_back(resultRows[id - 1]);
        }

        EvaluateOnlyViaNativeExecutionBackend(
            Format(
                "SELECT id, sum(t.value) AS total FROM `//t` AS t GROUP BY t.id AS id "
                "ORDER BY %v ASC, id ASC LIMIT 7", orderExpression),
            split,
            {"id=1;value=3", "id=2;value=2", "id=3;value=0", "id=4;value=7", "id=5;value=-4", "id=6;value=1",
             "id=7;value=#", "id=1;value=3", "id=2;value=-3", "id=3;value=0", "id=4;value=-1", "id=5;value=-2", "id=7;value=#"},
            ResultMatcher(YsonToRows(expectedRows, resultSplit), resultSplit.TableSchema),
            // COMPAT(dtorilov): Remove after 26.2.
            {.SyntaxVersion = 2, .EnableScalarSubqueryOrderByAndLimit = true});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
