#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/orm/library/query/query_optimizer.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryOptimizerTest, OptimizeJoin)
{
    TObjectsHolder objectsHolder;
    TQuery query;
    auto& select = query.SelectExprs.emplace();
    select.push_back(objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "meta.id", "p"));
    select.push_back(objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "book_id", "i"));

    query.FromClause = TTableDescriptor("//index", "i");
    query.Joins.push_back(TJoin(
        false,
        TTableDescriptor("//books", "p"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "book_id", "i"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "meta.id", "p"),
        std::nullopt));
    query.WherePredicate = MakeExpression<TFunctionExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        "F",
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "meta.id", "p"));
    query.GroupExprs = MakeExpression<TReferenceExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            "meta.id",
            "p");
    query.HavingPredicate = MakeExpression<TBinaryOpExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            NQueryClient::EBinaryOp::Equal,
            MakeExpression<TReferenceExpression>(
                &objectsHolder,
                NQueryClient::TSourceLocation(),
                "meta.id",
                "p"),
            MakeExpression<TLiteralExpression>(
                &objectsHolder,
                NQueryClient::TSourceLocation(),
                TLiteralValue(1)));
    query.OrderExpressions.emplace_back(
        MakeExpression<TReferenceExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            "meta.id",
            "p"),
        false);

    TStringBuf expectedSql = "p.`meta.id`, i.book_id FROM `//index` AS i"
                          " JOIN `//books` AS p ON (i.book_id) = (p.`meta.id`)"
                          " WHERE F(p.`meta.id`)"
                          " GROUP BY p.`meta.id`"
                          " HAVING (p.`meta.id`)=(1)"
                          " ORDER BY p.`meta.id`";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_TRUE(TryOptimizeJoin(&query));

    TStringBuf optimizedSql = "i.book_id, i.book_id FROM `//index` AS i"
                           " WHERE F(i.book_id)"
                           " GROUP BY i.book_id"
                           " HAVING (i.book_id)=(1)"
                           " ORDER BY i.book_id";
    EXPECT_EQ(optimizedSql, FormatQuery(query));
}

TEST(TQueryOptimizerTest, DoesNotOptimizeJoin)
{
    TObjectsHolder objectsHolder;
    TQuery query;
    auto& select = query.SelectExprs.emplace();
    select.push_back(objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "meta.id", "p"));
    select.push_back(objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "book_id", "i"));

    query.FromClause = TTableDescriptor("//index", "i");
    query.Joins.push_back(TJoin(
        false,
        TTableDescriptor("//books", "p"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "book_id", "i"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "meta.id", "p"),
        std::nullopt));
    query.WherePredicate = MakeExpression<TFunctionExpression>(
        &objectsHolder,
        NQueryClient::TSourceLocation(),
        "F",
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "meta.parent_id", "p"));

    TStringBuf expectedSql = "p.`meta.id`, i.book_id FROM `//index` AS i"
                          " JOIN `//books` AS p ON (i.book_id) = (p.`meta.id`)"
                          " WHERE F(p.`meta.parent_id`)";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_FALSE(TryOptimizeJoin(&query));

    EXPECT_EQ(expectedSql, FormatQuery(query));
}

TEST(TQueryOptimizerTest, OptimizeWhenUsingSelectExpressionInOrderBy)
{
    TObjectsHolder objectsHolder;
    TQuery query;
    auto& select = query.SelectExprs.emplace();
    select.push_back(
        objectsHolder.New<TAliasExpression>(
            NQueryClient::TSourceLocation(),
            objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "meta.id", "p"),
            "meta_id"));

    query.FromClause = TTableDescriptor("//index", "i");
    query.Joins.push_back(TJoin(
        false,
        TTableDescriptor("//books", "p"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "book_id", "i"),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), "meta.id", "p"),
        std::nullopt));
    query.OrderExpressions.emplace_back(
        MakeExpression<TReferenceExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            "meta_id"),
        false);

    TStringBuf expectedSql = "(p.`meta.id` as meta_id) FROM `//index` AS i "
                          "JOIN `//books` AS p ON (i.book_id) = (p.`meta.id`) "
                          "ORDER BY meta_id";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_TRUE(TryOptimizeJoin(&query));

    TStringBuf optimizedSql = "(i.book_id as meta_id) FROM `//index` AS i ORDER BY meta_id";
    EXPECT_EQ(optimizedSql, FormatQuery(query));
}

////////////////////////////////////////////////////////////////////////////////

bool RunGroupByOptimization(const std::string& filter, const std::vector<std::string>& refs, const std::string& tableName)
{
    auto parsedQuery = ParseSource(filter, NQueryClient::EParseMode::Expression);
    auto expression = std::get<NQueryClient::NAst::TExpressionPtr>(parsedQuery->AstHead.Ast);

    return TryOptimizeGroupByWithUniquePrefix(expression, refs, tableName);
}

TEST(TQueryOptimizerTest, OptimizeGroupBy)
{
    TObjectsHolder objectsHolder;
    std::vector<std::string> references{"permalink_ids"};
    std::string tableName{"i"};

    EXPECT_TRUE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1)",
            references,
            tableName));
    EXPECT_TRUE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1) and true",
            references,
            tableName));
    EXPECT_TRUE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1) and [some_other_ref] < 15",
            references,
            tableName));
    EXPECT_TRUE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1, 2) and i.[permalink_ids] = 15",
            references,
            tableName));
    EXPECT_TRUE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1, 2) and i.[permalink_ids] = 15",
            references,
            tableName));

    EXPECT_FALSE(
        RunGroupByOptimization(
            "i.[permalink_ids] = 2 or i.[permalink_ids] = 1",
            references,
            tableName));
    EXPECT_FALSE(
        RunGroupByOptimization(
            "i.[permalink_ids] in (1, 2)",
            references,
            tableName));
    EXPECT_FALSE(
        RunGroupByOptimization(
            "i.[permalink_ids] < 42",
            references,
            tableName));
}

////////////////////////////////////////////////////////////////////////////////

bool RunPushDownGroupByOptimization(const std::string& querySource)
{
    auto parsedQuery = ParseSource(querySource, NQueryClient::EParseMode::Query);
    auto query = std::get<NQueryClient::NAst::TQuery>(parsedQuery->AstHead.Ast);

    return TryHintPushDownGroupBy(&query);
}

TEST(TQueryOptimizerTest, PushDownGroupBy)
{
    EXPECT_TRUE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"));

    EXPECT_TRUE(RunPushDownGroupByOptimization(
        "first(t.v), min(t.p), max(t.q), first(r.w), sum(r.s), min(r.p), max(r.q)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"
        " having first(t.v) > 0 and (min(t.p) + max(t.q)) < 0 and ((first(r.w) + sum(r.s) * min(r.q)) / max(r.q)) > 300"
        " order by first(t.o1), min(t.o2), max(t.o3), first(r.o1), sum(r.o2), min(r.o3), max(r.o4)"));


    // There is no join or group by to push down.
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "t.v from [//home/table] as t"));

    // There is no group by to push down.
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "t.v, r.w from [//home/table] as t join [//home/table2] as r on t.x = r.x"));

    // Push down is only supported for queries with a signle join.
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w), first(r2.m)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " join [//home/table3] as r2 on t.x = r2.x"
        " group by t.x"));

    // All aggregations on left table must be idempotent.
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "sum(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"));

    // Only supported aggregation functions are allowed in any part of the query.
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "cardinality(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"));
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), cardinality(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"));
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"
        " order by cardinality(r.w)"));
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"
        " order by cardinality(t.v)"));
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"
        " having cardinality(r.w) > 3"));
    EXPECT_FALSE(RunPushDownGroupByOptimization(
        "first(t.v), first(r.w)"
        " from [//home/table] as t"
        " join [//home/table2] as r on t.x = r.x"
        " group by t.x"
        " having cardinality(t.v) > 3"));
}
////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
