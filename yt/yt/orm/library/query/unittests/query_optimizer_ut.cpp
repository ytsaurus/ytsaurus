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

    TString expectedSql = "p.`meta.id`, i.book_id FROM `//index` AS i"
                          " JOIN `//books` AS p ON (i.book_id) = (p.`meta.id`)"
                          " WHERE F(p.`meta.id`)"
                          " GROUP BY p.`meta.id`"
                          " HAVING (p.`meta.id`)=(1)"
                          " ORDER BY p.`meta.id`";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_TRUE(TryOptimizeJoin(&query));

    TString optimizedSql = "i.book_id, i.book_id FROM `//index` AS i"
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

    TString expectedSql = "p.`meta.id`, i.book_id FROM `//index` AS i"
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

    TString expectedSql = "(p.`meta.id` as meta_id) FROM `//index` AS i "
                          "JOIN `//books` AS p ON (i.book_id) = (p.`meta.id`) "
                          "ORDER BY meta_id";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_TRUE(TryOptimizeJoin(&query));

    TString optimizedSql = "(i.book_id as meta_id) FROM `//index` AS i ORDER BY meta_id";
    EXPECT_EQ(optimizedSql, FormatQuery(query));
}

////////////////////////////////////////////////////////////////////////////////

bool RunGroupByOptimization(const TString& filter, const std::vector<std::string>& refs, const TString& tableName)
{
    auto parsedQuery = ParseSource(filter, NQueryClient::EParseMode::Expression);
    auto expression = std::get<NQueryClient::NAst::TExpressionPtr>(parsedQuery->AstHead.Ast);

    return TryOptimizeGroupByWithUniquePrefix(expression, refs, tableName);
}

TEST(TQueryOptimizerTest, OptimizeGroupBy)
{
    TObjectsHolder objectsHolder;
    std::vector<std::string> references{"permalink_ids"};
    TString tableName{"i"};

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

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
