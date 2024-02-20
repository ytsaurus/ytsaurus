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

    query.Table = TTableDescriptor("//index", "i");
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
    query.GroupExprs.emplace(
        MakeExpression<TReferenceExpression>(
            &objectsHolder,
            NQueryClient::TSourceLocation(),
            "meta.id",
            "p"),
        NQueryClient::ETotalsMode::None);
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

    query.Table = TTableDescriptor("//index", "i");
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

    query.Table = TTableDescriptor("//index", "i");
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

TEST(TQueryOptimizerTest, OptimizeGroupBy)
{
    TObjectsHolder objectsHolder;
    std::vector<TString> references{"permalink_ids"};
    TString tableName{"i"};

    TExpression* filterExpression = objectsHolder.New<TInExpression>(
        NQueryClient::TSourceLocation(),
        MakeExpression<TReferenceExpression>(&objectsHolder, NQueryClient::TSourceLocation(), references[0], tableName),
        TLiteralValueTupleList{{"1"}, {"2"}});

    EXPECT_FALSE(
        TryOptimizeGroupByWithUniquePrefix(
            filterExpression,
            references,
            tableName));

    filterExpression->As<TInExpression>()->Values = TLiteralValueTupleList{{"1"}};

    EXPECT_TRUE(
        TryOptimizeGroupByWithUniquePrefix(
            filterExpression,
            references,
            tableName));

    filterExpression = objectsHolder.New<TBinaryOpExpression>(
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{filterExpression},
        TExpressionList{objectsHolder.New<TLiteralExpression>(NQueryClient::TSourceLocation(), true)});

    EXPECT_TRUE(
        TryOptimizeGroupByWithUniquePrefix(
            filterExpression,
            references,
            tableName));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryOptimizerTest, OptimizeStringTryGetString)
{
    TObjectsHolder objectsHolder;
    TQuery query;
    auto& select = query.SelectExprs.emplace();
    auto getMetaIdExpr = objectsHolder.New<TFunctionExpression>(
        NQueryClient::TSourceLocation(),
        "string",
        TExpressionList{
            objectsHolder.New<TFunctionExpression>(
                NQueryClient::TSourceLocation(),
                "try_get_string",
                TExpressionList{
                    objectsHolder.New<TReferenceExpression>(NQueryClient::TSourceLocation(), "meta"),
                    objectsHolder.New<TLiteralExpression>(NQueryClient::TSourceLocation(), "/id")
                }
            )
        }
    );

    select.push_back(
        objectsHolder.New<TAliasExpression>(
            NQueryClient::TSourceLocation(),
            getMetaIdExpr,
            "meta_id"));

    query.Table = TTableDescriptor("//db/entities");

    TString expectedSql = "(string(try_get_string(meta, \"/id\")) as meta_id) FROM `//db/entities`";
    EXPECT_EQ(expectedSql, FormatQuery(query));

    EXPECT_TRUE(TryOptimizeTryGetString(&query));
    TString optimizedSql = "(try_get_string(meta, \"/id\") as meta_id) FROM `//db/entities`";
    EXPECT_EQ(optimizedSql, FormatQuery(query));
}

} // namespace
} // namespace NYT::NOrm::NQuery::NTests
