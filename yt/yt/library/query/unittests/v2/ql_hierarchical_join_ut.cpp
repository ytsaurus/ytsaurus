#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryEvaluateTest, HierarchicalJoinWithGroupByInParentQueryThrows)
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"b", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::Int64},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a,
                    (select li, foreign_t.c
                        from (array_agg(t.b, true) as li)
                        join `//foreign` as foreign_t on li = foreign_t.x
                    ) as joined_data
                from `//t` as t
                group by a
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN with GROUP BY in parent query is not supported"));
}

////////////////////////////////////////////////////////////////////////////////

TSplitMap MakeSplitsWithListColumn()
{
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"arr", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::Int64},
    });
    return splits;
}

TSplitMap MakeSplitsForGetJoinSubquery()
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"arr", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::String},
    });

    splits["//foreign_keyed"] = MakeSplit({
        TColumnSchema("x", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("y", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("c", EValueType::String),
    });

    splits["//foreign_computed"] = MakeSplit({
        TColumnSchema("hash", EValueType::Int64, ESortOrder::Ascending)
            .SetExpression("int64(farm_hash(x) % 64)"),
        TColumnSchema("x", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("c", EValueType::String),
    });

    splits["//foreign2"] = MakeSplit({
        {"x", EValueType::Int64},
        {"v", EValueType::Int64},
    });

    return splits;
}

TSplitMap MakeSplitsWithListColumn2()
{
    TSplitMap splits;

    splits["//left"] = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"fks", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//right"] = MakeSplit({
        {"pk", EValueType::Int64, ESortOrder::Ascending},
        {"value", EValueType::Int64},
    });

    return splits;
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubquerySelectsKeyColumnFromPlainTable)
{
    auto query = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        MakeSplitsForGetJoinSubquery(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    auto fingerprint = InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT `f.x` AS `f.x`, `f.c` AS f.c");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubquerySelectsAllKeyColumnsFromKeyedTable)
{
    auto splits = MakeSplitsForGetJoinSubquery();
    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"arr1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    auto query = Prepare(
        R"(
            select t.a,
                (select li1, li2, f.c
                    from (t.arr1 as li1, t.arr2 as li2)
                    join `//foreign_keyed` as f on (li1, li2) = (f.x, f.y)
                ) as r
            from `//t` as t
        )",
        splits,
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    auto fingerprint = InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT `f.x` AS `f.x`, `f.y` AS `f.y`, `f.c` AS f.c");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubqueryIncludesComputedKeyColumn)
{
    auto query = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign_computed` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        MakeSplitsForGetJoinSubquery(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    auto fingerprint = InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT `f.hash` AS `f.hash`, `f.x` AS `f.x`, `f.c` AS f.c");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubqueryMatchesForInnerAndLeftJoin)
{
    auto splits = MakeSplitsForGetJoinSubquery();

    auto innerQuery = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        splits,
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    auto leftQuery = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    left join `//foreign` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        splits,
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(innerQuery->HierarchicalJoinsBeforeGroupBy), 1);
    ASSERT_EQ(std::ssize(leftQuery->HierarchicalJoinsBeforeGroupBy), 1);

    EXPECT_EQ(
        InferName(innerQuery->HierarchicalJoinsBeforeGroupBy[0]->GetJoinSubquery(), {.OmitValues = true}),
        InferName(leftQuery->HierarchicalJoinsBeforeGroupBy[0]->GetJoinSubquery(), {.OmitValues = true}));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubqueryIsIndependentPerJoinClause)
{
    auto splits = MakeSplitsForGetJoinSubquery();

    auto query = Prepare(
        R"(
            select t.a,
                (select li1, f1.c  from (t.arr as li1) join `//foreign`  as f1 on li1 = f1.x)  as r1,
                (select li2, f2.v  from (t.arr as li2) join `//foreign2` as f2 on li2 = f2.x)  as r2
            from `//t` as t
        )",
        splits,
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 2);

    auto fingerprint0 = InferName(query->HierarchicalJoinsBeforeGroupBy[0]->GetJoinSubquery(), {.OmitValues = true});
    auto fingerprint1 = InferName(query->HierarchicalJoinsBeforeGroupBy[1]->GetJoinSubquery(), {.OmitValues = true});

    EXPECT_NE(fingerprint0, fingerprint1);
    EXPECT_EQ(fingerprint0, "SELECT `f1.x` AS `f1.x`, `f1.c` AS f1.c");
    EXPECT_EQ(fingerprint1, "SELECT `f2.x` AS `f2.x`, `f2.v` AS f2.v");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubquerySetsNonNullForeignObjectId)
{
    auto query = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        MakeSplitsForGetJoinSubquery(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];

    EXPECT_NE(hierarchicalJoin->ForeignObjectId, NObjectClient::TObjectId{});

    auto fingerprint = InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true});
    EXPECT_EQ(fingerprint, "SELECT `f.x` AS `f.x`, `f.c` AS f.c");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubquerySchemaMatchesForeignTable)
{
    auto query = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign_computed` as f on li = f.x
                ) as r
            from `//t` as t
        )",
        MakeSplitsForGetJoinSubquery(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    auto subquery = hierarchicalJoin->GetJoinSubquery();

    ASSERT_NE(subquery->Schema.Original, nullptr);
    EXPECT_EQ(subquery->Schema.Original->GetColumnCount(), 3);

    auto fingerprint = InferName(subquery, {.OmitValues = true});
    EXPECT_EQ(fingerprint, "SELECT `f.hash` AS `f.hash`, `f.x` AS `f.x`, `f.c` AS f.c");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGetJoinSubqueryPreservesForeignWhereClause)
{
    auto query = Prepare(
        R"(
            select t.a,
                (select li, f.c
                    from (t.arr as li)
                    join `//foreign` as f on li = f.x
                    where f.c = 'foo'
                ) as r
            from `//t` as t
        )",
        MakeSplitsForGetJoinSubquery(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    auto fingerprint = InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT `f.x` AS `f.x`, `f.c` AS f.c WHERE `f.c` = ?");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinPlanStructureWithSingleInnerJoin)
{
    auto query = Prepare(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                ) as joined_data
            from `//t` as t
        )",
        MakeSplitsWithListColumn(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);

    const auto& hj = query->HierarchicalJoinsBeforeGroupBy[0];
    EXPECT_EQ(hj->ResultColumnName, "hierarchical_join_result_0");
    ASSERT_NE(hj->SelfSideJoinKeys, nullptr);
    ASSERT_NE(hj->JoiningSubquery, nullptr);

    EXPECT_EQ(InferName(query, {.OmitValues = true}),
        "SELECT `t.a` AS a, hierarchical_join_result_0 AS joined_data"
        " INNER HIERARCHICAL JOIN [result: hierarchical_join_result_0,"
        " keys: (SELECT li AS li FROM (`t.arr` AS li)),"
        " subquery: (SELECT li AS li, `foreign_t.c` AS foreign_t.c FROM (`t.arr` AS li)"
        " INNER JOIN [common prefix: 0, foreign prefix: 0] ON (li) = (`foreign_t.x`))]");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinSelfSideWherePredicateSplitIntoJoinKeys)
{
    auto query = Prepare(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                    where li > 0
                ) as joined_data
            from `//t` as t
        )",
        MakeSplitsWithListColumn(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    ASSERT_NE(query->HierarchicalJoinsBeforeGroupBy[0]->SelfSideJoinKeys, nullptr);

    EXPECT_EQ(InferName(query, {.OmitValues = true}),
        "SELECT `t.a` AS a, hierarchical_join_result_0 AS joined_data"
        " INNER HIERARCHICAL JOIN [result: hierarchical_join_result_0,"
        " keys: (SELECT li AS li FROM (`t.arr` AS li) WHERE li > ?),"
        " subquery: (SELECT li AS li, `foreign_t.c` AS foreign_t.c FROM (`t.arr` AS li)"
        " INNER JOIN [common prefix: 0, foreign prefix: 0] ON (li) = (`foreign_t.x`) WHERE li > ?)]");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinRewriterPreservesSubqueryWithoutJoin)
{
    auto splits = MakeSplitsWithListColumn();
    auto query = Prepare(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                ) as joined_data,
                (select li2 from (t.arr as li2)) as no_join
            from `//t` as t
        )",
        splits,
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    ASSERT_EQ(std::ssize(query->ProjectClause->Projections), 3);

    EXPECT_EQ(InferName(query, {.OmitValues = true}),
        "SELECT `t.a` AS a, hierarchical_join_result_0 AS joined_data,"
        " (SELECT li2 AS li2 FROM (`t.arr` AS li2)) AS no_join"
        " INNER HIERARCHICAL JOIN [result: hierarchical_join_result_0,"
        " keys: (SELECT li AS li FROM (`t.arr` AS li)),"
        " subquery: (SELECT li AS li, `foreign_t.c` AS foreign_t.c FROM (`t.arr` AS li)"
        " INNER JOIN [common prefix: 0, foreign prefix: 0] ON (li) = (`foreign_t.x`))]");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinLeftJoinSplitsWherePredicatesByColumnOrigin)
{
    auto query = Prepare(R"(
        select t.a as a,
            (select li, foreign_t.c
                from (t.arr as li)
                left join `//foreign` as foreign_t on li = foreign_t.x and (foreign_t.x > 1 and foreign_t.c < 2)
                where li = 42 and li + foreign_t.c > 42 and foreign_t.c = 5
            ) as joined_data
        from `//t` as t)",
        MakeSplitsWithListColumn(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    EXPECT_TRUE(hierarchicalJoin->IsLeft);

    EXPECT_EQ(
        InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true}),
        "SELECT `foreign_t.x` AS `foreign_t.x`, `foreign_t.c` AS foreign_t.c"
        " WHERE `foreign_t.x` > ? AND `foreign_t.c` < ? AND `foreign_t.c` = ?");

    EXPECT_EQ(
        InferName(hierarchicalJoin->JoiningSubquery->WhereClause, {.OmitValues = true}),
        "li = ? AND li + `foreign_t.c` > ? AND `foreign_t.c` = ?");

    EXPECT_EQ(InferName(query, {.OmitValues = true}),
        "SELECT `t.a` AS a, hierarchical_join_result_0 AS joined_data"
        " LEFT HIERARCHICAL JOIN [result: hierarchical_join_result_0,"
        " keys: (SELECT li AS li FROM (`t.arr` AS li) WHERE li = ?),"
        " subquery: (SELECT li AS li, `foreign_t.c` AS foreign_t.c FROM (`t.arr` AS li)"
        " LEFT JOIN [common prefix: 0, foreign prefix: 0] ON (li) = (`foreign_t.x`) AND `foreign_t.x` > ? AND `foreign_t.c` < ? WHERE li = ? AND li + `foreign_t.c` > ? AND `foreign_t.c` = ?)]");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInnerJoinMovesForeignWherePredicateToJoinClause)
{
    auto query = Prepare(R"(
        select t.a as a,
            (select li, foreign_t.c
                from (t.arr as li)
                join `//foreign` as foreign_t on li = foreign_t.x
                where foreign_t.c > 50
            ) as joined_data
        from `//t` as t)",
        MakeSplitsWithListColumn(),
        {},
        TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion});

    ASSERT_EQ(std::ssize(query->HierarchicalJoinsBeforeGroupBy), 1);
    const auto& hierarchicalJoin = query->HierarchicalJoinsBeforeGroupBy[0];
    EXPECT_FALSE(hierarchicalJoin->IsLeft);

    EXPECT_EQ(
        InferName(hierarchicalJoin->GetJoinSubquery(), {.OmitValues = true}),
        "SELECT `foreign_t.x` AS `foreign_t.x`, `foreign_t.c` AS foreign_t.c"
        " WHERE `foreign_t.c` > ?");

    EXPECT_FALSE(hierarchicalJoin->JoiningSubquery->WhereClause);

    EXPECT_EQ(InferName(query, {.OmitValues = true}),
        "SELECT `t.a` AS a, hierarchical_join_result_0 AS joined_data"
        " INNER HIERARCHICAL JOIN [result: hierarchical_join_result_0,"
        " keys: (SELECT li AS li FROM (`t.arr` AS li) WHERE ?),"
        " subquery: (SELECT li AS li, `foreign_t.c` AS foreign_t.c FROM (`t.arr` AS li)"
        " INNER JOIN [common prefix: 0, foreign prefix: 0] ON (li) = (`foreign_t.x`) AND `foreign_t.c` > ?)]");
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinNestedInsideNonJoiningSubqueryThrows)
{
    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a,
                    (
                        select li,
                            (select li2, foreign_t.c
                                from (t.arr as li2)
                                join `//foreign` as foreign_t on li2 = foreign_t.x
                            ) as nested
                        from (t.arr as li)
                    ) as outer_sub
                from `//t` as t
            )",
            MakeSplitsWithListColumn(),
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN nested inside another subquery is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinSubqueryNestedInsideJoiningSubqueryThrows)
{
    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a,
                    (
                        select li, foreign_t.c,
                        (
                            select li2 from (t.arr as li2)
                        ) as nested
                        from (t.arr as li)
                        join `//foreign` as foreign_t on li = foreign_t.x
                    ) as outer_sub
                from `//t` as t
            )",
            MakeSplitsWithListColumn(),
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery nested inside a subquery with JOIN is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinNestedJoinInsideJoiningSubqueryThrows)
{
    auto splits = MakeSplitsWithListColumn();
    splits["//foreign_2"] = MakeSplit({
        {"y", EValueType::Int64},
        {"d", EValueType::Int64},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a,
                    (select li, foreign_t.c,
                        (select li2, foreign_2.d
                            from (t.arr as li2)
                            join `//foreign_2` as foreign_2 on li2 = foreign_2.y
                        ) as nested
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                    ) as outer_sub
                from `//t` as t
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery nested inside a subquery with JOIN is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinMultipleJoinClausesInSubqueryThrows)
{
    auto splits = MakeSplitsWithListColumn();
    splits["//foreign_2"] = MakeSplit({
        {"y", EValueType::Int64},
        {"d", EValueType::Int64},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a,
                    (select li, f1.c, foreign_2.d
                        from (t.arr as li)
                        join `//foreign` as f1 on li = f1.x
                        join `//foreign_2` as foreign_2 on li = foreign_2.y
                    ) as joined_data
                from `//t` as t
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with more than one join clause is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInOuterWhereClauseThrows)
{
    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a
                from `//t` as t
                where list_contains(
                    (select li, foreign_t.c from (t.arr as li) join `//foreign` as foreign_t on li = foreign_t.x),
                    0
                )
            )",
            MakeSplitsWithListColumn(),
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN in WHERE clause is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInOrderByClauseThrows)
{
    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a
                from `//t` as t
                order by (select li, foreign_t.c from (t.arr as li) join `//foreign` as foreign_t on li = foreign_t.x)
                limit 1
            )",
            MakeSplitsWithListColumn(),
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN in ORDER BY clause is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInOuterJoinClauseThrows)
{
    // NB: TPathCollector does NOT visit the AND predicate of join clauses.
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"arr", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a
                from `//t` as t
                join `//foreign` as f on t.a = f.x
                and list_contains(
                    (select li, t2.a from (f.arr as li) join `//t` as t2 on li = t2.a),
                    0
                )
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN in JOIN clause is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInHavingClauseWithGroupByThrows)
{
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"b", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::Int64},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select t.a as a
                from `//t` as t
                group by a
                having list_contains(
                    (select li, foreign_t.c
                        from (array_agg(t.b, true) as li)
                        join `//foreign` as foreign_t on li = foreign_t.x
                    ),
                    0
                )
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Subquery with JOIN with GROUP BY in parent query is not supported"));
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInnerJoinMatchesRowsByKey)
{
    auto splits = MakeSplitsWithListColumn();

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10]",
        "a=2;arr=[20]",
    };

    auto foreignRows = std::vector<std::string>{
        "x=10;c=100",
        "x=20;c=200",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li", "li", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"foreign_t.c", "foreign_t.c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;100;];]",
        "a=2;joined_data=[[20;200;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinWorksWithStringJoinKey)
{
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"arr", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::String},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[\"a\";\"b\"]",
        "a=2;arr=[\"d\"]",
    };

    auto foreignRows = std::vector<std::string>{
        "x=\"a\";c=1",
        "x=\"b\";c=2",
        "x=\"c\";c=3",
        "x=\"d\";c=4",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"c", "c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[1;];[2;];]",
        "a=2;joined_data=[[4;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select foreign_t.c as c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinStressTestMatchesNaiveImplementation)
{
    auto splits = MakeSplitsWithListColumn2();

    i64 leftPksCount = 100;
    i64 leftFksMaxLength = 20;
    i64 rightPksCount = 100;
    i64 rightValuesCount = 100;
    i64 iterations = 20;

    for (bool isLeftJoin : {false, true}) {
        for (int iteration = 0; iteration < iterations; ++iteration) {
            auto leftRowsMap = THashMap<i64, std::vector<i64>>();
            {
                for (i64 i = 0; i < leftPksCount; ++i) {
                    i64 pk = std::rand() % leftPksCount;
                    if (leftRowsMap.contains(pk)) {
                        continue;
                    }

                    i64 length = std::rand() % leftFksMaxLength;
                    for (int j = 0; j < length; ++j) {
                        leftRowsMap[pk].push_back(std::rand() % rightPksCount);
                    }
                }
            }

            auto leftRowsSource = TSource();
            {
                auto buffer = std::vector<std::pair<i64, std::vector<i64>>>();
                {
                    for (const auto& [key, value] : leftRowsMap) {
                        buffer.emplace_back(key, value);
                    }
                    std::sort(buffer.begin(), buffer.end());
                }

                for (const auto& it : buffer) {
                    leftRowsSource.push_back(Format("pk=%v;fks=[%v;]", it.first, JoinToString(it.second, TStringBuf(";"))));
                }
            }

            auto rightRowsMap = THashMap<i64, i64>();
            {
                for (i64 i = 0; i < rightPksCount; ++i) {
                    i64 pk = std::rand() % rightPksCount;
                    if (rightRowsMap.contains(pk)) {
                        continue;
                    }
                    rightRowsMap[pk] = std::rand() % rightValuesCount;
                }
            }

            auto rightRowsSource = TSource();
            {
                auto buffer = std::vector<std::pair<i64, i64>>();
                {
                    for (const auto& [key, value] : rightRowsMap) {
                        buffer.emplace_back(key, value);
                    }
                    std::sort(buffer.begin(), buffer.end());
                }

                for (const auto& it : buffer) {
                    rightRowsSource.push_back(Format("pk=%v;value=%v;", it.first, it.second));
                }
            }

            auto resultSplit = MakeSplit({
                {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"joined_data", ListLogicalType(StructLogicalType({
                    {"fk", "fk", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                    {"value", "value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                }, /*removedFieldStableNames*/ {}))},
            });

            auto resultMap = THashMap<i64, std::vector<std::pair<i64, std::optional<i64>>>>();
            for (const auto& [leftPk, leftFks] : leftRowsMap) {
                auto joinedRow = std::vector<std::pair<i64, std::optional<i64>>>();

                for (i64 leftFk : leftFks) {
                    auto it = rightRowsMap.find(leftFk);
                    if (it != rightRowsMap.end()) {
                        joinedRow.emplace_back(leftFk, it->second);
                    } else if (isLeftJoin) {
                        joinedRow.emplace_back(leftFk, std::nullopt);
                    }
                }

                resultMap[leftPk] = joinedRow;
            }

            auto resultSource = TSource();
            {
                auto buffer = std::vector<std::pair<i64, std::vector<std::pair<i64, std::optional<i64>>>>>();
                {
                    for (const auto& [key, value] : resultMap) {
                        buffer.emplace_back(key, value);
                    }
                    std::sort(buffer.begin(), buffer.end());
                }

                for (const auto& [pk, joinedRow] : buffer) {
                    auto resultRow = TStringBuilder();
                    resultRow.AppendFormat("pk=%v;joined_data=[", pk);
                    for (const auto& [fk, value] : joinedRow) {
                        if (value) {
                            resultRow.AppendFormat("[%v;%v];", fk, *value);
                        } else {
                            resultRow.AppendFormat("[%v;#];", fk);
                        }
                    }
                    resultRow.AppendString("];");
                    resultSource.push_back(resultRow.Flush());
                }
            }

            auto result = YsonToRows(resultSource, resultSplit);

            EvaluateOnlyViaNativeExecutionBackend(
                Format(
                    R"(
                        select l.pk as pk,
                            (select fk, r.value as value
                                from (l.fks as fk)
                                %v join `//right` as r on fk = r.pk
                            ) as joined_data
                        from `//left` as l
                    )",
                    isLeftJoin ? "left" : ""),
                splits,
                {leftRowsSource, rightRowsSource},
                ResultMatcher(result, resultSplit.TableSchema),
                {.SyntaxVersion = 2, .MaxJoinBatchSize = leftFksMaxLength / 4});
        }
    }
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinCompositeJoinKeyFilteredByOnClausePredicate)
{
    auto splits = TSplitMap();

    splits["//l"] = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"fks1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks3", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks4_fk1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks4_fk2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    auto lRows = std::vector<std::string>{
        "pk=1;fks1=[10;11];fks2=[];fks3=[10;30;50];fks4_fk1=[10;10];fks4_fk2=[10;30];",
        "pk=2;fks1=[20;21];fks2=[10;20;30];fks3=[20;40;60];",
    };

    splits["//r4"] = MakeSplit({
        {"pk1", EValueType::Int64, ESortOrder::Ascending},
        {"pk2", EValueType::Int64, ESortOrder::Ascending},
        {"value1", EValueType::Int64},
    });
    auto r4Rows = std::vector<std::string>{
        "pk1=10;pk2=10;value1=11",
        "pk1=10;pk2=20;value1=12",
        "pk1=10;pk2=30;value1=13",
    };

    auto resultSplit = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"sq4", ListLogicalType(StructLogicalType({
            {"a", "a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "pk=1;sq4=[[11;];]",
        "pk=2;sq4=[]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select
                l.pk as pk,
                (select r4.value1 as a
                    from (l.fks4_fk1 as fks4_fk1, l.fks4_fk2 as fks4_fk2)
                    join `//r4` as r4 on (fks4_fk1, fks4_fk2) = (r4.pk1, r4.pk2) and r4.value1 < 12
                ) as sq4
            from `//l` as l
        )",
        splits,
        {lRows, r4Rows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinMultipleSubqueriesWithVariousJoinConfigurations)
{
    auto splits = TSplitMap();

    splits["//l"] = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"fks1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks3", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks4_fk1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))}, // TODO(dtorilov): Add list of structs after YT-28212.
        {"fks4_fk2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    auto lRows = std::vector<std::string>{
        "pk=1;fks1=[10;11];fks2=[];fks3=[10;30;50];fks4_fk1=[10;10];fks4_fk2=[10;30];",
        "pk=2;fks1=[20;21];fks2=[10;20;30];fks3=[20;40;60];",
    };

    splits["//r1"] = MakeSplit({
        {"value1", EValueType::Int64},
        {"value2", EValueType::Int64},
    });
    auto r1Rows = std::vector<std::string>{
        "value1=10;value2=11",
        "value1=20;value2=22",
    };

    splits["//r2"] = MakeSplit({
        {"pk1", EValueType::Int64, ESortOrder::Ascending},
        {"value1", EValueType::Int64},
    });
    auto r2Rows = std::vector<std::string>{
        "pk1=10;value1=11",
        "pk1=20;value1=22",
        "pk1=30;value1=33",
    };

    splits["//r3"] = MakeSplit({
        {"pk1", EValueType::Int64, ESortOrder::Ascending},
        {"value1", EValueType::Int64},
    });
    auto r3Rows = std::vector<std::string>{
        "pk1=10;value1=11",
        "pk1=20;value1=22",
        "pk1=30;value1=33",
        "pk1=40;value1=44",
        "pk1=50;value1=55",
        "pk1=60;value1=66",
    };

    splits["//r4"] = MakeSplit({
        {"pk1", EValueType::Int64, ESortOrder::Ascending},
        {"pk2", EValueType::Int64, ESortOrder::Ascending},
        {"value1", EValueType::Int64},
    });
    auto r4Rows = std::vector<std::string>{
        "pk1=10;pk2=10;value1=11",
        "pk1=10;pk2=20;value1=12",
        "pk1=10;pk2=30;value1=13",
    };

    auto resultSplit = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"sq1", ListLogicalType(StructLogicalType({
            {"a", "a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
        {"sq2", ListLogicalType(StructLogicalType({
            {"a", "a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
        {"sq3", ListLogicalType(StructLogicalType({
            {"a", "a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
        {"sq4", ListLogicalType(StructLogicalType({
            {"a", "a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "pk=1;sq1=[[11;];];sq2=[];sq3=[[99;];];sq4=[[11;];]", // TODO(dtorilov): Toggle output format.
        "pk=2;sq1=[[22;]];sq2=[[11;];[22;];[33;];];sq3=[];sq4=[]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select
                l.pk as pk,
                (select r1.value2 as a
                    from (l.fks1 as fk1)
                    join `//r1` as r1 on fk1 = r1.value1
                    where fk1 % 2 = 0
                ) as sq1,
                (select r2.value1 as a
                    from (l.fks2 as fk2)
                    join `//r2` as r2 on fk2 = r2.pk1
                ) as sq2,
                (select sum(r2.value1) as a
                    from (l.fks3 as fk3)
                    join `//r3` as r2 on fk3 = r2.pk1
                    where r2.value1 % 2 != 0
                    group by 0
                ) as sq3,
                (select r4.value1 as a
                    from (l.fks4_fk1 as fks4_fk1, l.fks4_fk2 as fks4_fk2)
                    join `//r4` as r4 on (fks4_fk1, fks4_fk2) = (r4.pk1, r4.pk2) and r4.value1 < 12
                ) as sq4
            from `//l` as l
        )",
        splits,
        {lRows, r1Rows, r2Rows, r3Rows, r4Rows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinSubqueryWhereClauseReferencingOuterColumn)
{
    // TODO(dtorilov): Omit OptionalLogicalType after YT-28225.
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10]",
        "a=2;arr=[20]",
    };

    auto foreignRows = std::vector<std::string>{
        "x=10;c=100",
        "x=20;c=200",
    };

    auto resultSplit = MakeSplit({
        {"a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li", "li", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"s", "s", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;101;];]",
        "a=2;joined_data=[[20;202;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li, t.a + foreign_t.c as s
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                    where
                        (li + t.a > 0) and
                        (foreign_t.x + t.a > 0) and
                        (li + foreign_t.x + t.a > 0)
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinSelfSideJoinKeyReferencingOuterColumn)
{
    TSplitMap splits;
    splits["//t"] = MakeSplit({
        {"a", EValueType::Int64},
        {"arr", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
    });
    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[9]",
        "a=2;arr=[18]",
    };

    auto foreignRows = std::vector<std::string>{
        "x=10;c=100",
        "x=20;c=200",
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"c", "c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[100;];]",
        "a=2;joined_data=[[200;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select foreign_t.c as c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li + t.a = foreign_t.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinForeignOnlyWherePredicatePushedToFetch)
{
    auto splits = MakeSplitsWithListColumn();

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10;20;30]",
        "a=2;arr=[40;50]",
    };

    auto foreignRows = std::vector<std::string>{
        "x=10;c=100",
        "x=20;c=30",
        "x=30;c=200",
        "x=40;c=10",
        "x=50;c=300",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li", "li", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"foreign_t.c", "foreign_t.c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;100;];[30;200;];]",
        "a=2;joined_data=[[50;300;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                    where foreign_t.c > 50
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInnerJoinForeignWherePredicateExcludesNonMatchingRows)
{
    auto splits = MakeSplitsWithListColumn();

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10;20;30]",
        "a=2;arr=[40]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=5",
        "x=20;c=3",
        "x=40;c=5",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li", "li", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"foreign_t.c", "foreign_t.c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;5;];]",
        "a=2;joined_data=[[40;5;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    join `//foreign` as foreign_t on li = foreign_t.x
                    where foreign_t.c = 5
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinLeftJoinForeignWherePredicateFiltersNullRows)
{
    auto splits = MakeSplitsWithListColumn();

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10;20;30]",
        "a=2;arr=[40]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=5",
        "x=20;c=3",
        "x=40;c=5",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li", "li", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"foreign_t.c", "foreign_t.c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;5;];]",
        "a=2;joined_data=[[40;5;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li, foreign_t.c
                    from (t.arr as li)
                    left join `//foreign` as foreign_t on li = foreign_t.x
                    where foreign_t.c = 5
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinInnerJoinFiltersUnmatchedKeysWithNoForeignColumnsSelected)
{
    auto splits = MakeSplitsWithListColumn();

    auto outerRows = std::vector<std::string>{
        "a=1;arr=[10;20;30]",
        "a=2;arr=[40;50]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=100",
        "x=20;c=200",
        "x=40;c=400",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"fk", "fk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;];[20;];]",
        "a=2;joined_data=[[40;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select fk
                    from (t.arr as fk)
                    join `//foreign` as f on fk = f.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinTwoFromExpressionsProjectsForeignColumn)
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"arr1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr1=[10;20];arr2=[100;200]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=999",
        "x=20;c=888",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"fc", "fc", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[999;];[888;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select f.c as fc
                    from (t.arr1 as li1, t.arr2 as li2)
                    join `//foreign` as f on li1 = f.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinTwoFromExpressionsProjectsKeyAndForeignColumn)
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"arr1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr1=[10;20];arr2=[100;200]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=999",
        "x=20;c=888",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li1", "li1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"fc", "fc", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[10;999;];[20;888;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li1, f.c as fc
                    from (t.arr1 as li1, t.arr2 as li2)
                    join `//foreign` as f on li1 = f.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinTwoFromExpressionsProjectsNonKeyFromExpression)
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"arr1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr1=[10;20];arr2=[100;200]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=999",
        "x=20;c=888",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li2", "li2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"fc", "fc", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[100;999;];[200;888;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li2, f.c as fc
                    from (t.arr1 as li1, t.arr2 as li2)
                    join `//foreign` as f on li1 = f.x
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinTwoFromExpressionsFiltersOnNonKeyFromExpression)
{
    TSplitMap splits;

    splits["//t"] = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"arr1", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    splits["//foreign"] = MakeSplit({
        {"x", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64},
    });

    auto outerRows = std::vector<std::string>{
        "a=1;arr1=[10;20];arr2=[100;200]",
    };
    auto foreignRows = std::vector<std::string>{
        "x=10;c=999",
        "x=20;c=888",
    };

    auto resultSplit = MakeSplit({
        {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"joined_data", ListLogicalType(StructLogicalType({
            {"li2", "li2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            {"fc", "fc", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }, /*removedFieldStableNames*/ {}))},
    });

    auto result = YsonToRows({
        "a=1;joined_data=[[200;888;];]",
    }, resultSplit);

    EvaluateOnlyViaNativeExecutionBackend(
        R"(
            select t.a as a,
                (select li2, f.c as fc
                    from (t.arr1 as li1, t.arr2 as li2)
                    join `//foreign` as f on li1 = f.x
                    where li2 > 150
                ) as joined_data
            from `//t` as t
        )",
        splits,
        {outerRows, foreignRows},
        ResultMatcher(result, resultSplit.TableSchema),
        {.SyntaxVersion = 2});
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinSubqueryGroupByVariousKeyTypes)
{
    TSplitMap splits;

    splits["//l"] = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"fks", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks_miss", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fks_bound", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
        {"fk1s", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"fk2s", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"arr2", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    splits["//r_keyed"] = MakeSplit({
        {"x", EValueType::Int64, ESortOrder::Ascending},
        {"val", EValueType::Int64},
    });
    splits["//r_plain"] = MakeSplit({
        {"x", EValueType::Int64},
        {"category", EValueType::Int64},
        {"val", EValueType::Int64},
    });
    splits["//r_comp"] = MakeSplit({
        {"pk1", EValueType::Int64, ESortOrder::Ascending},
        {"pk2", EValueType::Int64, ESortOrder::Ascending},
        {"val", EValueType::Int64},
    });

    auto lRows = std::vector<std::string>{
        "pk=1;fks=[10;10];fks_miss=[10;99];fks_bound=[9;9];fk1s=[10;10];fk2s=[1;1];arr2=[100;100]",
        "pk=2;fks=[20;20];fks_miss=[99];fks_bound=[8;8;8];fk1s=[10;10];fk2s=[5;5];arr2=[200;200]",
    };
    auto rKeyedRows = std::vector<std::string>{
        "x=10;val=1",
        "x=20;val=2",
        "x=30;val=3",
    };
    auto rPlainRows = std::vector<std::string>{
        "x=10;category=1;val=100",
        "x=20;category=1;val=200",
        "x=30;category=2;val=300",
    };
    auto rCompRows = std::vector<std::string>{
        "pk1=10;pk2=1;val=100",
        "pk1=20;pk2=2;val=200",
    };

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[2;2;];]",
            "pk=2;result=[[4;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_keyed` as r on fk = r.x
                        group by 0
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[1;1;];]",
            "pk=2;result=[]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select sum(r.val) as total, sum(1) as count
                        from (l.fks_miss as fk)
                        join `//r_keyed` as r on fk = r.x
                        group by 0
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[]",
            "pk=2;result=[[4;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_keyed` as r on fk = r.x
                        where r.val > 1
                        group by 0
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[1;2;];]",
            "pk=2;result=[[#;1;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select sum(r.val) as total, sum(1) as count
                        from (l.fks_miss as fk)
                        left join `//r_keyed` as r on fk = r.x
                        group by 0
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"fk", "fk", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[10;2;2;];]",
            "pk=2;result=[[20;4;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select fk, sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_keyed` as r on fk = r.x
                        group by fk
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"cat", "cat", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[1;200;2;];]",
            "pk=2;result=[[1;400;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select r.category as cat, sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_plain` as r on fk = r.x
                        group by cat
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rPlainRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"fk", "fk", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"cat", "cat", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[10;1;200;2;];]",
            "pk=2;result=[[20;1;400;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select fk, r.category as cat, sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_plain` as r on fk = r.x
                        group by fk, cat
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rPlainRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"cat", "cat", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"rv", "rv", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[1;100;200;2;];]",
            "pk=2;result=[[1;200;400;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select r.category as cat, r.val as rv, sum(r.val) as total, sum(1) as count
                        from (l.fks as fk)
                        join `//r_plain` as r on fk = r.x
                        group by cat, rv
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rPlainRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"fk1", "fk1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"fk2", "fk2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[10;1;200;2;];]",
            "pk=2;result=[]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select fk1, fk2, sum(r.val) as total, sum(1) as count
                        from (l.fk1s as fk1, l.fk2s as fk2)
                        join `//r_comp` as r on (fk1, fk2) = (r.pk1, r.pk2)
                        group by fk1, fk2
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rCompRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"key", "key", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[10;2;2;];]",
            "pk=2;result=[[10;3;3;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select fk + l.pk as key, sum(r.val) as total, sum(1) as count
                        from (l.fks_bound as fk)
                        join `//r_keyed` as r on fk + l.pk = r.x
                        group by key
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"li2", "li2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[100;2;2;];]",
            "pk=2;result=[[200;4;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select li2, sum(r.val) as total, sum(1) as count
                        from (l.fks as li1, l.arr2 as li2)
                        join `//r_keyed` as r on li1 = r.x
                        group by li2
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }

    {
        auto resultSplit = MakeSplit({
            {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"result", ListLogicalType(StructLogicalType({
                {"li1", "li1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"li2", "li2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"total", "total", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
                {"count", "count", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
            }, /*removedFieldStableNames*/ {}))},
        });
        auto result = YsonToRows({
            "pk=1;result=[[10;100;2;2;];]",
            "pk=2;result=[[20;200;4;2;];]",
        }, resultSplit);
        EvaluateOnlyViaNativeExecutionBackend(
            R"(
                select l.pk as pk,
                    (select li1, li2, sum(r.val) as total, sum(1) as count
                        from (l.fks as li1, l.arr2 as li2)
                        join `//r_keyed` as r on li1 = r.x
                        group by li1, li2
                    ) as result
                from `//l` as l
            )",
            splits,
            {lRows, rKeyedRows},
            ResultMatcher(result, resultSplit.TableSchema),
            {.SyntaxVersion = 2});
    }
}

TEST_F(TQueryEvaluateTest, HierarchicalJoinGroupByQualifiedForeignColumnThrows)
{
    // TODO(dtorilov): This should not throw after YT-28493.
    TSplitMap splits;

    splits["//l"] = MakeSplit({
        {"pk", SimpleLogicalType(ESimpleLogicalValueType::Int64), ESortOrder::Ascending},
        {"fks", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });
    splits["//r"] = MakeSplit({
        {"x", EValueType::Int64},
        {"category", EValueType::Int64},
    });

    EXPECT_THROW_THAT(
        Prepare(
            R"(
                select l.pk as pk,
                    (select r.category, sum(1) as count
                        from (l.fks as fk)
                        join `//r` as r on fk = r.x
                        group by r.category
                    ) as result
                from `//l` as l
            )",
            splits,
            {},
            TPreparePlanFragmentOptions{.SyntaxVersion = 2, .BuilderVersion = DefaultExpressionBuilderVersion}),
        testing::HasSubstr("Expression or its parts are not in GROUP BY keys"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
