#include <yt/yt/library/query/unittests/evaluate/test_evaluate.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPushDownGroupByTest
    : public TQueryEvaluateTest
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPushDownGroupByTest, Prepare)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//left"))
        .WillRepeatedly(Return(MakeFuture(MakeSplit({
            {"key_0", EValueType::Int64, ESortOrder::Ascending},
            {"value_0", EValueType::Int64},
        }))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//center"))
        .WillRepeatedly(Return(MakeFuture(MakeSplit({
            {"key_0", EValueType::Int64, ESortOrder::Ascending},
            {"key_1", EValueType::Int64, ESortOrder::Ascending},
            {"value_1", EValueType::Int64},
        }))));

    auto fmt = [] (const TNamedItemList& items) {
        auto namedItemFormatter = [&] (TStringBuilderBase* builder, const TNamedItem& item) {
            builder->AppendString(InferName(item.Expression));
        };
        return JoinToString(items, namedItemFormatter);
    };

    {
        auto query = TStringBuf(R"(select sum(C.value_1) from [//left] L
            left join [//center] C with hint "{push_down_group_by=%true}"
            on (L.key_0) = (C.key_0)
            group by C.key_0)");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        ASSERT_TRUE(plan->Query->JoinClauses[0]->GroupClause);
        EXPECT_EQ(fmt(plan->Query->JoinClauses[0]->GroupClause->GroupItems), TStringBuf("`C.key_0`"));
        EXPECT_EQ(plan->Query->JoinClauses[0]->GetForeignColumnIndices(), (std::vector<size_t>{0, 1}));
    }
    {
        auto query = TStringBuf(R"(select sum(C.value_1) from [//left] L
            left join [//center] C with hint "{push_down_group_by=%true}"
            on (L.key_0) = (C.key_0)
            group by L.key_0)");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        ASSERT_TRUE(plan->Query->JoinClauses[0]->GroupClause);
        EXPECT_EQ(fmt(plan->Query->JoinClauses[0]->GroupClause->GroupItems), TStringBuf("`C.key_0`"));
        EXPECT_EQ(plan->Query->JoinClauses[0]->GetForeignColumnIndices(), (std::vector<size_t>{1}));
    }
    {
        auto query = TStringBuf(R"(
            select min(L.value_0 + C.key_1 * 10 + C.value_1), sum(C.value_1)
            from [//left] L
            left join [//center] C with hint "{push_down_group_by=%true}"
            on L.key_0 = C.value_1
            group by L.key_0
        )");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        ASSERT_TRUE(plan->Query->JoinClauses[0]->GroupClause);
        EXPECT_EQ(plan->Query->JoinClauses[0]->GetForeignColumnIndices(), (std::vector<size_t>{1, 0, 2}));
    }
    {
        auto query = TStringBuf(R"(select sum(value_1) from [//left]
            left join [//center] with hint "{push_down_group_by=%true}"
            using key_0
            group by key_0)");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        ASSERT_TRUE(plan->Query->JoinClauses[0]->GroupClause);
        EXPECT_EQ(fmt(plan->Query->JoinClauses[0]->GroupClause->GroupItems), TStringBuf("key_0"));
    }
    {
        auto query = TStringBuf(R"(select sum(value_1) from [//left]
            left join [//center] with hint "{push_down_group_by=%true}"
            on 1 = 1
            group by value_0 + key_1)");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        ASSERT_TRUE(plan->Query->JoinClauses[0]->GroupClause);
        EXPECT_EQ(fmt(plan->Query->JoinClauses[0]->GroupClause->GroupItems), TStringBuf("key_1"));
    }
    {
        auto query = TStringBuf(R"(select sum(value_1 * value_0) from [//left]
            left join [//center] with hint "{push_down_group_by=%true}"
            on 1 = 1
            group by value_0 % 10)");

        EXPECT_THROW_THAT(
            ParseAndPreparePlanFragment(&PrepareMock_, query),
            HasSubstr("neither idempotent nor can be pushed behind join clause"));
    }
    {
        auto query = TStringBuf(R"(select sum(value_1) from [//left]
            left join [//center] with hint "{push_down_group_by=%true}"
            using key_0
            group by value_0 + key_1)");

        auto plan = ParseAndPreparePlanFragment(&PrepareMock_, query);

        EXPECT_FALSE(plan->Query->JoinClauses[0]->GroupClause);
    }
    {
        auto query = TStringBuf(R"(select sum(1) from [//left]
            left join [//center] with hint "{push_down_group_by=%true}"
            on 1 = 1
            left join [//center]
            on 2 = 2
            group by 3)");

        EXPECT_THROW_THAT(
            ParseAndPreparePlanFragment(&PrepareMock_, query),
            HasSubstr("is not supported at the current moment"));
    }
}

TEST_F(TPushDownGroupByTest, PreservedColumnOrder)
{
    TSplitMap splits;
    splits["//left"] = MakeSplit({
        {"id", EValueType::Int64},
    });
    splits["//right"] = MakeSplit({
        {"key", EValueType::Int64, ESortOrder::Ascending},
        {"unused_key_to_keep_pushdown", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64},
        {"a", EValueType::Int64},
    });
    std::vector<TSource> sources = {
        {"id=100"},
        {"key=100;unused_key_to_keep_pushdown=1;a=2;b=3"},
    };

    constexpr TStringBuf queryTemplate = R"(
        SELECT min(id + a * 10 + b) AS value
        FROM [//left]
        JOIN [//right] %v ON id = key
        GROUP BY id
    )";
    std::vector<TOwningRow> resultWithoutPushDown;
    Evaluate(
        Format(queryTemplate, ""),
        splits,
        sources,
        [&] (TRange<TRow> rows, const TTableSchema& /*schema*/) {
            resultWithoutPushDown = std::vector<TOwningRow>(rows.begin(), rows.end());
        });
    ASSERT_EQ(resultWithoutPushDown.size(), 1u);

    auto query = Evaluate(
        Format(queryTemplate, R"(WITH HINT "{push_down_group_by=%true}")"),
        splits,
        sources,
        ResultMatcher(resultWithoutPushDown));
    ASSERT_EQ(query->JoinClauses.size(), 1u);
    EXPECT_TRUE(query->JoinClauses[0]->GroupClause);
}

TEST_F(TPushDownGroupByTest, JoinKeyColumnOrder)
{
    TSplitMap splits;
    splits["//left"] = MakeSplit({
        {"id", EValueType::Int64},
    });
    splits["//right"] = MakeSplit({
        {"unused_key_to_keep_pushdown", EValueType::Int64, ESortOrder::Ascending},
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
    });
    std::vector<TSource> sources = {
        {"id=3", "id=5"},
        {
            "unused_key_to_keep_pushdown=1;a=2;b=3",
            "unused_key_to_keep_pushdown=2;a=2;b=3",
            "unused_key_to_keep_pushdown=3;a=4;b=3",
            "unused_key_to_keep_pushdown=4;a=7;b=5",
        },
    };

    constexpr TStringBuf queryTemplate = R"(
        SELECT min(id + a * 10 + b) AS value, sum(b) AS total
        FROM [//left]
        JOIN [//right] %v ON id = b
        GROUP BY id
    )";
    auto resultSplit = MakeSplit({
        {"value", EValueType::Int64},
        {"total", EValueType::Int64},
    });
    auto result = YsonToRows({"value=26;total=9", "value=80;total=5"}, resultSplit);
    Evaluate(
        Format(queryTemplate, ""),
        splits,
        sources,
        ResultMatcher(result));

    auto pushedQuery = Format(queryTemplate, R"(WITH HINT "{push_down_group_by=%true}")");
    auto query = Prepare(pushedQuery, splits, /*placeholderValues*/ {}, {.BuilderVersion = DefaultExpressionBuilderVersion});
    ASSERT_EQ(query->JoinClauses.size(), 1u);
    const auto& join = query->JoinClauses[0];
    ASSERT_TRUE(join->GroupClause);
    ASSERT_EQ(join->GroupClause->GroupItems.size(), 2u);
    EXPECT_EQ(InferName(join->GroupClause->GroupItems[0].Expression), "b");
    EXPECT_EQ(InferName(join->GroupClause->GroupItems[1].Expression), "a");
    // The join key comes first in the grouped rows, but follows a in the join schema.
    ASSERT_EQ(join->GetForeignColumnIndices(), (std::vector<size_t>{1, 0, 2}));

    Evaluate(pushedQuery, splits, sources, ResultMatcher(result));
}

TEST_F(TPushDownGroupByTest, AliasedJoinKey)
{
    TSplitMap splits;
    splits["//left"] = MakeSplit({
        {"id", EValueType::Int64},
    });
    splits["//right"] = MakeSplit({
        {"unused_key_to_keep_pushdown", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64},
    });
    std::vector<TSource> sources = {
        {"id=3"},
        {"unused_key_to_keep_pushdown=1;b=3"},
    };

    constexpr TStringBuf queryTemplate = R"(
        SELECT min(l.id + f.b) AS value
        FROM [//left] AS l
        JOIN [//right] AS f %v ON l.id = f.b
        GROUP BY l.id
    )";
    std::vector<TOwningRow> resultWithoutPushDown;
    Evaluate(
        Format(queryTemplate, ""),
        splits,
        sources,
        [&] (TRange<TRow> rows, const TTableSchema& /*schema*/) {
            resultWithoutPushDown = std::vector<TOwningRow>(rows.begin(), rows.end());
        });
    ASSERT_EQ(resultWithoutPushDown.size(), 1u);

    auto pushedQuery = Format(queryTemplate, R"(WITH HINT "{push_down_group_by=%true}")");
    auto preparedQuery = Prepare(pushedQuery, splits, /*placeholderValues*/ {}, {.BuilderVersion = DefaultExpressionBuilderVersion});
    ASSERT_EQ(preparedQuery->JoinClauses.size(), 1u);
    const auto& join = preparedQuery->JoinClauses[0];
    ASSERT_TRUE(join->GroupClause);
    ASSERT_EQ(join->GetForeignColumnIndices(), (std::vector<size_t>{0}));

    Evaluate(pushedQuery, splits, sources, ResultMatcher(resultWithoutPushDown));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
