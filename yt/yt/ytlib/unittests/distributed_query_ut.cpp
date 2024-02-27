#include <yt/yt/ytlib/query_client/join_tree.h>
#include <yt/yt/ytlib/query_client/shuffle.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/unittests/ql_helpers.h>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

TShuffleNavigator ConstructNavigatorFromPivots(std::vector<TUnversionedOwningRow> pivots)
{
    THROW_ERROR_EXCEPTION_IF(pivots.front() != MakeUnversionedOwningRow(),
        "The first pivot must be an empty row");

    TShuffleNavigator navigator;
    navigator.reserve(pivots.size());
    for (int index = 0; index < std::ssize(pivots); ++index) {
        navigator[Format("node-%v", index)] = MakeSharedRange(std::vector<TKeyRange>{
            {pivots[index],  index + 1 < std::ssize(pivots) ? pivots[index + 1] : MaxKey()}
        });
    }

    return navigator;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TShuffleTest, ExactKey)
{
    auto navigator = ConstructNavigatorFromPivots({
        MakeUnversionedOwningRow(),
        MakeUnversionedOwningRow(1),
        MakeUnversionedOwningRow(2),
        MakeUnversionedOwningRow(3),
    });

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows, 1);

    ASSERT_EQ(shuffle.size(), 4ul);
    for (int i = 0; i < 4; ++i) {
        auto& part = shuffle.at(Format("node-%v", i));
        ASSERT_EQ(part.Subranges.size(), 1ul);
        auto& subrange = part.Subranges[0];
        ASSERT_EQ(subrange.Size(), 1ul);
        EXPECT_EQ(subrange[0], rows[i]);
    }
}

TEST(TShuffleTest, BigKey)
{
    auto navigator = ConstructNavigatorFromPivots({
        MakeUnversionedOwningRow(),
        MakeUnversionedOwningRow(1),
        MakeUnversionedOwningRow(2),
        MakeUnversionedOwningRow(3),
    });

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i, 0));
        owningRows.push_back(MakeUnversionedOwningRow(i, 10));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows, 2);

    ASSERT_EQ(shuffle.size(), 4ul);
    for (int i = 0; i < 4; ++i) {
        auto& part = shuffle.at(Format("node-%v", i));
        ASSERT_EQ(part.Subranges.size(), 1ul);
        auto& subrange = part.Subranges[0];
        ASSERT_EQ(subrange.Size(), 2ul);
        EXPECT_EQ(subrange[0], rows[2 * i]);
        EXPECT_EQ(subrange[1], rows[2 * i + 1]);
    }
}

TEST(TShuffleTest, SmallKey)
{
    auto navigator = ConstructNavigatorFromPivots({
        MakeUnversionedOwningRow(),
        MakeUnversionedOwningRow(0, 100),
        MakeUnversionedOwningRow(1, 0),
        MakeUnversionedOwningRow(1, 100),
        MakeUnversionedOwningRow(2, 0),
        MakeUnversionedOwningRow(2, 100),
        MakeUnversionedOwningRow(3, 0),
        MakeUnversionedOwningRow(3, 100),
    });

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows, 1);

    ASSERT_EQ(shuffle.size(), 8ul);
    for (int i = 0; i < 8; ++i) {
        auto& part = shuffle.at(Format("node-%v", i));
        ASSERT_EQ(part.Subranges.size(), 1ul);
        auto& subrange = part.Subranges[0];
        if (i % 2 == 0 || i == 7) {
            ASSERT_EQ(subrange.Size(), 1ul);
            EXPECT_EQ(subrange[0], rows[i / 2]);
        } else {
            ASSERT_EQ(subrange.Size(), 2ul);
            EXPECT_EQ(subrange[0], rows[i / 2]);
            EXPECT_EQ(subrange[1], rows[i / 2 + 1]);
        }
    }
}

TEST(TShuffleTest, MultirangeDestination)
{
    TShuffleNavigator navigator = {
        {"node-0", MakeSharedRange(std::vector<TKeyRange>{
            {MakeUnversionedOwningRow(), MakeUnversionedOwningRow(1, 100)},
            {MakeUnversionedOwningRow(2, 200), MakeUnversionedOwningRow(3, 300)},
        })},
        {"node-1", MakeSharedRange(std::vector<TKeyRange>{
            {MakeUnversionedOwningRow(3, 300), MaxKey()},
            {MakeUnversionedOwningRow(1, 100), MakeUnversionedOwningRow(2, 200)},
        })},
    };

    std::vector<TOwningRow> owningRows;
    for (int i = 1; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i, 0));
        owningRows.push_back(MakeUnversionedOwningRow(i, 100 * i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows, 2);

    ASSERT_EQ(shuffle.size(), 2ul);
    for (int i = 0; i < 2; ++i) {
        auto& part = shuffle.at(Format("node-%v", i));
        ASSERT_EQ(part.Subranges.size(), 2ul);
        auto& smallSubrange = part.Subranges[0].Size() < part.Subranges[1].Size()
            ? part.Subranges[0]
            : part.Subranges[1];
        auto& bigSubrange = part.Subranges[0].Size() < part.Subranges[1].Size()
            ? part.Subranges[1]
            : part.Subranges[0];
        ASSERT_EQ(smallSubrange.Size(), 1ul);
        ASSERT_EQ(bigSubrange.Size(), 2ul);
        EXPECT_EQ(smallSubrange[0], i == 0 ? rows[0] : rows[5]);
        EXPECT_EQ(bigSubrange[0], i == 0 ? rows[3] : rows[1]);
        EXPECT_EQ(bigSubrange[1], i == 0 ? rows[4] : rows[2]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns)
{
    TDataSplit dataSplit;
    dataSplit.ObjectId = TGuid::Create();
    dataSplit.TableSchema = New<TTableSchema>(columns);
    return dataSplit;
}

class TQueryPrepareJoinTreeTest
    : public ::testing::Test
{
protected:
    TQueryPtr Prepare(const TString& query, const std::map<TString, TDataSplit>& dataSplits)
    {
        for (const auto& [name, split] : dataSplits) {
            EXPECT_CALL(PrepareMock_, GetInitialSplit(name))
                .WillOnce(Return(MakeFuture(split)));
        }

        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            query,
            DefaultFetchFunctions,
            {});

        return fragment->Query;
    }

    bool HasProjection(const TNamedItemList& items, const char name[])
    {
        auto it = std::find_if(
            items.begin(),
            items.end(),
            [&] (const TNamedItem& item) {
                return item.Name == name;
            });
        return it != items.end();
    }

private:
    StrictMock<TPrepareCallbacksMock> PrepareMock_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryPrepareJoinTreeTest, IndexJoinIsLeftSkewed)
{
    auto queryString =
        "SELECT * FROM [//Alpha]"
        " JOIN [//Beta] USING b JOIN [//Gamma] USING c JOIN [//Delta] USING d";
    auto primary = "//Alpha";

    std::map<TString, TDataSplit> splits;
    splits["//Alpha"] = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64},
    });
    splits["//Beta"] = MakeSplit({
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Boolean},
    });
    splits["//Gamma"] = MakeSplit({
        {"c", EValueType::Boolean, ESortOrder::Ascending},
        {"d", EValueType::String},
    });
    splits["//Delta"] = MakeSplit({
        {"d", EValueType::String, ESortOrder::Ascending},
        {"e", EValueType::Uint64},
    });

    auto originalQuery = Prepare(queryString, splits);

    auto joinTree = MakeIndexJoinTree(
        originalQuery,
        {.ObjectId = splits.at(primary).ObjectId});

    auto joinNode = joinTree->GetRoot();

    for (const auto* name : {"//Delta", "//Gamma", "//Beta"}) {
        auto children = joinNode->GetChildren();
        ASSERT_TRUE(children.first && children.second);
        EXPECT_EQ(children.second->GetTableId(), splits[name].ObjectId);
        joinNode = children.first;
    }
    EXPECT_EQ(joinNode->GetTableId(), splits[primary].ObjectId);
}

TEST_F(TQueryPrepareJoinTreeTest, IndexJoinCorrectProjectionAndOrder)
{
    auto queryString =
        "SELECT a4 FROM [//Alpha] "
        "JOIN [//Beta] ON (a1 - 1, a1 + 1, a1 * a1, a2 * 123) = (b1, b2, b3, b4)";
    auto primary = "//Alpha";

    std::map<TString, TDataSplit> splits;
    splits["//Alpha"] = MakeSplit({
        {"a1", EValueType::Int64, ESortOrder::Ascending},
        {"a2", EValueType::Int64},
        {"a3", EValueType::Int64},
        {"a4", EValueType::Int64},
    });

    splits["//Beta"] = MakeSplit({
        {"b1", EValueType::Int64, ESortOrder::Ascending},
        {"b2", EValueType::Int64, ESortOrder::Ascending},
        {"b3", EValueType::Int64, ESortOrder::Ascending},
        {"b4", EValueType::Int64},
    });

    auto primalQuery = Prepare(queryString, splits);

    auto joinTree = MakeIndexJoinTree(
        primalQuery,
        {.ObjectId = splits.at(primary).ObjectId});

    auto betaQuery = joinTree->GetRoot()->GetQuery();
    auto betaProjections = betaQuery->ProjectClause->Projections;

    auto alphaQuery = joinTree->GetRoot()->GetChildren().first->GetQuery();
    auto alphaProjections = alphaQuery->ProjectClause->Projections;
    auto alphaOrders = alphaQuery->OrderClause->OrderItems;

    ASSERT_EQ(alphaProjections.size(), 5ul);
    EXPECT_EQ(alphaOrders.size(), 3ul);

    int index = 0;
    for (TString col : {"a1 - 0#1", "a1 + 0#1", "a1 * a1"}) {
        EXPECT_EQ(InferName(alphaOrders[index].Expression), col);
        EXPECT_EQ(InferName(alphaProjections[index].Expression), col);
        index++;
    }
    EXPECT_TRUE(HasProjection(alphaProjections, "a2"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a4"));

    EXPECT_EQ(betaQuery->JoinClauses.front()->CommonKeyPrefix, 3ul);
    EXPECT_EQ(betaProjections.size(), 1ul);
    EXPECT_EQ(betaProjections.front().Name, "a4");
}

TEST_F(TQueryPrepareJoinTreeTest, IndexJoinGroup)
{
    auto queryString =
        "SELECT sum(a3 - b2), min(b3 * a2) FROM [//Alpha] "
        "JOIN [//Beta] on a6 - 1 = b1 "
        "GROUP BY a1 HAVING sum(a4 + a5) > 0 "
        "ORDER BY max(a8 * b5) "
        "LIMIT 1";
    auto primary = "//Alpha";

    std::map<TString, TDataSplit> splits;
    splits["//Alpha"] = MakeSplit({
        {"a1", EValueType::Int64, ESortOrder::Ascending},
        {"a2", EValueType::Int64},
        {"a3", EValueType::Int64},
        {"a4", EValueType::Int64},
        {"a5", EValueType::Int64},
        {"a6", EValueType::Int64},
        {"a7", EValueType::Int64},
        {"a8", EValueType::Int64},
    });

    splits["//Beta"] = MakeSplit({
        {"b1", EValueType::Int64, ESortOrder::Ascending},
        {"b2", EValueType::Int64},
        {"b3", EValueType::Int64},
        {"b4", EValueType::Int64},
        {"b5", EValueType::Int64},
    });

    auto primalQuery = Prepare(queryString, splits);

    auto joinTree = MakeIndexJoinTree(
        primalQuery,
        {.ObjectId = splits.at(primary).ObjectId});
    auto root = joinTree->GetRoot();
    auto betaQuery = root->GetQuery();
    auto alphaQuery = root->GetChildren().first->GetQuery();
    auto alphaProjections = alphaQuery->ProjectClause->Projections;

    ASSERT_EQ(alphaProjections.size(), 7ul);

    auto* firstProjection = alphaProjections[0].Expression->As<TBinaryOpExpression>();
    EXPECT_TRUE(firstProjection);
    EXPECT_EQ(firstProjection->Opcode, EBinaryOp::Minus);
    EXPECT_TRUE(HasProjection(alphaProjections, "a1"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a2"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a3"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a4"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a5"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a8"));

    EXPECT_FALSE(betaQuery->ProjectClause);
    EXPECT_EQ(betaQuery->GroupClause->CommonPrefixWithPrimaryKey, 0ul);
    EXPECT_FALSE(betaQuery->UseDisjointGroupBy);
}

TEST_F(TQueryPrepareJoinTreeTest, IndexJoinEvaluatedColumns)
{
    auto queryString =
        "SELECT a2 + b2_squared FROM [//Alpha] "
        "JOIN [//Beta] on (a3, a2, a1) = (b1, b2, b2_squared)";
    auto primary = "//Alpha";

    std::map<TString, TDataSplit> splits;
    splits["//Alpha"] = MakeSplit({
        {"a1", EValueType::Int64, ESortOrder::Ascending},
        {"a2", EValueType::Int64},
        TColumnSchema{"a3", EValueType::Int64}.SetExpression("a1 + a2"),
    });

    splits["//Beta"] = MakeSplit({
        TColumnSchema{"hash", EValueType::Int64}
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression("farm_hash(b1)"),
        {"b1", EValueType::Int64, ESortOrder::Ascending},
        {"b2", EValueType::Int64},
        TColumnSchema{"b2_squared", EValueType::Int64}.SetExpression("b2 * b2"),
    });

    auto primalQuery = Prepare(queryString, splits);

    auto joinTree = MakeIndexJoinTree(
        primalQuery,
        {.ObjectId = splits.at(primary).ObjectId});
    auto root = joinTree->GetRoot();
    auto betaQuery = root->GetQuery();
    auto alphaQuery = root->GetChildren().first->GetQuery();
    auto alphaProjections = alphaQuery->ProjectClause->Projections;

    ASSERT_EQ(alphaProjections.size(), 4ul);

    EXPECT_EQ(alphaProjections[0].Name, "farm_hash(a3)");
    EXPECT_EQ(alphaProjections[1].Name, "a3");
    EXPECT_TRUE(HasProjection(alphaProjections, "a1"));
    EXPECT_TRUE(HasProjection(alphaProjections, "a2"));

    for (const auto& [_, evaluated] : betaQuery->JoinClauses.front()->SelfEquations) {
        EXPECT_FALSE(evaluated);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
