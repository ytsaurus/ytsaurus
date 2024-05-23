#include <yt/yt/ytlib/query_client/shuffle.h>

#include <yt/yt/library/query/unittests/ql_helpers.h>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TShuffleNavigator ConstructNavigatorFromPivots(std::vector<TOwningRow> pivots, int prefixHint)
{
    THROW_ERROR_EXCEPTION_IF(pivots.front() != MakeUnversionedOwningRow(),
        "The first pivot must be an empty row");

    TShuffleNavigator navigator{
        .PrefixHint=prefixHint,
    };
    navigator.DestinationMap.reserve(pivots.size());
    for (int index = 0; index < std::ssize(pivots); ++index) {
        EmplaceOrCrash(navigator.DestinationMap, Format("node-%v", index), MakeSharedRange(std::vector<TKeyRange>{
            {pivots[index],  index + 1 < std::ssize(pivots) ? pivots[index + 1] : MaxKey()}
        }));
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
    }, /*prefixHint*/ 1);

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows);

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
    }, /*prefixHint*/ 2);

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i, 0));
        owningRows.push_back(MakeUnversionedOwningRow(i, 10));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows);

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
    }, /*prefixHint*/ 1);

    std::vector<TOwningRow> owningRows;
    for (int i = 0; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows);

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
    auto rowBuffer = New<TRowBuffer>();
    TShuffleNavigator navigator = {
        .DestinationMap={
            {"node-0", MakeSharedRange(std::vector<TKeyRange>{
                {MakeUnversionedOwningRow(), MakeUnversionedOwningRow(1, 100)},
                {MakeUnversionedOwningRow(2, 200), MakeUnversionedOwningRow(3, 300)},
            })},
            {"node-1", MakeSharedRange(std::vector<TKeyRange>{
                {MakeUnversionedOwningRow(3, 300), MaxKey()},
                {MakeUnversionedOwningRow(1, 100), MakeUnversionedOwningRow(2, 200)},
            })},
        },
        .PrefixHint=2,
    };

    std::vector<TOwningRow> owningRows;
    for (int i = 1; i < 4; ++i) {
        owningRows.push_back(MakeUnversionedOwningRow(i, 0));
        owningRows.push_back(MakeUnversionedOwningRow(i, 100 * i));
    }

    std::vector<TRow> rows(owningRows.begin(), owningRows.end());

    auto shuffle = Shuffle(navigator, rows);

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

} // namespace
} // namespace NYT::NQueryClient
