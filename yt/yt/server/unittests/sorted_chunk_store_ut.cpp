#include <yt/core/test_framework/framework.h>

#include <yt/server/tablet_node/sorted_chunk_store.h>

#include <yt/client/table_client/helpers.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadRange;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkStoreTestKeysFiltering
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        std::vector<TString>>>
{ };

TEST_P(TSortedChunkStoreTestKeysFiltering, Test)
{
    const auto& args = GetParam();
    TString lowerString = std::get<0>(args);
    TString upperString = std::get<1>(args);
    const auto& keyStrings = std::get<2>(args);

    TOwningKey lowerKey = lowerString.Empty() ? TOwningKey{} : YsonToKey(lowerString);
    TOwningKey upperKey = upperString.Empty() ? TOwningKey{} : YsonToKey(upperString);
    TRowRange readRange(lowerKey.Get(), upperKey.Get());

    TRowBufferPtr rowBuffer = New<TRowBuffer>();

    std::vector<TOwningKey> owningKeys;
    std::vector<TKey> keys;
    for (const auto& str : keyStrings) {
        owningKeys.push_back(YsonToKey(str));
        keys.push_back(owningKeys.back().Get());
    }
    auto keyRange = MakeSharedRange(keys);

    int actualSkippedBefore;
    int actualSkippedAfter;
    auto actualFiltered = FilterKeysByReadRange(readRange, keyRange, &actualSkippedBefore, &actualSkippedAfter);

    int expectedSkippedBefore = 0;
    int expectedSkippedAfter = 0;
    std::vector<TKey> expectedFiltered;

    for (const auto& key : keyRange) {
        if (key < lowerKey) {
            ++expectedSkippedBefore;
        } else if (key < upperKey) {
            expectedFiltered.push_back(key);
        } else {
            ++expectedSkippedAfter;
        }
    }

    EXPECT_EQ(expectedSkippedBefore, actualSkippedBefore);
    EXPECT_EQ(expectedSkippedAfter, actualSkippedAfter);
    EXPECT_TRUE(std::equal(
        expectedFiltered.begin(),
        expectedFiltered.end(),
        actualFiltered.begin(),
        actualFiltered.end()));
}

INSTANTIATE_TEST_SUITE_P(
    Test,
    TSortedChunkStoreTestKeysFiltering,
    ::testing::Values(
        std::make_tuple("0", "3", std::vector<TString>{"0", "1", "2"}),
        std::make_tuple("0", "2", std::vector<TString>{"0", "1", "2"}),
        std::make_tuple("1", "2", std::vector<TString>{"0", "1", "2"}),
        std::make_tuple("5", "5", std::vector<TString>{"1", "10"}),
        std::make_tuple("5", "5", std::vector<TString>{"1", "2"}),
        std::make_tuple("5", "5", std::vector<TString>{"2", "10"}),
        std::make_tuple("5", "5", std::vector<TString>{"4", "5", "6"}),
        std::make_tuple("1", "5", std::vector<TString>{"2", "4", "6"}),
        std::make_tuple("5", "8", std::vector<TString>{"2", "4", "6"}),
        std::make_tuple("5", "8", std::vector<TString>{"2", "4"}),
        std::make_tuple("5", "8", std::vector<TString>{"2", "4", "5"})
    )
);

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkStoreTestRangesFiltering
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        std::vector<std::pair<TString, TString>>>>
{ };

TEST_P(TSortedChunkStoreTestRangesFiltering, Test)
{
    const auto& args = GetParam();
    TString lowerString = std::get<0>(args);
    TString upperString = std::get<1>(args);
    const auto& keyStrings = std::get<2>(args);

    TOwningKey lowerKey = lowerString.Empty() ? TOwningKey{} : YsonToKey(lowerString);
    TOwningKey upperKey = upperString.Empty() ? TOwningKey{} : YsonToKey(upperString);
    TRowRange readRange(lowerKey.Get(), upperKey.Get());

    TRowBufferPtr rowBuffer = New<TRowBuffer>();

    std::vector<std::pair<TOwningKey, TOwningKey>> owningRanges;
    std::vector<TRowRange> ranges;
    for (const auto& pair : keyStrings) {
        owningRanges.emplace_back(YsonToKey(pair.first), YsonToKey(pair.second));
        ranges.emplace_back(owningRanges.back().first.Get(), owningRanges.back().second.Get());
    }
    auto rangeRange = MakeSharedRange(ranges);

    auto actualFiltered = FilterRowRangesByReadRange(readRange, rangeRange);

    std::vector<TRowRange> expectedFiltered;

    for (const auto& range : rangeRange) {
        const auto& lower = lowerKey ? std::max(lowerKey.Get(), range.first) : range.first;
        const auto& upper = upperKey ? std::min(upperKey.Get(), range.second) : range.second;
        if (lower < upper) {
            expectedFiltered.push_back(range);
        }
    }

    EXPECT_TRUE(std::equal(
        expectedFiltered.begin(),
        expectedFiltered.end(),
        actualFiltered.begin(),
        actualFiltered.end()));
}

INSTANTIATE_TEST_SUITE_P(
    Test,
    TSortedChunkStoreTestRangesFiltering,
    ::testing::Values(
        std::make_tuple("0", "3", std::vector<std::pair<TString, TString>>{
            {"0", "1"}, {"2", "3"}, {"4", "5"}}),
        std::make_tuple("0", "3", std::vector<std::pair<TString, TString>>{
            {"0", "1"}, {"3", "5"}}),
        std::make_tuple("5", "7", std::vector<std::pair<TString, TString>>{
            {"0", "3"}, {"8", "9"}}),
        std::make_tuple("5", "7", std::vector<std::pair<TString, TString>>{
            {"0", "3"}, {"7", "9"}}),
        std::make_tuple("5", "7", std::vector<std::pair<TString, TString>>{
            {"0", "5"}, {"6", "8"}, {"7", "9"}})
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT

