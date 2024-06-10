#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/node/tablet_node/sorted_chunk_store.h>

#include <yt/yt/client/table_client/helpers.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TLegacyReadRange;
using NChunkClient::TLegacyReadLimit;

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

    TLegacyOwningKey lowerKey = lowerString.Empty() ? TLegacyOwningKey{} : YsonToKey(lowerString);
    TLegacyOwningKey upperKey = upperString.Empty() ? TLegacyOwningKey{} : YsonToKey(upperString);
    TRowRange readRange(lowerKey.Get(), upperKey.Get());

    TRowBufferPtr rowBuffer = New<TRowBuffer>();

    std::vector<TLegacyOwningKey> owningKeys;
    std::vector<TLegacyKey> keys;
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
    std::vector<TLegacyKey> expectedFiltered;

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
        std::tuple("0", "3", std::vector<TString>{"0", "1", "2"}),
        std::tuple("0", "2", std::vector<TString>{"0", "1", "2"}),
        std::tuple("1", "2", std::vector<TString>{"0", "1", "2"}),
        std::tuple("5", "5", std::vector<TString>{"1", "10"}),
        std::tuple("5", "5", std::vector<TString>{"1", "2"}),
        std::tuple("5", "5", std::vector<TString>{"2", "10"}),
        std::tuple("5", "5", std::vector<TString>{"4", "5", "6"}),
        std::tuple("1", "5", std::vector<TString>{"2", "4", "6"}),
        std::tuple("5", "8", std::vector<TString>{"2", "4", "6"}),
        std::tuple("5", "8", std::vector<TString>{"2", "4"}),
        std::tuple("5", "8", std::vector<TString>{"2", "4", "5"}))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkServer
} // namespace NYT

