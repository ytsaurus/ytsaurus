#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/heap.h>

#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetRandomString(int len)
{
    TString str;
    for (int i = 0; i < len; ++i) {
        str.push_back('a' + rand() % 26);
    }
    return str;
}

TEST(THeapTest, MakeThenExtract)
{
    srand(0);

    std::vector<TString> words;
    for (int i = 0; i < 10000; ++i) {
        words.push_back(GetRandomString(10));
    }

    auto sorted = words;
    std::sort(sorted.begin(), sorted.end());

    NYT::MakeHeap(words.begin(), words.end(), std::greater<TString>());
    auto end = words.end();
    while (end != words.begin()) {
        NYT::ExtractHeap(words.begin(), end, std::greater<TString>());
        --end;
    }

    EXPECT_EQ(sorted, words);
}

TEST(THeapTest, MakeThenExtractMoveOnly)
{
    srand(0);

    std::vector<std::unique_ptr<TString>> words;
    for (int i = 0; i < 10000; ++i) {
        words.push_back(std::make_unique<TString>(GetRandomString(10)));
    }

    NYT::MakeHeap(words.begin(), words.end(), [] (const auto& lhs, const auto& rhs) { return *lhs > *rhs; });
    auto end = words.end();
    while (end != words.begin()) {
        NYT::ExtractHeap(words.begin(), end, [] (const auto& lhs, const auto& rhs) { return *lhs > *rhs; });
        --end;
    }

    EXPECT_TRUE(std::is_sorted(words.begin(), words.end(), [] (const auto& lhs, const auto& rhs) { return *lhs < *rhs; }));
}


TEST(THeapTest, InsertThenExtract)
{
    srand(0);

    std::vector<TString> words;
    for (int i = 0; i < 10000; ++i) {
        words.push_back(GetRandomString(10));
    }

    auto sorted = words;
    std::sort(sorted.begin(), sorted.end());

    for (auto it = words.begin(); it != words.end(); ++it) {
        NYT::AdjustHeapBack(words.begin(), it, std::greater<TString>());
    }

    auto end = words.end();
    while (end != words.begin()) {
        NYT::ExtractHeap(words.begin(), end, std::greater<TString>());
        --end;
    }

    EXPECT_EQ(sorted, words);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
