#include "stdafx.h"
#include "framework.h"

#include <core/misc/heap.h>

#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

Stroka GetRandomString(int len)
{
    Stroka str;
    for (int i = 0; i < len; ++i) {
        str.push_back('a' + rand() % 26);
    }
    return str;
}

TEST(THeapTest, MakeThenExtract)
{
    srand(0);

    std::vector<Stroka> words;
    for (int i = 0; i < 10000; ++i) {
        words.push_back(GetRandomString(10));
    }

    auto sorted = words;
    sort(sorted.begin(), sorted.end());

    NYT::MakeHeap(words.begin(), words.end(), std::greater<Stroka>());
    auto end = words.end();
    while (end != words.begin()) {
        NYT::ExtractHeap(words.begin(), end, std::greater<Stroka>());
        --end;
    }

    EXPECT_EQ(sorted, words);
}

TEST(THeapTest, InsertThenExtract)
{
    srand(0);

    std::vector<Stroka> words;
    for (int i = 0; i < 10000; ++i) {
        words.push_back(GetRandomString(10));
    }

    auto sorted = words;
    sort(sorted.begin(), sorted.end());

    for (auto it = words.begin(); it != words.end(); ++it) {
        NYT::AdjustHeapBack(words.begin(), it, std::greater<Stroka>());
    }

    auto end = words.end();
    while (end != words.begin()) {
        NYT::ExtractHeap(words.begin(), end, std::greater<Stroka>());
        --end;
    }

    EXPECT_EQ(sorted, words);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
