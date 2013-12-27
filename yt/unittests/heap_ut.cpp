#include "stdafx.h"
#include "framework.h"

#include <core/misc/heap.h>

#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class THeapTest
    : public ::testing::Test
{ };

static const int testSize = 10000;

Stroka GetRandomString(int len) {
    Stroka str;
    for (int i = 0; i < len; ++i) {
        str.push_back('a' + rand() % 26);
    }
    return str;
}

TEST(THeapTest, All)
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

    //for (int i = 0; i < testSize; ++i) {
    //    if (sorted[i] != words[i]) {
    //        std::cout << "FAIL " << sorted[i] << " " << words[i] << std::endl;
    //        break;
    //    }
    //    else {
    //        std::cout << "OK " << words[i] << std::endl;
    //    }
    //}

    EXPECT_EQ(sorted, words);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
