#include "stdafx.h"

#include <ytlib/misc/heap.h>

#include <contrib/testing/framework.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class THeapTest
    : public ::testing::Test
{
};
    
static const int testSize = 10000;

Stroka getRandomString(int len) {
    Stroka str;
    for (int i = 0; i < len; ++i) {
        str.push_back('a' + rand() % 26);
    }
    return str;
}

void print(const std::vector<Stroka>& words) {
    for (int i = 0; i < testSize; ++i) {
        std::cout << words[i] << "\t";
    }
    std::cout << std::endl;
}

TEST_F(THeapTest, All)
{
    srand(0);

    std::vector<Stroka> words;
    for (int i = 0; i < testSize; ++i) {
        words.push_back(getRandomString(10));
    }

    auto sorted = words;
    sort(sorted.begin(), sorted.end());

    MakeHeap(words.begin(), words.end(), std::greater<Stroka>());
    auto end = words.end();
    while (end != words.begin()) {
        ExtractHeap(words.begin(), end, std::greater<Stroka>());
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

} // namespace NYT
