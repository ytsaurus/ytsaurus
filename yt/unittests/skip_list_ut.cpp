#include "stdafx.h"
#include "framework.h"

#include <core/misc/skip_list.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TComparer
{
    int operator() (int lhs, int rhs) const
    {
        if (lhs < rhs) {
            return -1;
        }
        if (lhs > rhs) {
            return +1;
        }
        return 0;
    }
};

class TSkipListTest
    : public ::testing::Test
{
public:
    TChunkedMemoryPool Pool;
    TComparer Comparer;
    TSkipList<int, TComparer> List;
    
    TSkipListTest()
        : List(&Pool, &Comparer)
    { }

};

TEST_F(TSkipListTest, Empty)
{
    ASSERT_EQ(List.Size(), 0);

    ASSERT_FALSE(List.FindEqualTo(1).IsValid());

    ASSERT_FALSE(List.FindGreaterThanOrEqualTo(1).IsValid());
}

TEST_F(TSkipListTest, Singleton)
{
    ASSERT_TRUE(List.Insert(0));
    ASSERT_EQ(List.Size(), 1);

    ASSERT_FALSE(List.FindEqualTo(1).IsValid());

    ASSERT_FALSE(List.FindGreaterThanOrEqualTo(1).IsValid());

    {
        auto it = List.FindGreaterThanOrEqualTo(-1);
        ASSERT_TRUE(it.IsValid());
        ASSERT_EQ(it.GetCurrent(), 0);
        it.MoveNext();
        ASSERT_FALSE(it.IsValid());
    }

    {
        auto it = List.FindGreaterThanOrEqualTo(0);
        ASSERT_TRUE(it.IsValid());
        ASSERT_EQ(it.GetCurrent(), 0);
        it.MoveNext();
        ASSERT_FALSE(it.IsValid());
    }
}

TEST_F(TSkipListTest, 1to10)
{
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(List.Insert(i));
    }
    ASSERT_EQ(List.Size(), 10);

    for (int i = 0; i < 10; ++i) {
        auto it = List.FindGreaterThanOrEqualTo(i);
        for (int j = i; j < 10; ++j) {
            ASSERT_TRUE(it.IsValid());
            ASSERT_EQ(it.GetCurrent(), j);
            it.MoveNext();
        }
        ASSERT_FALSE(it.IsValid());
    }

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(List.FindEqualTo(i).IsValid());
    }

    ASSERT_FALSE(List.FindEqualTo(-1).IsValid());
    ASSERT_FALSE(List.FindEqualTo(11).IsValid());
}

TEST_F(TSkipListTest, Random1000000)
{
    srand(42);
    std::set<int> set;
    for (int i = 0; i < 1000000; ++i) {
        int value = rand();
        ASSERT_EQ(List.Insert(value), set.insert(value).second);
    }
    ASSERT_EQ(List.Size(), set.size());

    for (int value : set) {
        ASSERT_TRUE(List.FindEqualTo(value).IsValid());
    }

    auto it = List.FindGreaterThanOrEqualTo(*set.begin());
    for (int value : set) {
        ASSERT_TRUE(it.IsValid());
        ASSERT_EQ(it.GetCurrent(), value);
        it.MoveNext();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
