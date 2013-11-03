#include "stdafx.h"

#include <core/misc/rcu_tree.h>

#include <contrib/testing/framework.h>

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

TEST(TRcuTreeTest, Empty)
{
    TChunkedMemoryPool pool;
    TComparer comparer;
    TRcuTree<int, TComparer> tree(&pool, &comparer);

    ASSERT_EQ(tree.Size(), 0);

    auto* reader = tree.CreateReader();
    
    ASSERT_FALSE(reader->Find(1));

    reader->BeginScan(1);
    ASSERT_FALSE(reader->IsValid());
    reader->EndScan();
}

TEST(TRcuTreeTest, Singleton)
{
    TChunkedMemoryPool pool;
    TComparer comparer;
    TRcuTree<int, TComparer> tree(&pool, &comparer);

    ASSERT_TRUE(tree.Insert(0));
    ASSERT_EQ(tree.Size(), 1);

    auto* reader = tree.CreateReader();

    ASSERT_FALSE(reader->Find(1));

    reader->BeginScan(1);
    ASSERT_FALSE(reader->IsValid());
    reader->EndScan();

    reader->BeginScan(-1);
    ASSERT_TRUE(reader->IsValid());
    ASSERT_EQ(reader->GetCurrentKey(), 0);
    reader->Advance();
    ASSERT_FALSE(reader->IsValid());
    reader->EndScan();

    reader->BeginScan(0);
    ASSERT_TRUE(reader->IsValid());
    ASSERT_EQ(reader->GetCurrentKey(), 0);
    reader->Advance();
    ASSERT_FALSE(reader->IsValid());
    reader->EndScan();
}

TEST(TRcuTreeTest, 1to10)
{
    TChunkedMemoryPool pool;
    TComparer comparer;
    TRcuTree<int, TComparer> tree(&pool, &comparer);

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(tree.Insert(i));
    }
    ASSERT_EQ(tree.Size(), 10);

    auto* reader = tree.CreateReader();

    for (int i = 0; i < 10; ++i) {
        reader->BeginScan(i);
        for (int j = i; j < 10; ++j) {
            ASSERT_TRUE(reader->IsValid());
            ASSERT_EQ(reader->GetCurrentKey(), j);
            reader->Advance();
        }
        ASSERT_FALSE(reader->IsValid());
        reader->EndScan();
    }

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(reader->Find(i));
    }
    ASSERT_FALSE(reader->Find(-1));
    ASSERT_FALSE(reader->Find(11));
}

TEST(TRcuTreeTest, Random1000000)
{
    TChunkedMemoryPool pool;
    TComparer comparer;
    TRcuTree<int, TComparer> tree(&pool, &comparer);

    srand(42);
    std::set<int> set;
    for (int i = 0; i < 1000000; ++i) {
        int value = rand();
        ASSERT_EQ(tree.Insert(value), set.insert(value).second);
    }
    ASSERT_EQ(tree.Size(), set.size());

    auto* reader = tree.CreateReader();
    for (int value : set) {
        ASSERT_TRUE(reader->Find(value));
    }

    reader->BeginScan(*set.begin());
    for (int value : set) {
        ASSERT_TRUE(reader->IsValid());
        ASSERT_EQ(reader->GetCurrentKey(), value);
        reader->Advance();
    }
    reader->EndScan();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
