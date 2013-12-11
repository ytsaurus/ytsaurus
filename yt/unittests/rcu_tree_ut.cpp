#include "stdafx.h"
#include "framework.h"

#include <core/misc/rcu_tree.h>

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

class TRcuTreeTest
    : public ::testing::Test
{
public:
    TChunkedMemoryPool Pool;
    TComparer Comparer;
    TRcuTree<int, TComparer> Tree;
    
    TRcuTreeTest()
        : Tree(&Pool, &Comparer)
    { }

};

TEST_F(TRcuTreeTest, ScannerPooling1)
{
    auto* scanner1 = Tree.AllocateScanner();
    auto* scanner2 = Tree.AllocateScanner();
    ASSERT_NE(scanner1, scanner2);
}

TEST_F(TRcuTreeTest, ScannerPooling2)
{
    auto* scanner1 = Tree.AllocateScanner();
    auto* scanner2 = Tree.AllocateScanner();
    ASSERT_NE(scanner1, scanner2);

    Tree.FreeScanner(scanner1);
    auto* scanner3 = Tree.AllocateScanner();
    ASSERT_EQ(scanner1, scanner3);
}

TEST_F(TRcuTreeTest, ScannerPooling3)
{
    auto* scanner1 = Tree.AllocateScanner();
    auto* scanner2 = Tree.AllocateScanner();
    auto* scanner3 = Tree.AllocateScanner();
    ASSERT_NE(scanner1, scanner2);
    ASSERT_NE(scanner1, scanner3);

    Tree.FreeScanner(scanner2);
    auto* scanner4 = Tree.AllocateScanner();
    ASSERT_EQ(scanner2, scanner4);
}

TEST_F(TRcuTreeTest, Empty)
{
    ASSERT_EQ(Tree.Size(), 0);

    auto* scanner = Tree.AllocateScanner();
    
    ASSERT_FALSE(scanner->Find(1));

    scanner->BeginScan(1);
    ASSERT_FALSE(scanner->IsValid());
    scanner->EndScan();
}

TEST_F(TRcuTreeTest, Singleton)
{
    ASSERT_TRUE(Tree.Insert(0));
    ASSERT_EQ(Tree.Size(), 1);

    auto* scanner = Tree.AllocateScanner();

    ASSERT_FALSE(scanner->Find(1));

    scanner->BeginScan(1);
    ASSERT_FALSE(scanner->IsValid());
    scanner->EndScan();

    scanner->BeginScan(-1);
    ASSERT_TRUE(scanner->IsValid());
    ASSERT_EQ(scanner->GetCurrent(), 0);
    scanner->Advance();
    ASSERT_FALSE(scanner->IsValid());
    scanner->EndScan();

    scanner->BeginScan(0);
    ASSERT_TRUE(scanner->IsValid());
    ASSERT_EQ(scanner->GetCurrent(), 0);
    scanner->Advance();
    ASSERT_FALSE(scanner->IsValid());
    scanner->EndScan();
}

TEST_F(TRcuTreeTest, 1to10)
{
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(Tree.Insert(i));
    }
    ASSERT_EQ(Tree.Size(), 10);

    auto* scanner = Tree.AllocateScanner();

    for (int i = 0; i < 10; ++i) {
        scanner->BeginScan(i);
        for (int j = i; j < 10; ++j) {
            ASSERT_TRUE(scanner->IsValid());
            ASSERT_EQ(scanner->GetCurrent(), j);
            scanner->Advance();
        }
        ASSERT_FALSE(scanner->IsValid());
        scanner->EndScan();
    }

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(scanner->Find(i));
    }
    ASSERT_FALSE(scanner->Find(-1));
    ASSERT_FALSE(scanner->Find(11));
}

TEST_F(TRcuTreeTest, Random1000000)
{
    srand(42);
    std::set<int> set;
    for (int i = 0; i < 1000000; ++i) {
        int value = rand();
        ASSERT_EQ(Tree.Insert(value), set.insert(value).second);
    }
    ASSERT_EQ(Tree.Size(), set.size());

    auto* scanner = Tree.AllocateScanner();
    for (int value : set) {
        ASSERT_TRUE(scanner->Find(value));
    }

    scanner->BeginScan(*set.begin());
    for (int value : set) {
        ASSERT_TRUE(scanner->IsValid());
        ASSERT_EQ(scanner->GetCurrent(), value);
        scanner->Advance();
    }
    scanner->EndScan();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
