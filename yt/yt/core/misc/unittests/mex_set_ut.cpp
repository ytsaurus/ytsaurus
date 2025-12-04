#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/mex_set.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSlowMexSet
{
public:
    bool Insert(int value)
    {
        return Set_.insert(value).second;
    }

    bool Erase(int value)
    {
        return Set_.erase(value) > 0;
    }

    bool Contains(int value) const
    {
        return Set_.contains(value);
    }

    void Clear()
    {
        Set_.clear();
    }

    int GetMex() const
    {
        int mex = 0;
        while (Set_.contains(mex)) {
            ++mex;
        }
        return mex;
    }

private:
    THashSet<int> Set_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TMexIntSetTest, Stress)
{
    TMexIntSet mexSet;
    TSlowMexSet slowMexSet;

    std::mt19937 rng(1543);
    std::uniform_int_distribution<int> val(0, 2000);
    std::bernoulli_distribution shouldInsert(0.6); 

    for (int step = 0; step < 5000; ++step) {
        int v = val(rng);
        if (shouldInsert(rng)) {
            EXPECT_EQ(mexSet.Insert(v), slowMexSet.Insert(v));
        } else {
            EXPECT_EQ(mexSet.Erase(v), slowMexSet.Erase(v));
        }
        EXPECT_EQ(mexSet.Contains(v), slowMexSet.Contains(v));
        EXPECT_EQ(mexSet.GetMex(), slowMexSet.GetMex());

        for (int spotCheckIndex = 0; spotCheckIndex < 20; ++spotCheckIndex) {
            int checkValue = val(rng);
            EXPECT_EQ(mexSet.Contains(checkValue), slowMexSet.Contains(checkValue));
        }

        if (step % 1000 == 0) {
            mexSet.Clear();
            slowMexSet.Clear();
        }
    }
}

TEST(TMexIntSetTest, Empty)
{
    TMexIntSet mexSet;

    EXPECT_EQ(0, mexSet.GetMex());
    EXPECT_FALSE(mexSet.Contains(0));
    EXPECT_FALSE(mexSet.Contains(2));
    EXPECT_FALSE(mexSet.Contains(100500));

    EXPECT_FALSE(mexSet.Erase(0));
    EXPECT_FALSE(mexSet.Erase(100500));
}

TEST(TMexIntSetTest, InsertNonZeroFirstKeepsMexZero) {
    TMexIntSet m;

    EXPECT_TRUE(m.Insert(10));
    EXPECT_EQ(m.GetMex(), 0);
    EXPECT_TRUE(m.Contains(10));
    EXPECT_FALSE(m.Insert(10));
}

TEST(TMexIntSetTest, MexAdvancementAndLeftIntervalExtension) {
    TMexIntSet m;

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(m.Insert(i));
        EXPECT_EQ(m.GetMex(), i + 1);
    }
}

TEST(TMexIntSetTest, ExtendLeftIntervalToTheRight) {
    TMexIntSet m;

    EXPECT_TRUE(m.Insert(0));
    EXPECT_EQ(m.GetMex(), 1);
    EXPECT_TRUE(m.Insert(1));
    EXPECT_EQ(m.GetMex(), 2);
    EXPECT_TRUE(m.Insert(2));
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(5));
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(3));
    EXPECT_EQ(m.GetMex(), 4);
}

TEST(TMexIntSetTest, MergeBothSides) {
    TMexIntSet m;

    // Left interval: [0, 2).
    EXPECT_TRUE(m.Insert(0));
    EXPECT_TRUE(m.Insert(1));

    // Right interval: [3, 5).
    EXPECT_TRUE(m.Insert(3));
    EXPECT_TRUE(m.Insert(4));

    EXPECT_EQ(m.GetMex(), 2);

    // Now the intervals are merged.
    EXPECT_TRUE(m.Insert(2));
    EXPECT_EQ(m.GetMex(), 5);
}

TEST(TMexIntSetTest, EraseRightEdge) {
    TMexIntSet m;

    EXPECT_TRUE(m.Insert(2));
    EXPECT_TRUE(m.Insert(3));
    EXPECT_TRUE(m.Insert(4));
    EXPECT_EQ(m.GetMex(), 0);

    EXPECT_TRUE(m.Erase(4));
    EXPECT_TRUE(m.Contains(2));
    EXPECT_TRUE(m.Contains(3));
    EXPECT_FALSE(m.Contains(4));
    EXPECT_EQ(m.GetMex(), 0);
}

TEST(TMexIntSetTest, EraseLeftEdge) {
    TMexIntSet m;

    EXPECT_TRUE(m.Insert(2));
    EXPECT_TRUE(m.Insert(3));
    EXPECT_TRUE(m.Insert(4));
    EXPECT_EQ(m.GetMex(), 0);

    EXPECT_TRUE(m.Erase(2));
    EXPECT_TRUE(m.Contains(3));
    EXPECT_TRUE(m.Contains(4));
    EXPECT_FALSE(m.Contains(2));
    EXPECT_EQ(m.GetMex(), 0);
}

TEST(TMexIntSetTest, IntervalSplit) {
    TMexIntSet m;
    for (int v = 0; v < 5; ++v) {
        EXPECT_TRUE(m.Insert(v));
    }

    EXPECT_EQ(m.GetMex(), 5);

    EXPECT_TRUE(m.Erase(2));
    EXPECT_EQ(m.GetMex(), 2);
    EXPECT_TRUE(m.Contains(1));
    EXPECT_FALSE(m.Contains(2));
    EXPECT_TRUE(m.Contains(3));
}

TEST(TMexIntSetTest, EraseSingleElementInterval) {
    TMexIntSet m;
    m.Insert(5);
    EXPECT_TRUE(m.Erase(5));
    EXPECT_FALSE(m.Contains(5));
    EXPECT_EQ(m.GetMex(), 0);
}

TEST(TMexIntSetTest, InsertAboveMex) {
    TMexIntSet m;

    for (int v : {0,1,2,4,5}) {
        EXPECT_TRUE(m.Insert(v));
    }
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(10));
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(3));
    EXPECT_EQ(m.GetMex(), 6);
}

TEST(TMexIntSetTest, Clear) {
    TMexIntSet m;

    for (int v = 0; v < 7; ++v) {
        EXPECT_TRUE(m.Insert(v));
    }
    EXPECT_EQ(m.GetMex(), 7);

    m.Clear();
    EXPECT_EQ(m.GetMex(), 0);
    EXPECT_FALSE(m.Contains(0));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
