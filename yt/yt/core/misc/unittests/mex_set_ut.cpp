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

// NB(achulkov2): As you may have guessed, this test was written by ChatGPT, based on
// my previous non-perfect stress test. It is a bit verbose, but what else are tests for?
TEST(TMexSetTest, Stress)
{
    TMexSet mexSet;
    TSlowMexSet slowMexSet;

    // Tunables.
    constexpr int kSteps = 20000;
    constexpr int kSmallN = 100;
    constexpr int kLargeMax = 200000;
    constexpr int kSpotChecksPerStep = 12;

    // Action types: 0=InsertRandom, 1=EraseRandom, 2=InsertMex, 3=EraseNearMex.
    // Heavier weight to insert to keep growth and mex pressure.
    std::discrete_distribution<int> actionDist({
        45, // InsertRandom
        30, // EraseRandom
        15, // InsertMex        (targets mex advancement)
        10  // EraseNearMex     (targets gaps near mex)
    });

    // Where values come from: small vs large domain — 70% from [0..kSmallN].
    std::bernoulli_distribution smallDomainBias(0.70);

    std::mt19937 rng(1543);
    std::uniform_int_distribution<int> smallVal(0, kSmallN);
    std::uniform_int_distribution<int> largeVal(0, kLargeMax);

    // Helper to draw a value with small/large mix.
    auto drawMixedValue = [&] {
        if (smallDomainBias(rng)) return smallVal(rng);
        return largeVal(rng);
    };

    // Offsets around mex (ensure pressure right near mex).
    std::uniform_int_distribution<int> aroundMexOffset(-5, 5);

    for (int step = 1; step <= kSteps; ++step) {
        int action = actionDist(rng);

        int v = 0;
        bool resultFast = false;
        bool resultSlow = false;

        switch (action) {
            case 0: {
                // InsertRandom.
                v = drawMixedValue();
                resultFast = mexSet.Insert(v);
                resultSlow = slowMexSet.Insert(v);
                break;
            }
            case 1: {
                // EraseRandom.
                v = drawMixedValue();
                resultFast = mexSet.Erase(v);
                resultSlow = slowMexSet.Erase(v);
                break;
            }
            case 2: {
                // InsertMex (forces mex to move when possible).
                int m = mexSet.GetMex();
                v = m;
                resultFast = mexSet.Insert(v);
                resultSlow = slowMexSet.Insert(v);
                break;
            }
            case 3: {
                // EraseNearMex (focus deletions around mex to create gaps).
                int m = mexSet.GetMex();
                // Pick near (m-1, m, m+1, …) clamped to non-negative
                int candidate = m + aroundMexOffset(rng);
                if (candidate < 0) candidate = 0;
                v = candidate;
                resultFast = mexSet.Erase(v);
                resultSlow = slowMexSet.Erase(v);
                break;
            }
            default:
                YT_ABORT();
        }

        EXPECT_EQ(resultFast, resultSlow)
            << Format("Action mismatch on step %v action=%v v=%v", step, action, v);

        // Core invariants.
        EXPECT_EQ(mexSet.Contains(v), slowMexSet.Contains(v)) << "Contains(v) mismatch";
        EXPECT_EQ(mexSet.GetMex(), slowMexSet.GetMex()) << "GetMex mismatch";

        // Random spot checks that also prefer the small domain.
        for (int i = 0; i < kSpotChecksPerStep; ++i) {
            int checkValue = drawMixedValue();
            EXPECT_EQ(mexSet.Contains(checkValue), slowMexSet.Contains(checkValue))
                << Format("Contains(checkValue) mismatch for %v", checkValue);
        }

        // Occasionally stress full reset.
        if (step % 3000 == 0) {
            mexSet.Clear();
            slowMexSet.Clear();
            EXPECT_EQ(mexSet.GetMex(), slowMexSet.GetMex());
        }

        // Occasional "prefix fill" micro-phase: aggressively push mex upward,
        // then poke holes — this creates long runs and gap merging cases.
        if (step % 4000 == 0) {
            // Push mex up by inserting the next ~kSmallN/2 missing values.
            for (int i = 0; i < kSmallN / 2; ++i) {
                int m = mexSet.GetMex();
                EXPECT_EQ(mexSet.Insert(m), slowMexSet.Insert(m));
            }
            // Now poke a handful of holes near the new mex.
            for (int i = 0; i < 20; ++i) {
                int m = mexSet.GetMex();
                int hole = std::max(0, m - 1 - (i % 7));
                EXPECT_EQ(mexSet.Erase(hole), slowMexSet.Erase(hole));
            }
            EXPECT_EQ(mexSet.GetMex(), slowMexSet.GetMex());
        }
    }
}

TEST(TMexSetTest, Empty)
{
    TMexSet mexSet;

    EXPECT_EQ(0, mexSet.GetMex());
    EXPECT_FALSE(mexSet.Contains(0));
    EXPECT_FALSE(mexSet.Contains(2));
    EXPECT_FALSE(mexSet.Contains(100500));

    EXPECT_FALSE(mexSet.Erase(0));
    EXPECT_FALSE(mexSet.Erase(100500));
}

TEST(TMexSetTest, InsertNonZeroFirstKeepsMexZero) {
    TMexSet m;

    EXPECT_TRUE(m.Insert(10));
    EXPECT_EQ(m.GetMex(), 0);
    EXPECT_TRUE(m.Contains(10));
    EXPECT_FALSE(m.Insert(10));
}

TEST(TMexSetTest, MexAdvancementAndLeftIntervalExtension) {
    TMexSet m;

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(m.Insert(i));
        EXPECT_EQ(m.GetMex(), i + 1);
    }
}

TEST(TMexSetTest, ExtendLeftIntervalToTheRight) {
    TMexSet m;

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

TEST(TMexSetTest, MergeBothSides) {
    TMexSet m;

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

TEST(TMexSetTest, EraseRightEdge) {
    TMexSet m;

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

TEST(TMexSetTest, EraseLeftEdge) {
    TMexSet m;

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

TEST(TMexSetTest, IntervalSplit) {
    TMexSet m;
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

TEST(TMexSetTest, EraseSingleElementInterval) {
    TMexSet m;
    m.Insert(5);
    EXPECT_TRUE(m.Erase(5));
    EXPECT_FALSE(m.Contains(5));
    EXPECT_EQ(m.GetMex(), 0);
}

TEST(TMexSetTest, InsertAboveMex) {
    TMexSet m;

    for (int v : {0,1,2,4,5}) {
        EXPECT_TRUE(m.Insert(v));
    }
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(10));
    EXPECT_EQ(m.GetMex(), 3);

    EXPECT_TRUE(m.Insert(3));
    EXPECT_EQ(m.GetMex(), 6);
}

TEST(TMexSetTest, Clear) {
    TMexSet m;

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
