#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/library/heavy_hitters/misra_gries.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMisraGriesHeavyHitters, Basic)
{
    auto now = TInstant::Now() - TDuration::Days(100);
    auto window = TDuration::Minutes(1);
    auto counter = New<TMisraGriesHeavyHitters<int>>(1.0 / 3, window, 2);
    counter->Register(std::vector<int>{1, 2, 3, 1, 4, 5, 2, 2, 3, 2, 2, 6}, now);
    auto heavyHitters = counter->GetStatistics(now);
    EXPECT_NEAR(heavyHitters.Total, 12.0 / window.Seconds(), 0.1);
    EXPECT_TRUE(heavyHitters.Fractions.contains(2));
    EXPECT_NEAR(heavyHitters.Fractions[2], 0.333, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 1u);
    heavyHitters = counter->GetStatistics(now, std::optional<i64>{0});
    EXPECT_FALSE(heavyHitters.Fractions.contains(2));
    // forget previous
    now += TDuration::Days(10);
    counter->Register(std::vector<int>{5, 5, 5, 2, 5, 5, 1, 1, 5, 2, 2, 2}, now);
    heavyHitters = counter->GetStatistics(now);
    EXPECT_NEAR(heavyHitters.Total, 12.0 / window.Seconds(), 0.1);
    EXPECT_FALSE(heavyHitters.Fractions.contains(2));
    EXPECT_TRUE(heavyHitters.Fractions.contains(5));
    EXPECT_NEAR(heavyHitters.Fractions[5], 0.5, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 1u);
    heavyHitters = counter->GetStatistics(now, std::optional<i64>{1});
    EXPECT_FALSE(heavyHitters.Fractions.contains(2));
    EXPECT_TRUE(heavyHitters.Fractions.contains(5));

    // still remember about 5 but forget about 2
    now += TDuration::Minutes(1);
    counter->Register(std::vector<int>{7, 7, 7, 7, 7, 7, 5, 5}, now);
    heavyHitters = counter->GetStatistics(now);
    double total = 14; // 8 + 12 / 2 <- decay
    EXPECT_NEAR(heavyHitters.Total, total / window.Seconds(), 0.1);
    EXPECT_TRUE(heavyHitters.Fractions.contains(7));
    EXPECT_NEAR(heavyHitters.Fractions[7], 5.0 / total, 0.001);
    EXPECT_TRUE(heavyHitters.Fractions.contains(5));
    EXPECT_NEAR(heavyHitters.Fractions[5], (6.0 / 2 + 2) / total, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 2u);
    heavyHitters = counter->GetStatistics(now, std::optional<i64>{1});
    EXPECT_FALSE(heavyHitters.Fractions.contains(5));
    EXPECT_TRUE(heavyHitters.Fractions.contains(7));

    // check if we get overflow error
    now += TDuration::Days(10);
    for (int i = 0; i < 5; ++i) {
        now += TDuration::Minutes(300);
        counter->Register(std::vector<int>{5}, now);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMisraGriesHeavyHitters, Random)
{
    const int keyCount = 1e5;
    const double threshold = 0.0001;
    const int limit = 10;
    auto now = TInstant::Now();
    auto window = TDuration::Minutes(1);
    TFastRng64 rng(42);

    std::set<int> keySet = {1, 2, 3};
    auto counter = New<TMisraGriesHeavyHitters<int>>(threshold, window, limit);
    std::vector<int> keys;
    for (const auto& key : keySet) {
        for (int i = 0; i < keyCount * threshold * 10; ++i) {
            keys.push_back(key);
        }
    }
    while (keys.size() < keyCount) {
        int key = rng.Uniform(4, 10000);
        keys.push_back(key);
    }
    Shuffle(keys.begin(), keys.end());
    for (int i = 0; i < keyCount; ++i) {
        if (i % 300 == 0) {
            now += TDuration::Seconds(1);
        }
        counter->Register(std::vector<int>{keys[i]}, now);
    }
    auto heavyHitters = counter->GetStatistics(now);
    for (const auto& key : keySet) {
        EXPECT_TRUE(heavyHitters.Fractions.contains(key));
    }
    heavyHitters = counter->GetStatistics(now, 10000);
    double sumValues = 0;
    for (const auto& [key, value] : heavyHitters.Fractions) {
        sumValues += value;
    }
    EXPECT_TRUE(sumValues <= heavyHitters.Total);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMisraGriesHeavyHitters, Weighted)
{
    auto now = TInstant::Now() - TDuration::Days(100);
    auto window = TDuration::Minutes(1);
    auto counter = New<TMisraGriesHeavyHitters<int>>(1.0 / 3, window, 2);

    counter->RegisterWeighted(std::vector<std::pair<int, double>>{{6, 20}, {5, 5}, {4, 5}, {3, 1}, {2, 1}, {1, 19}}, now);
    auto heavyHitters = counter->GetStatistics(now);
    EXPECT_NEAR(heavyHitters.Total, 51.0 / window.Seconds(), 0.1);
    EXPECT_TRUE(heavyHitters.Fractions.contains(6));
    EXPECT_TRUE(heavyHitters.Fractions.contains(1));
    EXPECT_FALSE(heavyHitters.Fractions.contains(2));
    EXPECT_FALSE(heavyHitters.Fractions.contains(5));
    EXPECT_NEAR(heavyHitters.Fractions[6], 20.0 / 51.0, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 2u);
    heavyHitters = counter->GetStatistics(now, std::optional<i64>{1});
    EXPECT_FALSE(heavyHitters.Fractions.contains(1));
    // forget previous
    now += TDuration::Days(10);
    counter->RegisterWeighted(std::vector<std::pair<int, double>>{{1, 5}, {2, 10}, {3, 10}}, now);
    heavyHitters = counter->GetStatistics(now);
    EXPECT_NEAR(heavyHitters.Total, 25.0 / window.Seconds(), 0.1);
    EXPECT_FALSE(heavyHitters.Fractions.contains(1));
    EXPECT_TRUE(heavyHitters.Fractions.contains(2));
    EXPECT_NEAR(heavyHitters.Fractions[2], 10.0 / 25.0, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 2u);
    heavyHitters = counter->GetStatistics(now, std::optional<i64>{0});
    EXPECT_FALSE(heavyHitters.Fractions.contains(2));

    now += TDuration::Minutes(1);
    counter->RegisterWeighted(std::vector<std::pair<int, double>>{{2, 10}, {3, 10}}, now);
    heavyHitters = counter->GetStatistics(now);
    double total = 20.0 + 25.0 / 2; // decay
    EXPECT_NEAR(heavyHitters.Total, total / window.Seconds(), 0.1);
    EXPECT_TRUE(heavyHitters.Fractions.contains(3));
    EXPECT_NEAR(heavyHitters.Fractions[3], (5.0 + 10.0) / total, 0.001);
    EXPECT_EQ(heavyHitters.Fractions.size(), 2u);

    // check if we get overflow error
    now += TDuration::Days(10);
    for (int i = 0; i < 5; ++i) {
        now += TDuration::Minutes(300);
        counter->RegisterWeighted(std::vector<std::pair<int, double>>{{5, 100}}, now);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMisraGriesHeavyHitters, WeightedRandom)
{
    const double threshold = 0.0001;
    const int limit = 10;
    auto now = TInstant::Now();
    auto window = TDuration::Minutes(1);
    TFastRng64 rng(42);

    auto counter = New<TMisraGriesHeavyHitters<int>>(threshold, window, limit);

    std::vector<std::pair<int, double>> weighted_keys_1;
    weighted_keys_1.reserve(100);
    for (int i = 0; i < 100; ++i) {
        weighted_keys_1.push_back(std::pair(1, rng.Uniform(1000, 2000) / 3.0));
    }
    counter->RegisterWeighted(weighted_keys_1, now);

    now += TDuration::Minutes(1);

    std::vector<std::pair<int, double>> weighted_keys_2;
    weighted_keys_2.reserve(100);
    for (int i = 0; i < 100; ++i) {
        weighted_keys_2.push_back(std::pair(2, rng.Uniform(1000, 2000) / 34.0));
    }
    counter->RegisterWeighted(weighted_keys_2, now);

    now += TDuration::Minutes(1);

    for (int i = 0; i < 1000; ++i) {
        int key = static_cast<int>(rng.Uniform(3, 1000));
        double weight = rng.Uniform(1000, 2000) / 1000.0;
        counter->RegisterWeighted(std::vector<std::pair<int, double>>{{key, weight}}, now);
    }

    auto heavyHitters = counter->GetStatistics(now);

    EXPECT_TRUE(heavyHitters.Fractions.contains(1));
    EXPECT_TRUE(heavyHitters.Fractions.contains(2));
    EXPECT_TRUE(heavyHitters.Fractions[1] >= heavyHitters.Fractions[2]);

    now += TDuration::Minutes(10000);

    heavyHitters = counter->GetStatistics(now);
    EXPECT_TRUE(heavyHitters.Fractions.empty());
    EXPECT_NEAR(heavyHitters.Total, 0, 0.001);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
