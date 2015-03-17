#include "stdafx.h"
#include "framework.h"

#include <core/misc/bloom_filter.h>
#include <core/misc/farm_hash.h>

#include <vector>
#include <random>
#include <unordered_set>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 Random()
{
    static std::random_device random;
    static std::mt19937_64 generator(random());
    static std::uniform_int_distribution<ui64> uniform;
    return uniform(generator);
}

TEST(TBloomFilterTest, Null)
{
    TBloomFilter bloom(65636, 0.03);

    for (int index = 0; index < 1000; ++index){
        EXPECT_FALSE(bloom.Contains(Random()));
    }
}

TEST(TBloomFilterTest, Simple)
{
    TBloomFilter bloom(65636, 0.03);
    auto items = std::vector<ui64>{0,1,2};

    for (const auto item : items) {
        bloom.Insert(item);
    }

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }

    auto size = bloom.EstimateSize();
    EXPECT_EQ(size, 4);
    bloom.Shrink();
    EXPECT_EQ(bloom.Size(), size);

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }
}

TEST(TBloomFilterTest, FalsePositiveRate)
{
    TBloomFilter bloom(65636, 0.03);
    auto items = std::unordered_set<ui64>();

    for (int index = 0; index < 1000; ++index){
        auto item = Random();
        items.insert(item);
        bloom.Insert(item);
    }

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }

    int falsePositiveCount = 0;
    int lookupCount = 10000;
    for (int index = 0; index < lookupCount; ++index) {
        auto item = Random();
        if (items.find(item) == items.end() && bloom.Contains(item)) {
            ++falsePositiveCount;
        }
    }

    EXPECT_LT(static_cast<double>(falsePositiveCount) / lookupCount, 0.05);

    bloom.Shrink();

    EXPECT_LE(bloom.Size(), 8192);

    falsePositiveCount = 0;
    lookupCount = 10000;
    for (int index = 0; index < lookupCount; ++index) {
        auto item = Random();
        if (items.find(item) == items.end() && bloom.Contains(item)) {
            ++falsePositiveCount;
        }
    }

    EXPECT_LT(static_cast<double>(falsePositiveCount) / lookupCount, 0.05);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

