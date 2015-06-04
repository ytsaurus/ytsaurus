#include "stdafx.h"
#include "framework.h"

#include <core/misc/hyperloglog.h>
#include <core/misc/random.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::pair<THyperLogLog<8>, int> GenerateHyperLogLog(
    TRandomGenerator& rng,
    int size,
    int targetCardinality)
{
    auto hll = THyperLogLog<8>();

    int cardinality = 1;
    ui64 n = 0;
    hll.Add(n);
    for (int i = 0; i < size; i++) {
        if ((rng.Generate<ui64>() % size) < targetCardinality) {
            cardinality++;
            n += 1 + (rng.Generate<ui32>() % 100);
        }

        hll.Add(n);
    }

    return std::make_pair(hll, cardinality);
}

void TestCardinality(
    TRandomGenerator& rng,
    int size,
    int targetCardinality,
    int iterations)
{
    auto error = 0.0;

    for (int i = 0; i < iterations; i++) {
        auto hll = GenerateHyperLogLog(
            rng,
            size,
            targetCardinality);
        auto err = ((double)hll.first.EstimateCardinality() - hll.second) / hll.second;
        error += err;
    }

    auto meanError = error/iterations;

    EXPECT_NEAR(meanError, 0, 0.05);
}

TEST(HyperLogLogTest, Random)
{
    auto rng = TRandomGenerator(123);
    TestCardinality(rng, 100000, 200, 10);
    TestCardinality(rng, 100000, 1000, 10);
    TestCardinality(rng, 100000, 5000, 10);
    TestCardinality(rng, 100000, 10000, 10);
    TestCardinality(rng, 100000, 50000, 10);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
