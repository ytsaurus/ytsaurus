#include "consistent_hashing.h"

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/digest/old_crc/crc.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace {
    ui64 NthHash(size_t x) {
        TString tmp = Sprintf("prefix%" PRISZT "suffix", x);
        return crc64(tmp.data(), tmp.size());
    }

}

Y_UNIT_TEST_SUITE(ConsistentHashing) {
    Y_UNIT_TEST(FitLimits) {
        for (size_t i = 0; i < 100; ++i) {
            ui64 x = NthHash(i);
            for (size_t n = 1; n < 100; ++n) {
                size_t res = ConsistentHashing(x, n);
                UNIT_ASSERT_C(res < n, "ConsistentHashing(x, n) value must be less than n");
            }
        }
    }
    Y_UNIT_TEST(Consistency) {
        for (size_t i = 0; i < 100; ++i) {
            ui64 x = NthHash(i);
            size_t res = 0;
            for (size_t n = 1; n < 100; ++n) {
                size_t newRes = ConsistentHashing(x, n);
                UNIT_ASSERT_C(newRes == res || newRes + 1 == n, "ConsistentHashing(x, n) must change only to fill new n-th basket");
                res = newRes;
            }
        }
    }
    Y_UNIT_TEST(EqualDistribution) {
        // Uses 6 sigma rule: not more than 2 fails in billion cases. So, its mostly true, that all results must be in 6 sigma.
        // So, deviation must be in 6 * sqrt(blockSize) limits.
        // Done 40 experiments with 1 to 32 shards = 32 * (32 + 1) / 2 = 528 expermints.
        // Chance to get assert (even if everything is correct) 528 * 2 in billion cases. So, near one in million :)
        const size_t maxN = 32;
        const size_t blockSizeSqrt = 200;
        const size_t blockSize = blockSizeSqrt * blockSizeSqrt;
        TVector<TVector<size_t>> basket;
        for (size_t n = 0; n <= maxN; ++n) {
            basket.push_back(TVector<size_t>(n));
        }
        for (size_t k = 0; k < maxN; ++k) {
            size_t base = k * blockSize;
            for (size_t i = blockSize; i; --i) {
                ui64 x = NthHash(base + i);
                for (size_t n = 1 + k; n <= maxN; ++n) {
                    size_t res = ConsistentHashing(x, n);
                    basket[n][res]++;
                }
            }
        }
        for (size_t n = 1; n <= maxN; ++n) {
            TVector<size_t>& row = basket[n];
            for (size_t i = 0; i < row.size(); ++i) {
                ssize_t num = (ssize_t)row[i] - (ssize_t)blockSize;
                if (num < 0) {
                    num = -num;
                }
                UNIT_ASSERT_C((size_t)num < 6 * blockSizeSqrt, "Deviation must be in 6 sigma window.");
            }
        }
    }
    Y_UNIT_TEST(Stability) {
        ui64 hash = ULL(0xDEADBEEFDEADBEEF);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 2), 1);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 3), 1);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 4), 3);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 5), 3);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 6), 5);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 7), 5);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 8), 5);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 16), 15);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 20), 15);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 32), 15);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 45), 15);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 77), 45);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 100), 45);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 111), 100);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 120), 111);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 173), 111);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 255), 173);
    }

    Y_UNIT_TEST(ALotOfBins) {
        ui64 hash = ULL(18446744073709551610);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 2048), 2047);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 2751), 2047);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 2752), 2751);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 4090), 2751);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 4091), 4090);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 4097), 4090);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 4098), 4097);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 8191), 4097);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 8192), 8191);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 9215), 8191);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 9216), 9215);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 16378), 9215);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 16379), 16378);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 16447), 16378);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 16448), 16447);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 32767), 16447);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 32768), 32767);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 32771), 32767);

        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 32772), 32771);
        UNIT_ASSERT_EQUAL(ConsistentHashing(hash, 43966), 32771);
    }
}
