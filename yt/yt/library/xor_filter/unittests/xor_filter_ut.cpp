#include <gtest/gtest.h>

#include <yt/yt/library/xor_filter/xor_filter.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxTrialCount = 100;

////////////////////////////////////////////////////////////////////////////////

std::vector<TFingerprint> GenerateUniqueElements(int count)
{
    THashSet<TFingerprint> values;
    while (ssize(values) < count) {
        values.insert(RandomNumber<ui64>());
    }
    return {values.begin(), values.end()};
}

////////////////////////////////////////////////////////////////////////////////

class TXorFilterTestBase
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple</*size*/ int, /*bitsPerKey*/ int>>
{ };

////////////////////////////////////////////////////////////////////////////////

class TXorFilterCreateDifferentSizeSequentialKeys
    : public TXorFilterTestBase
{ };

TEST_P(TXorFilterCreateDifferentSizeSequentialKeys, Test)
{
    auto [maxSize, bitsPerKey] = GetParam();

    NProfiling::TWallTimer timer;
    timer.Start();

    std::vector<TFingerprint> keys;

    int totalTrialCount = 0;

    for (int size = 1; size <= maxSize; ++size) {
        keys.push_back(size);

        int trialIndex = 1;

        while (trialIndex < MaxTrialCount) {
            try {
                TXorFilter filter;
                filter.Initialize(TXorFilter::Build(keys, bitsPerKey, /*trialCount*/ 1));

                for (int i = 1; i <= size; ++i) {
                    ASSERT_TRUE(filter.Contains(i));
                }

                totalTrialCount += trialIndex;
                break;
            } catch (const std::exception& /*ex*/) {
                ++trialIndex;
            }
        }

        ASSERT_TRUE(trialIndex < MaxTrialCount) << Format("Failed to build filter in %d attempts", MaxTrialCount);
    }

    Cerr << Format("CreateDifferentSizeSequentialKeys (MaxSize: %v, BitsPerKey: %v): %v, %v avg trials",
        maxSize,
        bitsPerKey,
        timer.GetElapsedTime(),
        1.0 * totalTrialCount / maxSize) << Endl;
}

INSTANTIATE_TEST_SUITE_P(
    TXorFilterCreateDifferentSizeSequentialKeys,
    TXorFilterCreateDifferentSizeSequentialKeys,
    ::testing::Values(
        std::tuple(1000, 10),
        std::tuple(5000, 4),
        std::tuple(5000, 5),
        std::tuple(5000, 7),
        std::tuple(5000, 8),
        std::tuple(5000, 16),
        std::tuple(5000, 20),
        std::tuple(5000, 31),
        std::tuple(5000, 32),
        std::tuple(5000, 33)));

////////////////////////////////////////////////////////////////////////////////

class TXorFilterCreateSingleSizeRandomKeys
    : public TXorFilterTestBase
{ };

TEST_P(TXorFilterCreateSingleSizeRandomKeys, Test)
{
    auto [size, bitsPerKey] = GetParam();
    auto keys = GenerateUniqueElements(size);

    NProfiling::TWallTimer timer;
    timer.Start();
    TXorFilter::Build(keys, bitsPerKey, MaxTrialCount);

    Cerr << Format("CreateSingleSizeRandomKeys (Size: %v, BitsPerKey: %v): %v",
        size,
        bitsPerKey,
        timer.GetElapsedTime()) << Endl;
}

INSTANTIATE_TEST_SUITE_P(
    TXorFilterCreateSingleSizeRandomKeys,
    TXorFilterCreateSingleSizeRandomKeys,
    ::testing::Values(
        std::tuple(10'000, 8),
        std::tuple(50'000, 8),
        std::tuple(100'000, 8),
        std::tuple(200'000, 8),
        std::tuple(500'000, 8),
        std::tuple(1'000'000, 4),
        std::tuple(1'000'000, 7),
        std::tuple(1'000'000, 8),
        std::tuple(1'000'000, 9),
        std::tuple(1'000'000, 15),
        std::tuple(1'000'000, 16),
        std::tuple(1'000'000, 17),
        std::tuple(2'000'000, 8)));

////////////////////////////////////////////////////////////////////////////////

class TXorFilterContainsSingleSize
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple</*size*/ int, /*queryCount*/ int, /*bitsPerKey*/ int>>
{ };

TEST_P(TXorFilterContainsSingleSize, Test)
{
    auto [size, queryCount, bitsPerKey] = GetParam();
    auto keys = GenerateUniqueElements(size);
    ASSERT_LT(size, queryCount);
    std::vector<TFingerprint> ammo;
    for (int index = 0; index < queryCount - size; ++index) {
        ammo.push_back(RandomNumber<ui64>());
    }

    TXorFilter filter;
    filter.Initialize(TXorFilter::Build(keys, bitsPerKey, MaxTrialCount));

    int falsePositiveCount = 0;
    NProfiling::TWallTimer timer;
    timer.Start();

    for (auto key : keys) {
        ASSERT_TRUE(filter.Contains(key));
    }
    for (auto key : ammo) {
        falsePositiveCount += filter.Contains(key);
    }

    int expectedFalsePositiveCount = ssize(ammo) >> bitsPerKey;
    ASSERT_LT(falsePositiveCount, expectedFalsePositiveCount * 3);

    Cerr << Format("TestContainsSingleSize (Size: %v, QueryCount: %v, BitsPerKey: %v): %v",
        size,
        queryCount,
        bitsPerKey,
        timer.GetElapsedTime()) << Endl;
}

INSTANTIATE_TEST_SUITE_P(
    TXorFilterContainsSingleSize,
    TXorFilterContainsSingleSize,
    ::testing::Values(
        std::tuple(1000, 1'000'000, 8),
        std::tuple(10'000, 1'000'000, 8),
        std::tuple(50'000, 1'000'000, 8),
        std::tuple(100'000, 1'000'000, 8),
        std::tuple(100'000, 1'000'000, 9),
        std::tuple(100'000, 1'000'000, 16),
        std::tuple(200'000, 1'000'000, 8),
        std::tuple(400'000, 1'000'000, 4),
        std::tuple(100'000, 10'000'000, 4)));

////////////////////////////////////////////////////////////////////////////////

// Used in the next test, see the comment below.
[[maybe_unused]] void GenerateInputForStableSerialization()
{
    std::vector<TFingerprint> keys;
    for (int i = 100; i < 110; ++i) {
        keys.push_back(i * i);
    }

    auto data = TXorFilter::Build(keys, /*bitsPerKey*/ 8, MaxTrialCount);

    TXorFilter filter;
    filter.Initialize(data);

    for (int i = 100; i < 110; ++i) {
        ASSERT_TRUE(filter.Contains(i * i));
    }

    Cout << "    // BEGIN AUTOGENERATED CODE\n";
    Cout << "    auto data = TSharedRef::FromString(TString(\n";

    constexpr i64 BytesPerLine = 70 / 3;
    for (int line = 0; line < DivCeil(std::ssize(data), BytesPerLine); ++line) {
        int l = line * BytesPerLine;
        int r = std::min<int>(l + BytesPerLine, std::ssize(data));
        Cout << "        \"";
        for (int index = l; index < r; ++index) {
            Cout << Format("\\x%x", static_cast<ui8>(data[index]));
        }
        if (r == std::ssize(data)) {
            Cout << "\",\n";
        } else {
            Cout << "\"\n";
        }
    }
    Cout << Format("        %v));\n", data.Size());
    Cout << "    // END AUTOGENERATED CODE\n";
}

TEST(TXorFilterTest, StableSerialization)
{
    // This test checks the stability of the serialization format, i.e. that
    // the updated code will still be able to read old blocks. The code below
    // is generated using GenerateInputForStableSerialization() call and should
    // not be regenerated unless you are intentionally breaking compatibility.

    // BEGIN AUTOGENERATED CODE
    auto data = TSharedRef::FromString(TString(
        "\x1\x0\x0\x0\x0\xf9\x0\x0\x3b\x0\x0\x0\x0\x0\xff\x8c\x0\x11\x70\x0\x0\x0\x0"
        "\x0\x0\x0\x0\x40\x67\x0\x0\x8c\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\x0\xe6\x0\x0"
        "\x0\x0\x0\x0\x0\x0\x87\x88\xf9\xd5\x4e\x9b\xc\x4f\xc8\xc0\xc7\xd0\xbc\x33\xf5\x0\xaa"
        "\xe6\x8a\xb4\xbc\xea\x7b\x33\xe0\x54\xf0\xd8\x2\xa1\x66\x74\x8\x0\x0\x0\x2a\x0\x0\x0",
        92));
    // END AUTOGENERATED CODE

    TXorFilter filter;
    filter.Initialize(data);
    for (int i = 100; i < 110; ++i) {
        ASSERT_TRUE(filter.Contains(i * i));
    }

    int falsePositiveCount = 0;
    for (int i = 110; i < 120; ++i) {
        falsePositiveCount += filter.Contains(i * i);
    }
    ASSERT_LT(falsePositiveCount, 5);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
