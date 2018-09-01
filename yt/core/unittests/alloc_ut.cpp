#include <yt/core/test_framework/framework.h>

#include <yt/core/alloc/alloc.h>

namespace NYT {
namespace NYTAlloc {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t N>
TEnumIndexedVector<ssize_t, T> AggregateArenaCounters(const std::array<TEnumIndexedVector<ssize_t, T>, N>& counters)
{
    TEnumIndexedVector<ssize_t, T> result;
    for (size_t index = 0; index < counters.size(); ++index) {
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            result[counter] += counters[index][counter];
        }
    }
    return result;
}

class TYTAllocTest
    : public ::testing::TestWithParam<TMemoryTag>
{ };

TEST_P(TYTAllocTest, LargeCounters)
{
    TMemoryTagGuard guard(GetParam());
    constexpr auto N = 100_MB;
    constexpr auto Eps = 1_MB;
    auto total1 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto largeTotal1 = AggregateArenaCounters(GetLargeArenaCounters())[ELargeArenaCounter::BytesUsed];
    auto* ptr = YTAlloc(N);
    auto total2 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto largeTotal2 = AggregateArenaCounters(GetLargeArenaCounters())[ELargeArenaCounter::BytesUsed];
    EXPECT_LE(std::abs(total2 - total1 - N), Eps);
    EXPECT_LE(std::abs(largeTotal2 - largeTotal1 - N), Eps);
    YTFree(ptr);
    auto total3 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto largeTotal3 = AggregateArenaCounters(GetLargeArenaCounters())[ELargeArenaCounter::BytesUsed];
    EXPECT_LE(std::abs(total3 - total1), Eps);
    EXPECT_LE(std::abs(largeTotal3 - largeTotal1), Eps);
}

TEST_P(TYTAllocTest, HugeCounters)
{
    TMemoryTagGuard guard(GetParam());
    constexpr auto N = 10_GB;
    constexpr auto Eps = 1_MB;
    auto total1 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal1 = GetHugeCounters()[EHugeCounter::BytesUsed];
    auto* ptr = YTAlloc(N);
    auto total2 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal2 = GetHugeCounters()[EHugeCounter::BytesUsed];
    EXPECT_LE(std::abs(total2 - total1 - N), Eps);
    EXPECT_LE(std::abs(hugeTotal2 - hugeTotal1 - N), Eps);
    YTFree(ptr);
    auto total3 = GetTotalCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal3 = GetHugeCounters()[EHugeCounter::BytesUsed];
    EXPECT_LE(std::abs(total3 - total1), Eps);
    EXPECT_LE(std::abs(hugeTotal3 - hugeTotal1), Eps);
}

INSTANTIATE_TEST_CASE_P(
    LargeCounters,
    TYTAllocTest,
    ::testing::Values(0, 1));

INSTANTIATE_TEST_CASE_P(
    HugeCounters,
    TYTAllocTest,
    ::testing::Values(0, 1));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYTAlloc
} // namespace NYT
