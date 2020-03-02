#include <yt/core/test_framework/framework.h>

#include <library/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/memory_tag.h>

#include <thread>

namespace NYT::NYTAlloc {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ALLOC_ENABLED

template <class T, size_t N>
TEnumIndexedVector<T, ssize_t> AggregateArenaCounters(const std::array<TEnumIndexedVector<T, ssize_t>, N>& counters)
{
    TEnumIndexedVector<T, ssize_t> result;
    for (size_t index = 0; index < counters.size(); ++index) {
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            result[counter] += counters[index][counter];
        }
    }
    return result;
}

class TYTAllocTaggedTest
    : public ::testing::TestWithParam<TMemoryTag>
{ };

TEST_P(TYTAllocTaggedTest, LargeCounters)
{
    TMemoryTagGuard guard(GetParam());
    constexpr auto N = 100_MBs;
    constexpr auto Eps = 1_MBs;
    auto total1 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto largeTotal1 = AggregateArenaCounters(GetLargeArenaAllocationCounters())[ELargeArenaCounter::BytesUsed];
    auto* ptr = NYTAlloc::Allocate(N);
    auto total2 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto largeTotal2 = AggregateArenaCounters(GetLargeArenaAllocationCounters())[ELargeArenaCounter::BytesUsed];
    EXPECT_LE(std::abs(total2 - total1 - N), Eps);
    EXPECT_LE(std::abs(largeTotal2 - largeTotal1 - N), Eps);
    NYTAlloc::Free(ptr);
    auto total3 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto largeTotal3 = AggregateArenaCounters(GetLargeArenaAllocationCounters())[ELargeArenaCounter::BytesUsed];
    EXPECT_LE(std::abs(total3 - total1), Eps);
    EXPECT_LE(std::abs(largeTotal3 - largeTotal1), Eps);
}

TEST_P(TYTAllocTaggedTest, HugeCounters)
{
    TMemoryTagGuard guard(GetParam());
    constexpr auto N = 10_GBs;
    constexpr auto Eps = 1_MBs;
    auto total1 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal1 = GetHugeAllocationCounters()[EHugeCounter::BytesUsed];
    auto* ptr = NYTAlloc::Allocate(N);
    auto total2 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal2 = GetHugeAllocationCounters()[EHugeCounter::BytesUsed];
    EXPECT_LE(std::abs(total2 - total1 - N), Eps);
    EXPECT_LE(std::abs(hugeTotal2 - hugeTotal1 - N), Eps);
    NYTAlloc::Free(ptr);
    auto total3 = GetTotalAllocationCounters()[ETotalCounter::BytesUsed];
    auto hugeTotal3 = GetHugeAllocationCounters()[EHugeCounter::BytesUsed];
    EXPECT_LE(std::abs(total3 - total1), Eps);
    EXPECT_LE(std::abs(hugeTotal3 - hugeTotal1), Eps);
}

INSTANTIATE_TEST_SUITE_P(
    LargeCounters,
    TYTAllocTaggedTest,
    ::testing::Values(0, 1));

INSTANTIATE_TEST_SUITE_P(
    HugeCounters,
    TYTAllocTaggedTest,
    ::testing::Values(0, 1));

////////////////////////////////////////////////////////////////////////////////

TEST(TYTAllocTest, AroundLargeBlobThreshold)
{
    constexpr size_t HugeSizeThreshold = 1ULL << (LargeRankCount - 1);
    for (int i = -10; i <= 10; ++i) {
        size_t size = HugeSizeThreshold + i * 10;
        void* ptr = NYTAlloc::Allocate(size);
        NYTAlloc::Free(ptr);
    }
}

TEST(TYTAllocTest, PerThreadCacheReclaim)
{
    const int N = 1000;
    const int M = 200;
    const size_t S = 16_KB;

    auto getBytesCommitted = [] {
        static_assert(NYTAlloc::SmallRankCount >= 23, "SmallRankCount is too small");
        return NYTAlloc::GetSmallArenaAllocationCounters()[22][NYTAlloc::ESmallArenaCounter::BytesCommitted];
    };

    auto bytesBefore = getBytesCommitted();
    fprintf(stderr, "bytesBefore = %" PRISZT"\n", bytesBefore);

    char sum = 0;
    for (int i = 0; i < N; i++) {
        std::thread t([&] {
            std::vector<char*> ptrs;
            for (int j = 0; j < M; ++j) {
                auto* ptr = new char[S];
                ptrs.push_back(ptr);

                // To prevent allocations from being opitmized out.
                ::memset(ptr, 0, S);
                for (size_t k = 0; k < S; ++k) {
                    sum += ptr[k];
                }
            }
            for (int j = 0; j < M; ++j) {
                delete[] ptrs[j];
            }
        });
        t.join();
    }


    auto bytesAfter = getBytesCommitted();
    fprintf(stderr, "bytesAfter = %" PRISZT"\n", bytesAfter);

    EXPECT_GE(bytesAfter, bytesBefore);
    EXPECT_LE(bytesAfter, bytesBefore + 4_MBs);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTAlloc
