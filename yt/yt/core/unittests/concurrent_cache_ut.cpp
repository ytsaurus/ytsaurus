#include "lock_free_hash_table_and_concurrent_cache_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/slab_allocator.h>
#include <yt/core/misc/concurrent_cache.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/logging/log_manager.h>


namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TElement final
{
    ui64 Hash;
    ui32 Size;
    char Data[0];

    using TAllocator = TSlabAllocator;
    using TEnableHazard = void;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

template <>
struct THash<NYT::TElement>
{
    size_t operator()(const NYT::TElement* value) const
    {
        return value->Hash;
    }
};

template <>
struct TEqualTo<NYT::TElement>
{
    bool operator()(const NYT::TElement* lhs, const NYT::TElement* rhs) const
    {
        return lhs->Hash == rhs->Hash &&
            lhs->Size == rhs->Size &&
            memcmp(lhs->Data, rhs->Data, lhs->Size) == 0;
    }
};

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NConcurrency;

class TConcurrentCacheTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        int /*keyColumnCount*/,
        int /*threadCount*/,
        int /*iterations*/,
        bool /*reinsert*/>>
{ };

TEST_P(TConcurrentCacheTest, Stress)
{
    size_t keyColumnCount = std::get<0>(GetParam());
    size_t threadCount = std::get<1>(GetParam());
    size_t iterations = std::get<2>(GetParam());
    bool reinsert = std::get<3>(GetParam());

    size_t columnCount = keyColumnCount;
    size_t distinctElements = std::pow('z' - 'a' + 1 + '9' - '0' + 1, keyColumnCount);
    size_t tableSize = distinctElements / 2 + 1;

    YT_VERIFY(keyColumnCount <= columnCount);

    TSlabAllocator allocator;

    TConcurrentCache<TElement> concurrentCache(tableSize);

    auto threadPool = New<TThreadPool>(threadCount, "Workers");
    std::vector<TFuture<size_t>> asyncResults;

    for (size_t threadId = 0; threadId < threadCount; ++threadId) {
        asyncResults.push_back(BIND([&, threadId] () -> size_t {
            TRandomCharGenerator randomChar(threadId);

            size_t insertCount = 0;

            auto keyBuffer = std::make_unique<char[]>(sizeof(TElement) + columnCount);
            auto* key = reinterpret_cast<TElement*>(keyBuffer.get());

            for (size_t index = 0; index < iterations * tableSize; ++index) {
                key->Size = keyColumnCount;
                for (size_t pos = 0; pos < columnCount; ++pos) {
                    key->Data[pos] = randomChar();
                }

                key->Hash = THash<TStringBuf>{}(TStringBuf(&key->Data[0], keyColumnCount));

                auto accessor = concurrentCache.GetLookupAccessor();
                auto found = accessor.Lookup(key, reinsert);

                if (!found) {
                    auto value = NewWithExtraSpace<TElement>(&allocator, columnCount);
                    memcpy(value.Get(), key, sizeof(TElement) + columnCount);
                    bool inserted = accessor.Insert(std::move(value));

                    insertCount += inserted;
                }
            }

            return insertCount;
        })
        .AsyncVia(threadPool->GetInvoker())
        .Run());
    }

    auto results = Combine(asyncResults).Get().Value();

    EXPECT_LE(distinctElements, std::accumulate(results.begin(), results.end(), 0));
    threadPool->Shutdown();
}

INSTANTIATE_TEST_SUITE_P(
    Simple,
    TConcurrentCacheTest,
    ::testing::Values(
        std::make_tuple(2, 1, 1000, false),
        std::make_tuple(2, 5, 1000, false),
        std::make_tuple(2, 5, 1000, true)
        ));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
