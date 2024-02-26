#include <yt/yt/core/misc/slab_allocator.h>
#include <yt/yt/core/misc/concurrent_cache.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/future.h>

#include <random>

using namespace NYT;
using namespace NYT::NConcurrency;

struct TRandomCharGenerator
{
    std::default_random_engine Engine;
    std::uniform_int_distribution<int> Uniform;

    explicit TRandomCharGenerator(size_t seed)
        : Engine(seed)
        , Uniform(0, 'z' - 'a' + '9' - '0' + 1)
    { }

    char operator() ()
    {
        char symbol = Uniform(Engine);
        auto result = symbol >= 10 ? symbol - 10 + 'a' : symbol + '0';
        YT_VERIFY((result <= 'z' &&  result >= 'a') || (result <= '9' &&  result >= '0'));
        return result;
    }
};

static const auto& Logger = LockFreePtrLogger;
static bool EnableLogging = false;

struct TElement final
{
    ui64 Hash;
    ui32 Size;
    char Data[0];

    using TAllocator = TSlabAllocator;
    static constexpr bool EnableHazard = true;

    TElement()
    {
        YT_LOG_TRACE_IF(EnableLogging, "TElement() %v", this);
    }

    ~TElement()
    {
        YT_LOG_TRACE_IF(EnableLogging, "~TElement(%v) %v", TStringBuf(Data, Size), this);
    }
};

int main(int argc, char** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    try {
        size_t columnCount = 5;
        size_t keyColumnCount = 2;
        size_t distinctElements = std::pow('z' - 'a' + 1 + '9' - '0' + 1, keyColumnCount);

        size_t tableSize = distinctElements / 2 + 1;

        size_t threadCount = 3;

        size_t iterations = 300;

        THazardPtrReclaimOnContextSwitchGuard flushGuard;

        YT_VERIFY(keyColumnCount < columnCount);

        Cout << Format("Distinct elements: %v\n", distinctElements);

        TSlabAllocator allocator;

        TConcurrentCache<TElement> concurrentCache(tableSize);

        auto threadPool = CreateThreadPool(threadCount, "Workers");
        std::vector<TFuture<void>> asyncResults;

        for (size_t threadId = 0; threadId < threadCount; ++threadId) {
            asyncResults.push_back(BIND([&, threadId] () {

                THazardPtrReclaimOnContextSwitchGuard flushGuard;
                TRandomCharGenerator randomChar(threadId);
                size_t insertCount = 0;

                auto keyBuffer = std::make_unique<char[]>(sizeof(TElement) + keyColumnCount);
                auto* key = reinterpret_cast<TElement*>(keyBuffer.get());

                for (size_t index = 0; index < iterations * tableSize; ++index) {
                    key->Size = keyColumnCount;
                    for (size_t pos = 0; pos < keyColumnCount; ++pos) {
                        key->Data[pos] = randomChar();
                    }

                    key->Hash = THash<TStringBuf>{}(TStringBuf(&key->Data[0], keyColumnCount));

                    auto lookuper = concurrentCache.GetLookuper();
                    auto inserter = concurrentCache.GetInserter();

                    auto foundRef = lookuper(key);

                    if (!foundRef) {
                        auto value = NewWithExtraSpace<TElement>(&allocator, keyColumnCount);
                        memcpy(value.Get(), key, sizeof(TElement) + keyColumnCount);
                        auto inserted = static_cast<bool>(inserter.GetTable()->Insert(std::move(value)));

                        insertCount += inserted;

                        if (EnableLogging) {
                            Cout << Format("Thread: %v, [%v] %v",
                                threadId,
                                TStringBuf(key->Data, key->Size),
                                (inserted ? "inserted" : "not inserted")) << Endl;
                        }
                    } else {
                        auto found = foundRef.Get();
                        if (EnableLogging) {
                            Cout << Format("Thread: %v, [%v] found %v",
                                threadId,
                                TStringBuf(key->Data, key->Size),
                                TStringBuf(found->Data, found->Size)) << Endl;
                        }
                    }

                    if (index % tableSize == 0) {
                        Cout << Format("[%v] Completed: %v%% InsertCount: %v, FoundCount: %v\n",
                            threadId,
                            index * 100 / (iterations * tableSize),
                            insertCount,
                            index + 1 - insertCount);
                    }
                }
            })
            .AsyncVia(threadPool->GetInvoker())
            .Run());
        }

        AllSucceeded(asyncResults).Get();
        threadPool->Shutdown();

    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }
}

template <>
struct THash<TElement>
{
    inline size_t operator()(const TElement* value) const
    {
        return value->Hash;
    }
};

template <>
struct TEqualTo<TElement>
{
    inline bool operator()(const TElement* lhs, const TElement* rhs) const {
        return lhs->Hash == rhs->Hash &&
            lhs->Size == rhs->Size &&
            memcmp(lhs->Data, rhs->Data, lhs->Size) == 0;
    }
};
