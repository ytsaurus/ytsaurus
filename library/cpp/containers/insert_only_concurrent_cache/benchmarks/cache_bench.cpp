#include <benchmark/benchmark.h>

#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/containers/lock_free_map/harris_michael_hashtable.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace {

////////////////////////////////////////////////////////////////////////////////
// Saturation workload: a small fixed set of keys is find-or-inserted from different threads.
// This is the primary use-case for TInsertOnlyConcurrentCache.
//
// The key count is passed as a benchmark parameter (state.range(0)):
//   1  — extreme hot-key case (single key, always the same slot)
//   64 — realistic case (small set, fits in L1/L2)
//
// Key counts must be powers of 2 so that RunBench can use a bitmask instead of %.
////////////////////////////////////////////////////////////////////////////////

constexpr int OpsPerThread = 10000;

////////////////////////////////////////////////////////////////////////////////

struct TSmallHashMapFindFixture
    : public benchmark::Fixture
{
    std::unique_ptr<TInsertOnlyConcurrentCache<int, int>> InsertOnlyCache;
    TConcurrentHashMap<int, int> ConcurrentHashMap;
    THashMap<int, int> HashMap;
    NLockFreeMap::THarrisMichaelMap<int, int, 64> HarrisMichaelMap;

    // HashMap protected by a single spinlock — baseline for "trivial mutex" cost.
    THashMap<int, int> HashMapLocked;
    TSpinLock HashMapLock_;

    std::vector<int> Keys;

public:
    void SetUp(benchmark::State& state) override
    {
        int keyCount = static_cast<int>(state.range(0));

        if (static_cast<int>(Keys.size()) == keyCount) {
            return; // Already initialized for this key count.
        }

        Y_ABORT_UNLESS(keyCount > 0 && (keyCount & (keyCount - 1)) == 0,
            "Key count must be a power of 2 for bitmask indexing");

        Keys.resize(keyCount);
        for (int i = 0; i < keyCount; ++i) {
            Keys[i] = i;
        }

        InsertOnlyCache = std::make_unique<TInsertOnlyConcurrentCache<int, int>>();
        ConcurrentHashMap = {};
        HashMap.clear();
        HashMap.reserve(keyCount * 2);
        HashMapLocked.clear();
        HashMapLocked.reserve(keyCount * 2);

        // THashMap is not thread-safe, so warm it up in SetUp.
        for (int k : Keys) {
            HashMap.emplace(k, k * 7);
            HashMapLocked.emplace(k, k * 7);
        }
    }

    // Runs |lookup(key)| OpsPerThread times per iteration, cycling through Keys.
    // Accounts for thread index so threads access different keys.
    template <class TLookup>
    void RunBench(benchmark::State& state, TLookup lookup)
    {
        const int t = state.thread_index();
        const int keyMask = static_cast<int>(state.range(0)) - 1;
        for (auto _ : state) {
            for (int i = 0; i < OpsPerThread; ++i) {
                benchmark::DoNotOptimize(lookup(Keys[(i + t) & keyMask]));
            }
        }
        state.SetItemsProcessed(state.iterations() * OpsPerThread);
    }
};

////////////////////////////////////////////////////////////////////////////////

// clang-format off

BENCHMARK_DEFINE_F(TSmallHashMapFindFixture, InsertOnlyCache)(benchmark::State& state)
{
    RunBench(state, [&] (int k) {
        return &InsertOnlyCache->FindOrInsert(k, [k] { return k * 7; });
    });
}

BENCHMARK_DEFINE_F(TSmallHashMapFindFixture, ConcurrentHashMap)(benchmark::State& state)
{
    RunBench(state, [&] (int k) {
        return &ConcurrentHashMap.InsertIfAbsent(k, k * 7);
    });
}

BENCHMARK_DEFINE_F(TSmallHashMapFindFixture, HashMap)(benchmark::State& state)
{
    RunBench(state, [&] (int k) {
        return HashMap.find(k);
    });
}

BENCHMARK_DEFINE_F(TSmallHashMapFindFixture, HashMapLocked)(benchmark::State& state)
{
    RunBench(state, [&] (int k) {
        TGuard<TSpinLock> guard(HashMapLock_);
        return HashMapLocked.find(k);
    });
}

BENCHMARK_DEFINE_F(TSmallHashMapFindFixture, HarrisMichaelMap)(benchmark::State& state)
{
    RunBench(state, [&] (int k) {
        auto it = HarrisMichaelMap.find(k);
        if (it != HarrisMichaelMap.end()) {
            return &it->second;
        }
        HarrisMichaelMap.emplace(k, k * 7);
        return &HarrisMichaelMap.find(k)->second;
    });
}

// clang-format on

////////////////////////////////////////////////////////////////////////////////

BENCHMARK_REGISTER_F(TSmallHashMapFindFixture, InsertOnlyCache)->Arg(1)->Arg(64)->Threads(1)->Threads(4);
BENCHMARK_REGISTER_F(TSmallHashMapFindFixture, ConcurrentHashMap)->Arg(1)->Arg(64)->Threads(1)->Threads(4);
BENCHMARK_REGISTER_F(TSmallHashMapFindFixture, HashMap)->Arg(1)->Arg(64)->Threads(1)->Threads(4);
BENCHMARK_REGISTER_F(TSmallHashMapFindFixture, HashMapLocked)->Arg(1)->Arg(64)->Threads(1)->Threads(4);
BENCHMARK_REGISTER_F(TSmallHashMapFindFixture, HarrisMichaelMap)->Arg(1)->Arg(64)->Threads(1)->Threads(4);

////////////////////////////////////////////////////////////////////////////////

} // namespace
