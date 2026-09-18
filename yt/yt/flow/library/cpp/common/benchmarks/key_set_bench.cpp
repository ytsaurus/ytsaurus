#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <absl/container/flat_hash_set.h>

#include <util/generic/hash_set.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TFlatHashSet = absl::flat_hash_set<TKey, ::THash<TKey>, ::TEqualTo<TKey>>;

std::vector<TKey> MakeKeySequence(int batchSize, bool distinct)
{
    std::vector<TKey> keys;
    keys.reserve(batchSize);
    for (int index = 0; index < batchSize; ++index) {
        keys.push_back(MakeKey(static_cast<ui64>(distinct ? index : 0)));
    }
    return keys;
}

template <class TSet, bool Reserve = false>
void RunKeySetBenchmark(benchmark::State& state, bool distinct)
{
    const auto keys = MakeKeySequence(state.range(0), distinct);
    for (auto _ : state) {
        TSet keySet;
        if constexpr (Reserve) {
            keySet.reserve(keys.size());
        }
        for (const auto& key : keys) {
            keySet.insert(key);
        }
        benchmark::DoNotOptimize(keySet);
    }
    state.SetItemsProcessed(state.iterations() * std::ssize(keys));

    TSet keySet;
    if constexpr (Reserve) {
        keySet.reserve(keys.size());
    }
    keySet.insert(keys.begin(), keys.end());
    state.counters["capacity"] = keySet.bucket_count();
}

void BM_THashSetDistinct(benchmark::State& state)
{
    RunKeySetBenchmark<THashSet<TKey>>(state, true);
}

void BM_FlatHashSetDistinct(benchmark::State& state)
{
    RunKeySetBenchmark<TFlatHashSet>(state, true);
}

void BM_FlatHashSetReservedDistinct(benchmark::State& state)
{
    RunKeySetBenchmark<TFlatHashSet, true>(state, true);
}

void BM_THashSetRepeated(benchmark::State& state)
{
    RunKeySetBenchmark<THashSet<TKey>>(state, false);
}

void BM_FlatHashSetRepeated(benchmark::State& state)
{
    RunKeySetBenchmark<TFlatHashSet>(state, false);
}

void BM_FlatHashSetReservedRepeated(benchmark::State& state)
{
    RunKeySetBenchmark<TFlatHashSet, true>(state, false);
}

BENCHMARK(BM_THashSetDistinct)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_FlatHashSetDistinct)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_FlatHashSetReservedDistinct)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_THashSetRepeated)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_FlatHashSetRepeated)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_FlatHashSetReservedRepeated)->Arg(10)->Arg(100)->Arg(1000);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
