#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/keyed_heap.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

static const std::vector<i64>& GetKeys(i64 count)
{
    thread_local std::vector<i64> keys;
    if (std::ssize(keys) >= count) {
        return keys;
    }
    keys.resize(count);
    for (i64 i = 0; i < count; i++) {
        keys[i] = RandomNumber<ui64>();
    }
    return keys;
}

const auto Keys = GetKeys(100000);

void BM_KeyedHeapBench(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        TKeyedHeap<i64, i64> keyedHeap;
        for (auto key : Keys) {
            keyedHeap.Set(key, key);
            auto topValue = keyedHeap.TopValue();
            benchmark::DoNotOptimize(topValue);
        }
        total += Keys.size();
    }
    state.SetItemsProcessed(total);
}

BENCHMARK(BM_KeyedHeapBench);

} // namespace
} // namespace NYT::NFlow
