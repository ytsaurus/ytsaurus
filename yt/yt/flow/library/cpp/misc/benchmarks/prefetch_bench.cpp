#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/misc/prefetch.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/system/types.h>

#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNode
{
    ui64 Payload = 0;
    char Padding[56] = {}; // pad to a cache line so each node touches its own line
};

constexpr size_t NodeCount = 1 << 20; // 1M * 64B = 64 MiB, well past the LLC

// Scattered nodes reached through a shuffled pointer vector: consecutive items[i] land on
// unrelated cache lines, so every dereference misses. Built once, reused across iterations.
const std::vector<TNode*>& ScatteredNodes()
{
    static const std::vector<TNode*> nodes = [] {
        std::vector<TNode*> result;
        result.reserve(NodeCount);
        for (size_t i = 0; i < NodeCount; ++i) {
            auto* node = new TNode;
            node->Payload = i;
            result.push_back(node);
        }
        std::shuffle(result.begin(), result.end(), std::mt19937_64(0x9e3779b97f4a7c15ULL));
        return result;
    }();
    return nodes;
}

// A large cold map plus a shuffled key stream, mirroring the dedup probe in DoMarkPersisted.
const std::pair<absl::flat_hash_map<ui64, ui64>, std::vector<ui64>>& MapAndKeys()
{
    static const auto data = [] {
        absl::flat_hash_map<ui64, ui64> map;
        map.reserve(NodeCount);
        std::vector<ui64> keys;
        keys.reserve(NodeCount);
        for (size_t i = 0; i < NodeCount; ++i) {
            ui64 key = i * 0x9e3779b97f4a7c15ULL; // spread across buckets
            map.emplace(key, i);
            keys.push_back(key);
        }
        std::shuffle(keys.begin(), keys.end(), std::mt19937_64(0xdeadbeefULL));
        return std::pair(std::move(map), std::move(keys));
    }();
    return data;
}

////////////////////////////////////////////////////////////////////////////////
// Pointer-chase over scattered nodes.

void BM_ChaseNoPrefetch(benchmark::State& state)
{
    const auto& nodes = ScatteredNodes();
    for (auto _ : state) {
        ui64 sum = 0;
        for (auto* node : nodes) {
            sum += node->Payload;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * nodes.size());
}

BENCHMARK(BM_ChaseNoPrefetch);

void BM_ChasePrefetch(benchmark::State& state)
{
    const auto& nodes = ScatteredNodes();
    auto prefetcher = MakePrefetcher()
        .Add(state.range(0), [] (TNode* node) {
            Y_PREFETCH_READ(node, 3);
        });
    for (auto _ : state) {
        ui64 sum = 0;
        prefetcher.ForEach(nodes, [&] (TNode* node) {
            sum += node->Payload;
        });
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * nodes.size());
}

BENCHMARK(BM_ChasePrefetch)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(24)->Arg(32);

////////////////////////////////////////////////////////////////////////////////
// flat_hash_map probe (the dedup pattern).

void BM_MapNoPrefetch(benchmark::State& state)
{
    const auto& [map, keys] = MapAndKeys();
    for (auto _ : state) {
        ui64 sum = 0;
        for (ui64 key : keys) {
            sum += map.find(key)->second;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * keys.size());
}

BENCHMARK(BM_MapNoPrefetch);

void BM_MapPrefetch(benchmark::State& state)
{
    const auto& [map, keys] = MapAndKeys();
    auto prefetcher = MakePrefetcher()
        .Add(state.range(0), [&] (ui64 key) {
            map.prefetch(key);
        });
    for (auto _ : state) {
        ui64 sum = 0;
        prefetcher.ForEach(keys, [&] (ui64 key) {
            sum += map.find(key)->second;
        });
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * keys.size());
}

BENCHMARK(BM_MapPrefetch)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(24)->Arg(32);

////////////////////////////////////////////////////////////////////////////////
// Warm cache: a small contiguous array that stays resident across iterations, so prefetch has
// nothing to hide. The gap between the two is the prefetcher's pure overhead (prologue, split loop,
// offset resolution, the wasted prefetch instruction). state.range(0) is the element count.

void BM_WarmNoPrefetch(benchmark::State& state)
{
    std::vector<ui64> data(state.range(0));
    std::iota(data.begin(), data.end(), 0);
    for (auto _ : state) {
        ui64 sum = 0;
        for (ui64 x : data) {
            sum += x;
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * data.size());
}

BENCHMARK(BM_WarmNoPrefetch)->Arg(8)->Arg(32)->Arg(128)->Arg(512)->Arg(2048);

void BM_WarmPrefetch(benchmark::State& state)
{
    std::vector<ui64> data(state.range(0));
    std::iota(data.begin(), data.end(), 0);
    auto prefetcher = MakePrefetcher()
        .Add([] (const ui64& x) {
            Y_PREFETCH_READ(&x, 3);
        });
    for (auto _ : state) {
        ui64 sum = 0;
        prefetcher.ForEach(data, [&] (ui64 x) {
            sum += x;
        });
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * data.size());
}

BENCHMARK(BM_WarmPrefetch)->Arg(8)->Arg(32)->Arg(128)->Arg(512)->Arg(2048);

// Same, but with a serial-dependency body that does not vectorize either way (like a real per-element
// hot loop). Here the gap is purely the prefetcher's machinery + the wasted prefetch instruction.

Y_FORCE_INLINE ui64 Mix(ui64 acc, ui64 x)
{
    return (acc ^ x) * 6364136223846793005ULL;
}

void BM_WarmScalarNoPrefetch(benchmark::State& state)
{
    std::vector<ui64> data(state.range(0));
    std::iota(data.begin(), data.end(), 0);
    for (auto _ : state) {
        ui64 acc = 0;
        for (ui64 x : data) {
            acc = Mix(acc, x);
        }
        benchmark::DoNotOptimize(acc);
    }
    state.SetItemsProcessed(state.iterations() * data.size());
}

BENCHMARK(BM_WarmScalarNoPrefetch)->Arg(8)->Arg(32)->Arg(128)->Arg(512)->Arg(2048);

void BM_WarmScalarPrefetch(benchmark::State& state)
{
    std::vector<ui64> data(state.range(0));
    std::iota(data.begin(), data.end(), 0);
    auto prefetcher = MakePrefetcher()
        .Add([] (const ui64& x) {
            Y_PREFETCH_READ(&x, 3);
        });
    for (auto _ : state) {
        ui64 acc = 0;
        prefetcher.ForEach(data, [&] (ui64 x) {
            acc = Mix(acc, x);
        });
        benchmark::DoNotOptimize(acc);
    }
    state.SetItemsProcessed(state.iterations() * data.size());
}

BENCHMARK(BM_WarmScalarPrefetch)->Arg(8)->Arg(32)->Arg(128)->Arg(512)->Arg(2048);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
