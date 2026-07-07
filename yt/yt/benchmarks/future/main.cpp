#include <yt/yt/core/actions/future.h>

#include <benchmark/benchmark.h>
#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int PromiseCount = 100000;

////////////////////////////////////////////////////////////////////////////////

void BM_PrintSizeofs(benchmark::State& state)
{
    for (auto _ : state) {
        std::cerr << "sizeof(TFutureState<void>) = " << sizeof(NYT::NDetail::TFutureState<void>) << std::endl;
        std::cerr << "sizeof(TFutureState<int>) = " << sizeof(NYT::NDetail::TFutureState<int>) << std::endl;
        std::cerr << "sizeof(TPromiseState<void>) = " << sizeof(NYT::NDetail::TPromiseState<void>) << std::endl;
        std::cerr << "sizeof(TPromiseState<int>) = " << sizeof(NYT::NDetail::TPromiseState<int>) << std::endl;

        std::cerr << "sizeof(TFutureState<void>::TVoidResultHandlers) = " << sizeof(NYT::NDetail::TFutureState<void>::TVoidResultHandlers) << std::endl;
        std::cerr << "sizeof(TFutureState<void>::TCancelHandlers) = " << sizeof(NYT::NDetail::TFutureState<void>::TCancelHandlers) << std::endl;

        std::cerr << "sizeof(TFutureState<void>::TResultHandlers) = " << sizeof(NYT::NDetail::TFutureState<int>::TResultHandlers) << std::endl;
        std::cerr << "sizeof(TFutureState<void>::TUniqueResultHandler) = " << sizeof(NYT::NDetail::TFutureState<int>::TUniqueResultHandler) << std::endl;
        std::cerr << "sizeof(std::optional<TErrorOr<int>>) = " << sizeof(std::optional<TErrorOr<int>>) << std::endl;

        std::cerr << std::endl;
    }
}

void BM_FutureIntWorkflow(benchmark::State& state)
{
    std::vector<int> indices(PromiseCount);
    std::iota(indices.begin(), indices.end(), 0);

    std::mt19937 rng(42);

    ssize_t total = 0;
    for (auto _ : state) {
        // Create promises.
        std::vector<TPromise<int>> promises;
        promises.reserve(PromiseCount);
        for (int i = 0; i < PromiseCount; ++i) {
            promises.push_back(NewPromise<int>());
        }

        // Check that they are not set.
        for (const auto& promise : promises) {
            benchmark::DoNotOptimize(promise.IsSet());
        }

        // Get futures from promises.
        std::vector<TFuture<int>> futures;
        futures.reserve(PromiseCount);
        for (const auto& promise : promises) {
            futures.push_back(promise.ToFuture());
        }

        // Shuffle indices for random order setting.
        std::shuffle(indices.begin(), indices.end(), rng);

        // Set promises in random order.
        for (int idx : indices) {
            promises[idx].Set(idx);
        }

        // Get values from futures.
        for (int i = 0; i < PromiseCount; ++i) {
            auto result = futures[i].BlockingGet();
            benchmark::DoNotOptimize(result);
        }

        total += PromiseCount;
    }
    state.SetItemsProcessed(total);
}

void BM_FutureVoidWorkflow(benchmark::State& state)
{
    std::vector<int> indices(PromiseCount);
    std::iota(indices.begin(), indices.end(), 0);

    std::mt19937 rng(42);

    ssize_t total = 0;
    for (auto _ : state) {
        // Create promises.
        std::vector<TPromise<void>> promises;
        promises.reserve(PromiseCount);
        for (int i = 0; i < PromiseCount; ++i) {
            promises.push_back(NewPromise<void>());
        }

        // Check that they are not set.
        for (const auto& promise : promises) {
            benchmark::DoNotOptimize(promise.IsSet());
        }

        // Get futures from promises.
        std::vector<TFuture<void>> futures;
        futures.reserve(PromiseCount);
        for (const auto& promise : promises) {
            futures.push_back(promise.ToFuture());
        }

        // Shuffle indices for random order setting.
        std::shuffle(indices.begin(), indices.end(), rng);

        // Set promises in random order.
        for (int idx : indices) {
            promises[idx].Set();
        }

        // Get values from futures.
        for (int i = 0; i < PromiseCount; ++i) {
            auto result = futures[i].BlockingGet();
            benchmark::DoNotOptimize(result);
        }

        total += PromiseCount;
    }
    state.SetItemsProcessed(total);
}

void BM_PromiseCreation(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (int i = 0; i < PromiseCount; ++i) {
            auto promise = NewPromise<int>();
            benchmark::DoNotOptimize(promise);
        }
        total += PromiseCount;
    }
    state.SetItemsProcessed(total);
}

void BM_PromiseSetAndGet(benchmark::State& state)
{
    ssize_t total = 0;
    for (auto _ : state) {
        for (int i = 0; i < PromiseCount; ++i) {
            auto promise = NewPromise<int>();
            auto future = promise.ToFuture();
            promise.Set(i);
            auto result = future.BlockingGet();
            benchmark::DoNotOptimize(result);
        }
        total += PromiseCount;
    }
    state.SetItemsProcessed(total);
}

////////////////////////////////////////////////////////////////////////////////

BENCHMARK(BM_PrintSizeofs)->Iterations(1);
BENCHMARK(BM_FutureIntWorkflow)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_FutureVoidWorkflow)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_PromiseCreation)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_PromiseSetAndGet)->Unit(benchmark::kMillisecond);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

int main(int argc, char** argv)
{
    // Run benchmarks.
    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
    return 0;
}
