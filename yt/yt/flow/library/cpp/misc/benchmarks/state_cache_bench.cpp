#include <benchmark/benchmark.h>
#include <tcmalloc/malloc_extension.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/misc/enum.h>

#include <atomic>
#include <thread>
#include <vector>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStateCacheWorkload,
    (Churn)
    (CompressedRoundTrip)
    (CompressedMiss)
    (HotRoundTrip)
);

class TStateCacheBenchmarkValue
    : public IStateCacheValue
{
public:
    explicit TStateCacheBenchmarkValue(int payloadSize)
        : Payload_(payloadSize, 'x')
    {
        LiveCount.fetch_add(1, std::memory_order::relaxed);
    }

    ~TStateCacheBenchmarkValue() override
    {
        LiveCount.fetch_sub(1, std::memory_order::relaxed);
    }

    void Compress() override
    { }

    void Decompress() override
    { }

    i64 GetWeight() override
    {
        return sizeof(TStateCacheBenchmarkValue) + std::ssize(Payload_);
    }

    static inline std::atomic<i64> LiveCount = 0;

private:
    const std::vector<char> Payload_;
};

template <class TCallback>
void RunCacheThreads(int threadCount, TCallback callback)
{
    if (threadCount == 1) {
        callback(0);
        return;
    }
    std::vector<std::thread> threads;
    for (int threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
        threads.emplace_back(callback, threadIndex);
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

void RunStateCacheBenchmark(benchmark::State& state, EStateCacheWorkload workload)
{
    const int entryCount = static_cast<int>(state.range(0));
    const int payloadSize = static_cast<int>(state.range(1));
    const int threadCount = static_cast<int>(state.range(2));
    const int entriesPerThread = entryCount / threadCount;
    constexpr int PassCount = 4;
    const i64 operationCount = static_cast<i64>(entryCount) * PassCount;
    YT_VERIFY(entryCount % threadCount == 0);

    for (auto _ : state) {
        state.PauseTiming();
        {
            YT_VERIFY(TStateCacheBenchmarkValue::LiveCount.load() == 0);
            const auto allocatedBefore = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes");
            YT_VERIFY(allocatedBefore);
            const auto key = MakeKey(ui64{0});
            const i64 entryWeight = sizeof(TStateCacheBenchmarkValue) + payloadSize +
                sizeof(TJobId) + key.Underlying().GetSpaceUsed() + std::string("state").size();
            auto spec = New<TDynamicStateCacheSpec>();
            const i64 capacity = entryWeight * entryCount * (workload == EStateCacheWorkload::Churn ? 1 : 2);
            spec->UncompressedCacheWeight = NYTree::TSize(workload == EStateCacheWorkload::HotRoundTrip ? capacity : 0);
            spec->CompressedCacheWeight = NYTree::TSize(capacity);
            auto cache = New<TStateCache>(spec, NProfiling::TProfiler{});
            std::vector<TJobNamedStateCachePtr> jobCaches;
            std::vector<std::vector<TKey>> keys(threadCount);
            for (int threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
                const TJobId jobId(TGuid(/*part0*/ 0, /*part1*/ 0, /*part2*/ 0, threadIndex + 1));
                jobCaches.push_back(cache->WithJob(jobId, NProfiling::TProfiler{})->WithName("state"));
                if (workload != EStateCacheWorkload::Churn) {
                    for (int keyIndex = 0; keyIndex < entriesPerThread; ++keyIndex) {
                        const int offset = workload == EStateCacheWorkload::CompressedMiss ? 5 * entryCount : 0;
                        keys[threadIndex].push_back(MakeKey(static_cast<ui64>(offset + keyIndex)));
                    }
                }
            }
            RunCacheThreads(threadCount, [&] (int threadIndex) {
                for (int keyIndex = 0; keyIndex < entriesPerThread; ++keyIndex) {
                    jobCaches[threadIndex]->Insert(
                        MakeKey(static_cast<ui64>(keyIndex)),
                        New<TStateCacheBenchmarkValue>(payloadSize));
                }
            });

            state.ResumeTiming();
            RunCacheThreads(threadCount, [&] (int threadIndex) {
                for (int passIndex = 0; passIndex < PassCount; ++passIndex) {
                    for (int keyIndex = 0; keyIndex < entriesPerThread; ++keyIndex) {
                        if (workload == EStateCacheWorkload::Churn) {
                            jobCaches[threadIndex]->Insert(
                                MakeKey(static_cast<ui64>((passIndex + 1) * entriesPerThread + keyIndex)),
                                New<TStateCacheBenchmarkValue>(payloadSize));
                        } else if (workload == EStateCacheWorkload::CompressedMiss) {
                            YT_VERIFY(!jobCaches[threadIndex]->Extract(keys[threadIndex][keyIndex]));
                        } else {
                            auto value = jobCaches[threadIndex]->Extract(keys[threadIndex][keyIndex]);
                            YT_VERIFY(value);
                            jobCaches[threadIndex]->Insert(keys[threadIndex][keyIndex], std::move(value));
                        }
                    }
                }
            });
            state.PauseTiming();
            const auto allocatedAfter = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes");
            YT_VERIFY(allocatedAfter);
            const i64 liveCount = TStateCacheBenchmarkValue::LiveCount.load();
            state.counters["live_values"] = liveCount;
            state.counters["retained_bytes"] = *allocatedAfter - *allocatedBefore;
            state.counters["bytes_per_live_value"] = static_cast<double>(*allocatedAfter - *allocatedBefore) / liveCount;
            state.counters["operations"] = operationCount;
            state.counters["payload_bytes"] = payloadSize;
            state.counters["worker_threads"] = threadCount;
        }
        state.ResumeTiming();
    }
    state.SetItemsProcessed(state.iterations() * operationCount);
}

void BM_StateCacheChurn(benchmark::State& state)
{
    RunStateCacheBenchmark(state, EStateCacheWorkload::Churn);
}

void BM_StateCacheCompressedRoundTrip(benchmark::State& state)
{
    RunStateCacheBenchmark(state, EStateCacheWorkload::CompressedRoundTrip);
}

void BM_StateCacheCompressedMiss(benchmark::State& state)
{
    RunStateCacheBenchmark(state, EStateCacheWorkload::CompressedMiss);
}

void BM_StateCacheHotRoundTrip(benchmark::State& state)
{
    RunStateCacheBenchmark(state, EStateCacheWorkload::HotRoundTrip);
}

BENCHMARK(BM_StateCacheChurn)
    ->Args({100000, 340, 1})
    ->Args({100000, 1024, 1})
    ->Args({100000, 4096, 1})
    ->Args({100000, 1024, 4})
    ->Args({100000, 1024, 8})
    ->Iterations(1)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

BENCHMARK(BM_StateCacheCompressedRoundTrip)
    ->Args({20000, 1024, 1})
    ->Args({20000, 1024, 4})
    ->Args({20000, 1024, 8})
    ->Iterations(1)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

BENCHMARK(BM_StateCacheCompressedMiss)
    ->Args({20000, 1024, 1})
    ->Args({20000, 1024, 4})
    ->Args({20000, 1024, 8})
    ->Iterations(1)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

BENCHMARK(BM_StateCacheHotRoundTrip)
    ->Args({20000, 1024, 1})
    ->Args({20000, 1024, 8})
    ->Iterations(1)
    ->MeasureProcessCPUTime()
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
