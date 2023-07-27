#include <benchmark/benchmark.h>

#include <util/generic/string.h>
#include <util/generic/size_literals.h>

#include <yt/yt/library/ytprof/heap_profiler.h>
#include <yt/yt/library/ytprof/cpu_profiler.h>
#include <yt/yt/library/ytprof/backtrace.h>

#include <tcmalloc/common.h>

#include <absl/debugging/stacktrace.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NYTProf {
namespace {

////////////////////////////////////////////////////////////////////////////////

void BM_MemoryTagRead(benchmark::State& state)
{
    absl::SetStackUnwinder(AbslStackUnwinder);
    tcmalloc::MallocExtension::SetProfileSamplingRate(512_KB);

    std::vector<TString> data;
    for (int i = 0; i < state.range(); i++) {
        SetMemoryTag(i);
        data.push_back(TString(1024, 'x'));
        SetMemoryTag(0);
    }

    for (auto _ : state) {
        auto usage = GetEstimatedMemoryUsage();
        if (usage.size() < 2) {
            state.SkipWithError("seems like memory tags are not working");
        }
    }
}

BENCHMARK(BM_MemoryTagRead)
    ->Arg(1024 * 64);

////////////////////////////////////////////////////////////////////////////////

void BM_CpuProfilerOverhead(benchmark::State& state)
{
    TCpuProfilerOptions options;
    options.SamplingFrequency = state.range();
    TCpuProfiler profiler{options};

    if (IsProfileBuild()) {
        profiler.Start();        
    }

    for (auto _ : state) {
        NCrypto::TSha1Hasher hasher;
        hasher.Append("hello");
        benchmark::DoNotOptimize(hasher.GetDigest());
    }

    if (IsProfileBuild()) {
        profiler.Stop();
    }
}

BENCHMARK(BM_CpuProfilerOverhead)
    ->ArgName("frequency")
    ->UseRealTime()
    ->Range(10, 1000000);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTProf
