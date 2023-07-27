#include <yt/yt/core/profiling/timing.h>

#include <benchmark/benchmark.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Timing_GetCpuInstant(benchmark::State& state)
{
    while (state.KeepRunning()) {
        GetCpuInstant();
    }
}

BENCHMARK(Timing_GetCpuInstant);

void Timing_GetInstant(benchmark::State& state)
{
    while (state.KeepRunning()) {
        GetInstant();
    }
}

BENCHMARK(Timing_GetInstant);

void Timing_InstantNow(benchmark::State& state)
{
    while (state.KeepRunning()) {
        TInstant::Now();
    }
}

BENCHMARK(Timing_InstantNow);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
