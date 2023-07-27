#include <benchmark/benchmark.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TDummy
    : public TRefCounted
{
    int Value;

    explicit TDummy(int value)
        : Value(value)
    { }
};

void RefCountedTracker_CreateDestroyObject(benchmark::State& state)
{
    while (state.KeepRunning()) {
        auto ref = New<TDummy>(state.iterations());
        benchmark::DoNotOptimize(ref);
    }
}

BENCHMARK(RefCountedTracker_CreateDestroyObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
