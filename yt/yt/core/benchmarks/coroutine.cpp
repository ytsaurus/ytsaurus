#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/concurrency/coroutine.h>

#include <benchmark/benchmark.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

void Coroutine(TCoroutine<void()>& self)
{
    while (true) {
        self.Yield();
    }
}

void Coroutine_DoubleContextSwitch(benchmark::State& state)
{
    TCoroutine<void()> coroutine(BIND(&Coroutine));
    while (state.KeepRunning()) {
        coroutine.Run();
    }
}

BENCHMARK(Coroutine_DoubleContextSwitch);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
