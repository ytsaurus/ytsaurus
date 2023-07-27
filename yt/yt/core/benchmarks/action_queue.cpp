#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <benchmark/benchmark.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBenchmark
    : public benchmark::Fixture
{
public:
    void Begin(benchmark::State& /*state*/)
    {
        if (ThreadCount_++ == 0) {
            Items_ = 0;
            ActionQueue_ = New<TActionQueue>();
        }
    }

    void End(benchmark::State& state)
    {
        state.SetItemsProcessed(GetItems());
        state.SetBytesProcessed(state.iterations());
        if (--ThreadCount_ == 0) {
            ActionQueue_->Shutdown();
            ActionQueue_.Reset();
        }
    }

    void Invoke()
    {
        ActionQueue_->GetInvoker()->Invoke(BIND([this] {
            ++Items_;
        }));
    }

    int GetItems()
    {
        int i = Items_.exchange(-1000000, std::memory_order::relaxed);
        return i > 0 ? i : 0;
    }

private:
    TActionQueuePtr ActionQueue_;
    std::atomic<int> ThreadCount_ = 0;
    std::atomic<int> Items_ = 0;
};

BENCHMARK_DEFINE_F(TActionQueueBenchmark, Invoke)(benchmark::State& state)
{
    Begin(state);
    while (state.KeepRunning()) {
        Invoke();
    }
    End(state);
}

BENCHMARK_REGISTER_F(TActionQueueBenchmark, Invoke)
    ->ThreadRange(1, 16)
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
