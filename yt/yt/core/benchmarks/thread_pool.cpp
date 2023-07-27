#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <benchmark/benchmark.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TThreadPoolBenchmark
    : public benchmark::Fixture
{
public:
    void Begin(benchmark::State& state)
    {
        if (ThreadCount_++ == 0) {
            Items_ = 0;
            ThreadPool_ = CreateThreadPool(state.range(0), "Bench");
        }
    }

    void End(benchmark::State& state)
    {
        state.SetItemsProcessed(GetItems());
        if (--ThreadCount_ == 0) {
            ThreadPool_->Shutdown();
            ThreadPool_.Reset();
        }
    }

    template <typename F>
    void Invoke(F&& f)
    {
        ThreadPool_->GetInvoker()->Invoke(BIND(std::forward<F>(f)));
    }

    void InvokeUpdateItems()
    {
        Invoke([this] {
            Items_.fetch_add(1, std::memory_order::relaxed);
        });
    }

    int GetItems()
    {
        int i = Items_.exchange(-1000000, std::memory_order::relaxed);
        return i > 0 ? i : 0;
    }

private:
    IThreadPoolPtr ThreadPool_;
    std::atomic<int> ThreadCount_ {0};
    std::atomic<int> Items_ {0};
};

BENCHMARK_DEFINE_F(TThreadPoolBenchmark, Operations)(benchmark::State& state)
{
    Begin(state);
    while (state.KeepRunning()) {
        InvokeUpdateItems();
    }
    End(state);
}

BENCHMARK_REGISTER_F(TThreadPoolBenchmark, Operations)
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)
    ->ThreadRange(1 << 0, 1 << 4)
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

void BM_ThreadPoolScaling(benchmark::State& state)
{
    static IThreadPoolPtr ThreadPool;

    if (state.thread_index() == 0) {
        // Add extra sleeping threads.
        ThreadPool = CreateThreadPool(state.threads() + state.range(0), "Scaling");
    }

    while (state.KeepRunning()) {
        BIND([&] {
            for (int i = 0; i < 256; i++) {
                Yield();
            }
        })
            .AsyncVia(ThreadPool->GetInvoker())
            .Run()
            .Get();
    }

    if (state.thread_index() == 0) {
        ThreadPool->Shutdown();
        ThreadPool.Reset();
    }
}

BENCHMARK(BM_ThreadPoolScaling)
    ->Threads(4)
    ->DenseRange(1, 32)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
