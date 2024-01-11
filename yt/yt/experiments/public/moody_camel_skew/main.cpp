#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/count_down_latch.h>

#include <util/system/spinlock.h>

#include <random>

#include <thread>

using namespace NYT;
using namespace NConcurrency;
using namespace NThreading;
using namespace NProfiling;

void Spin(TCpuDuration duration)
{
    auto start = GetCpuInstant();
    while (GetCpuInstant() - start < duration) {
        SpinLockPause();
    }
}

void Spin(TDuration duration)
{
    Spin(DurationToCpuDuration(duration));
}

using TCountDownLatchPtr = NYT::TIntrusivePtr<TCountDownLatch>;

struct TTask final
{
    IInvokerPtr Invoker;
    TCountDownLatchPtr Latch;
    std::atomic<size_t> IterationsLeft;
    TCpuDuration SpinCpuDuration;
    TClosure Closure;

    TTask(
        IInvokerPtr invoker,
        TCountDownLatchPtr latch,
        size_t iterationCount,
        TDuration spinDuration)
        : Invoker(std::move(invoker))
        , Latch(std::move(latch))
        , IterationsLeft(iterationCount)
        , SpinCpuDuration(DurationToCpuDuration(spinDuration))
    {
        Closure = BIND(&TTask::Run, MakeStrong(this));
    }

    void Invoke() const
    {
        Invoker->Invoke(Closure);
    }

    void Run()
    {
        auto iteration = IterationsLeft--;
        YT_VERIFY(iteration > 0);

        Spin(SpinCpuDuration);

        if (iteration == 1) {
            Closure.Reset();
            Latch->CountDown();
        } else {
            Invoke();
        }
    }
};

int main(int /*argc*/, char** /*argv*/)
{
    size_t threadCount = 8;
    size_t iterations = 20;
    auto spinDuration = TDuration::MilliSeconds(100);

    auto threadPool = CreateThreadPool(threadCount, "Test");

    size_t taskCount = threadCount * 10;

    auto latch = New<TCountDownLatch>(taskCount);

    for (size_t index = 0; index < taskCount; ++index) {
        auto task = New<TTask>(
            threadPool->GetInvoker(),
            latch,
            iterations,
            spinDuration);
        task->Invoke();
    }

    auto fillProducersTime = spinDuration * taskCount;
    for (TDuration waitTime; waitTime < fillProducersTime; waitTime += TDuration::Seconds(1)) {
        Cout << Format("Waiting to fill per thread producers: %vs", (fillProducersTime - waitTime).Seconds()) << Endl;
        Sleep(TDuration::Seconds(1));
    }

    auto runActionsFromSizeThread = [&] {
        for (int i = 0; i < 10; ++i) {
            Cout << Format("Invoking action") << Endl;
            auto startTime = NProfiling::GetCpuInstant();

            auto future = BIND([=] {
                Cout << Format("Wait time: %v", NProfiling::CpuDurationToDuration(NProfiling::GetCpuInstant() - startTime)) << Endl;
            })
            .AsyncVia(threadPool->GetInvoker())
            .Run();

            WaitFor(future)
                .ThrowOnError();
        }
    };

// Run from main thread or from new thread.
#if 1
    runActionsFromSizeThread();
#else
    std::thread thread(runActionsFromSizeThread);
    auto finally = Finally([&] {
        thread.join();
    });
#endif

    latch->Wait();
    Cout << Format("Finishing") << Endl;

    return 0;
}

