#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NProfiling {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TTimerTest
    : public ::testing::Test
{
protected:
    TLazyIntrusivePtr<TActionQueue> Queue;

    virtual void TearDown()
    {
        if (Queue.HasValue()) {
            Queue->Shutdown();
        }
    }
};

TEST_F(TTimerTest, CpuEmpty)
{
    auto invoker = Queue->GetInvoker();
    TValue cpu = 0;
    BIND([&] () {
        TCpuTimer cpuTimer;
        cpu = cpuTimer.GetElapsedValue();
    })
    .AsyncVia(invoker).Run()
    .Get();

    EXPECT_LT(cpu, 10 * 1000);
}

TEST_F(TTimerTest, CpuWallCompare)
{
    auto invoker = Queue->GetInvoker();
    TValue cpu = 0;
    TValue wall = 0;
    BIND([&] () {
        TCpuTimer cpuTimer;
        TWallTimer wallTimer;

        WaitFor(TDelayedExecutor::MakeDelayed(SleepQuantum))
            .ThrowOnError();

        cpu = cpuTimer.GetElapsedValue();
        wall = wallTimer.GetElapsedValue();
    })
    .AsyncVia(invoker).Run()
    .Get();

    EXPECT_LT(cpu, 10 * 1000);
    EXPECT_GT(wall, 100 * 1000);
    EXPECT_LT(wall, 120 * 1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NProfiling
} // namespace NYT
