#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <util/generic/cast.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

static const auto SleepQuantum = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

void PersistWaitRestore(TWallTimer& timer)
{
    TBlobOutput output;
    TSaveContext saveContext;
    saveContext.SetOutput(&output);
    Save(saveContext, timer);
    auto blob = output.Flush();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext;
    loadContext.SetInput(&input);
    Load(loadContext, timer);
}

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
        TFiberWallTimer cpuTimer;
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
        TFiberWallTimer cpuTimer;
        TWallTimer wallTimer;

        TDelayedExecutor::WaitForDuration(SleepQuantum);

        cpu = cpuTimer.GetElapsedValue();
        wall = wallTimer.GetElapsedValue();
    })
    .AsyncVia(invoker).Run()
    .Get();

    EXPECT_LT(cpu, 10 * 1000);
    EXPECT_GT(wall, 80 * 1000);
    EXPECT_LT(wall, 120 * 1000);
}

TEST_F(TTimerTest, PersistAndRestoreActive)
{
    auto invoker = Queue->GetInvoker();
    TValue wall = 0;

    TWallTimer wallTimer;
    TDelayedExecutor::WaitForDuration(SleepQuantum);

    PersistWaitRestore(wallTimer);

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wall = wallTimer.GetElapsedValue();

    EXPECT_GT(wall, 180 * 1000);
    EXPECT_LT(wall, 220 * 1000);
}

TEST_F(TTimerTest, PersistAndRestoreNonActive)
{
    auto invoker = Queue->GetInvoker();
    TValue wall = 0;

    TWallTimer wallTimer;
    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wallTimer.Stop();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    PersistWaitRestore(wallTimer);

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wallTimer.Start();

    TDelayedExecutor::WaitForDuration(SleepQuantum);

    wall = wallTimer.GetElapsedValue();

    EXPECT_GT(wall, 180 * 1000);
    EXPECT_LT(wall, 220 * 1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
