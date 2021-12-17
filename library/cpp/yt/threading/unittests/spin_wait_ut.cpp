#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/threading/spin_wait.h>
#include <library/cpp/yt/threading/spin_wait_hook.h>

#include <thread>

namespace NYT::NThreading {
namespace {

////////////////////////////////////////////////////////////////////////////////

bool SpinWaitSlowPathHookInvoked;

void SpinWaitSlowPathHook(TCpuDuration cpuDelay)
{
    SpinWaitSlowPathHookInvoked = true;
    auto delay = CpuDurationToDuration(cpuDelay);
    EXPECT_GE(delay, TDuration::Seconds(1));
    EXPECT_LE(delay, TDuration::Seconds(5));
}

TEST(TSpinWaitTest, SlowPathHook)
{
    SetSpinWaitSlowPathHook(SpinWaitSlowPathHook);
    SpinWaitSlowPathHookInvoked = false;
    {
        TSpinWait spinWait;
        for (int i = 0; i < 1'000'000; ++i) {
            spinWait.Wait();
        }
    }
    SetSpinWaitSlowPathHook(nullptr);
    EXPECT_TRUE(SpinWaitSlowPathHookInvoked);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NThreading
