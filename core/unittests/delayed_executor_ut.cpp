#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TDelayedExecutorTest, Submit)
{
    std::atomic<int> fired = {0};
    TProbeState state;

    auto cookie = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(1, fired);
    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
}

TEST(TDelayedExecutorTest, SubmitAndCancel)
{
    std::atomic<int> fired = {0};
    TProbeState state;

    auto cookie = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::CancelAndClear(cookie);

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, fired);
    EXPECT_EQ(1, state.Constructors);
    EXPECT_EQ(1, state.Destructors);
}

// Here we shutdown global singleton, so this test should be run in isolation.
TEST(TDelayedExecutorTest, DISABLED_SubmitAfterShutdown)
{
    std::atomic<int> fired = {0};
    TProbeState state;

    auto cookie1 = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::StaticShutdown();

    auto cookie2 = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, fired);
    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(2, state.Destructors);
}

// Here we shutdown global singleton, so this test should be run in isolation.
TEST(TDelayedExecutorTest, DISABLED_SubmitAndCancelAfterShutdown)
{
    std::atomic<int> fired = {0};
    TProbeState state;

    auto cookie1 = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::StaticShutdown();

    auto cookie2 = TDelayedExecutor::Submit(
        BIND([&fired, probe = TProbe(&state)] () { ++fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::CancelAndClear(cookie1);
    TDelayedExecutor::CancelAndClear(cookie2);

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, fired);
    EXPECT_EQ(2, state.Constructors);
    EXPECT_EQ(2, state.Destructors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

