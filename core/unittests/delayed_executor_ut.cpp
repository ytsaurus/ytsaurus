#include <yt/core/test_framework/framework.h>
#include <yt/core/test_framework/probe.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TDelayedExecutorTest, Submit)
{
    auto fired = std::make_shared<std::atomic<int>>(0);
    auto state = std::make_shared<TProbeState>();

    auto cookie = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(100));

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, *fired);

    Sleep(TDuration::MilliSeconds(70));

    EXPECT_EQ(1, *fired);
    EXPECT_EQ(1, state->Constructors);
    EXPECT_EQ(1, state->Destructors);
}

TEST(TDelayedExecutorTest, SubmitZeroDelay)
{
    auto fired = std::make_shared<std::atomic<int>>(0);
    auto state = std::make_shared<TProbeState>();

    auto cookie1 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(0));

    Sleep(TDuration::MilliSeconds(10));

    EXPECT_EQ(1, *fired);

    auto cookie2 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())]() { ++*fired; }),
        TDuration::MilliSeconds(10));

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(2, *fired);
    EXPECT_EQ(2, state->Constructors);
    EXPECT_EQ(2, state->Destructors);
}

TEST(TDelayedExecutorTest, StressTest)
{
    auto fired = std::make_shared<std::atomic<int>>(0);

    int total = 100;
    for (int i = 0; i < total; ++i) {
        auto start = TInstant::Now();
        auto delay = rand() % 50;

        auto cookie = TDelayedExecutor::Submit(
            BIND([start, delay, fired] () {
                auto diff = (TInstant::Now() - start).MilliSeconds();
                EXPECT_LE(delay, diff + 10);
                EXPECT_LE(diff, delay + 50);
                ++*fired;
            }),
            TDuration::MilliSeconds(delay));

        Sleep(TDuration::MilliSeconds(rand() % 50));
    }

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(total, *fired);
}

TEST(TDelayedExecutorTest, SubmitAndCancel)
{
    auto fired = std::make_shared<std::atomic<int>>(0);
    auto state = std::make_shared<TProbeState>();

    auto cookie = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::CancelAndClear(cookie);

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, *fired);
    EXPECT_EQ(1, state->Constructors);
    EXPECT_EQ(1, state->Destructors);
}

// Here we shutdown global singleton, so this test should be run in isolation.
TEST(TDelayedExecutorTest, DISABLED_SubmitAfterShutdown)
{
    auto fired = std::make_shared<std::atomic<int>>(0);
    auto state = std::make_shared<TProbeState>();

    auto cookie1 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::StaticShutdown();

    auto cookie2 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(10));

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, *fired);
    EXPECT_EQ(2, state->Constructors);
    EXPECT_EQ(2, state->Destructors);
}

// Here we shutdown global singleton, so this test should be run in isolation.
TEST(TDelayedExecutorTest, DISABLED_SubmitAndCancelAfterShutdown)
{
    auto fired = std::make_shared<std::atomic<int>>(0);
    auto state = std::make_shared<TProbeState>();

    auto cookie1 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::StaticShutdown();

    auto cookie2 = TDelayedExecutor::Submit(
        BIND([fired, state, probe = TProbe(state.get())] () { ++*fired; }),
        TDuration::MilliSeconds(10));

    TDelayedExecutor::CancelAndClear(cookie1);
    TDelayedExecutor::CancelAndClear(cookie2);

    Sleep(TDuration::MilliSeconds(50));

    EXPECT_EQ(0, *fired);
    EXPECT_EQ(2, state->Constructors);
    EXPECT_EQ(2, state->Destructors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
