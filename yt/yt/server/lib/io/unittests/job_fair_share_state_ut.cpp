#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/job_fair_share_state.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <limits>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto Window = TDuration::Seconds(64);

TCpuInstant AddDuration(TCpuInstant instant, TDuration duration)
{
    return instant + DurationToCpuDuration(duration);
}

void AddLocalIO(const TIOJobStatePtr& state, i64 cost)
{
    state->Reserve(cost);
    state->Complete(cost);
}

////////////////////////////////////////////////////////////////////////////////

struct TCompareJobIOConsumptionsTestCase
{
    i64 LhsConsumption = 0;
    double LhsWeight = 0.0;
    i64 RhsConsumption = 0;
    double RhsWeight = 0.0;
    std::partial_ordering Expected;
};

class TCompareJobIOConsumptionsTest
    : public ::testing::TestWithParam<TCompareJobIOConsumptionsTestCase>
{ };

TEST_P(TCompareJobIOConsumptionsTest, Compare)
{
    const auto& testCase = GetParam();
    EXPECT_EQ(
        CompareJobIOConsumptions(testCase.LhsConsumption, testCase.LhsWeight, testCase.RhsConsumption, testCase.RhsWeight),
        testCase.Expected);
}

INSTANTIATE_TEST_SUITE_P(
    TCompareJobIOConsumptionsTest,
    TCompareJobIOConsumptionsTest,
    ::testing::Values(
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 10, .LhsWeight = 1.0, .RhsConsumption = 30, .RhsWeight = 2.0, .Expected = std::partial_ordering::less},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 30, .LhsWeight = 2.0, .RhsConsumption = 10, .RhsWeight = 1.0, .Expected = std::partial_ordering::greater},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 10, .LhsWeight = 1.0, .RhsConsumption = 20, .RhsWeight = 2.0, .Expected = std::partial_ordering::equivalent},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 0, .LhsWeight = 0.0, .RhsConsumption = 100, .RhsWeight = 1.0, .Expected = std::partial_ordering::greater},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 100, .LhsWeight = 1.0, .RhsConsumption = 0, .RhsWeight = 0.0, .Expected = std::partial_ordering::less},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 10, .LhsWeight = 0.0, .RhsConsumption = 20, .RhsWeight = 0.0, .Expected = std::partial_ordering::less},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 20, .LhsWeight = 0.0, .RhsConsumption = 10, .RhsWeight = 0.0, .Expected = std::partial_ordering::greater},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 10, .LhsWeight = 0.0, .RhsConsumption = 10, .RhsWeight = 0.0, .Expected = std::partial_ordering::equivalent},
        TCompareJobIOConsumptionsTestCase{.LhsConsumption = 2, .LhsWeight = 1e308, .RhsConsumption = 3, .RhsWeight = 1e308, .Expected = std::partial_ordering::less}));

////////////////////////////////////////////////////////////////////////////////

TEST(TCompareHeadRequestJobsTest, Compare)
{
    auto lighterJob = New<TIOJobState>(Window);
    auto heavierJob = New<TIOJobState>(Window);
    const THashMap<TIOJobState*, TJobIOConsumption> consumptions = {
        {lighterJob.Get(), {.Consumption = 10, .Weight = 1.0}},
        {heavierJob.Get(), {.Consumption = 30, .Weight = 1.0}},
    };

    EXPECT_EQ(CompareHeadRequestJobs(/*lhsJobState*/ nullptr, lighterJob.Get(), consumptions), std::partial_ordering::less);
    EXPECT_EQ(CompareHeadRequestJobs(lighterJob.Get(), /*rhsJobState*/ nullptr, consumptions), std::partial_ordering::greater);
    EXPECT_EQ(CompareHeadRequestJobs(/*lhsJobState*/ nullptr, /*rhsJobState*/ nullptr, consumptions), std::partial_ordering::equivalent);

    EXPECT_EQ(CompareHeadRequestJobs(lighterJob.Get(), heavierJob.Get(), consumptions), std::partial_ordering::less);
    EXPECT_EQ(CompareHeadRequestJobs(heavierJob.Get(), lighterJob.Get(), consumptions), std::partial_ordering::greater);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TIOJobStateTest, ReportDecaysLinearly)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);
    state->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 2.0, now);

    EXPECT_EQ(state->ComputeConsumption(now).Consumption, 1000);
    EXPECT_EQ(state->ComputeConsumption(now).Weight, 2.0);
    EXPECT_EQ(state->ComputeConsumption(now - 1).Consumption, 1000);
    EXPECT_NEAR(state->ComputeConsumption(AddDuration(now, Window / 4)).Consumption, 750, 1);
    EXPECT_NEAR(state->ComputeConsumption(AddDuration(now, Window / 2)).Consumption, 500, 1);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(now, Window)).Consumption, 0);
}

TEST(TIOJobStateTest, ConsumptionSaturatesWithReportedIO)
{
    auto now = GetCpuInstant();
    auto maxConsumption = std::numeric_limits<i64>::max();
    auto state = New<TIOJobState>(Window);
    state->UpdateReport(maxConsumption, /*weight*/ 1.0, now);
    state->Reserve(/*cost*/ 1);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, maxConsumption);

    state->UpdateReport(maxConsumption - 2, /*weight*/ 1.0, now);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, maxConsumption - 1);
    state->Reserve(/*cost*/ 4);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, maxConsumption);
    state->Complete(/*cost*/ 5);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, maxConsumption - 2);
}

TEST(TIOJobStateTest, NewerReportReplacesOlder)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);
    state->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 1.0, now);
    state->UpdateReport(/*ioConsumed*/ 100, /*weight*/ 3.0, now + 1);

    auto consumption = state->ComputeConsumption(now + 1);
    EXPECT_EQ(consumption.Consumption, 100);
    EXPECT_EQ(consumption.Weight, 3.0);

    state->UpdateReport(/*ioConsumed*/ 10, /*weight*/ 2.0, now + 1);
    consumption = state->ComputeConsumption(now + 1);
    EXPECT_EQ(consumption.Consumption, 10);
    EXPECT_EQ(consumption.Weight, 2.0);
}

TEST(TIOJobStateTest, OlderReportIsIgnored)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);
    state->UpdateReport(/*ioConsumed*/ 100, /*weight*/ 3.0, now);
    state->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 1.0, now - 1);

    auto consumption = state->ComputeConsumption(now);
    EXPECT_EQ(consumption.Consumption, 100);
    EXPECT_EQ(consumption.Weight, 3.0);
}

TEST(TIOJobStateTest, ReservationMovesToLocalHistoryOnCompletion)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);

    state->Reserve(/*cost*/ 100);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, 100);

    state->Complete(/*cost*/ 100);
    auto completedBy = GetCpuInstant();
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, 100);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(completedBy, Window)).Consumption, 0);
}

TEST(TIOJobStateTest, ConsumptionIsMaxOfReportAndLocalHistoryPlusReservation)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);
    state->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 1.0, now);
    AddLocalIO(state, /*cost*/ 300);
    state->Reserve(/*cost*/ 40);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, 1000 + 40);

    AddLocalIO(state, /*cost*/ 900);
    EXPECT_EQ(state->ComputeConsumption(now).Consumption, 1200 + 40);
}

TEST(TIOJobStateTest, LocalHistoryExpiresGradually)
{
    auto state = New<TIOJobState>(Window);
    auto beforeFirst = GetCpuInstant();
    AddLocalIO(state, /*cost*/ 100);
    auto afterFirst = GetCpuInstant();
    AddLocalIO(state, /*cost*/ 10);
    auto afterSecond = GetCpuInstant();

    EXPECT_EQ(state->ComputeConsumption(beforeFirst).Consumption, 110);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(beforeFirst, Window) - 1).Consumption, 110);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterFirst, Window)).Consumption, 10);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterSecond, Window)).Consumption, 0);
}

TEST(TIOJobStateTest, ExpiresWhenIndistinguishableFromNewState)
{
    auto now = GetCpuInstant();
    auto state = New<TIOJobState>(Window);
    EXPECT_TRUE(state->IsExpired(now));

    state->UpdateReport(/*ioConsumed*/ 100, /*weight*/ 1.0, now);
    EXPECT_FALSE(state->IsExpired(now));
    EXPECT_TRUE(state->IsExpired(AddDuration(now, Window)));

    state->AcquireRequest();
    EXPECT_FALSE(state->IsExpired(AddDuration(now, Window)));
    state->ReleaseRequest();

    state->Reserve(/*cost*/ 10);
    EXPECT_FALSE(state->IsExpired(AddDuration(now, Window)));
    auto beforeCompletion = GetCpuInstant();
    state->Complete(/*cost*/ 10);
    auto afterCompletion = GetCpuInstant();
    EXPECT_FALSE(state->IsExpired(AddDuration(beforeCompletion, Window) - 1));
    EXPECT_TRUE(state->IsExpired(AddDuration(afterCompletion, Window)));
}

TEST(TIOJobStateTest, ZeroReportIsRetainedForWindow)
{
    auto now = GetCpuInstant();
    auto snapshotAt = AddDuration(now, TDuration::Seconds(1));
    auto state = New<TIOJobState>(Window);

    // A report with zero consumption must still reject older reports,
    // so the state is retained for the whole window of the report.
    state->UpdateReport(/*ioConsumed*/ 0, /*weight*/ 1.0, snapshotAt);
    state->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 1.0, now);
    EXPECT_EQ(state->ComputeConsumption(snapshotAt).Consumption, 0);

    auto expiresAt = AddDuration(snapshotAt, Window);
    EXPECT_FALSE(state->IsExpired(expiresAt - 1));
    EXPECT_TRUE(state->IsExpired(expiresAt));
}

TEST(TIOJobStateTest, WindowChangeAppliesToReportAndLocalHistory)
{
    auto now = GetCpuInstant();

    auto reportedState = New<TIOJobState>(Window);
    reportedState->UpdateReport(/*ioConsumed*/ 1000, /*weight*/ 1.0, now);
    auto localState = New<TIOJobState>(Window);
    AddLocalIO(localState, /*cost*/ 100);
    auto completedBy = GetCpuInstant();
    localState->Reserve(/*cost*/ 10);

    reportedState->SetWindow(2 * Window, now);
    localState->SetWindow(2 * Window, now);

    EXPECT_NEAR(reportedState->ComputeConsumption(AddDuration(now, Window)).Consumption, 500, 1);

    EXPECT_EQ(localState->ComputeConsumption(AddDuration(now, 3 * Window / 2)).Consumption, 100 + 10);
    EXPECT_EQ(localState->ComputeConsumption(AddDuration(completedBy, 2 * Window)).Consumption, 10);
}

TEST(TIOJobStateTest, WindowChangeRetainsCompletionAfterSampledTime)
{
    auto state = New<TIOJobState>(Window);
    auto beforeCompletion = GetCpuInstant();
    AddLocalIO(state, /*cost*/ 100);
    auto afterCompletion = GetCpuInstant();
    auto sampledAt = beforeCompletion - DurationToCpuDuration(Window / 2);

    state->SetWindow(2 * Window, sampledAt);

    EXPECT_EQ(state->ComputeConsumption(AddDuration(beforeCompletion, 2 * Window) - 1).Consumption, 100);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterCompletion, 2 * Window)).Consumption, 0);
}

TEST(TIOJobStateTest, CompletionAfterWindowChangeUsesCurrentTime)
{
    constexpr auto ShrunkWindow = TDuration::MilliSeconds(1);

    auto state = New<TIOJobState>(Window);
    auto sampledAt = GetCpuInstant() - DurationToCpuDuration(Window);
    state->SetWindow(ShrunkWindow, sampledAt);
    auto beforeCompletion = GetCpuInstant();

    AddLocalIO(state, /*cost*/ 100);
    auto afterCompletion = GetCpuInstant();

    EXPECT_EQ(state->ComputeConsumption(AddDuration(beforeCompletion, ShrunkWindow) - 1).Consumption, 100);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterCompletion, ShrunkWindow)).Consumption, 0);
}

TEST(TIOJobStateTest, WindowShrinkRetainsCompletionAfterSampledTime)
{
    constexpr auto ShrunkWindow = TDuration::MilliSeconds(1);

    auto state = New<TIOJobState>(Window);
    auto beforeCompletion = GetCpuInstant();
    AddLocalIO(state, /*cost*/ 100);
    auto afterCompletion = GetCpuInstant();
    auto sampledAt = beforeCompletion - DurationToCpuDuration(2 * ShrunkWindow);

    state->SetWindow(ShrunkWindow, sampledAt);

    EXPECT_EQ(state->ComputeConsumption(AddDuration(beforeCompletion, ShrunkWindow) - 1).Consumption, 100);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterCompletion, ShrunkWindow)).Consumption, 0);
}

TEST(TIOJobStateTest, WindowShrinkDropsLocalHistoryOutsideWindow)
{
    constexpr auto ShrunkWindow = TDuration::MilliSeconds(1);

    auto state = New<TIOJobState>(Window);
    AddLocalIO(state, /*cost*/ 10);
    auto afterFirst = GetCpuInstant();
    AddLocalIO(state, /*cost*/ 100);
    auto afterSecond = GetCpuInstant();
    auto changedAt = AddDuration(afterFirst, ShrunkWindow);

    state->SetWindow(ShrunkWindow, changedAt);

    EXPECT_EQ(state->ComputeConsumption(changedAt).Consumption, 100);
    EXPECT_EQ(state->ComputeConsumption(AddDuration(afterSecond, ShrunkWindow)).Consumption, 0);
}

TEST(TIOJobStateTest, WindowGrowthDoesNotRestoreExpiredCompletions)
{
    auto state = New<TIOJobState>(Window);
    AddLocalIO(state, /*cost*/ 100);
    auto changedAt = AddDuration(GetCpuInstant(), Window);

    state->SetWindow(2 * Window, changedAt);

    EXPECT_EQ(state->ComputeConsumption(changedAt).Consumption, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
