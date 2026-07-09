#include <yt/yt/flow/library/cpp/misc/ema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

// Returns alpha = 1 - exp(-dt / halfLife) where halfLife = windowSec / 2.
double EmaAlpha(double dtSeconds, double windowSec)
{
    return 1.0 - std::exp(-dtSeconds / (windowSec / 2.0));
}

////////////////////////////////////////////////////////////////////////////////
// TEma tests
////////////////////////////////////////////////////////////////////////////////

TEST(TEmaTest, EmptyBeforeAnySet)
{
    // Average() must return nullopt before the first Set().
    TEma<double> ema(TDuration::Seconds(10));
    EXPECT_FALSE(ema.Average().has_value());
}

TEST(TEmaTest, NotWarmBeforeFullWindow)
{
    // Average() returns nullopt until a full window has elapsed since the
    // first Set().
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(42.0, t0);

    // One millisecond before the window expires — not warm yet.
    ema.Set(42.0, t0 + window - TDuration::MilliSeconds(1));
    EXPECT_FALSE(ema.Average().has_value());
}

TEST(TEmaTest, WarmAfterFullWindow)
{
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(42.0, t0);
    ema.Set(42.0, t0 + window);

    EXPECT_TRUE(ema.Average().has_value());
}

TEST(TEmaTest, SingleSetValue)
{
    // After a single Set() followed by a warm-up advance, the EMA equals the
    // seeded value (no subsequent observations to blend in).
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(5.0, t0);
    ema.Set(5.0, t0 + window); // Advance to warm-up boundary with same value.

    auto result = ema.Average();
    ASSERT_TRUE(result.has_value());
    EXPECT_NEAR(*result, 5.0, 1e-9);
}

TEST(TEmaTest, TwoDistinctInstants)
{
    // EMA after two observations at t0 and t1=t0+5s with window=10s.
    // Seed at t0=0: Ema=0.
    // Set(10, t1): alpha = 1 - exp(-5 / 5) = 1 - exp(-1).
    // Ema = alpha*10 + (1-alpha)*0.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(0.0, t0);

    auto t1 = t0 + TDuration::Seconds(5);
    ema.Set(10.0, t1);

    // Advance to warm-up boundary.
    ema.Set(10.0, t0 + window);

    double alpha = EmaAlpha(5.0, 10.0);
    // After Set(0, t0): Ema=0.
    // After Set(10, t1=5s): Ema = alpha*10 + (1-alpha)*0.
    double emaAfterT1 = alpha * 10.0;
    // After Set(10, t0+10s): dt=5s, same alpha.
    double expected = alpha * 10.0 + (1.0 - alpha) * emaAfterT1;

    auto result = ema.Average();
    ASSERT_TRUE(result.has_value());
    EXPECT_NEAR(*result, expected, 1e-9);
}

TEST(TEmaTest, DuplicateInstantAverages)
{
    // Multiple Set() calls at the same instant must be averaged before being
    // fed into the EMA.
    // Two calls at t1 with values 10 and 20 → mean = 15.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);

    // Reference: single Set with mean value 15.
    TEma<double> emaRef(window);
    emaRef.Set(0.0, t0);
    emaRef.Set(15.0, t0 + TDuration::Seconds(5));
    emaRef.Set(15.0, t0 + window);

    // Under test: two Sets at the same instant whose mean is 15.
    TEma<double> emaTest(window);
    emaTest.Set(0.0, t0);
    emaTest.Set(10.0, t0 + TDuration::Seconds(5));
    emaTest.Set(20.0, t0 + TDuration::Seconds(5)); // same instant
    emaTest.Set(15.0, t0 + window);

    auto refResult = emaRef.Average();
    auto testResult = emaTest.Average();

    ASSERT_TRUE(refResult.has_value());
    ASSERT_TRUE(testResult.has_value());
    EXPECT_NEAR(*testResult, *refResult, 1e-9);
}

TEST(TEmaTest, ThreeDuplicateInstants)
{
    // Three Set() calls at the same instant: mean = (3+6+9)/3 = 6.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);

    TEma<double> emaRef(window);
    emaRef.Set(0.0, t0);
    emaRef.Set(6.0, t0 + TDuration::Seconds(4));
    emaRef.Set(6.0, t0 + window);

    TEma<double> emaTest(window);
    emaTest.Set(0.0, t0);
    emaTest.Set(3.0, t0 + TDuration::Seconds(4));
    emaTest.Set(6.0, t0 + TDuration::Seconds(4));
    emaTest.Set(9.0, t0 + TDuration::Seconds(4));
    emaTest.Set(6.0, t0 + window);

    auto refResult = emaRef.Average();
    auto testResult = emaTest.Average();

    ASSERT_TRUE(refResult.has_value());
    ASSERT_TRUE(testResult.has_value());
    EXPECT_NEAR(*testResult, *refResult, 1e-9);
}

TEST(TEmaTest, OutOfOrderIgnored)
{
    // A Set() with a timestamp strictly earlier than the last one must be
    // silently ignored.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);

    TEma<double> emaRef(window);
    emaRef.Set(0.0, t0);
    emaRef.Set(10.0, t0 + TDuration::Seconds(5));
    emaRef.Set(10.0, t0 + window);

    TEma<double> emaTest(window);
    emaTest.Set(0.0, t0);
    emaTest.Set(10.0, t0 + TDuration::Seconds(5));
    emaTest.Set(999.0, t0 + TDuration::Seconds(3)); // out-of-order — ignored
    emaTest.Set(10.0, t0 + window);

    auto refResult = emaRef.Average();
    auto testResult = emaTest.Average();

    ASSERT_TRUE(refResult.has_value());
    ASSERT_TRUE(testResult.has_value());
    EXPECT_NEAR(*testResult, *refResult, 1e-9);
}

TEST(TEmaTest, ConstantSignalConverges)
{
    // Feeding a constant value should keep the EMA at that value.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    for (int i = 0; i <= 20; ++i) {
        ema.Set(7.0, t0 + TDuration::Seconds(i));
    }

    auto result = ema.Average();
    ASSERT_TRUE(result.has_value());
    EXPECT_NEAR(*result, 7.0, 1e-9);
}

TEST(TEmaTest, IntegerType)
{
    // TEma should compile and work with integer types.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<int> ema(window);

    ema.Set(0, t0);
    ema.Set(100, t0 + TDuration::Seconds(5));
    ema.Set(100, t0 + window);

    auto result = ema.Average();
    ASSERT_TRUE(result.has_value());
    EXPECT_GE(*result, 0);
    EXPECT_LE(*result, 100);
}

TEST(TEmaTest, ConstantAverageForConstantSignal)
{
    // A constant signal has the same constant average.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 100;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(5.0, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    EXPECT_NEAR(*average, 5.0, 1e-6);
}

TEST(TEmaTest, AverageOfIncreasingSignal)
{
    // A linearly increasing signal at 1 unit/second should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;

    for (int i = 0; i <= 200; ++i) {
        ema.Set(static_cast<double>(i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, maxI - window.SecondsFloat() / 2, 0.5);
}

TEST(TEmaTest, AverageOfRatioIncreasingSignal)
{
    // A linearly increasing signal at 0.3333 unit/second should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;
    double ratio = 0.3333;

    for (int i = 0; i <= 200; ++i) {
        ema.Set(static_cast<double>(i) * ratio, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, (maxI - window.SecondsFloat() / 2) * ratio, 0.5 * ratio);
}

TEST(TEmaTest, AverageOfBigStepIncreasingSignal)
{
    // A linearly increasing signal at 1 unit/second should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;

    for (int i = 0; i <= 200; i += 5) {
        ema.Set(static_cast<double>(i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, maxI - window.SecondsFloat() / 2, 2.5);
}

TEST(TEmaTest, AverageOfDecreasingSignal)
{
    // A linearly decreasing signal should should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(maxI - i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, 0. + window.SecondsFloat() / 2, 0.5);
}

TEST(TEmaTest, AverageOfRatioDecreasingSignal)
{
    // A linearly decreasing signal should should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;
    double ratio = 0.3333;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(maxI - i) * ratio, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, (0. + window.SecondsFloat() / 2) * ratio, 0.5 * ratio);
}

TEST(TEmaTest, AverageOfBigStepDecreasingSignal)
{
    // A linearly decreasing signal should should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; i += 5) {
        ema.Set(static_cast<double>(maxI - i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, 0. + window.SecondsFloat() / 2, 2.5);
}

TEST(TEmaTest, AverageOfOscillatingSignal)
{
    // A linearly decreasing signal should produce an average
    // close to the last value set but with half window lag.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, false> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(i % 2 ? 1 : 0), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    EXPECT_NEAR(*average, 0.5, 0.05);
}

////////////////////////////////////////////////////////////////////////////////
// TEma GrowthRate tests
////////////////////////////////////////////////////////////////////////////////

TEST(TEmaTest, GrowthRateEmptyBeforeAnySet)
{
    TEma<double> ema(TDuration::Seconds(10));
    EXPECT_FALSE(ema.GrowthRate().has_value());
}

TEST(TEmaTest, GrowthRateNotWarmBeforeFullWindow)
{
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(0.0, t0);
    ema.Set(1.0, t0 + window - TDuration::MilliSeconds(1));
    EXPECT_FALSE(ema.GrowthRate().has_value());
}

TEST(TEmaTest, GrowthRateWarmAfterFullWindow)
{
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double> ema(window);

    ema.Set(0.0, t0);
    ema.Set(1.0, t0 + window);
    EXPECT_TRUE(ema.GrowthRate().has_value());
}

TEST(TEmaTest, GrowthRateOfForConstantSignal)
{
    // A constant signal has zero rate of change.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 100;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(5.0, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    EXPECT_NEAR(*average, 5.0, 1e-6);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, 0.0, 1e-6);
}

TEST(TEmaTest, GrowthRateOfIncreasingSignal)
{
    // A linearly increasing signal at 1 unit/second should produce close to 1 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, maxI - window.SecondsFloat() / 2, 0.5);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, 1., 0.01);
}

TEST(TEmaTest, GrowthRateOfRatioIncreasingSignal)
{
    // A linearly increasing signal at 0.3333 unit/second should produce close to 0.3333 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;
    double ratio = 0.3333;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(i) * ratio, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, (maxI - window.SecondsFloat() / 2) * ratio, 0.5 * ratio);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, ratio, 0.01 * ratio);
}

TEST(TEmaTest, GrowthRateOfBigStepIncreasingSignal)
{
    // A linearly increasing signal at 1 unit/second should produce close to 1 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; i += 5) {
        ema.Set(static_cast<double>(i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, maxI - window.SecondsFloat() / 2, 2.5);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, 1., 0.2);
}

TEST(TEmaTest, GrowthRateOfDecreasingSignal)
{
    // A linearly decreasing signal at 1 unit/second should produce close to -1 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(maxI - i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, 0 + window.SecondsFloat() / 2, 0.5);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, -1., 0.01);
}

TEST(TEmaTest, GrowthRateOfRatioDecreasingSignal)
{
    // A linearly decreasing signal at 0.3333 unit/second should produce close to -0.3333 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;
    double ratio = 0.3333;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(maxI - i) * ratio, t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, (0 + window.SecondsFloat() / 2) * ratio, 0.5 * ratio);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, -ratio, 0.01 * ratio);
}

TEST(TEmaTest, GrowthRateBigStepOfDecreasingSignal)
{
    // A linearly decreasing signal at 1 unit/second should produce close to -1 growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; i += 5) {
        ema.Set(static_cast<double>(maxI - i), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    // Lags for a about half a window.
    EXPECT_NEAR(*average, 0 + window.SecondsFloat() / 2, 2.5);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, -1., 0.2);
}

TEST(TEmaTest, GrowthRateOfOscillatingSignal)
{
    // A linearly decreasing signal should produce a negative growth rate.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);
    TEma<double, true> ema(window);
    int maxI = 200;

    for (int i = 0; i <= maxI; ++i) {
        ema.Set(static_cast<double>(i % 2 ? 1 : 0), t0 + TDuration::Seconds(i));
    }

    auto average = ema.Average();
    ASSERT_TRUE(average.has_value());
    EXPECT_NEAR(*average, 0.5, 0.05);
    auto rate = ema.GrowthRate();
    ASSERT_TRUE(rate.has_value());
    EXPECT_NEAR(*rate, 0., 0.02);
}

////////////////////////////////////////////////////////////////////////////////
// TSimpleEma alias test
////////////////////////////////////////////////////////////////////////////////

TEST(TSimpleEmaTest, AliasWorks)
{
    // TSimpleEma is TEma<double, true> — verify it compiles and behaves
    // identically.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);

    TSimpleEma ema(window);
    ema.Set(3.0, t0);
    ema.Set(3.0, t0 + window);

    EXPECT_TRUE(ema.Average().has_value());
    EXPECT_TRUE(ema.GrowthRate().has_value());
}

////////////////////////////////////////////////////////////////////////////////
// TMultiWindowEma tests
////////////////////////////////////////////////////////////////////////////////

TEST(TMultiWindowEmaTest, EmptyBeforeAnySet)
{
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});
    EXPECT_FALSE(ema.Average()[0].has_value());
    EXPECT_FALSE(ema.Average()[1].has_value());
}

TEST(TMultiWindowEmaTest, NotWarmUntilRespectiveWindow)
{
    auto t0 = TInstant::Zero();
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});

    ema.Set(1.0, t0);

    // Neither window warm yet.
    ema.Set(1.0, t0 + TDuration::Seconds(3));
    EXPECT_FALSE(ema.Average()[0].has_value());
    EXPECT_FALSE(ema.Average()[1].has_value());

    // Short window warm, long window still not.
    ema.Set(1.0, t0 + TDuration::Seconds(5));
    EXPECT_TRUE(ema.Average()[0].has_value());
    EXPECT_FALSE(ema.Average()[1].has_value());

    // Both warm.
    ema.Set(1.0, t0 + TDuration::Seconds(20));
    EXPECT_TRUE(ema.Average()[0].has_value());
    EXPECT_TRUE(ema.Average()[1].has_value());
}

TEST(TMultiWindowEmaTest, IndependentWindowValues)
{
    // Each window of TMultiWindowEma must produce the same result as a
    // standalone TEma with the same window size.
    auto t0 = TInstant::Zero();
    auto w0 = TDuration::Seconds(5);
    auto w1 = TDuration::Seconds(20);

    TMultiWindowEma<double, 2> multi({w0, w1});
    TEma<double> ref0(w0);
    TEma<double> ref1(w1);

    auto feed = [&] (double v, TInstant t) {
        multi.Set(v, t);
        ref0.Set(v, t);
        ref1.Set(v, t);
    };

    feed(0.0, t0);
    feed(10.0, t0 + TDuration::Seconds(3));
    feed(5.0, t0 + TDuration::Seconds(7));
    feed(8.0, t0 + TDuration::Seconds(12));
    feed(8.0, t0 + TDuration::Seconds(25)); // Advance past both warm-up windows.

    ASSERT_TRUE(multi.Average()[0].has_value());
    ASSERT_TRUE(multi.Average()[1].has_value());
    ASSERT_TRUE(ref0.Average().has_value());
    ASSERT_TRUE(ref1.Average().has_value());

    EXPECT_NEAR(*multi.Average()[0], *ref0.Average(), 1e-9);
    EXPECT_NEAR(*multi.Average()[1], *ref1.Average(), 1e-9);
}

TEST(TMultiWindowEmaTest, DuplicateInstantAveragedPerWindow)
{
    // Duplicate-instant averaging must work identically to a single Set()
    // with the arithmetic mean, for every window independently.
    auto t0 = TInstant::Zero();
    auto w0 = TDuration::Seconds(5);
    auto w1 = TDuration::Seconds(20);

    TMultiWindowEma<double, 2> multi({w0, w1});
    TEma<double> ref0(w0);
    TEma<double> ref1(w1);

    auto t1 = t0 + TDuration::Seconds(3);
    auto tQuery = t0 + TDuration::Seconds(25);

    // Reference: single Set with mean value 15.
    ref0.Set(0.0, t0);
    ref1.Set(0.0, t0);
    ref0.Set(15.0, t1);
    ref1.Set(15.0, t1);
    ref0.Set(15.0, tQuery);
    ref1.Set(15.0, tQuery);

    // Under test: two Sets at the same instant whose mean is 15.
    multi.Set(0.0, t0);
    multi.Set(10.0, t1);
    multi.Set(20.0, t1); // Same instant — averaged to 15.
    multi.Set(15.0, tQuery);

    ASSERT_TRUE(multi.Average()[0].has_value());
    ASSERT_TRUE(multi.Average()[1].has_value());
    EXPECT_NEAR(*multi.Average()[0], *ref0.Average(), 1e-9);
    EXPECT_NEAR(*multi.Average()[1], *ref1.Average(), 1e-9);
}

TEST(TMultiWindowEmaTest, SingleWindowMatchesTEma)
{
    // TMultiWindowEma<T, 1> must behave identically to TEma<T>.
    auto t0 = TInstant::Zero();
    auto window = TDuration::Seconds(10);

    TMultiWindowEma<double, 1> multi({window});
    TEma<double> ref(window);

    auto feed = [&] (double v, TInstant t) {
        multi.Set(v, t);
        ref.Set(v, t);
    };

    feed(0.0, t0);
    feed(7.0, t0 + TDuration::Seconds(4));
    feed(3.0, t0 + TDuration::Seconds(9));
    feed(3.0, t0 + TDuration::Seconds(15)); // advance past warm-up

    ASSERT_TRUE(multi.Average()[0].has_value());
    ASSERT_TRUE(ref.Average().has_value());
    EXPECT_NEAR(*multi.Average()[0], *ref.Average(), 1e-9);
}

TEST(TMultiWindowEmaTest, GrowthRateEmptyBeforeAnySet)
{
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});
    EXPECT_FALSE(ema.GrowthRate()[0].has_value());
    EXPECT_FALSE(ema.GrowthRate()[1].has_value());
}

TEST(TMultiWindowEmaTest, GrowthRateNotWarmBeforeFullWindow)
{
    auto t0 = TInstant::Zero();
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});

    ema.Set(0.0, t0);
    ema.Set(1.0, t0 + TDuration::Seconds(3));
    EXPECT_FALSE(ema.GrowthRate()[0].has_value());
    EXPECT_FALSE(ema.GrowthRate()[1].has_value());
}

TEST(TMultiWindowEmaTest, GrowthRateWarmAfterRespectiveWindow)
{
    auto t0 = TInstant::Zero();
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});

    ema.Set(0.0, t0);
    ema.Set(1.0, t0 + TDuration::Seconds(5));
    EXPECT_TRUE(ema.GrowthRate()[0].has_value());
    EXPECT_FALSE(ema.GrowthRate()[1].has_value());

    ema.Set(2.0, t0 + TDuration::Seconds(20));
    EXPECT_TRUE(ema.GrowthRate()[0].has_value());
    EXPECT_TRUE(ema.GrowthRate()[1].has_value());
}

TEST(TMultiWindowEmaTest, GrowthRateZeroForConstantSignal)
{
    // A constant signal has zero growth rate in every window.
    auto t0 = TInstant::Zero();
    TMultiWindowEma<double, 2> ema({TDuration::Seconds(5), TDuration::Seconds(20)});

    for (int i = 0; i <= 100; ++i) {
        ema.Set(42.0, t0 + TDuration::Seconds(i));
    }

    ASSERT_TRUE(ema.GrowthRate()[0].has_value());
    ASSERT_TRUE(ema.GrowthRate()[1].has_value());
    EXPECT_NEAR(*ema.GrowthRate()[0], 0.0, 1e-6);
    EXPECT_NEAR(*ema.GrowthRate()[1], 0.0, 1e-6);
}

TEST(TMultiWindowEmaTest, GrowthRateIndependentPerWindow)
{
    // GrowthRate per window must match the corresponding standalone TEma.
    auto t0 = TInstant::Zero();
    auto w0 = TDuration::Seconds(5);
    auto w1 = TDuration::Seconds(20);

    TMultiWindowEma<double, 2, /*CalculateRate*/ true> multi({w0, w1});
    TEma<double, true> ref0(w0);
    TEma<double, true> ref1(w1);

    auto feed = [&] (double v, TInstant t) {
        multi.Set(v, t);
        ref0.Set(v, t);
        ref1.Set(v, t);
    };

    for (int i = 0; i <= 100; ++i) {
        feed(static_cast<double>(i), t0 + TDuration::Seconds(i));
    }

    ASSERT_TRUE(multi.GrowthRate()[0].has_value());
    ASSERT_TRUE(multi.GrowthRate()[1].has_value());
    ASSERT_TRUE(ref0.GrowthRate().has_value());
    ASSERT_TRUE(ref1.GrowthRate().has_value());

    EXPECT_NEAR(*multi.GrowthRate()[0], *ref0.GrowthRate(), 1e-9);
    EXPECT_NEAR(*multi.GrowthRate()[1], *ref1.GrowthRate(), 1e-9);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
