#ifndef EMA_INL_H_
    #error "Direct inclusion of this file is not allowed, include ema.h"
    // For the sake of sane code completion.
    #include "ema.h"
#endif

#include <cmath>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T, bool CalculateRate>
    requires std::is_arithmetic_v<T>
TEma<T, CalculateRate>::TEma(TDuration window)
    : Ema_({window})
{ }

template <class T, bool CalculateRate>
    requires std::is_arithmetic_v<T>
void TEma<T, CalculateRate>::Set(const T& value, TInstant instant)
{
    Ema_.Set(value, instant);
}

template <class T, bool CalculateRate>
    requires std::is_arithmetic_v<T>
const std::optional<T>& TEma<T, CalculateRate>::Average() const
{
    return Ema_.Average()[0];
}

template <class T, bool CalculateRate>
    requires std::is_arithmetic_v<T>
const std::optional<T>& TEma<T, CalculateRate>::GrowthRate() const
    requires CalculateRate
{
    return Ema_.GrowthRate()[0];
}

////////////////////////////////////////////////////////////////////////////////

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
TMultiWindowEma<T, WindowCount, CalculateRate>::TMultiWindowEma(const std::array<TDuration, WindowCount>& windowSizes)
{
    for (ssize_t i = 0; i < WindowCount; i++) {
        YT_VERIFY(windowSizes[i] > TDuration::Zero());
        Data_[i].HalfLife = windowSizes[i].SecondsFloat() / 2;
    }
    if constexpr (CalculateRate) {
        // Create another EMA with half window size.
        for (ssize_t i = 0; i < WindowCount; i++) {
            Data_[WindowCount + i].HalfLife = windowSizes[i].SecondsFloat() / 4;
        }
    }
}

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
void TMultiWindowEma<T, WindowCount, CalculateRate>::Set(const T& value, TInstant instant)
{
    double d = static_cast<double>(value);
    if (!IsStarted_) {
        IsStarted_ = true;
        for (auto& data : Data_) {
            data.Ema = d;
            data.LastInstantAlpha = 1.;
            data.LastInstantAverage = d;
        }
        StartTime_ = instant;
        LastInstantCount_ = 1;
    } else if (instant > LastTime_) {
        auto dt = (instant - LastTime_).SecondsFloat();
        for (auto& data : Data_) {
            double alpha = Alpha(data, dt);
            data.Ema = d * alpha + data.Ema * (1. - alpha);
            data.LastInstantAlpha = alpha;
            data.LastInstantAverage = d;
        }
        LastInstantCount_ = 1;
    } else if (instant == LastTime_) {
        for (auto& data : Data_) {
            data.Ema -= data.LastInstantAverage * data.LastInstantAlpha;
            data.LastInstantAverage = (d + data.LastInstantAverage * LastInstantCount_) / (LastInstantCount_ + 1);
            data.Ema += data.LastInstantAverage * data.LastInstantAlpha;
        }
        LastInstantCount_++;
    } else {
        // Just ignore obsolete update.
        return;
    }

    LastTime_ = instant;
    FillResults();
}

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
const std::array<std::optional<T>, WindowCount>& TMultiWindowEma<T, WindowCount, CalculateRate>::Average() const
{
    return ResultAverageValues;
}

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
const std::array<std::optional<T>, WindowCount>& TMultiWindowEma<T, WindowCount, CalculateRate>::GrowthRate() const
    requires CalculateRate
{
    return this->ResultGrowthRates;
}

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
double TMultiWindowEma<T, WindowCount, CalculateRate>::Alpha(const TOneWindowData& data, double dtSeconds) const
{
    return 1.0 - std::exp(-dtSeconds / data.HalfLife);
}

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
void TMultiWindowEma<T, WindowCount, CalculateRate>::FillResults()
{
    YT_ASSERT(IsStarted_);
    auto halfDuration = (LastTime_ - StartTime_).SecondsFloat() / 2;
    for (ssize_t i = 0; i < WindowCount; i++) {
        if (halfDuration < Data_[i].HalfLife) {
            continue;
        }
        ResultAverageValues[i] = static_cast<T>(Data_[i].Ema);
        if constexpr (CalculateRate) {
            // We have an EMA over timespan (-W, 0], where W is a user window size: fullWindowEma = Data_[i].Ema;
            // We also have EMA over timespan (-W/2, 0]: lastHalfWindowEma = Data_[i + WindowCount].Ema;
            // We can imagine EMA over timespan (-W, -W/2): prevHalfWindowEma;
            // Since fullWindowEma == (lastHalfWindowEma + prevHalfWindowEma) / 2,
            // then prevHalfWindowEma = fullWindowEma * 2 - lastHalfWindowEma.
            // The value is changed from lastHalfWindowEma to prevHalfWindowEma for about W/2.
            // Growth rate is (lastHalfWindowEma - prevHalfWindowEma) / (W/2).
            // Substituting: growthRate = (lastHalfWindowEma - fullWindowEma) / (W/4).
            double fullWindowEma = Data_[i].Ema;
            double lastHalfWindowEma = Data_[i + WindowCount].Ema;
            double quarterWindow = Data_[i + WindowCount].HalfLife; // Equal to (Data_[i].HalfLife / 2).
            double growthRate = (lastHalfWindowEma - fullWindowEma) / quarterWindow;
            this->ResultGrowthRates[i] = static_cast<T>(growthRate);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
