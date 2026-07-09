#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T, ssize_t WindowCount, bool CalculateRate = true>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
class TMultiWindowEma;

template <class T, bool CalculateRate = true>
    requires std::is_arithmetic_v<T>
class TEma;

////////////////////////////////////////////////////////////////////////////////

//! Exponential moving average tracker for a single time window and double type.
//! See TEma for description.
using TSimpleEma = TEma<double, true>;

////////////////////////////////////////////////////////////////////////////////

//! Exponential moving average tracker for a single time window.
//!
//! Tracks the EMA of a value and, optionally, the EMA of its rate of change.
//!
//! Template parameters:
//!   T             — arithmetic value type (e.g. double, i64).
//!   CalculateRate — when true, also tracks the EMA of the rate of change
//!                  (units per second). Adds no overhead when false.
//!
//! The half-life of the EMA equals window / 2, matching the convention used
//! by TEmaCounter in yt/core/misc/ema_counter.h.
//!
//! Values are considered "warm" (non-empty) only after at least one full
//! window duration has elapsed since the first Set() call.
//!
//! Usage example:
//!   TMultiWindowEma<double, 3> ema(TDuration::Seconds(10));
//!   ema.Set(value);
//!   const auto& avgs = ema.Average();  // const std::optional<double>&
template <class T, bool CalculateRate>
    requires std::is_arithmetic_v<T>
class TEma
{
public:
    //! Constructs the tracker with the given smoothing window.
    explicit TEma(TDuration window);

    //! Feed a new observed value at the given |instant|.
    void Set(const T& value, TInstant instant = TInstant::Now());

    //! Returns the EMA-smoothed value, or std::nullopt if not yet warmed up.
    const std::optional<T>& Average() const;

    //! Returns the EMA-smoothed rate of change (units/second), or std::nullopt if not yet warmed up.
    const std::optional<T>& GrowthRate() const
        requires CalculateRate;

private:
    TMultiWindowEma<T, 1, CalculateRate> Ema_;
};

////////////////////////////////////////////////////////////////////////////////

//! Multi-window EMA tracker.
//!
//! Maintains WindowCount independent TEma instances, one per window,
//! with common Set method.
//!
//! Template parameters:
//!   T             — arithmetic value type (e.g. double, i64).
//!   WindowCount   — number of windows (must be > 0).
//!   CalculateRate — when true, also tracks the EMA of the rate of change
//!                   for every window.
//!
//! The half-life of the EMA equals window / 2, matching the convention used
//! by TEmaCounter in yt/core/misc/ema_counter.h.
//!
//! Values are considered "warm" (non-empty) only after at least one full
//! window duration has elapsed since the first Set() call.
//!
//! Usage example:
//!   TMultiWindowEma<double, 3> ema({TDuration::Seconds(1),
//!                                   TDuration::Seconds(10),
//!                                   TDuration::Seconds(60)});
//!   ema.Set(value);
//!   const auto& avgs = ema.Average();  // const std::optional<std::array<double, 3>>&
namespace NDetail {

template <class T, ssize_t WindowCount, bool CalculateRate>
struct TGrowthRatesStorage
{
    std::array<std::optional<T>, WindowCount> ResultGrowthRates;
};

template <class T, ssize_t WindowCount>
struct TGrowthRatesStorage<T, WindowCount, false>
{ };

} // namespace NDetail

template <class T, ssize_t WindowCount, bool CalculateRate>
    requires(std::is_arithmetic_v<T> && WindowCount > 0)
class TMultiWindowEma
    : private NDetail::TGrowthRatesStorage<T, WindowCount, CalculateRate>
{
public:
    using TResultArray = std::array<std::optional<T>, WindowCount>;

    //! Constructs the tracker with the given list of smoothing windows.
    explicit TMultiWindowEma(const std::array<TDuration, WindowCount>& windowSizes);

    //! Feed a new observed value at the given |instant| to all windows.
    void Set(const T& value, TInstant instant = TInstant::Now());

    //! Returns per-window EMA-smoothed values.
    const TResultArray& Average() const;

    //! Returns per-window EMA-smoothed rates of change (units/second).
    const TResultArray& GrowthRate() const
        requires CalculateRate;

private:
    static constexpr ssize_t RealWindowCount = WindowCount * (CalculateRate ? 2 : 1);

    bool IsStarted_ = false;
    TInstant StartTime_{};
    TInstant LastTime_{};
    ssize_t LastInstantCount_{};

    struct TOneWindowData
    {
        double HalfLife{};
        double Ema{};
        double LastInstantAlpha{};
        double LastInstantAverage{};
    };

    std::array<TOneWindowData, RealWindowCount> Data_;
    std::array<std::optional<T>, WindowCount> ResultAverageValues;

    double Alpha(const TOneWindowData& data, double dtSeconds) const;
    void FillResults();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define EMA_INL_H_
#include "ema-inl.h"
#undef EMA_INL_H_
