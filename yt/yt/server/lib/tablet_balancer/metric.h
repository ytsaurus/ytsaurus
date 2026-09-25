#pragma once

#include "public.h"

#include <library/cpp/yt/misc/preprocessor.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string_builder.h>

#include <util/generic/fwd.h>

#include <array>

#define YT_FOR_EACH_METRIC_SIZE(macro) \
    PP_FOR_EACH(macro, PP_RANGE(1, YT_TABLET_BALANCER_MAX_METRIC_COUNT))

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

static constexpr double MinimumAcceptableMetricValue = 1e-30;

////////////////////////////////////////////////////////////////////////////////

template <int Size>
class TGenericMetric
{
    static_assert(1 <= Size && Size <= MaxMetricCount);

public:
    Y_FORCE_INLINE TGenericMetric() = default;

    Y_FORCE_INLINE TGenericMetric(const std::array<double, Size>& values);

    Y_FORCE_INLINE static TGenericMetric Zero();
    Y_FORCE_INLINE static TGenericMetric Unit();

    Y_FORCE_INLINE double& operator[](int index);
    Y_FORCE_INLINE double operator[](int index) const;

    Y_FORCE_INLINE TGenericMetric operator+(TGenericMetric other) const;
    Y_FORCE_INLINE TGenericMetric operator-(TGenericMetric other) const;
    Y_FORCE_INLINE TGenericMetric operator*(TGenericMetric other) const;
    Y_FORCE_INLINE TGenericMetric operator/(TGenericMetric other) const;

    Y_FORCE_INLINE TGenericMetric operator+(double scalar) const;
    Y_FORCE_INLINE TGenericMetric operator-(double scalar) const;
    Y_FORCE_INLINE TGenericMetric operator*(double scalar) const;
    Y_FORCE_INLINE TGenericMetric operator/(double scalar) const;

    Y_FORCE_INLINE TGenericMetric& operator+=(TGenericMetric other);
    Y_FORCE_INLINE TGenericMetric& operator-=(TGenericMetric other);
    Y_FORCE_INLINE TGenericMetric& operator*=(TGenericMetric other);
    Y_FORCE_INLINE TGenericMetric& operator/=(TGenericMetric other);

    Y_FORCE_INLINE TGenericMetric& operator+=(double scalar);
    Y_FORCE_INLINE TGenericMetric& operator-=(double scalar);
    Y_FORCE_INLINE TGenericMetric& operator*=(double scalar);
    Y_FORCE_INLINE TGenericMetric& operator/=(double scalar);

    Y_FORCE_INLINE bool IsLessOrEqualComponentwise(const TGenericMetric& other) const;

    Y_FORCE_INLINE double GetTotalValue() const;

    Y_FORCE_INLINE TGenericMetric AsNormalizationFactor(double scalar) const;

private:
    std::array<double, Size> Values_ = {};
};

////////////////////////////////////////////////////////////////////////////////

template <int Size>
void FormatValue(TStringBuilderBase* builder, TGenericMetric<Size> metric, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define METRIC_INL_H_
#include "metric-inl.h"
#undef METRIC_INL_H_
