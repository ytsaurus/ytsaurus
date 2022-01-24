#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLogDigestConfig
    : public NYTree::TYsonStruct
{
public:
    // We will round each sample x to the range from [(1 - RelativePrecision)*x, (1 + RelativePrecision)*x].
    // This parameter affects the memory usage of the digest, it is proportional to
    // log(UpperBound / LowerBound) / log(1 + RelativePrecision).
    double RelativePrecision;

    // The bounds of the range operated by the class.
    double LowerBound;
    double UpperBound;

    // The value that is returned when there are no samples in the digest.
    std::optional<double> DefaultValue;

    TLogDigestConfig(double lowerBound, double upperBound, double defaultValue);

    REGISTER_YSON_STRUCT(TLogDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogDigestConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHistoricUsageAggregationMode,
    ((None)                     (0))
    ((ExponentialMovingAverage) (1))
);

class THistoricUsageConfig
    : public NYTree::TYsonStruct
{
public:
    EHistoricUsageAggregationMode AggregationMode;

    //! Parameter of exponential moving average (EMA) of the aggregated usage.
    //! Roughly speaking, it means that current usage ratio is twice as relevant for the
    //! historic usage as the usage ratio alpha seconds ago.
    //! EMA for unevenly spaced time series was adapted from here: https://clck.ru/HaGZs
    double EmaAlpha;

    REGISTER_YSON_STRUCT(THistoricUsageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistoricUsageConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
