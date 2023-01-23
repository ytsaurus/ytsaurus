#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Frozen.
class TBundleTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableInMemoryCellBalancer;
    bool EnableCellBalancer;
    bool EnableTabletSizeBalancer;
    bool EnableStandaloneTabletBalancer;

    bool EnableTabletCellSmoothing;

    double HardInMemoryCellBalanceThreshold;
    double SoftInMemoryCellBalanceThreshold;

    i64 MinTabletSize;
    i64 MaxTabletSize;
    i64 DesiredTabletSize;

    i64 MinInMemoryTabletSize;
    i64 MaxInMemoryTabletSize;
    i64 DesiredInMemoryTabletSize;

    double TabletToCellRatio;

    TTimeFormula TabletBalancerSchedule;

    bool EnableVerboseLogging;

    //! Formula for calculating the metric of one tablet in parameterized balancing.
    TString ParameterizedBalancingMetric;

    REGISTER_YSON_STRUCT(TBundleTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Frozen.
class TTableTabletBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableAutoReshard;
    bool EnableAutoTabletMove;

    std::optional<i64> MinTabletSize;
    std::optional<i64> MaxTabletSize;
    std::optional<i64> DesiredTabletSize;
    std::optional<int> DesiredTabletCount;
    std::optional<int> MinTabletCount;
    bool EnableVerboseLogging;

    REGISTER_YSON_STRUCT(TTableTabletBalancerConfig);

    static void Register(TRegistrar registrar);

public:
    // COMPAT(ifsmirnov)
    void SetMinTabletSize(std::optional<i64> value);

    void SetDesiredTabletSize(std::optional<i64> value);

    void SetMaxTabletSize(std::optional<i64> value);

private:
    void CheckTabletSizeInequalities() const;

    void SetTabletSizeConstraint(std::optional<i64>* member, std::optional<i64> value);
};

DEFINE_REFCOUNTED_TYPE(TTableTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
