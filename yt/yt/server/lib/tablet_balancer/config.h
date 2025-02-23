#pragma once

#include "public.h"

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTabletBalancer {

const TString DefaultGroupName = "default";
const TString LegacyGroupName = "legacy";
const TString LegacyInMemoryGroupName = "legacy_in_memory";

////////////////////////////////////////////////////////////////////////////////

struct TComponentFactorConfig
    : public NYTree::TYsonStruct
{
    std::optional<double> Cell;
    std::optional<double> Node;
    std::optional<double> TableCell;
    std::optional<double> TableNode;

    static TComponentFactorConfigPtr MakeDefaultIdentity();
    static TComponentFactorConfigPtr MakeUniformIdentity();

    TComponentFactorConfigPtr MergeWith(
        const TComponentFactorConfigPtr& otherConfig) const;

    REGISTER_YSON_STRUCT(TComponentFactorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TComponentFactorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TParameterizedBalancingConfig
    : public NYTree::TYsonStruct
{
    TString Metric;
    std::optional<bool> EnableReshard;
    std::optional<bool> EnableUniform;
    std::optional<int> MaxActionCount;
    std::optional<int> BoundedPriorityQueueSize;
    std::optional<double> NodeDeviationThreshold;
    std::optional<double> CellDeviationThreshold;
    std::optional<double> MinRelativeMetricImprovement;
    TComponentFactorConfigPtr Factors;

    REGISTER_YSON_STRUCT(TParameterizedBalancingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParameterizedBalancingConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletBalancingGroupConfig
    : public NYTree::TYsonStruct
{
    bool EnableMove;
    bool EnableReshard;

    EBalancingType Type;
    TParameterizedBalancingConfigPtr Parameterized;
    TTimeFormula Schedule;

    REGISTER_YSON_STRUCT(TTabletBalancingGroupConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancingGroupConfig)

////////////////////////////////////////////////////////////////////////////////

//! Frozen.
struct TMasterBundleTabletBalancerConfig
    : public NYTree::TYsonStruct
{
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

    REGISTER_YSON_STRUCT(TMasterBundleTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterBundleTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBundleTabletBalancerConfig
    : public TMasterBundleTabletBalancerConfig
{
    bool EnableParameterizedByDefault;
    std::optional<TString> DefaultInMemoryGroup;
    bool EnablePickPivotKeys;
    double SafeUsedTabletStaticRatio;

    THashMap<TString, TTabletBalancingGroupConfigPtr> Groups;

    REGISTER_YSON_STRUCT(TBundleTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Frozen.
struct TMasterTableTabletBalancerConfig
    : public NYTree::TYsonStruct
{
    bool EnableAutoReshard;
    bool EnableAutoTabletMove;

    std::optional<i64> MinTabletSize;
    std::optional<i64> MaxTabletSize;
    std::optional<i64> DesiredTabletSize;
    std::optional<int> DesiredTabletCount;
    std::optional<int> MinTabletCount;
    bool EnableVerboseLogging;

    REGISTER_YSON_STRUCT(TMasterTableTabletBalancerConfig);

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

DEFINE_REFCOUNTED_TYPE(TMasterTableTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableTabletBalancerConfig
    : public TMasterTableTabletBalancerConfig
{
    std::optional<bool> EnableParameterized;
    std::optional<TString> Group;

    REGISTER_YSON_STRUCT(TTableTabletBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
