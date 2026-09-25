#pragma once

#include "public.h"

#include <yt/yt/core/logging/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IParameterizedResharder
    : public TRefCounted
{
    virtual std::vector<TReshardDescriptor> BuildTableActionDescriptors(const TTablePtr& table) = 0;
};

DEFINE_REFCOUNTED_TYPE(IParameterizedResharder)

////////////////////////////////////////////////////////////////////////////////

struct TParameterizedResharderConfig
{
    bool EnableReshardByDefault = false;
    std::vector<std::string> Metrics;

    TParameterizedResharderConfig MergeWith(const TParameterizedBalancingConfigPtr& groupConfig) const;
};

void FormatValue(TStringBuilderBase* builder, const TParameterizedResharderConfig& config, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

IParameterizedResharderPtr CreateParameterizedResharder(
    TTabletCellBundlePtr bundle,
    std::vector<std::string> performanceCountersKeys,
    TParameterizedResharderConfig config,
    TGroupName groupName,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
