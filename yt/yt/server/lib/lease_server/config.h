#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

struct TLeaseManagerConfig
    : public NYTree::TYsonStruct
{
    TDuration LeaseRemovalPeriod;

    int MaxLeasesPerRemoval;

    void ApplyDynamicInplace(const TLeaseManagerDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TLeaseManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLeaseManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TLeaseManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> LeaseRemovalPeriod;

    std::optional<int> MaxLeasesPerRemoval;

    REGISTER_YSON_STRUCT(TLeaseManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLeaseManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
