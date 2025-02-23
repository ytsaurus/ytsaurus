#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

struct TIODispatcherConfig
    : public NYTree::TYsonStruct
{
    TDuration ThreadPoolPollingPeriod;

    TIODispatcherConfigPtr ApplyDynamic(const TIODispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TIODispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIODispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TIODispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> ThreadPoolPollingPeriod;

    REGISTER_YSON_STRUCT(TIODispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIODispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
