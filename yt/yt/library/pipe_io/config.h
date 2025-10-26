#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipeIO {

////////////////////////////////////////////////////////////////////////////////

struct TPipeIODispatcherConfig
    : public NYTree::TYsonStruct
{
    TDuration ThreadPoolPollingPeriod;

    TPipeIODispatcherConfigPtr ApplyDynamic(const TPipeIODispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TPipeIODispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipeIODispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPipeIODispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> ThreadPoolPollingPeriod;

    REGISTER_YSON_STRUCT(TPipeIODispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPipeIODispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
