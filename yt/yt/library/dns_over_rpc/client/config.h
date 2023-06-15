#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

class TDnsOverRpcResolverConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ResolveBatchingPeriod;
    TDuration ResolveRpcTimeout;

    REGISTER_YSON_STRUCT(TDnsOverRpcResolverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDnsOverRpcResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
