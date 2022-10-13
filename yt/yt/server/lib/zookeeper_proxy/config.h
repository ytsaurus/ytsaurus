#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

struct TZookeeperProxyConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TZookeeperProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZookeeperProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
