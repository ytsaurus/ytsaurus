#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

class TZookeeperConfig
    : public NYTree::TYsonStruct
{
public:
    int Port;

    REGISTER_YSON_STRUCT(TZookeeperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZookeeperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
