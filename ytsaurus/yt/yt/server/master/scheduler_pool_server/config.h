#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSchedulerPoolManagerConfig
    : public NYTree::TYsonStruct
{
public:
    int MaxSchedulerPoolSubtreeSize;

    REGISTER_YSON_STRUCT(TDynamicSchedulerPoolManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSchedulerPoolManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
