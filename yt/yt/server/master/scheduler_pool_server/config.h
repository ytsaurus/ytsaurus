#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicSchedulerPoolManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    int MaxSchedulerPoolSubtreeSize;

    TDynamicSchedulerPoolManagerConfig() {
        RegisterParameter("max_scheduler_pool_subtree_size", MaxSchedulerPoolSubtreeSize)
            .Default(1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicSchedulerPoolManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
