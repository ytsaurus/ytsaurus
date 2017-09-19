#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TDynamicClusterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableSafeMode;
    bool EnableTabletBalancer;
    bool EnableChunkReplicator;

    TDynamicClusterConfig()
    {
        RegisterParameter("enable_safe_mode", EnableSafeMode)
            .Default(false);
        RegisterParameter("enable_tablet_balancer", EnableTabletBalancer)
            .Default(true);
        RegisterParameter("enable_chunk_replicator", EnableChunkReplicator)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
