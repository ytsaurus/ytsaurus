#pragma once

#include "public.h"

#include <yt/server/tablet_node/public.h>

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
    NTabletNode::EDynamicTableProfilingMode DynamicTableProfilingMode;

    TDynamicClusterConfig()
    {
        RegisterParameter("enable_safe_mode", EnableSafeMode)
            .Default(false);
        RegisterParameter("enable_tablet_balancer", EnableTabletBalancer)
            .Default(true);
        RegisterParameter("enable_chunk_replicator", EnableChunkReplicator)
            .Default(true);
        RegisterParameter("dynamc_table_profiling_mode", DynamicTableProfilingMode)
            .Default(NTabletNode::EDynamicTableProfilingMode::Path);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
