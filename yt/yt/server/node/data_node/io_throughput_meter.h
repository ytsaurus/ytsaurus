#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/core/logging/log.h>


namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct IIOThroughputMeter
    : public TRefCounted
{
    struct TIOCapacity
    {
        i64 DiskReadCapacity = 0;
        i64 DiskWriteCapacity = 0;
    };

    virtual TIOCapacity GetLocationIOCapacity(TChunkLocationUuid uuid) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IIOThroughputMeter)

////////////////////////////////////////////////////////////////////////////////

IIOThroughputMeterPtr CreateIOThroughputMeter(
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

}
