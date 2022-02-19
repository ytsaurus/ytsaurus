#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/core/logging/log.h>


namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

TRefCountedPtr CreateIOThroughputMeter(
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkStorePtr chunkStore,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

}
