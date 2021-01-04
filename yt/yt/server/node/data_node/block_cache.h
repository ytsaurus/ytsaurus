#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::IClientBlockCachePtr CreateDataNodeBlockCache(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
