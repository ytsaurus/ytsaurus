#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::IBlockCachePtr CreateServerBlockCache(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
