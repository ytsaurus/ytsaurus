#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore,
    IAllyReplicaManagerPtr allyReplicaManager);

NYTree::IYPathServicePtr CreateCachedChunkMapService(
    NExecNode::TChunkCachePtr chunkCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
