#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_agent/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore);

NYTree::IYPathServicePtr CreateCachedChunkMapService(
    NExecAgent::TChunkCachePtr chunkCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
