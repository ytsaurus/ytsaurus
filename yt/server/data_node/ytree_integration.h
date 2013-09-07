#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore);

NYTree::IYPathServicePtr CreateCachedChunkMapService(
    TChunkCachePtr chunkCache);

///////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
