#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore);

NYTree::IYPathServicePtr CreateCachedChunkMapService(
    TChunkCachePtr chunkCache);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
