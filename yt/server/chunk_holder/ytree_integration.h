#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStore* chunkStore);

NYTree::IYPathServicePtr CreateCachedChunkMapService(
    TChunkCache* chunkCache);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
