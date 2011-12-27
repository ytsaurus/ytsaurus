#pragma once

#include "common.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "session_manager.h"

#include "../ytree/ypath_service.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathService::TPtr CreateStoredChunkMapService(
    TChunkStore* chunkStore);

NYTree::IYPathService::TPtr CreateCachedChunkMapService(
    TChunkCache* chunkCache);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
