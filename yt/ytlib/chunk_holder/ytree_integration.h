#pragma once

#include "common.h"
#include "chunk_store.h"
#include "session_manager.h"

#include "../ytree/ypath_service.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathService::TPtr CreateChunkMapService(
    TChunkStore* chunkStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
