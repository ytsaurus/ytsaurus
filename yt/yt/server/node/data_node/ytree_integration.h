#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore,
    IAllyReplicaManagerPtr allyReplicaManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
