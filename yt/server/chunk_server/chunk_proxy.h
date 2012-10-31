#pragma once

#include "public.h"

#include <ytlib/meta_state/map.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateChunkProxy(
    NCellMaster::TBootstrap* bootstrap,
    NMetaState::TMetaStateMap<TChunkId, TChunk>* map,
    const TChunkId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
