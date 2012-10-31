#pragma once

#include "public.h"

#include <ytlib/meta_state/map.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateChunkListProxy(
    NCellMaster::TBootstrap* bootstrap,
    NMetaState::TMetaStateMap<TChunkListId, TChunkList>* map,
    const TChunkListId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
