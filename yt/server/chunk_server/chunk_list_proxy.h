#pragma once

#include "public.h"

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateChunkListProxy(
    NCellMaster::TBootstrap* bootstrap,
    TChunkList* chunkList);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
