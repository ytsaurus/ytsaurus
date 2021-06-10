#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateMasterTableSchemaProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TMasterTableSchema* schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
