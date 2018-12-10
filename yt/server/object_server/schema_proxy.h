#pragma once

#include "object.h"

#include <yt/server/cell_master/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateSchemaProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TSchemaObject* object);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
