#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateMasterCellGroupProxy(
    TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TMasterCellGroup* group);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
