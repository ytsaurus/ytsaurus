#pragma once

#include "private.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/public.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

NObjectServer::IObjectProxyPtr CreateTransactionProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TTransaction* transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer

