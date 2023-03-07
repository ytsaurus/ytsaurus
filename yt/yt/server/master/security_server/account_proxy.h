#pragma once

#include "private.h"

#include <yt/server/master/object_server/map_object_proxy.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<NObjectServer::TNonversionedMapObjectProxyBase<TAccount>> CreateAccountProxy(
    NCellMaster::TBootstrap* bootstrap,
    NObjectServer::TObjectTypeMetadata* metadata,
    TAccount* account);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

