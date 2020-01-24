#pragma once

#include "public.h"

#include <yp/server/master/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreatePodSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSetTypeHandlerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
