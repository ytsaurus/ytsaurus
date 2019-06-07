#pragma once

#include "public.h"

#include <yp/server/master/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreatePodTypeHandler(NMaster::TBootstrap* bootstrap, TPodTypeHandlerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
