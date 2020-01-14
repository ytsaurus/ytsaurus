#pragma once

#include "public.h"

#include <yp/server/master/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateNodeSegmentTypeHandler(
    NMaster::TBootstrap* bootstrap,
    TNodeSegmentTypeHandlerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
