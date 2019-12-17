#pragma once

#include "public.h"

#include <yp/server/master/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateMultiClusterReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap,
    TPodSpecValidationConfigPtr validationConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
