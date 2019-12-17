#pragma once

#include "public.h"

#include <yp/server/access_control/public.h>
#include <yp/server/master/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreatePodTypeHandler(NMaster::TBootstrap* bootstrap, TPodTypeHandlerConfigPtr config);

void ValidateDeployPodSpecTemplate(
    const NAccessControl::TAccessControlManagerPtr& accessControlManager,
    TTransaction* transaction,
    const NClient::NApi::NProto::TPodSpec& podSpec,
    const TPodSpecValidationConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
