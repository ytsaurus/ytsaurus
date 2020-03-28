#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IServicePtr CreateObjectService(NMaster::TBootstrap* bootstrap, TObjectServiceConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi
