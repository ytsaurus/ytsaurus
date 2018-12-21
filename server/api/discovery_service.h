#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

namespace NYP::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IServicePtr CreateDiscoveryService(
    NMaster::TBootstrap* bootstrap,
    EMasterInterface interface);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NApi
