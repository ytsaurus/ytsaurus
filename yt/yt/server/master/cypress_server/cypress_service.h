#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateCypressService(NCellMaster::TBootstrap* boostrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

