#pragma once

#include "public.h"

#include <yp/server/master/public.h>

#include <yt/core/rpc/public.h>

namespace NYP {
namespace NServer {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

NYT::NRpc::IServicePtr CreateObjectService(NMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NServer
} // namespace NYP
