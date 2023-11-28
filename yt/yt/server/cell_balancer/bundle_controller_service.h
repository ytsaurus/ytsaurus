#pragma once

#include "bootstrap.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NBundleController {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateBundleControllerService(
    NCellBalancer::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBundleController
