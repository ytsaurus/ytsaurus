#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateCoordinatorService(
    IChaosSlotPtr slot,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
