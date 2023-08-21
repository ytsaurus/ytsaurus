#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateChaosNodeService(
    IChaosSlotPtr slot,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
