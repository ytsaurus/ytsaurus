#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreatePushBasedShuffleService(
    TPushBasedShuffleManagerPtr manager,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
