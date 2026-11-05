#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateMessageService(
    IInputManagerPtr inputManager,
    NRpc::IAuthenticatorPtr authenticator,
    TStreamSpecStoragePtr streamSpecStorage,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
