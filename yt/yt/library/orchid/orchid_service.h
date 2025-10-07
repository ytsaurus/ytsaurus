#pragma once

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOrchidService(
    NYTree::INodePtr root,
    IInvokerPtr invoker,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
