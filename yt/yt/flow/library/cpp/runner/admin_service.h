#pragma once

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
