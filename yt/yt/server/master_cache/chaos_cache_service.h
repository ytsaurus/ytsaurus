#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateChaosCacheService(
    IInvokerPtr invoker,
    NApi::IClientPtr client,
    TChaosCachePtr cache,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
