#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOffshoreNodeService(
    IInvokerPtr invoker,
    IInvokerPtr storageInvoker,
    NRpc::IAuthenticatorPtr authenticator,
    NChunkClient::TMediumDirectoryPtr mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
