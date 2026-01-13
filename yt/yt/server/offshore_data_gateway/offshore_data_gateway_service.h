#pragma once

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOffshoreDataGatewayService(
    IInvokerPtr invoker,
    IInvokerPtr storageInvoker,
    NConcurrency::IPollerPtr s3Poller,
    NRpc::IAuthenticatorPtr authenticator,
    NChunkClient::TMediumDirectoryPtr mediumDirectory,
    NChunkClient::TMediumDirectorySynchronizerPtr mediumDirectorySynchronizer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
