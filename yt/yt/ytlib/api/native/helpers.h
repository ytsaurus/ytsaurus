#include "public.h"

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

//! Creates an authenticator which is used for all the internal RPC services.
//!
//! This authenticator uses connection to verify if the ticket source is valid. Now,
//! it's considered valid if it's either from the same cluster or from some cluster
//! specified in the connection's cluster directory.
NRpc::IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection);

//! Wraps channel factory in a way that it injects service tickets for native
//! authentication. #tvmId is the TVM id of the destination cluster (if set to nullopt,
//! then the destination cluster doesn't require service tickets).
//!
//! If either there native TVM service is not configured or tvm ID is empty, the channel
//! is unchanged.
NRpc::IChannelFactoryPtr CreateNativeAuthenticationInjectingChannelFactory(
    NRpc::IChannelFactoryPtr channelFactory,
    std::optional<NAuth::TTvmId> tvmId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
