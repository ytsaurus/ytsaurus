#pragma once

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateUserInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TString& user);

NRpc::IChannelPtr CreateTokenInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TString& user,
    const TString& token,
    const TString& userIP);

NRpc::IChannelPtr CreateCookieInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TString& user,
    const TString& domain,
    const TString& sessionId,
    const TString& sslSessionId,
    const TString& userIP);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
