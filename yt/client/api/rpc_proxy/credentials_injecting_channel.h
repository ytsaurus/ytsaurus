#pragma once

#include <yt/core/rpc/public.h>

#include <yt/core/misc/optional.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateUserInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const std::optional<TString>& user);

NRpc::IChannelPtr CreateTokenInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const std::optional<TString>& user,
    const TString& token);

NRpc::IChannelPtr CreateCookieInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const std::optional<TString>& user,
    const TString& sessionId,
    const TString& sslSessionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
