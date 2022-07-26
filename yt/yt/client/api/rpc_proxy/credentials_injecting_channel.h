#pragma once

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/optional.h>

#include <yt/yt/library/auth/public.h>

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

NRpc::IChannelPtr CreateServiceTicketInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const std::optional<TString>& user,
    const TString& ticket);

NRpc::IChannelPtr CreateServiceTicketInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const std::optional<TString> &user,
    const NAuth::IServiceTicketAuthPtr& serviceTicketAuth);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
