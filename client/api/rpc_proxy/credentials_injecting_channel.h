#pragma once

#include <yt/core/rpc/public.h>

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateUserInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TNullable<TString>& user);

NRpc::IChannelPtr CreateTokenInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TNullable<TString>& user,
    const TString& token);

NRpc::IChannelPtr CreateCookieInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const TNullable<TString>& user,
    const TString& sessionId,
    const TString& sslSessionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
