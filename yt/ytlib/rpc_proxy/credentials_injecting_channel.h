#pragma once

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IChannelPtr CreateTokenInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& token,
    const Stroka& userIP);

NRpc::IChannelPtr CreateCookieInjectingChannel(
    NRpc::IChannelPtr underlyingChannel,
    const Stroka& user,
    const Stroka& domain,
    const Stroka& sessionId,
    const Stroka& sslSessionId,
    const Stroka& userIP);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
