#include "rpc_proxy_client.h"
#include "rpc_proxy_connection.h"
#include "credentials_injecting_channel.h"
#include "api_service_proxy.h"
#include "private.h"

#include <yt/core/misc/address.h>

#include <yt/ytlib/api/connection.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyClient::TRpcProxyClient(
    TRpcProxyConnectionPtr connection,
    const NApi::TClientOptions& options)
    : Connection_(std::move(connection))
{
    Channel_ = Connection_->Channel_;

    // TODO(sandello): Extract this to a new TAddressResolver method.
    auto localHostname = TAddressResolver::Get()->GetLocalHostName();
    auto localAddress = TAddressResolver::Get()->Resolve(localHostname).Get().ValueOrThrow();

    auto localAddressString = ToString(localAddress);
    YCHECK(localAddressString.StartsWith("tcp://"));
    localAddressString = localAddressString.substr(6);
    {
        auto index = localAddressString.rfind(':');
        if (index != Stroka::npos) {
            localAddressString = localAddressString.substr(0, index);
        }
    }
    if (localAddressString.StartsWith("[") && localAddressString.has_suffix("]")) {
        localAddressString = localAddressString.substr(1, localAddressString.length() - 2);
    }

    LOG_DEBUG("Originating address is %v", localAddressString);

    if (options.Token) {
        Channel_ = CreateTokenInjectingChannel(
            Channel_,
            options.User,
            *options.Token,
            localAddressString);
    } else if (options.SessionId || options.SslSessionId) {
        Channel_ = CreateCookieInjectingChannel(
            Channel_,
            options.User,
            "yt.yandex-team.ru", // TODO(sandello): where to get this?
            options.SessionId.Get(Stroka()),
            options.SslSessionId.Get(Stroka()),
            localAddressString);
    }
}

TRpcProxyClient::~TRpcProxyClient()
{ }

TFuture<NYson::TYsonString> TRpcProxyClient::GetNode(
    const NYPath::TYPath& path,
    const NApi::TGetNodeOptions& options)
{
    TApiServiceProxy proxy(Channel_);

    auto req = proxy.GetNode();
    req->set_path(path);

    return req->Invoke().Apply(BIND([] (const TErrorOr<TApiServiceProxy::TRspGetNodePtr>& rsp) {
        return NYson::TYsonString(rsp.ValueOrThrow()->data());
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
