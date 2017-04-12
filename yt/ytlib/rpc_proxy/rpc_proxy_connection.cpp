#include "rpc_proxy_connection.h"
#include "rpc_proxy_client.h"
#include "config.h"
#include "credentials_injecting_channel.h"
#include "private.h"

#include <yt/core/misc/address.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/rpc/bus_channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyClientLogger;

using namespace NApi;
using namespace NBus;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyConnection::TRpcProxyConnection(
    TRpcProxyConnectionConfigPtr config,
    NConcurrency::TActionQueuePtr actionQueue)
    : Config_(std::move(config))
    , ActionQueue_(std::move(actionQueue))
{ }

TRpcProxyConnection::~TRpcProxyConnection()
{ }

NObjectClient::TCellTag TRpcProxyConnection::GetCellTag()
{
    Y_UNIMPLEMENTED();
}

NTabletClient::ITableMountCachePtr TRpcProxyConnection::GetTableMountCache()
{
    Y_UNIMPLEMENTED();
}

NTransactionClient::ITimestampProviderPtr TRpcProxyConnection::GetTimestampProvider()
{
    Y_UNIMPLEMENTED();
}

IInvokerPtr TRpcProxyConnection::GetLightInvoker()
{
    return ActionQueue_->GetInvoker();
}

IInvokerPtr TRpcProxyConnection::GetHeavyInvoker()
{
    return ActionQueue_->GetInvoker();
}

IAdminPtr TRpcProxyConnection::CreateAdmin(const TAdminOptions& options)
{
    Y_UNIMPLEMENTED();
}

IClientPtr TRpcProxyConnection::CreateClient(const TClientOptions& options)
{
    // TODO(sandello): Extract this to a new TAddressResolver method.
    auto localHostname = TAddressResolver::Get()->GetLocalHostName();
    auto localAddress = TAddressResolver::Get()->Resolve(localHostname).Get().ValueOrThrow();

    auto localAddressString = ToString(localAddress);
    YCHECK(localAddressString.has_prefix("tcp://"));
    localAddressString = localAddressString.substr(6);
    {
        auto index = localAddressString.rfind(':');
        if (index != Stroka::npos) {
            localAddressString = localAddressString.substr(0, index);
        }
    }
    if (localAddressString.has_prefix("[") && localAddressString.has_suffix("]")) {
        localAddressString = localAddressString.substr(1, localAddressString.length() - 2);
    }

    LOG_DEBUG("Originating address is %v", localAddressString);

    const auto& address = Config_->Addresses[RandomNumber(Config_->Addresses.size())];
    auto channel = GetBusChannelFactory()->CreateChannel(address);

    if (options.Token) {
        channel = CreateTokenInjectingChannel(
            channel,
            options.User,
            *options.Token,
            localAddressString);
    } else if (options.SessionId || options.SslSessionId) {
        channel = CreateCookieInjectingChannel(
            channel,
            options.User,
            "yt.yandex-team.ru", // TODO(sandello): where to get this?
            options.SessionId.Get(Stroka()),
            options.SslSessionId.Get(Stroka()),
            localAddressString);
    }

    return New<TRpcProxyClient>(MakeStrong(this), std::move(channel));
}

NHiveClient::ITransactionParticipantPtr TRpcProxyConnection::CreateTransactionParticipant(
    const NHiveClient::TCellId& cellId,
    const TTransactionParticipantOptions& options)
{
    Y_UNIMPLEMENTED();
}

void TRpcProxyConnection::ClearMetadataCaches()
{
    Y_UNIMPLEMENTED();
}

void TRpcProxyConnection::Terminate()
{
    Y_UNIMPLEMENTED();
}

IConnectionPtr CreateRpcProxyConnection(TRpcProxyConnectionConfigPtr config)
{
    auto actionQueue = New<TActionQueue>("RpcConnect");
    auto connection = New<TRpcProxyConnection>(std::move(config), std::move(actionQueue));
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
