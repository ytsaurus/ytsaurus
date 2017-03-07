#include "rpc_proxy_connection.h"
#include "rpc_proxy_client.h"
#include "config.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/rpc/bus_channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NBus;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyConnection::TRpcProxyConnection(TRpcProxyConnectionConfigPtr config)
    : Config_(std::move(config))
{ }

TRpcProxyConnection::~TRpcProxyConnection()
{ }

void TRpcProxyConnection::Initialize()
{
    ActionQueue_ = New<TActionQueue>("RpcConnect");

    // TODO(sandello): Implement balancing on top of available peers.
    const auto& address = Config_->Addresses[RandomNumber(Config_->Addresses.size())];
    Channel_ = GetBusChannelFactory()->CreateChannel(address);
}

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
    return New<TRpcProxyClient>(MakeStrong(this), options);
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
    auto connection = New<TRpcProxyConnection>(std::move(config));
    connection->Initialize();
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
