#include "rpc_proxy_connection.h"
#include "rpc_proxy_client.h"
#include "rpc_proxy_transaction.h"
#include "config.h"
#include "credentials_injecting_channel.h"
#include "private.h"

#include <yt/core/misc/address.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/bus_channel.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

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
    , Logger(NLogging::TLogger(RpcProxyClientLogger)
        .AddTag("ConnectionId: %v", TGuid::Create()))
{ }

TRpcProxyConnection::~TRpcProxyConnection() = default;

NObjectClient::TCellTag TRpcProxyConnection::GetCellTag()
{
    Y_UNIMPLEMENTED();
}

const NTabletClient::ITableMountCachePtr& TRpcProxyConnection::GetTableMountCache()
{
    Y_UNIMPLEMENTED();
}

const NTransactionClient::ITimestampProviderPtr& TRpcProxyConnection::GetTimestampProvider()
{
    Y_UNIMPLEMENTED();
}

const IInvokerPtr& TRpcProxyConnection::GetLightInvoker()
{
    return ActionQueue_->GetInvoker();
}

const IInvokerPtr& TRpcProxyConnection::GetHeavyInvoker()
{
    return ActionQueue_->GetInvoker();
}

IAdminPtr TRpcProxyConnection::CreateAdmin(const TAdminOptions&)
{
    Y_UNIMPLEMENTED();
}

NApi::IClientPtr TRpcProxyConnection::CreateClient(const TClientOptions& options)
{
    // TODO(sandello): Extract this to a new TAddressResolver method.
    auto localHostname = GetLocalHostName();
    auto localAddress = TAddressResolver::Get()->Resolve(localHostname).Get().ValueOrThrow();

    auto localAddressString = ToString(localAddress);
    YCHECK(localAddressString.StartsWith("tcp://"));
    localAddressString = localAddressString.substr(6);
    {
        auto index = localAddressString.rfind(':');
        if (index != TString::npos) {
            localAddressString = localAddressString.substr(0, index);
        }
    }
    if (localAddressString.StartsWith("[") && localAddressString.EndsWith("]")) {
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
            options.SessionId.Get(TString()),
            options.SslSessionId.Get(TString()),
            localAddressString);
    }

    return New<TRpcProxyClient>(MakeStrong(this), std::move(channel));
}

NHiveClient::ITransactionParticipantPtr TRpcProxyConnection::CreateTransactionParticipant(
    const NHiveClient::TCellId&,
    const TTransactionParticipantOptions&)
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

void TRpcProxyConnection::RegisterTransaction(TRpcProxyTransaction* transaction)
{
    auto guard = Guard(SpinLock_);
    YCHECK(Transactions_.insert(MakeWeak(transaction)).second);

    if (!PingExecutor_) {
        PingExecutor_ = New<TPeriodicExecutor>(
            ActionQueue_->GetInvoker(),
            BIND(&TRpcProxyConnection::OnPing, MakeWeak(this)),
            Config_->PingPeriod);
        PingExecutor_->Start();
    }
}

void TRpcProxyConnection::UnregisterTransaction(TRpcProxyTransaction* transaction)
{
    auto guard = Guard(SpinLock_);
    Transactions_.erase(MakeWeak(transaction));

    if (Transactions_.empty() && PingExecutor_) {
        PingExecutor_->Stop();
        PingExecutor_.Reset();
    }
}

void TRpcProxyConnection::OnPing()
{
    std::vector<TRpcProxyTransactionPtr> activeTransactions;

    {
        auto guard = Guard(SpinLock_);
        activeTransactions.reserve(Transactions_.size());
        for (const auto& transaction : Transactions_) {
            auto activeTransaction = transaction.Lock();
            if (activeTransaction) {
                activeTransactions.push_back(std::move(activeTransaction));
            }
        }
    }

    std::vector<TFuture<void>> pingResults;
    pingResults.reserve(activeTransactions.size());
    for (const auto& activeTransaction : activeTransactions) {
        pingResults.push_back(activeTransaction->Ping());
    }

    CombineAll(pingResults).Subscribe(BIND(&TRpcProxyConnection::OnPingCompleted, MakeWeak(this)));
}

void TRpcProxyConnection::OnPingCompleted(const TErrorOr<std::vector<TError>>& pingResults)
{
    if (pingResults.IsOK()) {
        LOG_DEBUG("Pinged %v transactions", pingResults.Value().size());
    }
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
